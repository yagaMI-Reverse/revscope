"""revscope ingest: one idempotent layer, two delivery paths.

apply_events() is the core: INSERT into raw_events ON CONFLICT (stripe_id)
DO NOTHING. Only events that actually landed in raw_events are normalized
and folded into the rollups -- in the same transaction. A duplicate delivery
therefore touches nothing: no normalized rows, no rollup increments.

Modes:
  python ingest.py backfill [--max-events N]   paged backfill with checkpoint
  python ingest.py webhook --replay N          re-deliver N random already-
                                               processed events (dup storm)
  python ingest.py reset                       apply schema.sql (drops data)

stdout is ASCII-only on purpose (Windows cp1251 console).
"""

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timezone

import psycopg

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(HERE, "data", "events.ndjson")
SCHEMA_PATH = os.path.join(HERE, "schema.sql")
DSN = os.environ.get("REVSCOPE_DSN",
                     "postgresql://revscope:revscope@127.0.0.1:5433/revscope")

RETRYABLE = {"insufficient_funds", "processing_error", "try_again_later"}

INSERT_RAW = """
INSERT INTO raw_events (stripe_id, type, payload)
SELECT e->>'id', e->>'type', e
FROM jsonb_array_elements(%s::jsonb) AS e
ON CONFLICT (stripe_id) DO NOTHING
RETURNING stripe_id
"""

UPSERT_ROLLUP = """
INSERT INTO rollup_daily (day, product, status, gross_cents, refund_cents, tx_count)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (day, product, status) DO UPDATE SET
    gross_cents  = rollup_daily.gross_cents  + EXCLUDED.gross_cents,
    refund_cents = rollup_daily.refund_cents + EXCLUDED.refund_cents,
    tx_count     = rollup_daily.tx_count     + EXCLUDED.tx_count
"""

UPSERT_CSTATS = """
INSERT INTO customer_stats
    (customer_id, ltv_cents, first_paid_at, last_paid_at,
     paid_count, failed_count, refund_cents)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (customer_id) DO UPDATE SET
    ltv_cents     = customer_stats.ltv_cents     + EXCLUDED.ltv_cents,
    first_paid_at = LEAST(customer_stats.first_paid_at, EXCLUDED.first_paid_at),
    last_paid_at  = GREATEST(customer_stats.last_paid_at, EXCLUDED.last_paid_at),
    paid_count    = customer_stats.paid_count    + EXCLUDED.paid_count,
    failed_count  = customer_stats.failed_count  + EXCLUDED.failed_count,
    refund_cents  = customer_stats.refund_cents  + EXCLUDED.refund_cents
"""

UPSERT_CHECKPOINT = """
INSERT INTO ingest_checkpoint (worker, last_stripe_id, updated_at)
VALUES (%s, %s, now())
ON CONFLICT (worker) DO UPDATE SET
    last_stripe_id = EXCLUDED.last_stripe_id, updated_at = now()
"""


def connect():
    return psycopg.connect(DSN)


def reset_db(conn):
    with open(SCHEMA_PATH, encoding="utf-8") as f:
        conn.execute(f.read())
    conn.commit()


def _dt(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def _product(price_id):
    # canonical plan is structural: "price_starter_900" -> "starter_900"
    return price_id[len("price_"):]


def apply_events(conn, lines):
    """Apply a batch of raw event JSON lines. Returns how many were new.

    The whole batch runs in the caller's transaction. Duplicates are dropped
    by the raw_events UNIQUE constraint; only newly-inserted events flow into
    normalized tables and rollups, so re-delivery is a strict no-op.
    """
    cur = conn.execute(INSERT_RAW, ("[" + ",".join(lines) + "]",))
    inserted = {r[0] for r in cur.fetchall()}
    if not inserted:
        return 0

    cust_rows, sub_rows, charge_rows, refund_rows, review_rows = [], [], [], [], []
    roll = {}     # (day, product, status) -> [gross, refund, tx]
    cstats = {}   # customer_id -> [ltv, first, last, paid, failed, refund]

    def stat(cid):
        return cstats.setdefault(cid, [0, None, None, 0, 0, 0])

    for line in lines:
        e = json.loads(line)
        if e["id"] not in inserted:
            continue
        etype, o = e["type"], e["data"]["object"]

        if etype == "customer.created":
            cust_rows.append((o["id"], o["email"], o["country"], _dt(o["created"])))

        elif etype == "customer.subscription.created":
            product = _product(o["price"])
            sub_rows.append((o["id"], o["customer"], o["price"], product,
                             o["amount"], o["status"], _dt(o["created"]),
                             _dt(o["canceled_at"]) if o["canceled_at"] else None))

        elif etype in ("charge.succeeded", "charge.failed"):
            product = _product(o["price"])
            dt = _dt(o["created"])
            day, amount, cid = dt.date(), o["amount"], o["customer"]
            # metadata never feeds canonical fields; anything that does not
            # match the price_id-derived plan goes to quarantine instead
            for k, v in (o.get("metadata") or {}).items():
                if not (k == "plan" and v == product):
                    review_rows.append((o["id"], k, str(v)))
            code = o.get("decline_code")
            dclass = None
            if o["status"] == "succeeded":
                r = roll.setdefault((day, product, "succeeded"), [0, 0, 0])
                r[0] += amount
                r[2] += 1
                s = stat(cid)
                s[0] += amount
                s[1] = dt if s[1] is None else min(s[1], dt)
                s[2] = dt if s[2] is None else max(s[2], dt)
                s[3] += 1
            else:
                dclass = "retryable" if code in RETRYABLE else "terminal"
                r = roll.setdefault((day, product, "failed_" + dclass), [0, 0, 0])
                r[0] += amount
                r[2] += 1
                stat(cid)[4] += 1
            # currency/amount_local are read with [] and not .get(): a stream
            # without them is a stale dataset, and booking every row as USD
            # by default is the exact silent mistake this layer exists to
            # prevent. Fail on the first page instead.
            charge_rows.append((o["id"], cid, o.get("subscription"), o["price"],
                                product, amount, o["currency"], o["amount_local"],
                                o["status"], code, dclass, dt))

        elif etype == "charge.refunded":
            product = _product(o["price"])
            dt = _dt(o["created"])
            r = roll.setdefault((dt.date(), product, "refund"), [0, 0, 0])
            r[1] += o["amount"]
            r[2] += 1
            stat(o["customer"])[5] += o["amount"]
            refund_rows.append((o["id"], o["charge"], o["customer"], product,
                                o["amount"], o["currency"], o["amount_local"],
                                o["partial"], dt))

    cur = conn.cursor()
    if cust_rows:
        cur.executemany(
            "INSERT INTO customers (id, email, country, created) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", cust_rows)
    if sub_rows:
        cur.executemany(
            "INSERT INTO subscriptions (id, customer_id, price_id, product, "
            "amount_cents, status, created, canceled_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (id) DO NOTHING", sub_rows)
    if charge_rows:
        cur.executemany(
            "INSERT INTO charges (id, customer_id, subscription_id, price_id, "
            "product, amount_cents, currency, amount_local, status, "
            "decline_code, decline_class, created) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (id) DO NOTHING", charge_rows)
    if refund_rows:
        cur.executemany(
            "INSERT INTO refunds (id, charge_id, customer_id, product, "
            "amount_cents, currency, amount_local, partial, created) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (id) DO NOTHING", refund_rows)
    if review_rows:
        cur.executemany(
            "INSERT INTO metadata_review (stripe_id, key, value) "
            "VALUES (%s, %s, %s)", review_rows)
    if roll:
        cur.executemany(UPSERT_ROLLUP,
                        [(d, p, s, g, r, t) for (d, p, s), (g, r, t) in roll.items()])
    if cstats:
        cur.executemany(UPSERT_CSTATS,
                        [(c, v[0], v[1], v[2], v[3], v[4], v[5])
                         for c, v in cstats.items()])
    return len(inserted)


def apply_event(conn, event):
    """Single-event path (webhook shape). True if the event was new."""
    return apply_events(conn, [json.dumps(event, separators=(",", ":"))]) == 1


def backfill(conn, path=DATA_PATH, worker="backfill", max_events=None,
             page_size=100, quiet=False):
    """Paged backfill (100 events per page, like Stripe pagination).

    Each page commits atomically together with its checkpoint, so a kill at
    any point loses at most one uncommitted page, which is then re-read and
    deduplicated by the raw_events constraint on restart.
    """
    with open(path, "rb") as f:
        total = sum(1 for _ in f)
    if max_events is not None:
        total = min(total, max_events)

    row = conn.execute("SELECT last_stripe_id FROM ingest_checkpoint "
                       "WHERE worker = %s", (worker,)).fetchone()
    done = 0
    if row and row[0]:
        done = int(row[0].rsplit("@", 1)[1])
        if not quiet:
            print(f"resuming from checkpoint: {done} events already processed",
                  flush=True)

    t0 = time.perf_counter()
    inserted_total = 0
    processed = done
    next_print = (processed // 10_000 + 1) * 10_000

    def flush(page):
        nonlocal inserted_total
        inserted_total += apply_events(conn, page)
        last_id = json.loads(page[-1])["id"]
        conn.execute(UPSERT_CHECKPOINT, (worker, f"{last_id}@{processed}"))
        conn.commit()

    with open(path, encoding="utf-8") as f:
        it = iter(f)
        for _ in range(done):
            next(it)
        page = []
        for line in it:
            if processed >= total:
                break
            line = line.strip()
            if not line:
                continue
            page.append(line)
            processed += 1
            if len(page) >= page_size:
                flush(page)
                page = []
                if not quiet and processed >= next_print:
                    print(f"processed {processed} / {total}", flush=True)
                    next_print += 10_000
        if page:
            flush(page)

    seconds = time.perf_counter() - t0
    stats = {"processed": processed - done, "total": total,
             "inserted": inserted_total, "seconds": seconds}
    if not quiet:
        eps = stats["processed"] / seconds if seconds > 0 else 0.0
        print(f"backfill done: {stats['processed']} events in {seconds:.1f}s "
              f"({eps:.0f} events/sec), newly inserted {inserted_total}",
              flush=True)
    return stats


def replay(conn, n, path=DATA_PATH, seed=1337, batch_size=1000, quiet=False):
    """Webhook duplicate storm: re-deliver n random already-seen events."""
    with open(path, encoding="utf-8") as f:
        lines = [ln.strip() for ln in f if ln.strip()]
    rng = random.Random(seed)
    sample = rng.sample(lines, n)
    t0 = time.perf_counter()
    inserted = 0
    for i in range(0, n, batch_size):
        inserted += apply_events(conn, sample[i:i + batch_size])
        conn.commit()
    seconds = time.perf_counter() - t0
    if not quiet:
        print(f"replayed {n} events in {seconds:.1f}s, "
              f"newly inserted {inserted} (expected 0 on a full database)",
              flush=True)
    return {"replayed": n, "inserted": inserted, "seconds": seconds}


def main():
    ap = argparse.ArgumentParser(description="revscope ingest")
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("backfill", help="paged backfill with checkpoint")
    b.add_argument("--max-events", type=int, default=None)
    w = sub.add_parser("webhook", help="duplicate-storm replay")
    w.add_argument("--replay", type=int, required=True)
    w.add_argument("--seed", type=int, default=1337)
    sub.add_parser("reset", help="apply schema.sql (drops all data)")
    args = ap.parse_args()

    with connect() as conn:
        if args.cmd == "reset":
            reset_db(conn)
            print("schema applied, database is empty", flush=True)
        elif args.cmd == "backfill":
            backfill(conn, max_events=args.max_events)
        elif args.cmd == "webhook":
            replay(conn, args.replay, seed=args.seed)


if __name__ == "__main__":
    main()
