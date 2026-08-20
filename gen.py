"""Deterministic Stripe-scale dataset generator for revscope.

Writes:
  data/events.ndjson      - event stream "as from Stripe" (newest first)
  data/ground_truth.json  - totals computed independently of the database
  data/fx_rates.json      - daily FX feed, one rate per currency per day

Everything is driven by random.Random(SEED): same seed -> byte-identical
dataset and ground truth. No external dependencies, no network.

Money is charged in the customer's local currency (10 countries, 7
currencies): "amount" stays the USD list price and "amount_local" is what was
actually presented, converted at the rate of that day. The FX feed is built
from its own Random(FX_SEED) so the main rng stream -- and therefore every
count and cent published before this layer existed -- is byte-identical.

Volumes: 100k customers, ~25k subscriptions, exactly 500k charges
(subscription invoices billed on a 30-day cycle + one-off charges; invoices
are sampled down to INVOICE_CAP if the raw billing schedule would exceed it,
so the 500k total always keeps a one-off share), ~5% of succeeded charges
refunded. All money is integer cents everywhere.
"""

import json
import os
import random
import time
from bisect import bisect_right
from datetime import datetime, timezone

import fx

SEED = 42
N_CUSTOMERS = 100_000
N_TARGET_CHARGES = 500_000
N_SUBS = 25_000
INVOICE_CAP = 430_000

DAY = 86_400
START = datetime(2021, 8, 1, tzinfo=timezone.utc)
ASOF = datetime(2026, 8, 1, tzinfo=timezone.utc)   # dataset "now"
START_TS = int(START.timestamp())
ASOF_TS = int(ASOF.timestamp())
CUT_30 = ASOF_TS - 30 * DAY
CUT_90 = ASOF_TS - 90 * DAY

# product = "<name>_<cents>"; price_id = "price_" + product.
# The canonical plan and amount are ALWAYS recoverable from price_id alone.
PLANS = [
    ("starter", 900), ("hobby", 1500), ("basic", 1900), ("launch", 2500),
    ("solo", 2900), ("team", 3900), ("plus", 4900), ("growth", 5900),
    ("pro", 6900), ("scale", 7900), ("studio", 8900), ("agency", 9900),
    ("business", 10900), ("premium", 12900), ("advanced", 14900),
    ("platform", 16900), ("enterprise", 19900), ("ultimate", 22900),
    ("titan", 24900), ("apex", 29900),
]
ONEOFFS = [
    ("oneoff_addon", 1900), ("oneoff_credits", 4900), ("oneoff_setup", 9900),
    ("oneoff_training", 14900), ("oneoff_migration", 29900),
]
RETRYABLE = ["insufficient_funds", "processing_error", "try_again_later"]
TERMINAL = ["stolen_card", "do_not_honor", "fraudulent"]
COUNTRIES = ["US", "DE", "GB", "KZ", "NL", "FR", "CA", "AU", "PL", "ES"]

# Retention is measured in 30-day periods, not calendar months, because the
# billing cycle is 30 days: a customer paying on Jan 2 and Feb 1 sits in one
# calendar month twice and in the next one never, which reads as churn that
# never happened. Period 0 is the first payment, so "survived to the 2nd /
# 3rd / 6th / 12th month" is period 1 / 2 / 5 / 11.
RETENTION_PERIOD = 30 * DAY
COHORT_KEEP = (1, 2, 5, 11)
MAX_PERIOD = 12
ACTIVE_GRACE = 45 * DAY    # one billing cycle plus a half: still a payer
FUNNEL_MATURITY = 90 * DAY  # three cycles: enough time to reach stage 3

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data")


def month_grid():
    """61 month-boundary timestamps and 60 'YYYY-MM' keys (2021-08..2026-07)."""
    ts, keys = [], []
    y, m = 2021, 8
    for i in range(61):
        ts.append(int(datetime(y, m, 1, tzinfo=timezone.utc).timestamp()))
        if i < 60:
            keys.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            m, y = 1, y + 1
    return ts, keys


MONTH_TS, MONTH_KEYS = month_grid()


def month_key(ts):
    return MONTH_KEYS[bisect_right(MONTH_TS, ts) - 1]


def dirty_metadata(rng, product):
    """~30% of charges get deliberately messy metadata.

    Returns (metadata, n_review) where n_review is how many key/value pairs
    an ingest that trusts only price_id must quarantine.
    """
    if rng.random() < 0.70:
        return {"plan": product}, 0
    v = rng.randrange(7)
    if v == 0:
        md = {"plan_name": product}
    elif v == 1:
        md = {"planName": product}
    elif v == 2:
        md = {}                                   # plan key absent entirely
    elif v == 3:
        md = {"plan": "undefined"}
    elif v == 4:
        md = {"plan": ""}
    elif v == 5:
        md = {"plan": product[: max(3, len(product) // 2)]}  # truncated
    else:
        md = {"plan": product, "utm_source": "faceb"}        # truncated utm
    n_review = sum(1 for k, val in md.items() if not (k == "plan" and val == product))
    return md, n_review


def main():
    t0 = time.perf_counter()
    rng = random.Random(SEED)
    os.makedirs(DATA_DIR, exist_ok=True)

    fx_days = fx.n_days(START.date(), ASOF.date())
    rates = fx.build_rates(fx_days)
    rate_now = {c: r[-1] for c, r in rates.items()}   # "today" on the as-of day

    events = []          # (ts, event_id, json_line)
    n_last_90d = 0

    def emit(ts, evt_id, evt_type, obj):
        nonlocal n_last_90d
        line = json.dumps(
            {"id": evt_id, "type": evt_type, "created": ts, "data": {"object": obj}},
            separators=(",", ":"),
        )
        events.append((ts, evt_id, line))
        if ts >= CUT_90:
            n_last_90d += 1

    # --- customers: creation spread over 5 years with linear ~4x growth ---
    print("generating customers...", flush=True)
    weights = [1.0 + 3.0 * i / 59.0 for i in range(60)]
    cust_months = rng.choices(range(60), weights=weights, k=N_CUSTOMERS)
    cust_created, cust_country = [], []
    for i in range(N_CUSTOMERS):
        m = cust_months[i]
        ts = rng.randint(MONTH_TS[m], MONTH_TS[m + 1] - 1)
        cust_created.append(ts)
        country = rng.choice(COUNTRIES)  # same draw, same order: kept, not inlined
        cust_country.append(country)
        cid = f"cus_{i:06d}"
        emit(ts, f"evt_{cid}", "customer.created", {
            "id": cid, "object": "customer",
            "email": f"user{i:06d}@example.com",
            "country": country,
            "created": ts,
        })

    # --- subscriptions: 25k customers, statuses ~70/25/5 ---
    print("generating subscriptions...", flush=True)
    mrr_cents = 0
    sub_specs = []       # (sub_id, cust_id, product, amount, created, bill_end)
    for j, c in enumerate(sorted(rng.sample(range(N_CUSTOMERS), N_SUBS))):
        sid = f"sub_{j:05d}"
        created = min(cust_created[c] + rng.randint(0, 60 * DAY), ASOF_TS - 1)
        r = rng.random()
        name, cents = rng.choice(PLANS)
        product = f"{name}_{cents}"
        canceled_at = None
        if r < 0.70:
            status, bill_end = "active", ASOF_TS
            mrr_cents += cents
        elif r < 0.95:
            status = "canceled"
            canceled_at = min(created + rng.randint(30 * DAY, 365 * DAY), ASOF_TS - 1)
            bill_end = canceled_at
        else:
            status, bill_end = "past_due", ASOF_TS
        cust_id = f"cus_{c:06d}"
        emit(created, f"evt_{sid}", "customer.subscription.created", {
            "id": sid, "object": "subscription",
            "customer": cust_id,
            "price": f"price_{product}",
            "amount": cents,
            "status": status,
            "created": created,
            "canceled_at": canceled_at,
        })
        sub_specs.append((sid, cust_id, product, cents, created, bill_end))

    # --- billing schedule: 30-day invoices per subscription ---
    print("generating billing schedule...", flush=True)
    invoices = []        # (sub_id, cust_id, product, amount, ts)
    for sid, cust_id, product, cents, created, bill_end in sub_specs:
        t = created
        while t < bill_end:
            invoices.append((sid, cust_id, product, cents, t))
            t += 30 * DAY
    if len(invoices) > INVOICE_CAP:
        keep = set(rng.sample(range(len(invoices)), INVOICE_CAP))
        invoices = [inv for i, inv in enumerate(invoices) if i in keep]

    n_oneoff = N_TARGET_CHARGES - len(invoices)
    oneoffs = []
    for _ in range(n_oneoff):
        c = rng.randrange(N_CUSTOMERS)
        ts = rng.randint(cust_created[c], ASOF_TS - 1)
        name, cents = rng.choice(ONEOFFS)
        oneoffs.append((None, f"cus_{c:06d}", f"{name}_{cents}", cents, ts))

    all_charges = invoices + oneoffs
    assert len(all_charges) == N_TARGET_CHARGES

    # --- charges + refunds + ground truth ---
    print(f"generating {len(all_charges)} charges "
          f"({len(invoices)} invoices + {n_oneoff} one-off)...", flush=True)

    months = {k: {"gross_cents": 0, "refunded_cents": 0} for k in MONTH_KEYS}
    win = {
        "30": {"gross_cents": 0, "refunded_cents": 0, "charge_count": 0, "refund_count": 0},
        "90": {"gross_cents": 0, "refunded_cents": 0, "charge_count": 0, "refund_count": 0},
    }
    ltv = {}
    totals = {"gross": 0, "refunded": 0, "succeeded": 0, "failed": 0,
              "refunds": 0, "recoverable": 0, "review": 0}

    # Product-layer ground truth, computed here from the same stream the events
    # are minted from, so the marts are later checked against an independent
    # implementation instead of against another SQL query.
    ctry = {c: {"currency": fx.CURRENCY_BY_COUNTRY[c], "charges": 0,
                "gross_local": 0, "gross_usd_hist": 0, "gross_usd_current": 0,
                "refunded_local": 0, "refunded_usd_hist": 0,
                "refunded_usd_current": 0} for c in COUNTRIES}
    pays = {}   # customer -> [(ts, usd_hist_cents), ...], succeeded only
    tries = {}  # customer -> [first_ts, last_ts, last_charge_id, last_ok]

    for idx, (sub_id, cust_id, product, amount, ts) in enumerate(all_charges):
        ch_id = f"ch_{idx:06d}"
        failed = rng.random() < 0.08
        md, n_review = dirty_metadata(rng, product)
        totals["review"] += n_review
        # Presentment currency follows the customer country, and the local
        # amount is the list price converted at THAT DAY's rate -- what Stripe
        # does with automatic currency conversion. The rate is deliberately not
        # copied into the event: the marts must join the rate feed on the
        # transaction date, which is exactly the join a report gets wrong when
        # it silently joins "today" instead.
        country = cust_country[int(cust_id[4:])]
        cur = fx.CURRENCY_BY_COUNTRY[country]
        rate = rates[cur][(ts - START_TS) // DAY]   # START is midnight UTC
        amount_local = fx.to_local(amount, rate)
        # Last attempt wins ties by charge id, the same order the SQL uses
        # (ORDER BY created DESC, id DESC): whether the customer left on a
        # decline or on a decision must not depend on row order.
        t = tries.get(cust_id)
        if t is None:
            tries[cust_id] = [ts, ts, ch_id, not failed]
        else:
            t[0] = min(t[0], ts)
            if (ts, ch_id) > (t[1], t[2]):
                t[1], t[2], t[3] = ts, ch_id, not failed
        obj = {
            "id": ch_id, "object": "charge",
            "customer": cust_id,
            "subscription": sub_id,
            "price": f"price_{product}",
            "amount": amount,
            "currency": cur,
            "amount_local": amount_local,
            "created": ts,
            "metadata": md,
        }
        if failed:
            code = rng.choice(RETRYABLE) if rng.random() < 0.60 else rng.choice(TERMINAL)
            obj["status"] = "failed"
            obj["decline_code"] = code
            totals["failed"] += 1
            if code in RETRYABLE:
                totals["recoverable"] += amount
            emit(ts, f"evt_{ch_id}", "charge.failed", obj)
            continue

        obj["status"] = "succeeded"
        emit(ts, f"evt_{ch_id}", "charge.succeeded", obj)
        totals["succeeded"] += 1
        totals["gross"] += amount
        months[month_key(ts)]["gross_cents"] += amount
        ltv[cust_id] = ltv.get(cust_id, 0) + amount
        usd_hist = fx.to_usd(amount_local, rate)
        cc = ctry[country]
        cc["charges"] += 1
        cc["gross_local"] += amount_local
        cc["gross_usd_hist"] += usd_hist
        cc["gross_usd_current"] += fx.to_usd(amount_local, rate_now[cur])
        pays.setdefault(cust_id, []).append((ts, usd_hist))
        for cut, w in ((CUT_30, win["30"]), (CUT_90, win["90"])):
            if ts >= cut:
                w["gross_cents"] += amount
                w["charge_count"] += 1

        if rng.random() < 0.05:
            re_id = f"re_{totals['refunds']:05d}"
            totals["refunds"] += 1
            rts = min(ts + rng.randint(DAY, 30 * DAY), ASOF_TS - 1)
            partial = rng.random() < (1 / 3)
            ramt = rng.randint(max(1, amount // 10), max(2, amount * 9 // 10)) if partial else amount
            # A refund settles on its own day at its own rate: a full refund
            # of a charge made a month earlier does not give back the number of
            # USD cents it brought in. That gap is real money, so the generator
            # books it and the marts have to show it.
            r_rate = rates[cur][(rts - START_TS) // DAY]
            ramt_local = fx.to_local(ramt, r_rate)
            emit(rts, f"evt_{re_id}", "charge.refunded", {
                "id": re_id, "object": "refund",
                "charge": ch_id,
                "customer": cust_id,       # denormalized on purpose: the file
                "price": f"price_{product}",  # is newest-first, parents arrive later
                "amount": ramt,
                "currency": cur,
                "amount_local": ramt_local,
                "partial": partial,
                "created": rts,
            })
            cc["refunded_local"] += ramt_local
            cc["refunded_usd_hist"] += fx.to_usd(ramt_local, r_rate)
            cc["refunded_usd_current"] += fx.to_usd(ramt_local, rate_now[cur])
            totals["refunded"] += ramt
            months[month_key(rts)]["refunded_cents"] += ramt
            for cut, w in ((CUT_30, win["30"]), (CUT_90, win["90"])):
                if rts >= cut:
                    w["refunded_cents"] += ramt
                    w["refund_count"] += 1

    # --- cohorts and funnel, from the payment histories just built ---
    print("computing cohort and funnel ground truth...", flush=True)
    # The funnel is nested by construction: every stage is a subset of the one
    # above it. Stages that are merely correlated give a funnel where a later
    # step is wider than an earlier one and the drop-off percentages stop
    # meaning anything. The churn split is NOT a stage of that funnel but a
    # separate cut of the same base: a customer with two payments, the last
    # one yesterday, is neither "regular" nor churned.
    funnel = {"attempted": 0, "paid_once": 0, "paid_twice": 0,
              "regular_3plus": 0, "still_paying": 0,
              "never_converted": 0, "churn_involuntary": 0,
              "churn_voluntary": 0}
    for cust_id, t in tries.items():
        # Only customers old enough to have gone through three billing cycles
        # enter the funnel. Someone who first paid last week has not "failed
        # to reach a third payment", they simply have not had the time.
        if t[0] > ASOF_TS - FUNNEL_MATURITY:
            continue
        funnel["attempted"] += 1
        plist = pays.get(cust_id)
        if not plist:
            funnel["never_converted"] += 1     # tried, never got through
            continue
        plist.sort()
        funnel["paid_once"] += 1
        if len(plist) >= 2:
            funnel["paid_twice"] += 1
            if len(plist) >= 3:
                funnel["regular_3plus"] += 1
                if plist[-1][0] >= ASOF_TS - ACTIVE_GRACE:
                    funnel["still_paying"] += 1
        if plist[-1][0] < ASOF_TS - ACTIVE_GRACE:
            if t[3]:
                funnel["churn_voluntary"] += 1     # last attempt went through
            else:
                funnel["churn_involuntary"] += 1   # last attempt was a decline

    cohorts = {}
    for cust_id, plist in pays.items():
        plist.sort()
        first_ts = plist[0][0]
        last_ts = plist[-1][0]
        c = cohorts.setdefault(month_key(first_ts), {
            "size": 0,
            "eligible": {str(k): 0 for k in COHORT_KEEP},
            "retained": {str(k): 0 for k in COHORT_KEEP},
            "survived": {str(k): 0 for k in COHORT_KEEP},
            "revenue_usd_cents": {str(k): 0 for k in COHORT_KEEP},
        })
        c["size"] += 1
        by_period = {}
        for ts, usd in plist:
            per = (ts - first_ts) // RETENTION_PERIOD
            if per <= MAX_PERIOD:
                by_period[per] = by_period.get(per, 0) + usd
        last_period = (last_ts - first_ts) // RETENTION_PERIOD
        for k in COHORT_KEEP:
            # A customer counts in period k only once period k has fully
            # elapsed FOR THAT CUSTOMER. Dividing by the whole cohort instead
            # is the standard way to invent a retention cliff at the right
            # edge of the chart: the youngest members never had the chance.
            if first_ts + (k + 1) * RETENTION_PERIOD > ASOF_TS:
                continue
            c["eligible"][str(k)] += 1
            if k in by_period:
                # "retained" = paid inside period k. A single failed charge
                # opens a hole here, so "survived" (paid in k or later) is
                # carried alongside: the gap between the two is exactly the
                # churn that dunning invents.
                c["retained"][str(k)] += 1
                c["revenue_usd_cents"][str(k)] += by_period[k]
            if last_period >= k:
                c["survived"][str(k)] += 1

    # --- write events, newest first (like Stripe list APIs) ---
    print(f"sorting and writing {len(events)} events...", flush=True)
    events.sort(key=lambda e: (-e[0], e[1]))
    path = os.path.join(DATA_DIR, "events.ndjson")
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        buf = []
        for _, _, line in events:
            buf.append(line + "\n")
            if len(buf) >= 50_000:
                f.writelines(buf)
                buf = []
        f.writelines(buf)

    with open(os.path.join(DATA_DIR, "fx_rates.json"), "w", encoding="utf-8") as f:
        json.dump({"seed": fx.FX_SEED, "scale": fx.RATE_SCALE,
                   "start": START.date().isoformat(), "days": fx_days,
                   "rates": {c: rates[c] for c in sorted(rates)}},
                  f, separators=(",", ":"))

    for k in MONTH_KEYS:
        months[k]["net_cents"] = months[k]["gross_cents"] - months[k]["refunded_cents"]
    for w in win.values():
        w["net_cents"] = w["gross_cents"] - w["refunded_cents"]

    top10 = sorted(ltv.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
    gt = {
        "seed": SEED,
        "asof": ASOF.isoformat(),
        "counts": {
            "customers": N_CUSTOMERS,
            "subscriptions": N_SUBS,
            "charges": N_TARGET_CHARGES,
            "charges_succeeded": totals["succeeded"],
            "charges_failed": totals["failed"],
            "invoices": len(invoices),
            "oneoff_charges": n_oneoff,
            "refunds": totals["refunds"],
            "metadata_review": totals["review"],
            "events_total": len(events),
            "last_90d_events": n_last_90d,
        },
        "mrr_cents": mrr_cents,
        "recoverable_cents": totals["recoverable"],
        "total_gross_cents": totals["gross"],
        "total_refunded_cents": totals["refunded"],
        "total_net_cents": totals["gross"] - totals["refunded"],
        "months": months,
        "last_30d": win["30"],
        "last_90d": win["90"],
        "top10_ltv": top10,
        # Everything below is the product layer. It is appended after the
        # original keys on purpose: the file the first seven benches read is
        # byte-identical up to this point.
        "fx": {
            "seed": fx.FX_SEED,
            "scale": fx.RATE_SCALE,
            "days": fx_days,
            "reporting_currency": fx.REPORTING_CURRENCY,
            "rate_first_last": {c: [rates[c][0], rates[c][-1]]
                                for c in sorted(rates)},
        },
        "by_country": ctry,
        "cohorts": cohorts,
        "funnel": funnel,
        "retention": {"period_days": RETENTION_PERIOD // DAY,
                      "keep_periods": list(COHORT_KEEP),
                      "max_period": MAX_PERIOD,
                      "active_grace_days": ACTIVE_GRACE // DAY,
                      "funnel_maturity_days": FUNNEL_MATURITY // DAY},
    }
    with open(os.path.join(DATA_DIR, "ground_truth.json"), "w", encoding="utf-8") as f:
        json.dump(gt, f, indent=1)

    dt = time.perf_counter() - t0
    print(f"done in {dt:.1f}s: {len(events)} events "
          f"({N_CUSTOMERS} customers, {N_SUBS} subscriptions, "
          f"{N_TARGET_CHARGES} charges, {totals['refunds']} refunds)", flush=True)
    print(f"ground truth: gross {totals['gross']} c, refunded {totals['refunded']} c, "
          f"mrr {mrr_cents} c, recoverable {totals['recoverable']} c, "
          f"last-90d events {n_last_90d}", flush=True)
    g_hist = sum(v["gross_usd_hist"] for v in ctry.values())
    g_cur = sum(v["gross_usd_current"] for v in ctry.values())
    print(f"fx: gross at the rate of the day {g_hist} c, at today's rate "
          f"{g_cur} c, gap {(g_cur - g_hist) / g_hist * 100:+.2f}%; "
          f"cohorts {len(cohorts)}, funnel base {funnel['attempted']} "
          f"customers with >= 90 days of history", flush=True)


if __name__ == "__main__":
    main()
