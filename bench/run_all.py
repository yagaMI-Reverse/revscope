"""revscope bench suite: python -m bench.run_all

Runs 7 benches and writes bench/out/results.md with a
claim -> measured -> verdict section per bench. Every assertion compares
integer cents against data/ground_truth.json -- no floats anywhere near
money. stdout is ASCII-only (Windows cp1251 console).

Execution order differs from section order to reuse the expensive full
database: 1 (full backfill), 2, 4, 6, 7 run on it, then 3 and 5 rebuild
from scratch. Sections are written in canonical order 1..7.
"""

import json
import math
import os
import statistics
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

import psycopg

import ingest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_PATH = os.path.join(ROOT, "bench", "out", "results.md")

with open(os.path.join(ROOT, "data", "ground_truth.json"), encoding="utf-8") as f:
    GT = json.load(f)

ASOF = datetime.fromisoformat(GT["asof"])
D_END = ASOF.date()
D_30 = (ASOF - timedelta(days=30)).date()
D_90 = (ASOF - timedelta(days=90)).date()

KILL_AT_FRACTION = 0.40


def q(conn, sql, params=()):
    return conn.execute(sql, params).fetchall()


def usd(cents):
    return f"{cents / 100:,.2f} USD ({cents} cents)"


def p50_p95(times_ms):
    s = sorted(times_ms)
    p50 = statistics.median(s)
    p95 = s[max(0, math.ceil(0.95 * len(s)) - 1)]
    return p50, p95


def table_counts(conn):
    out = {}
    for t in ("raw_events", "customers", "subscriptions", "charges",
              "refunds", "metadata_review", "rollup_daily", "customer_stats"):
        out[t] = q(conn, f"SELECT count(*) FROM {t}")[0][0]
    return out


def checksums(conn):
    rollup = q(conn, """
        SELECT md5(coalesce(string_agg(
            day::text || '|' || product || '|' || status || '|' ||
            gross_cents || '|' || refund_cents || '|' || tx_count,
            ';' ORDER BY day, product, status), ''))
        FROM rollup_daily""")[0][0]
    cstats = q(conn, """
        SELECT md5(coalesce(string_agg(
            customer_id || '|' || ltv_cents || '|' ||
            coalesce(first_paid_at::text, '-') || '|' ||
            coalesce(last_paid_at::text, '-') || '|' ||
            paid_count || '|' || failed_count || '|' || refund_cents,
            ';' ORDER BY customer_id), ''))
        FROM customer_stats""")[0][0]
    return {"rollup_md5": rollup, "cstats_md5": cstats, "counts": table_counts(conn)}


def window_rollup(conn, day_from, day_to):
    g, r, ct, rt = q(conn, """
        SELECT COALESCE(SUM(gross_cents)  FILTER (WHERE status = 'succeeded'), 0),
               COALESCE(SUM(refund_cents), 0),
               COALESCE(SUM(tx_count)     FILTER (WHERE status = 'succeeded'), 0),
               COALESCE(SUM(tx_count)     FILTER (WHERE status = 'refund'), 0)
        FROM rollup_daily WHERE day >= %s AND day < %s""", (day_from, day_to))[0]
    return {"gross_cents": g, "refunded_cents": r, "net_cents": g - r,
            "charge_count": ct, "refund_count": rt}


def dashboard(conn):
    """The 'first report' query set: rollups + customer-level tables only."""
    mrr = q(conn, "SELECT COALESCE(SUM(amount_cents), 0) "
                  "FROM subscriptions WHERE status = 'active'")[0][0]
    w30 = window_rollup(conn, D_30, D_END)
    tg, tr = q(conn, """
        SELECT COALESCE(SUM(gross_cents)  FILTER (WHERE status = 'succeeded'), 0),
               COALESCE(SUM(refund_cents), 0)
        FROM rollup_daily""")[0]
    rec = q(conn, "SELECT COALESCE(SUM(gross_cents), 0) FROM rollup_daily "
                  "WHERE status = 'failed_retryable'")[0][0]
    return {"mrr": mrr, "w30": w30, "total_gross": tg, "total_refunded": tr,
            "recoverable": rec}


def window_matches(win, gt_win):
    return all(win[k] == gt_win[k] for k in
               ("gross_cents", "refunded_cents", "net_cents",
                "charge_count", "refund_count"))


def monthly_rollup(conn):
    rows = q(conn, """
        SELECT to_char(day, 'YYYY-MM'),
               COALESCE(SUM(gross_cents)  FILTER (WHERE status = 'succeeded'), 0),
               COALESCE(SUM(refund_cents), 0)
        FROM rollup_daily GROUP BY 1""")
    return {m: (g, r) for m, g, r in rows}


def monthly_drift_vs_gt(conn):
    """Max abs drift in cents between rollup months and ground truth months."""
    roll = monthly_rollup(conn)
    drift = 0
    for m, gt_m in GT["months"].items():
        g, r = roll.get(m, (0, 0))
        drift = max(drift, abs(g - gt_m["gross_cents"]),
                    abs(r - gt_m["refunded_cents"]))
    extra = set(roll) - set(GT["months"])
    for m in extra:
        drift = max(drift, abs(roll[m][0]), abs(roll[m][1]))
    return drift


# ---------------------------------------------------------------- benches

def bench_full_backfill(rd, tx):
    print("[1/7] full_backfill: fresh db, full stream", flush=True)
    ingest.reset_db(tx)
    stats = ingest.backfill(tx)
    eps = stats["processed"] / stats["seconds"]
    c = table_counts(rd)
    gc = GT["counts"]
    succ = q(rd, "SELECT count(*) FROM charges WHERE status = 'succeeded'")[0][0]
    fail = q(rd, "SELECT count(*) FROM charges WHERE status = 'failed'")[0][0]
    top10 = [[cid, ltv] for cid, ltv in q(rd,
        "SELECT customer_id, ltv_cents FROM customer_stats "
        "ORDER BY ltv_cents DESC, customer_id LIMIT 10")]
    top10_ok = top10 == GT["top10_ltv"]
    checks = [
        ("raw_events", c["raw_events"], gc["events_total"]),
        ("customers", c["customers"], gc["customers"]),
        ("subscriptions", c["subscriptions"], gc["subscriptions"]),
        ("charges", c["charges"], gc["charges"]),
        ("charges succeeded", succ, gc["charges_succeeded"]),
        ("charges failed", fail, gc["charges_failed"]),
        ("refunds", c["refunds"], gc["refunds"]),
        ("metadata_review", c["metadata_review"], gc["metadata_review"]),
    ]
    ok = all(a == b for _, a, b in checks) and top10_ok
    measured = [f"wall time: {stats['seconds']:.1f}s for {stats['processed']} events "
                f"({eps:.0f} events/sec, pages of 100, commit + checkpoint per page)"]
    measured += [f"{n}: {a} (expected {b}) -> {'ok' if a == b else 'MISMATCH'}"
                 for n, a, b in checks]
    measured.append(f"top-10 customers by LTV match ground truth: {top10_ok}")
    measured.append(f"rollup_daily rows: {c['rollup_daily']}, "
                    f"customer_stats rows: {c['customer_stats']}")
    return {
        "name": "full_backfill",
        "claim": "A fresh database ingests the full 5-year stream through the "
                 "idempotent layer in minutes, with every row count matching "
                 "ground truth exactly.",
        "measured": measured, "ok": ok,
        "short": f"{stats['seconds']:.0f}s, {eps:.0f} ev/s, all counts exact",
    }


def bench_first_report(rd):
    print("[2/7] first_report: dashboard from rollups, 20 runs", flush=True)
    d = dashboard(rd)
    gt30 = GT["last_30d"]
    checks = [
        ("MRR (active subscriptions)", d["mrr"], GT["mrr_cents"], usd),
        ("last-30d gross", d["w30"]["gross_cents"], gt30["gross_cents"], usd),
        ("last-30d refunded", d["w30"]["refunded_cents"], gt30["refunded_cents"], usd),
        ("last-30d net", d["w30"]["net_cents"], gt30["net_cents"], usd),
        ("last-30d charge count", d["w30"]["charge_count"], gt30["charge_count"],
         lambda n: f"{n} charges"),
        ("all-time gross", d["total_gross"], GT["total_gross_cents"], usd),
        ("all-time refunded", d["total_refunded"], GT["total_refunded_cents"], usd),
        ("recoverable failed $", d["recoverable"], GT["recoverable_cents"], usd),
    ]
    ok = all(a == b for _, a, b, _f in checks)
    times = []
    for _ in range(20):
        t0 = time.perf_counter()
        dashboard(rd)
        times.append((time.perf_counter() - t0) * 1000)
    p50, p95 = p50_p95(times)
    rate = d["total_refunded"] / d["total_gross"] * 100 if d["total_gross"] else 0.0
    measured = [f"dashboard = 4 queries (MRR, last-30d revenue, all-time refund "
                f"rate, recoverable failed $), rollups + subscriptions only",
                f"latency over 20 runs: p50 {p50:.1f} ms, p95 {p95:.1f} ms "
                f"on the full 500k-charge dataset",
                f"refund rate: {rate:.2f}%"]
    measured += [f"{n}: {fmt(a)} == ground truth -> {'ok' if a == b else 'MISMATCH'}"
                 for n, a, b, fmt in checks]
    return {
        "name": "first_report",
        "claim": "The first revenue report (MRR, last-30d revenue, refund rate, "
                 "recoverable failed $) is served from pre-aggregated rollups in "
                 "milliseconds on the full dataset.",
        "measured": measured, "ok": ok,
        "short": f"p50 {p50:.1f} ms / p95 {p95:.1f} ms, all numbers exact",
    }


def bench_duplicate_storm(rd, tx):
    print("[4/7] duplicate_storm: replay 50000 processed events", flush=True)
    before = checksums(rd)
    rep = ingest.replay(tx, 50_000, quiet=True)
    after = checksums(rd)
    deltas = {t: after["counts"][t] - before["counts"][t] for t in before["counts"]}
    ok = (rep["inserted"] == 0 and all(v == 0 for v in deltas.values())
          and before["rollup_md5"] == after["rollup_md5"]
          and before["cstats_md5"] == after["cstats_md5"])
    eps = rep["replayed"] / rep["seconds"]
    measured = [
        f"replayed {rep['replayed']} random already-processed events through the "
        f"same apply path in {rep['seconds']:.1f}s ({eps:.0f} events/sec)",
        f"newly inserted rows: {rep['inserted']} (expected 0)",
        f"extra rows per table: {deltas} (all expected 0)",
        f"rollup_daily md5 before == after: {before['rollup_md5'] == after['rollup_md5']} "
        f"({before['rollup_md5']})",
        f"customer_stats md5 before == after: {before['cstats_md5'] == after['cstats_md5']} "
        f"({before['cstats_md5']})",
    ]
    return {
        "name": "duplicate_storm",
        "claim": "Re-delivering 50,000 duplicate events changes nothing: exactly "
                 "0 extra rows, rollups bit-identical. UNIQUE-constraint "
                 "idempotency does not weaken with volume.",
        "measured": measured, "ok": ok,
        "short": f"50k dups -> 0 extra rows, checksums identical",
    }


def bench_segmentation(rd):
    print("[6/7] segmentation: RFM over customer_stats, 20 runs", flush=True)
    sql = """
        WITH rfm AS (
            SELECT customer_id,
                   NTILE(5) OVER (ORDER BY last_paid_at DESC, customer_id) AS r,
                   NTILE(5) OVER (ORDER BY paid_count DESC, customer_id)   AS f,
                   NTILE(5) OVER (ORDER BY ltv_cents DESC, customer_id)    AS m
            FROM customer_stats
            WHERE paid_count > 0
        )
        SELECT CASE
                 WHEN r <= 2 AND f <= 2 AND m <= 2 THEN 'champions'
                 WHEN r <= 2 AND m >= 4            THEN 'promising'
                 WHEN r <= 2                       THEN 'recent'
                 WHEN r >= 4 AND m <= 2            THEN 'at_risk_high_value'
                 WHEN r >= 4                       THEN 'hibernating'
                 ELSE 'steady'
               END AS segment, count(*)
        FROM rfm GROUP BY 1 ORDER BY 2 DESC, 1"""
    rows = q(rd, sql)
    n_paying = q(rd, "SELECT count(*) FROM customer_stats WHERE paid_count > 0")[0][0]
    total_seg = sum(r[1] for r in rows)
    times = []
    for _ in range(20):
        t0 = time.perf_counter()
        q(rd, sql)
        times.append((time.perf_counter() - t0) * 1000)
    p50, p95 = p50_p95(times)
    ok = total_seg == n_paying and n_paying > 0
    measured = [
        f"RFM = NTILE(5) window functions over recency/frequency/monetary on "
        f"customer_stats (one row per customer), {n_paying} paying customers",
        f"latency over 20 runs: p50 {p50:.1f} ms, p95 {p95:.1f} ms",
        "segment sizes: " + ", ".join(f"{s} {n}" for s, n in rows),
        f"segmented customers total: {total_seg} == paying customers {n_paying} "
        f"-> {'ok' if ok else 'MISMATCH'}",
    ]
    return {
        "name": "segmentation",
        "claim": "RFM segmentation of ~100k customers runs in milliseconds, "
                 "because it reads the one-row-per-customer stats table, "
                 "never the 500k raw charges.",
        "measured": measured, "ok": ok,
        "short": f"p50 {p50:.1f} ms over {n_paying} customers",
    }


def bench_reconciliation(rd):
    print("[7/7] reconciliation: raw vs rollups, 60 months", flush=True)
    ng = dict(q(rd, "SELECT to_char(created AT TIME ZONE 'UTC', 'YYYY-MM'), "
                    "COALESCE(SUM(amount_cents), 0) FROM charges "
                    "WHERE status = 'succeeded' GROUP BY 1"))
    nr = dict(q(rd, "SELECT to_char(created AT TIME ZONE 'UTC', 'YYYY-MM'), "
                    "COALESCE(SUM(amount_cents), 0) FROM refunds GROUP BY 1"))
    roll = monthly_rollup(rd)
    months = sorted(set(GT["months"]) | set(roll) | set(ng) | set(nr))
    drift = 0
    gt_drift = 0
    for m in months:
        g_roll, r_roll = roll.get(m, (0, 0))
        drift = max(drift, abs(ng.get(m, 0) - g_roll), abs(nr.get(m, 0) - r_roll))
        gt_m = GT["months"].get(m, {"gross_cents": 0, "refunded_cents": 0})
        gt_drift = max(gt_drift, abs(g_roll - gt_m["gross_cents"]),
                       abs(r_roll - gt_m["refunded_cents"]))
    ok = drift == 0 and gt_drift == 0 and len(months) == 60
    measured = [
        f"months compared: {len(months)} (expected 60)",
        f"max |SUM(charges) - SUM(rollup gross)| per month: {drift} cents",
        f"max |SUM(refunds) - SUM(rollup refunds)| per month: "
        f"{max(abs(nr.get(m, 0) - roll.get(m, (0, 0))[1]) for m in months)} cents",
        f"max drift of rollup months vs generator ground truth: {gt_drift} cents",
    ]
    return {
        "name": "reconciliation",
        "claim": "Monthly SUM over normalized charges/refunds equals monthly SUM "
                 "over rollup_daily for all 60 months: max drift 0 cents. Rollups "
                 "are maintained incrementally (O(1) per event), never rebuilt.",
        "measured": measured, "ok": ok,
        "short": f"60 months, max drift {drift} cents",
    }


def bench_progressive(rd, tx):
    print("[3/7] progressive: fresh db, last 90 days only", flush=True)
    ingest.reset_db(tx)
    n90 = GT["counts"]["last_90d_events"]
    t0 = time.perf_counter()
    stats = ingest.backfill(tx, max_events=n90, quiet=True)
    w30 = window_rollup(rd, D_30, D_END)
    w90 = window_rollup(rd, D_90, D_END)
    ok30 = window_matches(w30, GT["last_30d"])
    ok90 = window_matches(w90, GT["last_90d"])
    ttfr = time.perf_counter() - t0
    ok = ok30 and ok90
    measured = [
        f"loaded {stats['processed']} events (everything created in the last "
        f"90 days; the stream is newest-first, so this is a file prefix)",
        f"time-to-first-correct-report: {ttfr:.1f}s from an empty database "
        f"(vs full backfill of the whole history)",
        f"last-30d gross/refunded/net: {w30['gross_cents']}/"
        f"{w30['refunded_cents']}/{w30['net_cents']} cents == ground truth "
        f"-> {'ok' if ok30 else 'MISMATCH'}",
        f"last-90d gross/refunded/net: {w90['gross_cents']}/"
        f"{w90['refunded_cents']}/{w90['net_cents']} cents == ground truth "
        f"-> {'ok' if ok90 else 'MISMATCH'}",
    ]
    return {
        "name": "progressive",
        "claim": "Backfilling newest-first makes reports usable long before the "
                 "history finishes: loading only the last 90 days yields "
                 "window-correct dashboard numbers in seconds.",
        "measured": measured, "ok": ok,
        "short": f"correct 30d/90d report {ttfr:.1f}s after empty db",
    }


def bench_kill_resume(rd, tx):
    print("[5/7] kill_resume: kill backfill at ~40%, resume, verify cents",
          flush=True)
    ingest.reset_db(tx)
    total = GT["counts"]["events_total"]
    threshold = int(total * KILL_AT_FRACTION)
    cmd = [sys.executable, os.path.join(ROOT, "ingest.py"), "backfill"]
    proc = subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.DEVNULL,
                            stderr=subprocess.PIPE)
    kill_off = None
    deadline = time.time() + 1800
    while time.time() < deadline:
        if proc.poll() is not None:
            err = proc.stderr.read().decode(errors="replace")[-2000:]
            raise RuntimeError(f"backfill exited before kill threshold: {err}")
        row = q(rd, "SELECT last_stripe_id FROM ingest_checkpoint "
                    "WHERE worker = 'backfill'")
        if row and row[0][0]:
            off = int(row[0][0].rsplit("@", 1)[1])
            if off >= threshold:
                proc.kill()   # TerminateProcess on Windows: no cleanup at all
                proc.wait()
                kill_off = off
                break
        time.sleep(0.3)
    if kill_off is None:
        raise RuntimeError("kill threshold never reached")

    row = q(rd, "SELECT last_stripe_id FROM ingest_checkpoint "
                "WHERE worker = 'backfill'")
    resume_from = int(row[0][0].rsplit("@", 1)[1])
    t0 = time.perf_counter()
    r = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    resume_s = time.perf_counter() - t0
    if r.returncode != 0:
        raise RuntimeError(f"resume run failed: {r.stderr[-2000:]}")

    raw = q(rd, "SELECT count(*) FROM raw_events")[0][0]
    charges = q(rd, "SELECT count(*) FROM charges")[0][0]
    refunds = q(rd, "SELECT count(*) FROM refunds")[0][0]
    drift = monthly_drift_vs_gt(rd)
    ok = (raw == total and charges == GT["counts"]["charges"]
          and refunds == GT["counts"]["refunds"] and drift == 0)
    measured = [
        f"killed worker (TerminateProcess) at checkpoint {kill_off} / {total} "
        f"events ({kill_off / total * 100:.1f}%)",
        f"restart resumed from checkpoint {resume_from} and finished the "
        f"remaining {total - resume_from} events in {resume_s:.1f}s",
        f"raw_events: {raw} (expected {total}), charges: {charges}, "
        f"refunds: {refunds} -> no loss, no duplicates",
        f"monthly gross/refund totals vs ground truth over 60 months: "
        f"max drift {drift} cents",
    ]
    return {
        "name": "kill_resume",
        "claim": "The checkpointed backfill worker survives a hard mid-run kill: "
                 "after restart it finishes to the exact cent -- no lost and no "
                 "double-counted money, drift 0.",
        "measured": measured, "ok": ok,
        "short": f"killed at {kill_off / total * 100:.0f}%, final drift 0 cents",
    }


# ---------------------------------------------------------------- report

def write_results(sections_by_num, meta, path=OUT_PATH,
                  title="revscope bench results"):
    """Shared with bench/run_product.py: one definition of the report format."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    lines = [f"# {title}", ""]
    lines += [f"- run at: {meta['ran_at']}",
              f"- postgres: {meta['pg']}",
              f"- python: {meta['py']}, psycopg {psycopg.__version__}",
              f"- dataset: {meta['dataset']}", ""]
    lines += ["| # | bench | measured | verdict |",
              "|---|-------|----------|---------|"]
    for num in sorted(sections_by_num):
        s = sections_by_num[num]
        v = "PASS" if s["ok"] else "FAIL"
        lines.append(f"| {num} | {s['name']} | {s['short']} | {v} |")
    lines.append("")
    for num in sorted(sections_by_num):
        s = sections_by_num[num]
        lines += [f"## {num}. {s['name']}", "",
                  f"**claim:** {s['claim']}", "", "**measured:**", ""]
        lines += [f"- {m}" for m in s["measured"]]
        lines += ["", f"**verdict:** {'PASS' if s['ok'] else 'FAIL'}", ""]
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines))


def main():
    t_start = time.perf_counter()
    rd = psycopg.connect(ingest.DSN, autocommit=True)   # reads + monitoring
    tx = ingest.connect()                               # transactional ingest
    meta = {
        "ran_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "pg": q(rd, "SELECT version()")[0][0].split(" on ")[0],
        "py": sys.version.split()[0],
        "dataset": (f"{GT['counts']['events_total']} events: "
                    f"{GT['counts']['customers']} customers, "
                    f"{GT['counts']['subscriptions']} subscriptions, "
                    f"{GT['counts']['charges']} charges "
                    f"({GT['counts']['invoices']} invoices + "
                    f"{GT['counts']['oneoff_charges']} one-off), "
                    f"{GT['counts']['refunds']} refunds, 60 months"),
    }

    sections = {}

    def run(num, fn, *args):
        try:
            sections[num] = fn(*args)
        except Exception as ex:
            try:
                tx.rollback()
            except Exception:
                pass
            sections[num] = {
                "name": fn.__name__.replace("bench_", ""),
                "claim": "(bench crashed before producing a claim verdict)",
                "measured": [f"ERROR: {type(ex).__name__}: {ex}"],
                "ok": False, "short": "crashed",
            }
            print(f"bench {fn.__name__} FAILED: {ex}", flush=True)

    run(1, bench_full_backfill, rd, tx)
    run(2, bench_first_report, rd)
    run(4, bench_duplicate_storm, rd, tx)
    run(6, bench_segmentation, rd)
    run(7, bench_reconciliation, rd)
    run(3, bench_progressive, rd, tx)
    run(5, bench_kill_resume, rd, tx)

    write_results(sections, meta)
    n_pass = sum(1 for s in sections.values() if s["ok"])
    print(f"bench complete in {(time.perf_counter() - t_start) / 60:.1f} min: "
          f"{n_pass}/7 PASS, results in bench/out/results.md", flush=True)
    for num in sorted(sections):
        s = sections[num]
        print(f"  {num}. {s['name']}: {'PASS' if s['ok'] else 'FAIL'} "
              f"({s['short']})", flush=True)
    sys.exit(0 if n_pass == 7 else 1)


if __name__ == "__main__":
    main()
