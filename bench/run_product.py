"""revscope product bench suite: python -m bench.run_product

Four benches over the product layer, same shape as bench/run_all.py:
claim -> measured -> verdict, every number from a real run, every money
assertion in integer cents against data/ground_truth.json.

It runs on an already-ingested database and does not touch the ingest core,
so the seven benches in run_all.py keep their published numbers. Order:

  8  cohort_retention   marts vs an independent Python implementation
  9  funnel             stage counts, nesting, the biggest drop
 10  fx_reporting       rate of the day vs today's rate, in dollars
 11  product_dashboard  the whole page, served from marts, with filters

stdout is ASCII-only (Windows cp1251 console).
"""

import json
import os
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Корень проекта в пути — иначе `python bench/run_product.py` падает на
# `No module named 'product'`: скрипт лежит в подпапке, и Python кладёт в sys.path
# именно её, а не корень репозитория.
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import product  # noqa: E402
from bench.run_all import GT, p50_p95, q, usd, write_results  # noqa: E402

OUT_PATH = os.path.join(ROOT, "bench", "out", "product_results.md")

KEEP = [int(k) for k in GT["retention"]["keep_periods"]]
FUNNEL_ORDER = ["attempted", "paid_once", "paid_twice", "regular_3plus",
                "still_paying"]
CHURN_ROWS = ["never_converted", "churn_involuntary", "churn_voluntary"]


def pctf(part, whole):
    return part / whole * 100 if whole else 0.0


# ---------------------------------------------------------------- benches

def bench_cohorts(rd):
    print("[8/11] cohort_retention: mart vs generator ground truth", flush=True)
    rows = q(rd, """
        SELECT to_char(cohort_month, 'YYYY-MM'), period_index,
               SUM(cohort_size)::int, SUM(eligible)::int, SUM(retained)::int,
               SUM(survived)::int, SUM(revenue_usd_cents)::bigint
        FROM mart_cohort_retention
        WHERE period_index = ANY(%s) GROUP BY 1, 2""", (KEEP,))
    mart = {(m, p): r for m, p, *r in rows}
    gt = GT["cohorts"]

    bad, checked = [], 0
    for month, c in gt.items():
        for k in KEEP:
            got = mart.get((month, k))
            want = (c["size"], c["eligible"][str(k)], c["retained"][str(k)],
                    c["survived"][str(k)], c["revenue_usd_cents"][str(k)])
            checked += 1
            if got is None or tuple(got) != want:
                bad.append(f"{month} P{k}: SQL {got} != python {want}")
    extra = set(mart) - {(m, k) for m in gt for k in KEEP}

    tot = {k: [0, 0, 0, 0] for k in KEEP}   # size, eligible, retained, survived
    for (m, k), (size, elig, ret, sur, _rev) in mart.items():
        tot[k][0] += size
        tot[k][1] += elig
        tot[k][2] += ret
        tot[k][3] += sur

    ok = not bad and not extra
    n_cohorts = len(gt)

    measured = [
        f"mart: {q(rd, 'SELECT count(*) FROM mart_cohort_retention')[0][0]} "
        f"rows (cohort x country x 13 periods) over {n_cohorts} monthly "
        f"cohorts and {tot[KEEP[0]][0]} paying customers",
        f"cells compared against the Python implementation in gen.py: {checked} "
        f"(cohort x period, four values each), mismatches: {len(bad)}",
    ]
    for k in KEEP:
        size, elig, ret, sur = tot[k]
        measured.append(
            f"month {k + 1}: retained {ret} of {elig} eligible = "
            f"{pctf(ret, elig):.1f}% (survived-to-or-past: {sur} = "
            f"{pctf(sur, elig):.1f}%)")
    # The same numerator over the wrong denominator. This is the single most
    # common way a retention chart lies, so it is measured, not asserted.
    naive = [f"month {k + 1}: {pctf(tot[k][2], tot[k][0]):.1f}% instead of "
             f"{pctf(tot[k][2], tot[k][1]):.1f}%" for k in KEEP]
    measured.append("dividing by the whole cohort instead of the customers the "
                    "period has elapsed for reports " + "; ".join(naive))
    measured.append(
        f"gap between 'paid in this period' and 'paid in it or later' at month "
        f"{KEEP[-1] + 1}: {tot[KEEP[-1]][3] - tot[KEEP[-1]][2]} customers "
        f"({pctf(tot[KEEP[-1]][3] - tot[KEEP[-1]][2], tot[KEEP[-1]][1]):.1f} "
        f"points) -- customers a single failed charge would have written off")
    if bad:
        measured += [f"MISMATCH {b}" for b in bad[:5]]
    if extra:
        measured.append(f"MISMATCH cohorts in the mart but not in ground truth: "
                        f"{sorted(extra)[:5]}")
    return {
        "name": "cohort_retention",
        "claim": "Cohort retention computed in SQL over the marts matches, cell "
                 "for cell, an independent Python implementation that never "
                 "touches the database -- including the per-customer maturity "
                 "rule that keeps young cohorts out of the denominator.",
        "measured": measured, "ok": ok,
        "short": f"{checked} cells, {len(bad)} mismatches, "
                 f"M12 {pctf(tot[KEEP[-1]][2], tot[KEEP[-1]][1]):.1f}%",
    }


def bench_funnel(rd):
    print("[9/11] funnel: stage counts, nesting, biggest drop", flush=True)
    rows = q(rd, "SELECT stage, SUM(customers)::int FROM mart_funnel "
                 "GROUP BY 1")
    mart = dict(rows)
    gt = GT["funnel"]
    bad = [f"{s}: SQL {mart.get(s)} != python {gt[s]}"
           for s in gt if mart.get(s) != gt[s]]

    nested = all(mart[a] >= mart[b]
                 for a, b in zip(FUNNEL_ORDER, FUNNEL_ORDER[1:]))
    drops = [(b, mart[a] - mart[b], pctf(mart[a] - mart[b], mart[a]))
             for a, b in zip(FUNNEL_ORDER, FUNNEL_ORDER[1:])]
    worst = max(drops, key=lambda d: d[2])
    ok = not bad and nested

    measured = [
        f"funnel base: {mart['attempted']} customers with at least "
        f"{GT['retention']['funnel_maturity_days']} days of history "
        f"(younger ones are excluded, not counted as lost)",
    ]
    prev = None
    for s in FUNNEL_ORDER:
        d = "" if prev is None else f" (-{pctf(prev - mart[s], prev):.1f}%)"
        measured.append(f"{s}: {mart[s]}{d}")
        prev = mart[s]
    measured.append(f"biggest drop-off: {worst[0]} loses {worst[1]} customers, "
                    f"{worst[2]:.1f}% of the previous stage")
    measured.append("churn split of the same base: " + ", ".join(
        f"{s} {mart[s]} ({pctf(mart[s], mart['attempted']):.1f}%)"
        for s in CHURN_ROWS))
    invol = mart["churn_involuntary"]
    left = invol + mart["churn_voluntary"]
    measured.append(f"of {left} customers who stopped paying, {invol} "
                    f"({pctf(invol, left):.1f}%) left on a declined charge, not "
                    f"on a decision -- the part dunning can actually address")
    measured.append(f"every stage a subset of the previous one: {nested}")
    measured.append(f"stage counts vs the Python implementation: "
                    f"{len(gt)} stages, {len(bad)} mismatches")
    if bad:
        measured += [f"MISMATCH {b}" for b in bad]
    return {
        "name": "funnel",
        "claim": "The payment funnel is built from transactions only (no "
                 "invented product events), every stage is a strict subset of "
                 "the one above it, and all eight counts match the independent "
                 "Python implementation exactly.",
        "measured": measured, "ok": ok,
        "short": f"{len(bad)} mismatches, worst drop {worst[2]:.1f}% at {worst[0]}",
    }


def bench_fx(rd):
    print("[10/11] fx_reporting: rate of the day vs today's rate", flush=True)
    rows = q(rd, """
        SELECT country, min(currency), SUM(tx_count)::int,
               SUM(gross_local)::bigint, SUM(gross_usd_hist)::bigint,
               SUM(gross_usd_current)::bigint, SUM(refund_local)::bigint,
               SUM(refund_usd_hist)::bigint, SUM(refund_usd_current)::bigint
        FROM mart_revenue_country GROUP BY 1""")
    mart = {r[0]: r[1:] for r in rows}
    gt = GT["by_country"]
    bad = []
    for c, g in gt.items():
        m = mart.get(c)
        want = (g["currency"], g["charges"], g["gross_local"],
                g["gross_usd_hist"], g["gross_usd_current"],
                g["refunded_local"], g["refunded_usd_hist"],
                g["refunded_usd_current"])
        if m is None or tuple(m) != want:
            bad.append(f"{c}: SQL {m} != python {want}")

    hist = sum(g["gross_usd_hist"] for g in gt.values())
    cur = sum(g["gross_usd_current"] for g in gt.values())
    ledger = GT["total_gross_cents"]

    # No rate for a transaction day means an INNER JOIN silently drops the row
    # and the report is quietly short. Coverage is asserted, not assumed.
    missing = q(rd, """
        SELECT count(*) FROM charges c
        LEFT JOIN fx_rates f ON f.currency = c.currency
                            AND f.day = (c.created AT TIME ZONE 'UTC')::date
        WHERE f.day IS NULL""")[0][0]
    in_mart = q(rd, "SELECT COALESCE(SUM(tx_count), 0) "
                    "FROM mart_revenue_country")[0][0]
    succeeded = GT["counts"]["charges_succeeded"]

    # A floor of $10k on the slice: the deepest percentage gap in the whole
    # mart sits on a month with three charges in it, and quoting that as the
    # headline would be exactly the kind of number this project refuses to
    # print.
    worst_slice = q(rd, """
        SELECT to_char(month, 'YYYY-MM'), country, gross_usd_hist,
               gross_usd_current
        FROM mart_revenue_country
        WHERE gross_usd_hist >= 1000000
        ORDER BY (gross_usd_current - gross_usd_hist)::numeric
                 / gross_usd_hist LIMIT 1""")[0]
    # Why the all-time gap is smaller than any single old year: revenue grows
    # ~4x over the period, so most of it was booked at rates close to today's.
    by_year = q(rd, """
        SELECT to_char(month, 'YYYY'), SUM(gross_usd_hist)::bigint,
               SUM(gross_usd_current)::bigint
        FROM mart_revenue_country GROUP BY 1 ORDER BY 1""")
    ws_pct = pctf(worst_slice[3] - worst_slice[2], worst_slice[2])
    worst_ctry = min(gt.items(),
                     key=lambda kv: pctf(kv[1]["gross_usd_current"]
                                         - kv[1]["gross_usd_hist"],
                                         kv[1]["gross_usd_hist"]))

    ok = (not bad and missing == 0 and in_mart == succeeded
          and abs(hist - ledger) * 10_000 < ledger)   # residual < 0.01%
    measured = [
        f"{succeeded} succeeded charges in "
        f"{len(gt)} countries / {len(set(g['currency'] for g in gt.values()))} "
        f"currencies, every one of them converted per transaction at the rate "
        f"of its own day",
        f"rate feed: {q(rd, 'SELECT count(*) FROM fx_rates')[0][0]} daily rows "
        f"({GT['fx']['days']} days x "
        f"{len(GT['fx']['rate_first_last'])} currencies), charges with no rate "
        f"for their day: {missing} (expected 0)",
        f"charges reaching the mart: {in_mart} of {succeeded} succeeded "
        f"-> {'ok' if in_mart == succeeded else 'MISMATCH'}",
        f"gross at the rate of the day: {usd(hist)}",
        f"gross at today's rate:        {usd(cur)}",
        f"difference: {usd(cur - hist)}, {pctf(cur - hist, hist):+.2f}% of "
        f"reported revenue -- the same local money, one join condition apart",
        f"worst country: {worst_ctry[0]} ({worst_ctry[1]['currency'].upper()}) "
        f"{pctf(worst_ctry[1]['gross_usd_current'] - worst_ctry[1]['gross_usd_hist'], worst_ctry[1]['gross_usd_hist']):+.2f}% "
        f"= {usd(worst_ctry[1]['gross_usd_current'] - worst_ctry[1]['gross_usd_hist'])}",
        f"worst month x country slice above 10,000 USD: {worst_slice[1]} "
        f"{worst_slice[0]} {ws_pct:+.2f}% "
        f"({usd(worst_slice[2])} -> {usd(worst_slice[3])})",
        "gap by year of the transaction: " + ", ".join(
            f"{y} {pctf(c - h, h):+.2f}%" for y, h, c in by_year)
        + " -- the all-time number is small only because the business grew "
          "into the recent, near-current rates",
        f"refunds: {usd(sum(g['refunded_usd_hist'] for g in gt.values()))} at "
        f"the rate of the day vs "
        f"{usd(sum(g['refunded_usd_current'] for g in gt.values()))} at today's",
        f"per-country totals vs the Python implementation: {len(gt)} countries "
        f"x 8 values, {len(bad)} mismatches",
        f"round trip against the already-published ledger: converting every "
        f"presentment amount back at its own day's rate gives {hist} cents vs "
        f"{ledger} cents of USD list price, residual {hist - ledger} cents "
        f"({pctf(hist - ledger, ledger):+.6f}%) -- rounding of the presented "
        f"amount, not FX error",
    ]
    if bad:
        measured += [f"MISMATCH {b}" for b in bad[:5]]
    return {
        "name": "fx_reporting",
        "claim": "Revenue is converted at the rate of each transaction day, "
                 "not at today's rate. On this dataset the difference between "
                 "the two is a measured amount of money, not a rounding "
                 "detail, and it is larger for weak-currency countries.",
        "measured": measured, "ok": ok,
        "short": f"{pctf(cur - hist, hist):+.2f}% ({usd(cur - hist).split(' (')[0]}) "
                 f"if converted at today's rate",
    }


def bench_dashboard(rd):
    print("[11/11] product_dashboard: 4 mart queries, 20 runs x 2 filters",
          flush=True)
    conn = product.connect(autocommit=True)

    def page(country=None, mfrom=None, mto=None):
        return (product.summary(conn, country, mfrom, mto),
                product.revenue(conn, country, mfrom, mto),
                product.funnel(conn, country, mfrom, mto),
                product.cohorts(conn, country, mfrom, mto))

    def timed(**kw):
        ts = []
        for _ in range(20):
            t0 = time.perf_counter()
            page(**kw)
            ts.append((time.perf_counter() - t0) * 1000)
        return p50_p95(ts)

    p50_all, p95_all = timed()
    p50_f, p95_f = timed(country="KZ", mfrom="2024-01-01", mto="2024-12-01")

    # The page itself, not just the query functions: the same handler
    # product.py serves, on a real socket, over the pooled connections.
    srv = ThreadingHTTPServer(("127.0.0.1", 0), product.Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_address[1]}"
    ends = ["/api/summary", "/api/revenue", "/api/funnel", "/api/cohorts"]
    try:
        for e in ends:                       # warm the pool, then measure
            urllib.request.urlopen(base + e).read()
        http_ms, sql_ms = [], []
        for _ in range(20):
            t0 = time.perf_counter()
            got = [json.load(urllib.request.urlopen(base + e)) for e in ends]
            http_ms.append((time.perf_counter() - t0) * 1000)
            sql_ms.append(sum(g["query_ms"] for g in got))
        page_bytes = len(urllib.request.urlopen(base + "/").read())
    finally:
        srv.shutdown()
    p50_http, p95_http = p50_p95(http_ms)
    p50_sql, _ = p50_p95(sql_ms)

    s, r, f, c = page()
    s_kz, r_kz, _f_kz, _c_kz = page(country="KZ", mfrom="2024-01-01",
                                    mto="2024-12-01")
    mart_rows = sum(q(rd, f"SELECT count(*) FROM {t}")[0][0] for t in
                    ("mart_revenue_country", "mart_cohort_retention",
                     "mart_funnel"))
    charges = GT["counts"]["charges"]

    # The filtered page must be a real slice, not the same numbers again.
    filtered_ok = (s_kz["gross_hist"] < s["gross_hist"]
                   and len(r_kz["by_country"]) == 1
                   and r_kz["by_country"][0]["country"] == "KZ"
                   and len(r_kz["by_month"]) == 12)
    totals_ok = (s["gross_hist"]
                 == sum(g["gross_usd_hist"] for g in GT["by_country"].values()))
    ok = filtered_ok and totals_ok and p95_all < 1000 and p95_http < 1000

    measured = [
        f"page = 4 queries (summary, revenue, funnel, cohorts), all of them "
        f"against the three marts: {mart_rows} rows total, versus {charges} "
        f"charges the marts were built from ({charges / mart_rows:.0f}x fewer "
        f"rows to read)",
        f"unfiltered page over all {s['countries']} countries and 60 months: "
        f"p50 {p50_all:.1f} ms, p95 {p95_all:.1f} ms over 20 runs",
        f"filtered page (country=KZ, 2024-01..2024-12): p50 {p50_f:.1f} ms, "
        f"p95 {p95_f:.1f} ms over 20 runs",
        f"end to end over HTTP, the four endpoints the browser actually calls: "
        f"p50 {p50_http:.1f} ms, p95 {p95_http:.1f} ms over 20 runs "
        f"(of which {p50_sql:.1f} ms is SQL); the page itself is "
        f"{page_bytes} bytes with no external requests at all",
        f"filter actually cuts the data: {len(r_kz['by_month'])} months, "
        f"{len(r_kz['by_country'])} country, gross "
        f"{usd(s_kz['gross_hist'])} vs {usd(s['gross_hist'])} unfiltered "
        f"-> {'ok' if filtered_ok else 'MISMATCH'}",
        f"unfiltered page totals equal generator ground truth: "
        f"{'ok' if totals_ok else 'MISMATCH'}",
        f"headline numbers the page shows: retention M2 "
        f"{s['retention']['1']['pct']}%, M12 {s['retention']['11']['pct']}%, "
        f"worst funnel step {s['worst_stage']} -{s['worst_drop_pct']}%, "
        f"FX gap {s['fx_gap_pct']:+.2f}%",
        f"cohorts returned: {len(c)}, funnel stages: {len(f['stages'])}, "
        f"months on the revenue chart: {len(r['by_month'])}",
    ]
    conn.close()
    return {
        "name": "product_dashboard",
        "claim": "The whole product dashboard -- cohorts, funnel and revenue "
                 "by country, filtered by country and period -- is served from "
                 "the marts in milliseconds, reading a few thousand mart rows "
                 "instead of the 500,000 charges they were built from.",
        "measured": measured, "ok": ok,
        "short": f"p50 {p50_http:.1f} ms over HTTP, {p50_all:.1f} ms in-process",
    }


# -------------------------------------------------------------------- main

def main():
    t_start = time.perf_counter()
    rd = product.connect(autocommit=True)
    n_charges = q(rd, "SELECT count(*) FROM charges")[0][0]
    if n_charges != GT["counts"]["charges"]:
        print(f"database holds {n_charges} charges, expected "
              f"{GT['counts']['charges']}: run `python ingest.py backfill` "
              f"first", flush=True)
        sys.exit(2)

    print("rebuilding marts before measuring...", flush=True)
    tx = product.connect()
    build = product.build_marts(tx)
    tx.close()

    meta = {
        "ran_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "pg": q(rd, "SELECT version()")[0][0].split(" on ")[0],
        "py": sys.version.split()[0],
        "dataset": (f"{GT['counts']['charges']} charges over "
                    f"{len(GT['by_country'])} countries / "
                    f"{len(GT['fx']['rate_first_last'])} currencies, "
                    f"{GT['fx']['days']} days of rates, 60 months; marts "
                    f"rebuilt in "
                    f"{sum(v['seconds'] for v in build.values()):.1f}s "
                    f"({build['mart_revenue_country']['seconds']:.1f}s revenue, "
                    f"{build['mart_cohort_retention']['seconds']:.1f}s cohorts, "
                    f"{build['mart_funnel']['seconds']:.1f}s funnel)"),
    }

    sections = {}

    def run(num, fn, *args):
        try:
            sections[num] = fn(*args)
        except Exception as ex:
            sections[num] = {
                "name": fn.__name__.replace("bench_", ""),
                "claim": "(bench crashed before producing a claim verdict)",
                "measured": [f"ERROR: {type(ex).__name__}: {ex}"],
                "ok": False, "short": "crashed",
            }
            print(f"bench {fn.__name__} FAILED: {ex}", flush=True)

    run(8, bench_cohorts, rd)
    run(9, bench_funnel, rd)
    run(10, bench_fx, rd)
    run(11, bench_dashboard, rd)

    write_results(sections, meta, path=OUT_PATH,
                  title="revscope product bench results")
    n_pass = sum(1 for s in sections.values() if s["ok"])
    print(f"product bench complete in {time.perf_counter() - t_start:.0f}s: "
          f"{n_pass}/4 PASS, results in bench/out/product_results.md", flush=True)
    for num in sorted(sections):
        s = sections[num]
        print(f"  {num}. {s['name']}: {'PASS' if s['ok'] else 'FAIL'} "
              f"({s['short']})", flush=True)
    sys.exit(0 if n_pass == 4 else 1)


if __name__ == "__main__":
    main()
