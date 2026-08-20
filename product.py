"""revscope product layer: three marts, one dashboard.

  python product.py build            load the FX feed, rebuild all three marts
  python product.py show             print the marts to stdout
  python product.py serve [--port]   dashboard on http://127.0.0.1:8000

The marts are built with the same rule the rest of the project follows: money
is aggregated once, incrementally-maintained rollups answer the report, and
nothing user-facing ever scans the 500k charges. Building is a single pass per
mart; every dashboard query then reads a table of a few thousand rows.

The one thing this layer adds to the ingest core is currency. Every charge is
presented in the customer's local currency, and revenue is reported in USD at
the rate of the transaction day -- never at today's rate. The difference
between those two is measured in bench/run_product.py; it is the number this
module exists for.

stdout is ASCII-only on purpose (Windows cp1251 console).
"""

import argparse
import json
import os
import time
from contextlib import contextmanager
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock
from urllib.parse import parse_qs, urlparse

import psycopg

import fx
import ingest

HERE = os.path.dirname(os.path.abspath(__file__))
FX_PATH = os.path.join(HERE, "data", "fx_rates.json")
SCHEMA_PATH = os.path.join(HERE, "schema_product.sql")
PAGE_PATH = os.path.join(HERE, "dashboard.html")

# 30-day retention period and the two horizons the marts are cut at. They are
# duplicated from gen.py rather than imported so the SQL side never silently
# follows a change on the generator side: if the two drift apart, the product
# bench fails instead of quietly reporting a different metric.
PERIOD_SECS = 30 * 86_400
MAX_PERIOD = 12
ACTIVE_GRACE_DAYS = 45
FUNNEL_MATURITY_DAYS = 90
# The milestones the header reports: survived to the 2nd, 3rd, 6th, 12th
# month. Period 0 is the first payment, so those are periods 1, 2, 5, 11.
KEEP_PERIODS = (1, 2, 5, 11)

# Local minor units -> USD cents, in integers, exactly as fx.to_usd does it in
# Python. Written out once and formatted in, because it appears in three marts
# and a copy-paste divergence here is a silent money bug.
USD = "(({amt} * {scale} + {rate}.rate_scaled / 2) / {rate}.rate_scaled)"


def usd_expr(amount_col, rate_alias):
    return USD.format(amt=amount_col, rate=rate_alias, scale=fx.RATE_SCALE)


BUILD_REVENUE = f"""
INSERT INTO mart_revenue_country
WITH now_rate AS (
    -- "today" is the last day the feed carries, not the wall clock: a report
    -- must close on a day it actually has rates for.
    SELECT currency, rate_scaled FROM fx_rates
    WHERE day = (SELECT max(day) FROM fx_rates)
),
g AS (
    SELECT date_trunc('month', c.created AT TIME ZONE 'UTC')::date AS month,
           cu.country, c.currency,
           count(*)::int AS tx_count,
           SUM(c.amount_local) AS local_sum,
           SUM({usd_expr('c.amount_local', 'f')}) AS usd_hist,
           SUM({usd_expr('c.amount_local', 'n')}) AS usd_now
    FROM charges c
    JOIN customers cu ON cu.id = c.customer_id
    -- the join that decides whether the report agrees with the bank: the rate
    -- is taken on the transaction day, not on the day the report is run
    JOIN fx_rates f ON f.currency = c.currency
                   AND f.day = (c.created AT TIME ZONE 'UTC')::date
    JOIN now_rate n ON n.currency = c.currency
    WHERE c.status = 'succeeded'
    GROUP BY 1, 2, 3
),
r AS (
    SELECT date_trunc('month', rf.created AT TIME ZONE 'UTC')::date AS month,
           cu.country, rf.currency,
           count(*)::int AS refund_count,
           SUM(rf.amount_local) AS local_sum,
           SUM({usd_expr('rf.amount_local', 'f')}) AS usd_hist,
           SUM({usd_expr('rf.amount_local', 'n')}) AS usd_now
    FROM refunds rf
    JOIN customers cu ON cu.id = rf.customer_id
    JOIN fx_rates f ON f.currency = rf.currency
                   AND f.day = (rf.created AT TIME ZONE 'UTC')::date
    JOIN now_rate n ON n.currency = rf.currency
    GROUP BY 1, 2, 3
)
SELECT COALESCE(g.month, r.month), COALESCE(g.country, r.country),
       COALESCE(g.currency, r.currency),
       COALESCE(g.tx_count, 0), COALESCE(g.local_sum, 0),
       COALESCE(g.usd_hist, 0), COALESCE(g.usd_now, 0),
       COALESCE(r.refund_count, 0), COALESCE(r.local_sum, 0),
       COALESCE(r.usd_hist, 0), COALESCE(r.usd_now, 0)
-- FULL OUTER, not LEFT: a month whose only event in a country is a refund of
-- an older charge is a real month and must not vanish from the report.
FROM g FULL OUTER JOIN r ON r.month = g.month AND r.country = g.country
"""

BUILD_COHORTS = f"""
INSERT INTO mart_cohort_retention
WITH asof AS (SELECT (max(day)::timestamp AT TIME ZONE 'UTC') AS ts FROM fx_rates),
pay AS (
    SELECT c.customer_id, cu.country, c.created,
           {usd_expr('c.amount_local', 'f')} AS usd
    FROM charges c
    JOIN customers cu ON cu.id = c.customer_id
    JOIN fx_rates f ON f.currency = c.currency
                   AND f.day = (c.created AT TIME ZONE 'UTC')::date
    WHERE c.status = 'succeeded'
),
firsts AS (
    SELECT customer_id, country, min(created) AS first_at
    FROM pay GROUP BY 1, 2
),
per AS (
    SELECT p.customer_id,
           floor(EXTRACT(EPOCH FROM (p.created - fs.first_at)) / {PERIOD_SECS})::int
               AS period_index,
           p.usd
    FROM pay p JOIN firsts fs USING (customer_id)
),
in_period AS (
    SELECT customer_id, period_index, SUM(usd) AS usd
    FROM per WHERE period_index <= {MAX_PERIOD} GROUP BY 1, 2
),
reach AS (SELECT customer_id, max(period_index) AS last_period FROM per GROUP BY 1),
grid AS (
    SELECT fs.customer_id, fs.country,
           date_trunc('month', fs.first_at AT TIME ZONE 'UTC')::date AS cohort_month,
           k.k AS period_index,
           -- eligibility is per customer, not per cohort: the period has to
           -- have fully elapsed for THIS customer before their absence from
           -- it is allowed to count as churn
           fs.first_at + make_interval(secs => (k.k + 1) * {PERIOD_SECS})
               <= (SELECT ts FROM asof) AS mature
    FROM firsts fs CROSS JOIN generate_series(0, {MAX_PERIOD}) AS k(k)
)
SELECT g.cohort_month, g.country, g.period_index,
       count(*)::int,
       count(*) FILTER (WHERE g.mature)::int,
       count(*) FILTER (WHERE g.mature AND ip.usd IS NOT NULL)::int,
       count(*) FILTER (WHERE g.mature AND rc.last_period >= g.period_index)::int,
       COALESCE(SUM(ip.usd) FILTER (WHERE g.mature), 0)
FROM grid g
LEFT JOIN in_period ip
       ON ip.customer_id = g.customer_id AND ip.period_index = g.period_index
LEFT JOIN reach rc ON rc.customer_id = g.customer_id
GROUP BY 1, 2, 3
"""

BUILD_FUNNEL = f"""
INSERT INTO mart_funnel
WITH asof AS (SELECT (max(day)::timestamp AT TIME ZONE 'UTC') AS ts FROM fx_rates),
att AS (
    SELECT c.customer_id, cu.country,
           min(c.created) AS first_at,
           count(*) FILTER (WHERE c.status = 'succeeded')::int AS n_paid,
           max(c.created) FILTER (WHERE c.status = 'succeeded') AS last_paid_at
    FROM charges c JOIN customers cu ON cu.id = c.customer_id
    GROUP BY 1, 2
),
-- Last attempt decides whether the customer left on a decline or on a
-- decision. Ties break by id, the same order the generator uses, so the
-- answer never depends on which row Postgres happened to read first.
last_try AS (
    SELECT DISTINCT ON (customer_id) customer_id, status
    FROM charges ORDER BY customer_id, created DESC, id DESC
),
m AS (
    SELECT a.*, l.status AS last_status,
           (SELECT ts FROM asof) - interval '{ACTIVE_GRACE_DAYS} days' AS active_cut
    FROM att a JOIN last_try l USING (customer_id)
    -- only customers with a full three cycles behind them: someone who first
    -- paid last week has not failed to reach a third payment, they have not
    -- had the time
    WHERE a.first_at <= (SELECT ts FROM asof) - interval '{FUNNEL_MATURITY_DAYS} days'
),
agg AS (
    SELECT date_trunc('month', m.first_at AT TIME ZONE 'UTC')::date AS cohort_month,
           m.country,
           count(*)::int AS attempted,
           count(*) FILTER (WHERE n_paid >= 1)::int AS paid_once,
           count(*) FILTER (WHERE n_paid >= 2)::int AS paid_twice,
           count(*) FILTER (WHERE n_paid >= 3)::int AS regular_3plus,
           count(*) FILTER (WHERE n_paid >= 3
                              AND last_paid_at >= active_cut)::int AS still_paying,
           count(*) FILTER (WHERE n_paid = 0)::int AS never_converted,
           count(*) FILTER (WHERE n_paid >= 1 AND last_paid_at < active_cut
                              AND last_status <> 'succeeded')::int AS churn_involuntary,
           count(*) FILTER (WHERE n_paid >= 1 AND last_paid_at < active_cut
                              AND last_status = 'succeeded')::int AS churn_voluntary
    FROM m GROUP BY 1, 2
)
SELECT a.cohort_month, a.country, v.kind, v.stage, v.ord, v.n
FROM agg a
CROSS JOIN LATERAL (VALUES
    ('funnel', 'attempted',         1, a.attempted),
    ('funnel', 'paid_once',         2, a.paid_once),
    ('funnel', 'paid_twice',        3, a.paid_twice),
    ('funnel', 'regular_3plus',     4, a.regular_3plus),
    ('funnel', 'still_paying',      5, a.still_paying),
    ('churn',  'never_converted',   6, a.never_converted),
    ('churn',  'churn_involuntary', 7, a.churn_involuntary),
    ('churn',  'churn_voluntary',   8, a.churn_voluntary)
) AS v(kind, stage, ord, n)
"""

FUNNEL_LABELS = {
    "attempted": "tried to pay",
    "paid_once": "first payment went through",
    "paid_twice": "came back for a second",
    "regular_3plus": "reached a third (regular)",
    "still_paying": "still paying now",
    "never_converted": "never got a payment through",
    "churn_involuntary": "left on a decline",
    "churn_voluntary": "just stopped",
}


def connect(autocommit=False):
    conn = psycopg.connect(ingest.DSN, autocommit=autocommit)
    # Every date bucket in this module is UTC. Pinning the session removes the
    # one class of bug that only shows up when the server moves timezone.
    conn.execute("SET TIME ZONE 'UTC'")
    return conn


# ----------------------------------------------------------------- building

def load_fx(conn, path=FX_PATH):
    """Load data/fx_rates.json into fx_rates. Returns (rows, seconds)."""
    with open(path, encoding="utf-8") as f:
        feed = json.load(f)
    if feed["scale"] != fx.RATE_SCALE:
        raise RuntimeError(f"feed scale {feed['scale']} != fx.RATE_SCALE {fx.RATE_SCALE}")
    start = date.fromisoformat(feed["start"])
    t0 = time.perf_counter()
    n = 0
    base = start.toordinal()
    with conn.cursor().copy(
            "COPY fx_rates (day, currency, rate_scaled) FROM STDIN") as cp:
        for cur, series in feed["rates"].items():
            for i, rate in enumerate(series):
                cp.write_row((date.fromordinal(base + i), cur, rate))
                n += 1
    conn.commit()
    return n, time.perf_counter() - t0


def build_marts(conn, quiet=False):
    """Apply schema_product.sql and rebuild all three marts. Returns timings."""
    with open(SCHEMA_PATH, encoding="utf-8") as f:
        conn.execute(f.read())
    conn.commit()
    rows, secs = load_fx(conn)
    out = {"fx_rates": {"rows": rows, "seconds": secs}}
    for name, sql in (("mart_revenue_country", BUILD_REVENUE),
                      ("mart_cohort_retention", BUILD_COHORTS),
                      ("mart_funnel", BUILD_FUNNEL)):
        t0 = time.perf_counter()
        cur = conn.execute(sql)
        conn.commit()
        out[name] = {"rows": cur.rowcount, "seconds": time.perf_counter() - t0}
        if not quiet:
            print(f"  {name}: {cur.rowcount} rows in "
                  f"{out[name]['seconds']:.1f}s", flush=True)
    return out


# ------------------------------------------------------------ mart readers
# Everything below reads marts only -- a few thousand rows -- and never the
# 500k charges. That is the whole reason the dashboard answers in
# milliseconds while it filters by country and period.

def _window(country, month_from, month_to, col="month"):
    """Build the shared WHERE fragment + params for the dashboard filters."""
    where, params = [], []
    if country and country != "ALL":
        where.append("country = %s")
        params.append(country)
    # ::date on the parameter, not on the column: casting the column would
    # throw away the index and, worse, silently compare a date to text.
    if month_from:
        where.append(f"{col} >= %s::date")
        params.append(month_from)
    if month_to:
        where.append(f"{col} <= %s::date")
        params.append(month_to)
    return ("WHERE " + " AND ".join(where) if where else ""), params


def revenue(conn, country=None, month_from=None, month_to=None):
    w, p = _window(country, month_from, month_to)
    by_month = conn.execute(f"""
        SELECT month::text,
               SUM(gross_usd_hist)::bigint, SUM(gross_usd_current)::bigint,
               SUM(refund_usd_hist)::bigint, SUM(tx_count)::int
        FROM mart_revenue_country {w}
        GROUP BY 1 ORDER BY 1""", p).fetchall()
    by_country = conn.execute(f"""
        SELECT country, min(currency),
               SUM(gross_usd_hist)::bigint, SUM(gross_usd_current)::bigint,
               SUM(refund_usd_hist)::bigint, SUM(gross_local)::bigint,
               SUM(tx_count)::int
        FROM mart_revenue_country {w}
        GROUP BY 1 ORDER BY 3 DESC""", p).fetchall()
    return {
        "by_month": [{"month": m, "gross_hist": g, "gross_current": c,
                      "refund_hist": r, "tx": t} for m, g, c, r, t in by_month],
        "by_country": [{"country": k, "currency": cur, "gross_hist": g,
                        "gross_current": c, "refund_hist": r,
                        "gross_local": loc, "tx": t}
                       for k, cur, g, c, r, loc, t in by_country],
    }


def cohorts(conn, country=None, month_from=None, month_to=None):
    w, p = _window(country, month_from, month_to, col="cohort_month")
    rows = conn.execute(f"""
        SELECT cohort_month::text, period_index,
               SUM(cohort_size)::int, SUM(eligible)::int,
               SUM(retained)::int, SUM(survived)::int,
               SUM(revenue_usd_cents)::bigint
        FROM mart_cohort_retention {w}
        GROUP BY 1, 2 ORDER BY 1, 2""", p).fetchall()
    out = {}
    for m, per, size, elig, ret, sur, rev in rows:
        c = out.setdefault(m, {"cohort": m, "size": 0, "periods": {}})
        if per == 0:
            c["size"] = size
        c["periods"][per] = {"eligible": elig, "retained": ret,
                             "survived": sur, "revenue": rev}
    return list(out.values())


def funnel(conn, country=None, month_from=None, month_to=None):
    w, p = _window(country, month_from, month_to, col="cohort_month")
    rows = conn.execute(f"""
        SELECT kind, stage, stage_order, SUM(customers)::int
        FROM mart_funnel {w}
        GROUP BY 1, 2, 3 ORDER BY 3""", p).fetchall()
    stages = [{"kind": k, "stage": s, "label": FUNNEL_LABELS.get(s, s),
               "order": o, "customers": n} for k, s, o, n in rows]
    prev = None
    for s in stages:
        if s["kind"] != "funnel":
            continue
        s["drop_pct"] = (round((prev - s["customers"]) / prev * 100, 1)
                         if prev else 0.0)
        prev = s["customers"]
    worst = max((s for s in stages if s["kind"] == "funnel"),
                key=lambda s: s.get("drop_pct", 0.0), default=None)
    return {"stages": stages, "worst_stage": worst["stage"] if worst else None,
            "worst_drop_pct": worst["drop_pct"] if worst else 0.0}


def summary(conn, country=None, month_from=None, month_to=None):
    """The dashboard header: one row of numbers, all of them from marts."""
    w, p = _window(country, month_from, month_to)
    g, c, r, tx, n_ctry = conn.execute(f"""
        SELECT COALESCE(SUM(gross_usd_hist), 0)::bigint,
               COALESCE(SUM(gross_usd_current), 0)::bigint,
               COALESCE(SUM(refund_usd_hist), 0)::bigint,
               COALESCE(SUM(tx_count), 0)::int,
               count(DISTINCT country)::int
        FROM mart_revenue_country {w}""", p).fetchone()
    wc, pc = _window(country, month_from, month_to, col="cohort_month")
    keep = wc + " AND " if wc else "WHERE "
    ret = conn.execute(f"""
        SELECT period_index, SUM(eligible)::int, SUM(retained)::int
        FROM mart_cohort_retention
        {keep} period_index = ANY(%s)
        GROUP BY 1 ORDER BY 1""", pc + [list(KEEP_PERIODS)]).fetchall()
    f = funnel(conn, country, month_from, month_to)
    return {
        "gross_hist": g, "gross_current": c, "refund_hist": r,
        "net_hist": g - r, "tx": tx, "countries": n_ctry,
        "fx_gap_cents": c - g,
        "fx_gap_pct": round((c - g) / g * 100, 2) if g else 0.0,
        "retention": {str(k): {"eligible": e, "retained": rr,
                               "pct": round(rr / e * 100, 1) if e else None}
                      for k, e, rr in ret},
        "worst_stage": f["worst_stage"], "worst_drop_pct": f["worst_drop_pct"],
    }


def meta(conn):
    rows = conn.execute("""
        SELECT DISTINCT country, currency FROM mart_revenue_country
        ORDER BY country""").fetchall()
    lo, hi = conn.execute(
        "SELECT min(month)::text, max(month)::text FROM mart_revenue_country"
    ).fetchone()
    asof, = conn.execute("SELECT max(day)::text FROM fx_rates").fetchone()
    # `ingest.py reset` (and therefore bench.run_all) wipes the charges but not
    # the marts, which would leave the dashboard confidently showing numbers
    # from a database that no longer exists. One count per page load is a
    # cheap price for never doing that silently.
    in_mart, = conn.execute(
        "SELECT COALESCE(SUM(tx_count), 0) FROM mart_revenue_country").fetchone()
    in_db, = conn.execute(
        "SELECT count(*) FROM charges WHERE status = %s", ("succeeded",)
    ).fetchone()
    return {"countries": [{"code": c, "currency": cur} for c, cur in rows],
            "month_min": lo, "month_max": hi, "asof": asof,
            "reporting_currency": fx.REPORTING_CURRENCY.upper(),
            "charges_in_marts": in_mart, "charges_in_db": in_db,
            "stale": in_mart != in_db}


# ----------------------------------------------------------------- serving

POOL_MAX = 8
_pool, _pool_lock = [], Lock()


@contextmanager
def _lease():
    """Tiny read-only connection pool.

    ThreadingHTTPServer starts a thread per request, so a thread-local
    connection is a NEW PostgreSQL connection every time: measured at ~15 ms
    of handshake on top of a ~3 ms query, which is most of the page. Leasing
    from a pool keeps them warm.
    """
    with _pool_lock:
        conn = _pool.pop() if _pool else None
    if conn is None:
        conn = connect(autocommit=True)
    try:
        yield conn
    except Exception:
        conn.close()          # never hand a broken connection back out
        raise
    else:
        with _pool_lock:
            if len(_pool) < POOL_MAX:
                _pool.append(conn)
            else:
                conn.close()


class Handler(BaseHTTPRequestHandler):
    ROUTES = {"/api/summary": summary, "/api/revenue": revenue,
              "/api/cohorts": cohorts, "/api/funnel": funnel}

    def log_message(self, fmt, *args):
        pass    # the bench measures latency; the console stays readable

    def do_GET(self):
        u = urlparse(self.path)
        if u.path in ("/", "/index.html"):
            with open(PAGE_PATH, "rb") as f:
                return self._send(f.read(), "text/html; charset=utf-8")
        if u.path == "/api/meta":
            with _lease() as c:
                return self._json(meta(c))
        fn = self.ROUTES.get(u.path)
        if fn is None:
            return self._send(b"not found", "text/plain", 404)
        qs = parse_qs(u.query)
        t0 = time.perf_counter()
        with _lease() as c:
            data = fn(c, qs.get("country", [None])[0],
                      qs.get("from", [None])[0], qs.get("to", [None])[0])
        ms = (time.perf_counter() - t0) * 1000
        self._json({"data": data, "query_ms": round(ms, 2)})

    def _json(self, obj, code=200):
        self._send(json.dumps(obj).encode("utf-8"),
                   "application/json; charset=utf-8", code)

    def _send(self, body, ctype, code=200):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def serve(port=8000):
    # Warm the pool before the first request. The page fires its four calls at
    # once, so a cold pool means four PostgreSQL handshakes on the very load a
    # visitor judges the dashboard by.
    with _pool_lock:
        _pool.extend(connect(autocommit=True) for _ in range(4))
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"dashboard on http://127.0.0.1:{port}  (Ctrl+C to stop)", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("stopped", flush=True)


# -------------------------------------------------------------------- cli

def show(conn):
    m = meta(conn)
    s = summary(conn)
    print(f"as of {m['asof']}, reporting in {m['reporting_currency']}, "
          f"{len(m['countries'])} countries, months "
          f"{m['month_min']}..{m['month_max']}")
    print(f"gross at the rate of the day: {s['gross_hist'] / 100:,.2f} USD")
    print(f"gross at today's rate:        {s['gross_current'] / 100:,.2f} USD "
          f"({s['fx_gap_pct']:+.2f}%, {s['fx_gap_cents'] / 100:+,.2f} USD)")
    print()
    print("retention (share of eligible customers paying in that period):")
    for k, label in (("1", "2nd month"), ("2", "3rd month"),
                     ("5", "6th month"), ("11", "12th month")):
        r = s["retention"].get(k)
        if r:
            print(f"  {label:10} {r['pct']:5.1f}%  ({r['retained']} of "
                  f"{r['eligible']} eligible)")
    print()
    print("funnel:")
    for st in funnel(conn)["stages"]:
        drop = f"  -{st['drop_pct']:.1f}%" if st.get("drop_pct") else ""
        print(f"  [{st['kind']:6}] {st['label']:32} {st['customers']:7}{drop}")
    print()
    print("revenue by country:")
    print(f"  {'ctry':4} {'cur':4} {'at rate of day':>16} {'at today rate':>16} "
          f"{'gap':>8}")
    for row in revenue(conn)["by_country"]:
        gap = ((row["gross_current"] - row["gross_hist"]) / row["gross_hist"] * 100
               if row["gross_hist"] else 0.0)
        print(f"  {row['country']:4} {row['currency']:4} "
              f"{row['gross_hist'] / 100:16,.0f} "
              f"{row['gross_current'] / 100:16,.0f} {gap:+7.2f}%")


def main():
    ap = argparse.ArgumentParser(description="revscope product layer")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("build", help="load FX feed and rebuild the three marts")
    sub.add_parser("show", help="print the marts to stdout")
    sv = sub.add_parser("serve", help="run the dashboard")
    sv.add_argument("--port", type=int, default=8000)
    args = ap.parse_args()

    if args.cmd == "serve":
        return serve(args.port)
    with connect() as conn:
        if args.cmd == "build":
            t0 = time.perf_counter()
            print("building product marts...", flush=True)
            out = build_marts(conn)
            print(f"done in {time.perf_counter() - t0:.1f}s "
                  f"({out['fx_rates']['rows']} fx rows)", flush=True)
        else:
            show(conn)


if __name__ == "__main__":
    main()
