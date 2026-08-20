"""revscope product tests: python -m tests.test_product

Two things deserve a test rather than a bench. First, that a customer with a
payment history you can work out on paper lands in the cohort and the periods
you worked out. Second, that converting at the rate of the transaction day and
converting at today's rate really do produce different money -- if they ever
came out equal, every claim in the FX bench would be vacuous.

The fixture is nine hand-written customers in their own schema, and the tests
run the REAL mart SQL from product.py against it. A reimplementation of the
query in the test would only prove that the reimplementation works.

search_path is pinned to the fixture schema alone, with no public in it, and
every DROP is stripped out of the DDL before it is applied: a product query
that names a table the fixture does not have must fail loudly, never quietly
read (or drop) the 500k-charge dataset in the schema next door.

No pytest: this is a plain script, like everything else here.
stdout is ASCII-only (Windows cp1251 console).
"""

import os
import sys
import traceback
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import fx           # noqa: E402
import product      # noqa: E402

SCHEMA = "revscope_test"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

UTC = timezone.utc
D0 = datetime(2024, 1, 1, tzinfo=UTC)          # day 0 of the fixture cohort
ASOF = date(2026, 8, 1)                        # last day of the fixture feed
KZT_RATE = 450 * fx.RATE_SCALE                 # every day but the last
KZT_TODAY = 540 * fx.RATE_SCALE                # the "current" rate


def day(n):
    return D0 + timedelta(days=n)


# customer -> country. kz1 is the only one that pays in something other than
# USD, so the cohort numbers below stay pure arithmetic.
CUSTOMERS = {"c1": "US", "c2": "US", "c3": "US", "c4": "US", "c5": "US",
             "c6": "US", "c7": "US", "c8": "US", "kz1": "KZ"}

# (charge_id, customer, when, usd_cents, status)
# c1  pays in periods 0,1,2,6,11        -> the full retention curve
# c2  pays in periods 0 and 2           -> a hole in period 1
# c3  pays once                         -> churns immediately
# c4  pays twice inside period 0        -> a period counts once, revenue adds
# c5  first pays 17 days before as-of   -> too young to be observed at all
# c6  one declined charge, never paid   -> never converted
# c7  three payments, last one recent   -> still paying
# c8  three payments, then a decline    -> left involuntarily
CHARGES = [
    ("ch_c1_0", "c1", day(0), 10_000, "succeeded"),
    ("ch_c1_1", "c1", day(30), 10_000, "succeeded"),
    ("ch_c1_2", "c1", day(60), 10_000, "succeeded"),
    ("ch_c1_6", "c1", day(180), 10_000, "succeeded"),
    ("ch_c1_11", "c1", day(330), 10_000, "succeeded"),
    ("ch_c2_0", "c2", day(0), 10_000, "succeeded"),
    ("ch_c2_2", "c2", day(65), 10_000, "succeeded"),
    ("ch_c3_0", "c3", day(0), 10_000, "succeeded"),
    ("ch_c4_0", "c4", day(0), 10_000, "succeeded"),
    ("ch_c4_0b", "c4", day(29), 10_000, "succeeded"),
    ("ch_c4_1", "c4", day(59), 10_000, "succeeded"),
    ("ch_c5_0", "c5", datetime(2026, 7, 15, tzinfo=UTC), 10_000, "succeeded"),
    ("ch_c6_0", "c6", day(60), 10_000, "failed"),
    ("ch_c7_0", "c7", datetime(2025, 1, 1, tzinfo=UTC), 10_000, "succeeded"),
    ("ch_c7_1", "c7", datetime(2025, 1, 31, tzinfo=UTC), 10_000, "succeeded"),
    ("ch_c7_2", "c7", datetime(2026, 7, 20, tzinfo=UTC), 10_000, "succeeded"),
    ("ch_c8_0", "c8", datetime(2024, 5, 1, tzinfo=UTC), 10_000, "succeeded"),
    ("ch_c8_1", "c8", datetime(2024, 5, 31, tzinfo=UTC), 10_000, "succeeded"),
    ("ch_c8_2", "c8", datetime(2024, 6, 30, tzinfo=UTC), 10_000, "succeeded"),
    ("ch_c8_x", "c8", datetime(2024, 12, 1, tzinfo=UTC), 10_000, "failed"),
    ("ch_kz_0", "kz1", day(0), 10_000, "succeeded"),
]
REFUNDS = [("re_0", "ch_c3_0", "c3", datetime(2024, 2, 1, tzinfo=UTC), 3_000)]


def _apply_without_drops(conn, path):
    """Apply a project .sql file with every DROP removed.

    The DDL is taken from the real schema files so the fixture cannot drift
    away from production, but an unqualified DROP resolves through
    search_path, and one missing table in the fixture schema would send it
    straight at the real one. Stripping them removes the possibility.
    """
    with open(path, encoding="utf-8") as f:
        body = "\n".join(ln for ln in f.read().splitlines()
                         if not ln.strip().upper().startswith("DROP "))
    conn.execute(body)


def setup(conn):
    conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    conn.execute(f"CREATE SCHEMA {SCHEMA}")
    conn.execute(f"SET search_path TO {SCHEMA}")
    got = conn.execute("SELECT current_schemas(false)").fetchone()[0]
    assert got == [SCHEMA], f"search_path is {got}, refusing to touch it"

    _apply_without_drops(conn, os.path.join(ROOT, "schema.sql"))
    _apply_without_drops(conn, os.path.join(ROOT, "schema_product.sql"))

    conn.execute(
        "INSERT INTO customers (id, email, country, created) "
        "SELECT k, k || '@example.com', v, %s FROM "
        "jsonb_each_text(%s::jsonb) AS t(k, v)",
        (D0 - timedelta(days=1), __import__("json").dumps(CUSTOMERS)))

    n_days = (ASOF - D0.date()).days + 1
    rows = []
    for i in range(n_days):
        d = D0.date() + timedelta(days=i)
        rows.append((d, "usd", fx.RATE_SCALE))
        rows.append((d, "kzt", KZT_TODAY if d == ASOF else KZT_RATE))
    conn.cursor().executemany(
        "INSERT INTO fx_rates (day, currency, rate_scaled) VALUES (%s, %s, %s)",
        rows)

    for cid, cust, when, usd, status in CHARGES:
        cur = "kzt" if CUSTOMERS[cust] == "KZ" else "usd"
        rate = KZT_RATE if cur == "kzt" else fx.RATE_SCALE
        conn.execute(
            "INSERT INTO charges (id, customer_id, subscription_id, price_id, "
            "product, amount_cents, currency, amount_local, status, "
            "decline_code, decline_class, created) VALUES "
            "(%s, %s, NULL, 'price_test_10000', 'test_10000', %s, %s, %s, %s, "
            "%s, %s, %s)",
            (cid, cust, usd, cur, fx.to_local(usd, rate), status,
             None if status == "succeeded" else "do_not_honor",
             None if status == "succeeded" else "terminal", when))
    for rid, ch, cust, when, usd in REFUNDS:
        conn.execute(
            "INSERT INTO refunds (id, charge_id, customer_id, product, "
            "amount_cents, currency, amount_local, partial, created) "
            "VALUES (%s, %s, %s, 'test_10000', %s, 'usd', %s, TRUE, %s)",
            (rid, ch, cust, usd, usd, when))

    for sql in (product.BUILD_REVENUE, product.BUILD_COHORTS,
                product.BUILD_FUNNEL):
        conn.execute(sql)
    conn.commit()


def teardown(conn):
    conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    conn.commit()


# ------------------------------------------------------------------ tests

def test_known_customer_lands_in_the_known_cohort(conn):
    """c1..c4 all first paid on 2024-01-01, so the cohort is 2024-01 and its
    retention curve is the one written out in the fixture comments."""
    rows = conn.execute("""
        SELECT period_index, cohort_size, eligible, retained, survived,
               revenue_usd_cents
        FROM mart_cohort_retention
        WHERE cohort_month = DATE '2024-01-01' AND country = 'US'
        ORDER BY period_index""").fetchall()
    got = {r[0]: tuple(r[1:]) for r in rows}
    # period: (size, eligible, retained, survived, revenue_cents)
    want = {
        0: (4, 4, 4, 4, 50_000),   # c4 pays twice in period 0 -> 5 payments
        1: (4, 4, 2, 3, 20_000),   # c1 and c4 pay; c2 pays later, so survives
        2: (4, 4, 2, 2, 20_000),   # c1 and c2
        3: (4, 4, 0, 1, 0),        # only c1 is still alive past here
        6: (4, 4, 1, 1, 10_000),
        11: (4, 4, 1, 1, 10_000),
        12: (4, 4, 0, 0, 0),
    }
    for per, exp in want.items():
        assert got[per] == exp, f"period {per}: got {got[per]}, want {exp}"
    assert len(got) == 13, f"expected periods 0..12, got {sorted(got)}"


def test_young_cohort_is_not_counted_as_churned(conn):
    """c5 first paid 17 days before the as-of day. Not one period has elapsed
    for them, so they are in the cohort but eligible nowhere -- the difference
    between 'we lost them' and 'we have not asked them yet'."""
    rows = conn.execute("""
        SELECT period_index, cohort_size, eligible, retained
        FROM mart_cohort_retention
        WHERE cohort_month = DATE '2026-07-01' AND country = 'US'
        ORDER BY period_index""").fetchall()
    assert rows, "the young cohort disappeared from the mart entirely"
    for per, size, eligible, retained in rows:
        assert size == 1, f"period {per}: cohort size {size}, want 1"
        assert eligible == 0, f"period {per}: eligible {eligible}, want 0"
        assert retained == 0, f"period {per}: retained {retained}, want 0"


def test_funnel_stages_are_the_hand_counted_ones(conn):
    rows = conn.execute(
        "SELECT stage, SUM(customers)::int FROM mart_funnel GROUP BY 1"
    ).fetchall()
    got = dict(rows)
    want = {
        "attempted": 8,          # everyone but c5, who is younger than 90 days
        "paid_once": 7,          # c6 only ever got declined
        "paid_twice": 5,         # c1, c2, c4, c7, c8
        "regular_3plus": 4,      # c1, c4, c7, c8
        "still_paying": 1,       # c7 paid 12 days before the as-of day
        "never_converted": 1,    # c6
        "churn_involuntary": 1,  # c8: last attempt was a decline
        "churn_voluntary": 5,    # c1, c2, c3, c4, kz1: they simply stopped
    }
    assert got == want, f"got {got}, want {want}"


def test_funnel_stages_are_nested(conn):
    order = ["attempted", "paid_once", "paid_twice", "regular_3plus",
             "still_paying"]
    rows = dict(conn.execute(
        "SELECT stage, SUM(customers)::int FROM mart_funnel "
        "WHERE kind = 'funnel' GROUP BY 1").fetchall())
    for a, b in zip(order, order[1:]):
        assert rows[a] >= rows[b], f"{b} ({rows[b]}) is wider than {a} ({rows[a]})"


def test_rate_of_the_day_is_not_the_rate_of_today(conn):
    """The one number this whole module exists for. kz1 was charged 45,000.00
    KZT on 2024-01-01 at 450 KZT/USD. That is 100.00 USD on the day it
    happened and 83.33 USD if the report converts it at today's 540."""
    local, hist, now = conn.execute("""
        SELECT gross_local, gross_usd_hist, gross_usd_current
        FROM mart_revenue_country
        WHERE country = 'KZ' AND month = DATE '2024-01-01'""").fetchone()
    assert local == 4_500_000, f"presented {local} tiyin, want 4500000"
    assert hist == 10_000, f"at the rate of the day {hist} cents, want 10000"
    assert now == 8_333, f"at today's rate {now} cents, want 8333"
    assert hist != now, "the two conversions cannot be allowed to be equal"
    assert hist - now == 1_667, f"gap {hist - now} cents, want 1667 (-16.67%)"


def test_usd_country_is_untouched_by_the_conversion(conn):
    """A control: the same query on a USD country must return the presented
    amount unchanged, both ways. A conversion that quietly moves USD would
    make every other assertion here meaningless."""
    hist, now = conn.execute("""
        SELECT SUM(gross_usd_hist)::bigint, SUM(gross_usd_current)::bigint
        FROM mart_revenue_country WHERE country = 'US'""").fetchone()
    paid = sum(c[3] for c in CHARGES if c[4] == "succeeded"
               and CUSTOMERS[c[1]] == "US")
    assert hist == paid == now, f"US: hist {hist}, now {now}, charged {paid}"


def test_sql_and_python_convert_to_the_same_integer(conn):
    """The marts convert in SQL, the generator's ground truth converts in
    Python. If those two ever round differently, every 0-cent reconciliation
    in this project becomes a coincidence."""
    cases = [(amount, rate)
             for amount in (1, 2, 899, 900, 1_900, 12_900, 29_900, 1_234_567)
             for rate in (fx.RATE_SCALE, 84_300_000, 71_900_000,
                          125_300_000, 384_000_000, 45_000_000_000,
                          KZT_TODAY, 3)]
    rows = conn.execute("""
        SELECT c.amount, c.rate,
               ((c.amount * %s + c.rate / 2) / c.rate)::bigint
        FROM unnest(%s::bigint[], %s::bigint[]) AS c(amount, rate)""",
        (fx.RATE_SCALE, [a for a, _ in cases], [r for _, r in cases])
    ).fetchall()
    bad = [(a, r, sql, fx.to_usd(a, r))
           for a, r, sql in rows if sql != fx.to_usd(a, r)]
    assert not bad, f"{len(bad)} of {len(cases)} disagree, first: {bad[0]}"


def test_presentment_round_trip_stays_within_one_cent(conn):
    """Converting a list price into a local amount and back cannot always be
    exact -- 19.00 USD at 0.8430 is 16.02 EUR, and 16.02 EUR is 19.00 or
    18.99 back. The rule is that it never drifts further than that."""
    worst = 0
    rates = fx.build_rates(fx.n_days())
    for cur, series in rates.items():
        for i in (0, 500, 1000, len(series) - 1):
            for amount in (900, 1_900, 6_900, 12_900, 29_900):
                back = fx.to_usd(fx.to_local(amount, series[i]), series[i])
                worst = max(worst, abs(back - amount))
    assert worst <= 1, f"round trip drifts by {worst} cents"


# ------------------------------------------------------------------ runner

def main():
    conn = product.connect()
    failed = []
    try:
        print(f"setting up fixture in schema {SCHEMA}...", flush=True)
        setup(conn)
        tests = [(n, f) for n, f in sorted(globals().items())
                 if n.startswith("test_") and callable(f)]
        for name, fn in tests:
            try:
                fn(conn)
                print(f"  PASS  {name}", flush=True)
            except Exception:
                failed.append(name)
                print(f"  FAIL  {name}", flush=True)
                print("        " + traceback.format_exc().strip().splitlines()[-1],
                      flush=True)
                conn.rollback()
                conn.execute(f"SET search_path TO {SCHEMA}")
        print(f"{len(tests) - len(failed)}/{len(tests)} passed", flush=True)
    finally:
        teardown(conn)
        conn.close()
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
