# revscope

An architectural proof, not a product: a revenue-analytics core at the scale of
a real Stripe account (500k charges / 100k customers / 5 years of history),
built on nothing but Postgres, where every architectural claim is measured by a
benchmark and written down as a number. Events flow through one idempotent
layer into normalized tables and incrementally-maintained rollups; reports read
rollups only; a checkpointed backfill worker survives hard kills; and a
60-month raw-vs-rollup reconciliation must come out at exactly 0 cents drift.

On top of that core sits a product layer: cohort retention, a payment funnel
and multi-currency revenue, all built from the same transactions, all served
from marts, and all measured the same way. Its headline number is what a
report loses by converting foreign revenue at today's rate instead of the rate
of each transaction day.

## Results

Run of 2026-08-20 (PostgreSQL 16.14 in Docker, Python 3.12, psycopg 3.3.4;
dataset: 648,191 events = 100k customers, 25k subscriptions, 500k charges in
7 currencies, 23,191 refunds across 60 months, plus a 1,827-day rate feed):

| # | bench | measured | verdict |
|---|-------|----------|---------|
| 1 | full_backfill | 131s, 4953 ev/s, all counts exact | PASS |
| 2 | first_report | p50 15.3 ms / p95 18.6 ms, all numbers exact | PASS |
| 3 | progressive | correct 30d/90d report 16.5s after empty db | PASS |
| 4 | duplicate_storm | 50k dups -> 0 extra rows, checksums identical | PASS |
| 5 | kill_resume | killed at 40%, final drift 0 cents | PASS |
| 6 | segmentation | p50 125.5 ms over 60460 customers | PASS |
| 7 | reconciliation | 60 months, max drift 0 cents | PASS |

Product layer, same dataset:

| # | bench | measured | verdict |
|---|-------|----------|---------|
| 8 | cohort_retention | 240 cells vs an independent implementation, 0 mismatches | PASS |
| 9 | funnel | 0 mismatches, worst drop 38.0% at the second payment | PASS |
| 10 | fx_reporting | -0.91% (-$454,312.40) if converted at today's rate | PASS |
| 11 | product_dashboard | p50 15.4 ms over HTTP, 10.1 ms in-process | PASS |

Headline numbers from the core: the dashboard over $49.7M of lifetime gross
answers in **15.3 ms** (p50); **50,000** duplicate deliveries produce
**0 extra rows** with bit-identical rollup checksums; the worker killed at
**40.1%** finishes to **0 cents** of drift against generator ground truth;
raw-vs-rollup reconciliation across **60 months** is exact to the cent.

From the product layer: converting foreign revenue at today's rate instead of
the rate of each transaction day moves the five-year total by
**-$454,312.40** (**-0.91%**), and by **-6.99%** for Australia on its own --
against a rounding residual of **$11.85** for the conversion itself. Month-12
retention is **34.2%** of the customers who actually had a twelfth month,
where dividing by the whole cohort reports **18.3%** from the same numerator.
The biggest hole in the funnel is the second payment: **38.0%** of customers
who pay once never make it.

Full details per bench: [bench/out/results.md](bench/out/results.md) and
[bench/out/product_results.md](bench/out/product_results.md). Numbers in this
README are pasted from real runs of those suites, never written by hand.

## Quickstart

```
docker compose up -d
pip install "psycopg[binary]"
python gen.py
python ingest.py backfill        # optional: bench resets and re-runs everything itself
python -m bench.run_all

python product.py build          # FX feed + the three product marts
python -m bench.run_product      # product benches 8-11
python -m tests.test_product     # cohort/FX correctness on a hand-written fixture
python product.py serve          # dashboard on http://127.0.0.1:8000
```

`bench.run_all` resets the database, so the marts have to be rebuilt with
`python product.py build` after it. `bench.run_product` does that itself.

Requires Python 3.11+ and Docker. Postgres runs in a container on port 5433
(so a local 5432 stays untouched). The dataset generator is fully
deterministic (`random.Random(42)`): same seed, same 648k events, same ground
truth, byte for byte.

## Data flow

```
 events.ndjson (648k events, newest first, "as from Stripe")
      |
      |  backfill worker: pages of 100,        webhook path:
      |  commit + checkpoint per page          same apply_events()
      v                                        v
 +---------------------------------------------------------+
 |             ONE IDEMPOTENT LAYER (apply_events)          |
 |  INSERT INTO raw_events ON CONFLICT (stripe_id) DO       |
 |  NOTHING -- only events that actually land here flow on, |
 |  in the same transaction                                 |
 +---------------------------------------------------------+
      |                     |                      |
      v                     v                      v
 normalized tables    rollup_daily            customer_stats
 customers/subs/      (day,product,status)    (one row per customer:
 charges/refunds      += O(1) per event       ltv, recency, counts)
      |                     |                      |
      |                     v                      v
      |               dashboard queries       RFM segmentation
      |               (MRR, 30d revenue,      (NTILE window fns,
      |               refund rate,            milliseconds)
      |               recoverable $)
      v
 metadata_review  <-  any metadata that disagrees with the
 (quarantine)         canonical plan derived from price_id
```

The product layer hangs off the same normalized tables:

```
 charges / refunds                     data/fx_rates.json
 (currency + amount_local:             (one rate per currency per UTC day,
  what was actually presented)          weekends carry Friday's close)
       |                                        |
       +--------------------+-------------------+
                            |  join on the TRANSACTION day
                            v  -- never on the day the report runs
        +--------------------------------------------------+
        |   product.py build -- one pass per mart, 5.2s     |
        +--------------------------------------------------+
             |                  |                     |
             v                  v                     v
   mart_cohort_retention   mart_funnel        mart_revenue_country
   cohort x country x      nested stages +    month x country:
   30-day period           churn split        local, USD at the rate
   (7,800 rows)            (4,640 rows)       of the day, USD at
                                              today's rate (600 rows)
             |                  |                     |
             +------------------+---------------------+
                                v
                         dashboard.html
                 4 queries over 13,040 mart rows,
                 filtered by country and period
```

## What each bench proves

1. **full_backfill** -- a fresh database ingests the full 5-year stream
   through the idempotent layer; every row count matches generator ground
   truth exactly.
2. **first_report** -- the dashboard (MRR, last-30d revenue, refund rate,
   recoverable failed $) reads pre-aggregated rollups only, in milliseconds
   on the full dataset.
3. **progressive** -- the stream is newest-first, so loading just the last 90
   days yields a window-correct report seconds after an empty database:
   report first, history later.
4. **duplicate_storm** -- 50,000 re-delivered events change nothing: 0 extra
   rows, rollup and customer_stats checksums bit-identical. Idempotency lives
   in a UNIQUE constraint, so it does not weaken with volume.
5. **kill_resume** -- the backfill worker is hard-killed (TerminateProcess)
   mid-run; after restart it resumes from its checkpoint and finishes with
   monthly totals matching ground truth to the cent. Each page commits
   atomically together with its checkpoint, so a kill loses at most one
   uncommitted page, which is then re-read and deduplicated.
6. **segmentation** -- RFM over ~100k customers uses NTILE window functions
   on the one-row-per-customer table, never the 500k raw charges.
7. **reconciliation** -- monthly SUM over normalized charges/refunds vs
   monthly SUM over rollup_daily, 60 months: max drift must be exactly 0
   cents. Rollups are incremented O(1) per event and never rebuilt from raw.

Product layer (`bench/run_product.py`):

8. **cohort_retention** -- the SQL mart is compared cell for cell against an
   independent Python implementation in `gen.py` that never touches the
   database: 240 cohort/period cells, four values each, 0 mismatches.
   Eligibility is per customer, so a cohort that is three months old is not
   counted as having churned out of month twelve.
9. **funnel** -- five nested stages plus a churn split, built from
   transactions only. Every stage is a strict subset of the one above it, all
   eight counts match the Python implementation, and the biggest drop-off is
   found rather than asserted.
10. **fx_reporting** -- every charge converted per transaction at the rate of
    its own day, then again at today's rate, and the two totals put side by
    side. Also asserts that the rate feed covers every transaction day (a
    missing rate would make an INNER JOIN quietly drop revenue) and that
    converting back at the rate of the day reproduces the ledger the first
    seven benches already published.
11. **product_dashboard** -- the whole page, including over real HTTP, served
    from 13,040 mart rows instead of 500,000 charges, with the country and
    period filters proven to actually cut the data.

## Design notes

- **All money is integer cents.** Ground truth, rollups, assertions -- no
  floats anywhere near money.
- **Canonical fields never come from metadata.** The plan/product is always
  derived from `price_id`. ~30% of generated charges carry deliberately dirty
  metadata (`plan` / `plan_name` / `planName` / absent / `"undefined"` /
  truncated values); anything that disagrees with the structural truth lands
  in `metadata_review` instead of silently polluting segments.
- **raw_events is append-only** -- a trigger raises on UPDATE/DELETE.
- **Refund events are denormalized** (carry customer and price): the stream
  is newest-first, so a refund is processed before its parent charge. In
  production you would expand the charge or buffer; here denormalization
  keeps ingestion single-pass and O(1).
- **No Redis/Kafka on purpose.** The proof is that a single Postgres carries
  all five invariants at this scale. In production the hot payload path would
  move to Redis and the delivery path to a queue -- deliberately out of scope
  here.

Product layer:

- **The presented amount is stored, not derived.** `charges.amount_local` and
  `charges.currency` are what the customer was actually charged; `amount_cents`
  stays the USD list price. If the mart derived the local amount from the USD
  one, converting it back would be circular and would prove nothing.
- **Rates are integers, scaled by 1e8.** The same conversion runs twice --
  in Python for the generator's ground truth and in SQL for the marts -- and
  two float pipelines disagree in the last cent. Integer arithmetic gives
  bit-identical results on both sides, which is what makes a 0-mismatch
  assertion meaningful. `tests/test_product.py` pins that equality directly.
- **Retention is counted in 30-day periods, not calendar months.** Billing is
  a 30-day cycle: a customer paying on Jan 2 and Feb 1 lands in one calendar
  month twice and in the next one never, which reads as churn that never
  happened.
- **Eligibility is per customer.** A customer counts in period *k* only once
  period *k* has fully elapsed for them. Dividing by the whole cohort instead
  reports month-12 retention of 18.3% where the real number is 34.2% -- same
  numerator, wrong denominator, and it is measured in bench 8 rather than
  argued about.
- **Two retention numbers, on purpose.** `retained` is "paid inside this
  period"; `survived` is "paid in this period or any later one". The gap
  between them (19.3 points at month 12) is customers a single failed charge
  would have written off.
- **The funnel is nested by construction.** Each stage is built as a subset of
  the previous one, so drop-off percentages mean something. The churn split
  (left on a decline / just stopped / never converted) is deliberately *not* a
  funnel stage: a customer who paid twice, the second time yesterday, is
  neither regular nor churned.
- **Cut-offs are duplicated, not imported.** The 30-day period, the 45-day
  active grace and the 90-day funnel maturity exist in both `gen.py` and
  `product.py`. If one side changes, the benches fail instead of quietly
  reporting a different metric.
- **No dbt, no Airflow, no BI tool.** Three marts rebuilt by one Python file
  in 5.2 seconds do not need an orchestrator, and a scheduler that runs one
  command is a dependency, an upgrade path and a second place for the schema
  to live. The moment there is a dependency graph worth resolving or a
  backfill worth scheduling, that changes -- there is not one here.

## Honest limits

Synthetic data, deterministic generator, mocked Stripe (no live API, no real
rate limits), single-node Postgres, one run on one machine. This proves the
invariants and the aggregation architecture -- it does not prove live Stripe
API behavior or production ops. Also: the generator samples the subscription
billing schedule down to a cap to keep a one-off share within exactly 500k
charges; segmentation "milliseconds" means low hundreds of milliseconds for
3x NTILE over ~60k paying customers (see the measured number, not the vibe).

The product layer has its own, and they matter more, because product metrics
are easier to make look authoritative than they are to make true:

- **There are no product events.** No signups, sessions, feature usage or
  trials exist in this dataset, and none were invented. Everything here is
  computed from payment transactions, so "funnel" means the payment funnel
  (tried to pay -> paid -> paid again -> regular -> still paying), not
  signup -> activation -> paid. A real product funnel needs event data this
  project does not have.
- **The FX feed is synthetic.** Rates are a seeded random walk anchored at
  plausible start and end levels for each pair over 2021-2026, with weekends
  carrying Friday's close. It is not a real rate history, so the -0.91%
  all-time gap is a property of this path and this revenue distribution, not
  a fact about the world. The shape of the finding generalizes; the digit
  does not. The per-year numbers (-4.34% in 2021 down to +0.41% in 2026) show
  exactly why the all-time figure is the least interesting of them.
- **Pricing model is Stripe-style automatic conversion.** The local amount is
  the USD list price converted at the rate of the charge day, which is what
  Stripe does when you present in the customer's currency without a local
  price book. A company with fixed local prices (EUR 19 forever) would show a
  *larger* gap, not a smaller one, because the local amount would not move
  with the rate at all.
- **Refunds settle at the refund day's rate.** Real refunds return the
  originally presented amount; here the refund is converted on its own day.
  The difference is small at this scale but it is a modelling choice, not a
  fact.
- **Retention holes are partly an artifact.** The generator samples the
  billing schedule down to a cap, so some scheduled invoices simply do not
  exist, on top of the 8% of charges that fail. That inflates the gap between
  `retained` and `survived`. It is reported as two numbers rather than one for
  exactly that reason.
- **The marts are rebuilt, not maintained.** Unlike `rollup_daily`, which is
  incremented O(1) per event, the three product marts are a full rebuild:
  5.2 seconds over 500k charges. That is cheap enough that incremental
  maintenance would be complexity with no measured benefit. At 50M charges it
  would not be, and this is the first thing that would have to change.
- **Cohort and funnel definitions are choices.** 30-day periods, a 45-day
  active grace, a 90-day maturity cut-off, cohort by month of first *payment*
  and funnel by month of first *attempt*. All defensible, none inevitable;
  every one of them is written down in `gen.py` and `product.py` next to the
  reason for it.
