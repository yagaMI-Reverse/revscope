# revscope

An architectural proof, not a product: a revenue-analytics core at the scale of
a real Stripe account (500k charges / 100k customers / 5 years of history),
built on nothing but Postgres, where every architectural claim is measured by a
benchmark and written down as a number. Events flow through one idempotent
layer into normalized tables and incrementally-maintained rollups; reports read
rollups only; a checkpointed backfill worker survives hard kills; and a
60-month raw-vs-rollup reconciliation must come out at exactly 0 cents drift.

## Results

Run of 2026-08-10 (PostgreSQL 16.14 in Docker, Python 3.12, psycopg 3.3.4;
dataset: 648,191 events = 100k customers, 25k subscriptions, 500k charges,
23,191 refunds across 60 months):

| # | bench | measured | verdict |
|---|-------|----------|---------|
| 1 | full_backfill | 138s, 4683 ev/s, all counts exact | PASS |
| 2 | first_report | p50 13.6 ms / p95 15.2 ms, all numbers exact | PASS |
| 3 | progressive | correct 30d/90d report 17.3s after empty db | PASS |
| 4 | duplicate_storm | 50k dups -> 0 extra rows, checksums identical | PASS |
| 5 | kill_resume | killed at 40%, final drift 0 cents | PASS |
| 6 | segmentation | p50 119.0 ms over 60460 customers | PASS |
| 7 | reconciliation | 60 months, max drift 0 cents | PASS |

Headline numbers from that run: the dashboard over $49.7M of lifetime gross
answers in **13.6 ms** (p50); **50,000** duplicate deliveries produce
**0 extra rows** with bit-identical rollup checksums; the worker killed at
**40.1%** finishes to **0 cents** of drift against generator ground truth;
raw-vs-rollup reconciliation across **60 months** is exact to the cent.

Full details per bench: [bench/out/results.md](bench/out/results.md).
Numbers in this README are pasted from a real run of that bench suite, never
written by hand.

## Quickstart

```
docker compose up -d
pip install "psycopg[binary]"
python gen.py
python ingest.py backfill        # optional: bench resets and re-runs everything itself
python -m bench.run_all
```

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

## Honest limits

Synthetic data, deterministic generator, mocked Stripe (no live API, no real
rate limits), single-node Postgres, one run on one machine. This proves the
invariants and the aggregation architecture -- it does not prove live Stripe
API behavior or production ops. Also: the generator samples the subscription
billing schedule down to a cap to keep a one-off share within exactly 500k
charges; segmentation "milliseconds" means low hundreds of milliseconds for
3x NTILE over ~60k paying customers (see the measured number, not the vibe).
