# revscope bench results

- run at: 2026-08-20 19:29 UTC
- postgres: PostgreSQL 16.14
- python: 3.12.7, psycopg 3.3.4
- dataset: 648191 events: 100000 customers, 25000 subscriptions, 500000 charges (430000 invoices + 70000 one-off), 23191 refunds, 60 months

| # | bench | measured | verdict |
|---|-------|----------|---------|
| 1 | full_backfill | 131s, 4953 ev/s, all counts exact | PASS |
| 2 | first_report | p50 15.3 ms / p95 18.6 ms, all numbers exact | PASS |
| 3 | progressive | correct 30d/90d report 16.5s after empty db | PASS |
| 4 | duplicate_storm | 50k dups -> 0 extra rows, checksums identical | PASS |
| 5 | kill_resume | killed at 40%, final drift 0 cents | PASS |
| 6 | segmentation | p50 125.5 ms over 60460 customers | PASS |
| 7 | reconciliation | 60 months, max drift 0 cents | PASS |

## 1. full_backfill

**claim:** A fresh database ingests the full 5-year stream through the idempotent layer in minutes, with every row count matching ground truth exactly.

**measured:**

- wall time: 130.9s for 648191 events (4953 events/sec, pages of 100, commit + checkpoint per page)
- raw_events: 648191 (expected 648191) -> ok
- customers: 100000 (expected 100000) -> ok
- subscriptions: 25000 (expected 25000) -> ok
- charges: 500000 (expected 500000) -> ok
- charges succeeded: 459691 (expected 459691) -> ok
- charges failed: 40309 (expected 40309) -> ok
- refunds: 23191 (expected 23191) -> ok
- metadata_review: 128977 (expected 128977) -> ok
- top-10 customers by LTV match ground truth: True
- rollup_daily rows: 85452, customer_stats rows: 62676

**verdict:** PASS

## 2. first_report

**claim:** The first revenue report (MRR, last-30d revenue, refund rate, recoverable failed $) is served from pre-aggregated rollups in milliseconds on the full dataset.

**measured:**

- dashboard = 4 queries (MRR, last-30d revenue, all-time refund rate, recoverable failed $), rollups + subscriptions only
- latency over 20 runs: p50 15.3 ms, p95 18.6 ms on the full 500k-charge dataset
- refund rate: 4.20%
- MRR (active subscriptions): 1,851,495.00 USD (185149500 cents) == ground truth -> ok
- last-30d gross: 2,621,014.00 USD (262101400 cents) == ground truth -> ok
- last-30d refunded: 161,913.42 USD (16191342 cents) == ground truth -> ok
- last-30d net: 2,459,100.58 USD (245910058 cents) == ground truth -> ok
- last-30d charge count: 23598 charges == ground truth -> ok
- all-time gross: 49,694,275.00 USD (4969427500 cents) == ground truth -> ok
- all-time refunded: 2,086,317.37 USD (208631737 cents) == ground truth -> ok
- recoverable failed $: 2,632,583.00 USD (263258300 cents) == ground truth -> ok

**verdict:** PASS

## 3. progressive

**claim:** Backfilling newest-first makes reports usable long before the history finishes: loading only the last 90 days yields window-correct dashboard numbers in seconds.

**measured:**

- loaded 82603 events (everything created in the last 90 days; the stream is newest-first, so this is a file prefix)
- time-to-first-correct-report: 16.5s from an empty database (vs full backfill of the whole history)
- last-30d gross/refunded/net: 262101400/16191342/245910058 cents == ground truth -> ok
- last-90d gross/refunded/net: 695549500/33668214/661881286 cents == ground truth -> ok

**verdict:** PASS

## 4. duplicate_storm

**claim:** Re-delivering 50,000 duplicate events changes nothing: exactly 0 extra rows, rollups bit-identical. UNIQUE-constraint idempotency does not weaken with volume.

**measured:**

- replayed 50000 random already-processed events through the same apply path in 1.5s (33577 events/sec)
- newly inserted rows: 0 (expected 0)
- extra rows per table: {'raw_events': 0, 'customers': 0, 'subscriptions': 0, 'charges': 0, 'refunds': 0, 'metadata_review': 0, 'rollup_daily': 0, 'customer_stats': 0} (all expected 0)
- rollup_daily md5 before == after: True (4f59286cfdafee086324c55c1c3ccfdb)
- customer_stats md5 before == after: True (b40bbe9793ead30f78faaa3e3f83b3d8)

**verdict:** PASS

## 5. kill_resume

**claim:** The checkpointed backfill worker survives a hard mid-run kill: after restart it finishes to the exact cent -- no lost and no double-counted money, drift 0.

**measured:**

- killed worker (TerminateProcess) at checkpoint 260700 / 648191 events (40.2%)
- restart resumed from checkpoint 260700 and finished the remaining 387491 events in 82.6s
- raw_events: 648191 (expected 648191), charges: 500000, refunds: 23191 -> no loss, no duplicates
- monthly gross/refund totals vs ground truth over 60 months: max drift 0 cents

**verdict:** PASS

## 6. segmentation

**claim:** RFM segmentation of ~100k customers runs in milliseconds, because it reads the one-row-per-customer stats table, never the 500k raw charges.

**measured:**

- RFM = NTILE(5) window functions over recency/frequency/monetary on customer_stats (one row per customer), 60460 paying customers
- latency over 20 runs: p50 125.5 ms, p95 137.7 ms
- segment sizes: hibernating 19636, champions 14716, steady 12092, promising 4974, at_risk_high_value 4548, recent 4494
- segmented customers total: 60460 == paying customers 60460 -> ok

**verdict:** PASS

## 7. reconciliation

**claim:** Monthly SUM over normalized charges/refunds equals monthly SUM over rollup_daily for all 60 months: max drift 0 cents. Rollups are maintained incrementally (O(1) per event), never rebuilt.

**measured:**

- months compared: 60 (expected 60)
- max |SUM(charges) - SUM(rollup gross)| per month: 0 cents
- max |SUM(refunds) - SUM(rollup refunds)| per month: 0 cents
- max drift of rollup months vs generator ground truth: 0 cents

**verdict:** PASS
