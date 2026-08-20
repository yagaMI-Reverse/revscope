# revscope product bench results

- run at: 2026-08-20 19:53 UTC
- postgres: PostgreSQL 16.14
- python: 3.12.7, psycopg 3.3.4
- dataset: 500000 charges over 10 countries / 7 currencies, 1827 days of rates, 60 months; marts rebuilt in 4.7s (0.7s revenue, 2.8s cohorts, 1.2s funnel)

| # | bench | measured | verdict |
|---|-------|----------|---------|
| 8 | cohort_retention | 240 cells, 0 mismatches, M12 34.2% | PASS |
| 9 | funnel | 0 mismatches, worst drop 38.0% at paid_twice | PASS |
| 10 | fx_reporting | -0.91% (-454,312.40 USD) if converted at today's rate | PASS |
| 11 | product_dashboard | p50 14.6 ms over HTTP, 10.1 ms in-process | PASS |

## 8. cohort_retention

**claim:** Cohort retention computed in SQL over the marts matches, cell for cell, an independent Python implementation that never touches the database -- including the per-customer maturity rule that keeps young cohorts out of the denominator.

**measured:**

- mart: 7800 rows (cohort x country x 13 periods) over 60 monthly cohorts and 60460 paying customers
- cells compared against the Python implementation in gen.py: 240 (cohort x period, four values each), mismatches: 0
- month 2: retained 19870 of 52808 eligible = 37.6% (survived-to-or-past: 31374 = 59.4%)
- month 3: retained 18767 of 50010 eligible = 37.5% (survived-to-or-past: 29462 = 58.9%)
- month 6: retained 15748 of 43026 eligible = 36.6% (survived-to-or-past: 24633 = 57.3%)
- month 12: retained 11063 of 32391 eligible = 34.2% (survived-to-or-past: 17323 = 53.5%)
- dividing by the whole cohort instead of the customers the period has elapsed for reports month 2: 32.9% instead of 37.6%; month 3: 31.0% instead of 37.5%; month 6: 26.0% instead of 36.6%; month 12: 18.3% instead of 34.2%
- gap between 'paid in this period' and 'paid in it or later' at month 12: 6260 customers (19.3 points) -- customers a single failed charge would have written off

**verdict:** PASS

## 9. funnel

**claim:** The payment funnel is built from transactions only (no invented product events), every stage is a strict subset of the one above it, and all eight counts match the independent Python implementation exactly.

**measured:**

- funnel base: 51842 customers with at least 90 days of history (younger ones are excluded, not counted as lost)
- attempted: 51842
- paid_once: 50233 (-3.1%)
- paid_twice: 31141 (-38.0%)
- regular_3plus: 23692 (-23.9%)
- still_paying: 16343 (-31.0%)
- biggest drop-off: paid_twice loses 19092 customers, 38.0% of the previous stage
- churn split of the same base: never_converted 1609 (3.1%), churn_involuntary 1990 (3.8%), churn_voluntary 30817 (59.4%)
- of 32807 customers who stopped paying, 1990 (6.1%) left on a declined charge, not on a decision -- the part dunning can actually address
- every stage a subset of the previous one: True
- stage counts vs the Python implementation: 8 stages, 0 mismatches

**verdict:** PASS

## 10. fx_reporting

**claim:** Revenue is converted at the rate of each transaction day, not at today's rate. On this dataset the difference between the two is a measured amount of money, not a rounding detail, and it is larger for weak-currency countries.

**measured:**

- 459691 succeeded charges in 10 countries / 7 currencies, every one of them converted per transaction at the rate of its own day
- rate feed: 12789 daily rows (1827 days x 7 currencies), charges with no rate for their day: 0 (expected 0)
- charges reaching the mart: 459691 of 459691 succeeded -> ok
- gross at the rate of the day: 49,694,286.85 USD (4969428685 cents)
- gross at today's rate:        49,239,974.45 USD (4923997445 cents)
- difference: -454,312.40 USD (-45431240 cents), -0.91% of reported revenue -- the same local money, one join condition apart
- worst country: AU (AUD) -6.99% = -346,457.70 USD (-34645770 cents)
- worst month x country slice above 10,000 USD: KZ 2023-10 -17.58% (55,291.00 USD (5529100 cents) -> 45,572.11 USD (4557211 cents))
- gap by year of the transaction: 2021 -4.34%, 2022 -3.97%, 2023 -1.31%, 2024 -2.03%, 2025 -0.77%, 2026 +0.41% -- the all-time number is small only because the business grew into the recent, near-current rates
- refunds: 2,086,317.48 USD (208631748 cents) at the rate of the day vs 2,069,264.38 USD (206926438 cents) at today's
- per-country totals vs the Python implementation: 10 countries x 8 values, 0 mismatches
- round trip against the already-published ledger: converting every presentment amount back at its own day's rate gives 4969428685 cents vs 4969427500 cents of USD list price, residual 1185 cents (+0.000024%) -- rounding of the presented amount, not FX error

**verdict:** PASS

## 11. product_dashboard

**claim:** The whole product dashboard -- cohorts, funnel and revenue by country, filtered by country and period -- is served from the marts in milliseconds, reading a few thousand mart rows instead of the 500,000 charges they were built from.

**measured:**

- page = 4 queries (summary, revenue, funnel, cohorts), all of them against the three marts: 13040 rows total, versus 500000 charges the marts were built from (38x fewer rows to read)
- unfiltered page over all 10 countries and 60 months: p50 10.1 ms, p95 12.4 ms over 20 runs
- filtered page (country=KZ, 2024-01..2024-12): p50 4.2 ms, p95 5.0 ms over 20 runs
- end to end over HTTP, the four endpoints the browser actually calls: p50 14.6 ms, p95 16.9 ms over 20 runs (of which 10.2 ms is SQL); the page itself is 18804 bytes with no external requests at all
- filter actually cuts the data: 12 months, 1 country, gross 975,244.00 USD (97524400 cents) vs 49,694,286.85 USD (4969428685 cents) unfiltered -> ok
- unfiltered page totals equal generator ground truth: ok
- headline numbers the page shows: retention M2 37.6%, M12 34.2%, worst funnel step paid_twice -38.0%, FX gap -0.91%
- cohorts returned: 60, funnel stages: 8, months on the revenue chart: 60

**verdict:** PASS
