-- revscope product layer: the FX feed and three marts. Idempotent.
--
-- Deliberately NOT part of schema.sql: `python ingest.py reset` has to stay a
-- reset of the ingest core that the first seven benches measure. Everything
-- here is derived data, rebuilt in one pass by `python product.py build`.

DROP TABLE IF EXISTS fx_rates CASCADE;
DROP TABLE IF EXISTS mart_cohort_retention CASCADE;
DROP TABLE IF EXISTS mart_funnel CASCADE;
DROP TABLE IF EXISTS mart_revenue_country CASCADE;

-- Daily rate feed: rate_scaled is "minor units of `currency` per 1 USD"
-- times 1e8, stored as BIGINT. The same conversion runs in Python (generator
-- ground truth) and in SQL (the marts); with NUMERIC or float division the
-- two disagree in the last cent and a 0-cent reconciliation is no longer
-- possible. With integers they agree bit for bit.
-- The feed also defines the report's "as of": the last day it carries is the
-- last day the business can legitimately close, and it is where the marts
-- read "today's rate" from.
CREATE TABLE fx_rates (
    day         DATE   NOT NULL,
    currency    TEXT   NOT NULL,
    rate_scaled BIGINT NOT NULL,
    PRIMARY KEY (day, currency)
);

-- Cohort = calendar month of the customer's FIRST succeeded payment.
-- period_index counts 30-day periods since that payment, not calendar
-- months, because billing runs on a 30-day cycle (see gen.py).
CREATE TABLE mart_cohort_retention (
    cohort_month      DATE   NOT NULL,
    country           TEXT   NOT NULL,
    period_index      INT    NOT NULL,
    cohort_size       INT    NOT NULL,  -- customers in this cohort/country
    eligible          INT    NOT NULL,  -- of them, those this period elapsed for
    retained          INT    NOT NULL,  -- paid inside this period
    survived          INT    NOT NULL,  -- paid in this period or any later one
    revenue_usd_cents BIGINT NOT NULL,  -- at the rate of each transaction day
    PRIMARY KEY (cohort_month, country, period_index)
);

-- kind='funnel' rows are nested (each stage a subset of the previous one);
-- kind='churn' rows are a separate cut of the same base and do not add up to
-- a funnel stage. Cohort here is the month of the first charge ATTEMPT, so
-- customers who never got a payment through are still in the base.
CREATE TABLE mart_funnel (
    cohort_month DATE NOT NULL,
    country      TEXT NOT NULL,
    kind         TEXT NOT NULL,
    stage        TEXT NOT NULL,
    stage_order  INT  NOT NULL,
    customers    INT  NOT NULL,
    PRIMARY KEY (cohort_month, country, stage)
);

-- The whole point of the module in one table: the same local money converted
-- two ways. gross_usd_hist uses the rate of the transaction day, which is
-- what the bank statement says; gross_usd_current uses today's rate, which is
-- what a report joined on the wrong date says.
CREATE TABLE mart_revenue_country (
    month              DATE   NOT NULL,
    country            TEXT   NOT NULL,
    currency           TEXT   NOT NULL,
    tx_count           INT    NOT NULL,
    gross_local        BIGINT NOT NULL,
    gross_usd_hist     BIGINT NOT NULL,
    gross_usd_current  BIGINT NOT NULL,
    refund_count       INT    NOT NULL,
    refund_local       BIGINT NOT NULL,
    refund_usd_hist    BIGINT NOT NULL,
    refund_usd_current BIGINT NOT NULL,
    PRIMARY KEY (month, country)
);
