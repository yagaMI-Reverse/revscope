-- revscope schema. Idempotent: drops and recreates everything.

DROP TABLE IF EXISTS raw_events CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS subscriptions CASCADE;
DROP TABLE IF EXISTS charges CASCADE;
DROP TABLE IF EXISTS refunds CASCADE;
DROP TABLE IF EXISTS metadata_review CASCADE;
DROP TABLE IF EXISTS rollup_daily CASCADE;
DROP TABLE IF EXISTS customer_stats CASCADE;
DROP TABLE IF EXISTS ingest_checkpoint CASCADE;
DROP FUNCTION IF EXISTS raw_events_immutable CASCADE;

-- Append-only event log. The UNIQUE constraint on stripe_id IS the
-- idempotency layer: everything downstream runs only if this insert lands.
CREATE TABLE raw_events (
    stripe_id   TEXT PRIMARY KEY,
    type        TEXT NOT NULL,
    payload     JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE FUNCTION raw_events_immutable() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'raw_events is append-only: % not allowed', TG_OP;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER raw_events_append_only
    BEFORE UPDATE OR DELETE ON raw_events
    FOR EACH ROW EXECUTE FUNCTION raw_events_immutable();

-- Normalized tables. Canonical fields (product, amount) come ONLY from
-- structural fields (price_id); never from metadata.
CREATE TABLE customers (
    id      TEXT PRIMARY KEY,
    email   TEXT,
    country TEXT,
    created TIMESTAMPTZ NOT NULL
);

CREATE TABLE subscriptions (
    id           TEXT PRIMARY KEY,
    customer_id  TEXT NOT NULL,
    price_id     TEXT NOT NULL,
    product      TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    status       TEXT NOT NULL,
    created      TIMESTAMPTZ NOT NULL,
    canceled_at  TIMESTAMPTZ
);
CREATE INDEX idx_subscriptions_status ON subscriptions (status);

CREATE TABLE charges (
    id              TEXT PRIMARY KEY,
    customer_id     TEXT NOT NULL,
    subscription_id TEXT,
    price_id        TEXT NOT NULL,
    product         TEXT NOT NULL,
    amount_cents    INTEGER NOT NULL,
    status          TEXT NOT NULL,          -- succeeded | failed
    decline_code    TEXT,
    decline_class   TEXT,                   -- retryable | terminal | NULL
    created         TIMESTAMPTZ NOT NULL
);

CREATE TABLE refunds (
    id           TEXT PRIMARY KEY,
    charge_id    TEXT NOT NULL,
    customer_id  TEXT NOT NULL,
    product      TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    partial      BOOLEAN NOT NULL,
    created      TIMESTAMPTZ NOT NULL
);

-- Quarantine for metadata that does not match the canonical value derived
-- from price_id. Nothing from here ever feeds reports or segments.
CREATE TABLE metadata_review (
    stripe_id TEXT NOT NULL,
    key       TEXT NOT NULL,
    value     TEXT
);

-- Incremental rollups: updated O(1) per event, never rebuilt from raw.
-- status: succeeded | failed_retryable | failed_terminal | refund
CREATE TABLE rollup_daily (
    day          DATE NOT NULL,
    product      TEXT NOT NULL,
    status       TEXT NOT NULL,
    gross_cents  BIGINT NOT NULL DEFAULT 0,
    refund_cents BIGINT NOT NULL DEFAULT 0,
    tx_count     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day, product, status)
);
CREATE INDEX idx_rollup_daily_status ON rollup_daily (status, day);

-- One row per customer; segmentation runs on this, never on raw charges.
CREATE TABLE customer_stats (
    customer_id  TEXT PRIMARY KEY,
    ltv_cents    BIGINT NOT NULL DEFAULT 0,   -- gross paid, cents
    first_paid_at TIMESTAMPTZ,
    last_paid_at  TIMESTAMPTZ,
    paid_count   INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    refund_cents BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE ingest_checkpoint (
    worker         TEXT PRIMARY KEY,
    last_stripe_id TEXT,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
