-- Requires PostgreSQL 15+
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TYPE payment_status AS ENUM ('pending', 'processing', 'completed', 'failed');

-- Month-based partitioning for high volumes
CREATE TABLE payments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key TEXT NOT NULL,
    user_id         TEXT NOT NULL,
    recipient       TEXT NOT NULL,
    amount          BIGINT NOT NULL CHECK (amount > 0),
    currency        CHAR(3) NOT NULL,
    status          payment_status NOT NULL DEFAULT 'pending',
    failure_reason  TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
) PARTITION BY RANGE (created_at);

CREATE TABLE payments_default PARTITION OF payments DEFAULT;

CREATE UNIQUE INDEX ux_payments_idempotency_key ON payments (idempotency_key);
CREATE INDEX ix_payments_user_id ON payments (user_id);
CREATE INDEX ix_payments_status ON payments (status);

-- Transactional Outbox pattern
CREATE TABLE outbox_events (
    id            BIGSERIAL PRIMARY KEY,
    aggregate_id  UUID NOT NULL,
    topic         TEXT NOT NULL,
    payload       JSONB NOT NULL,
    published     BOOLEAN NOT NULL DEFAULT false,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);

CREATE INDEX ix_outbox_unpublished ON outbox_events (id) WHERE NOT published;

-- Double-entry bookkeeping
CREATE TABLE ledger_entries (
    id          BIGSERIAL PRIMARY KEY,
    payment_id  UUID NOT NULL,
    account_id  TEXT NOT NULL,
    direction   TEXT NOT NULL CHECK (direction IN ('debit', 'credit')),
    amount      BIGINT NOT NULL CHECK (amount > 0),
    currency    CHAR(3) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX ux_ledger_payment_direction ON ledger_entries (payment_id, direction);