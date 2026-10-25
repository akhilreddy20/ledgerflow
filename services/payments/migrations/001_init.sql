-- =====================================================================================
-- PAYMENTS SERVICE SCHEMA - saga state
--
--   PENDING ──FundsHeld──▶ FUNDS_HELD ──PSP ok──▶ CAPTURING ──HoldCaptured──▶ COMPLETED
--      │                       │
--      │ HoldRejected          │ PSP declined / saga timeout
--      ▼                       ▼
--   FAILED ◀──HoldReleased── COMPENSATING
-- =====================================================================================

CREATE TABLE payments (
  id                UUID PRIMARY KEY,
  idempotency_key   TEXT        NOT NULL UNIQUE,
  request_hash      TEXT        NOT NULL,
  payer_account_id  UUID        NOT NULL,
  payee_account_id  UUID        NOT NULL,
  amount            BIGINT      NOT NULL CHECK (amount > 0),
  currency          CHAR(3)     NOT NULL,
  description       TEXT,
  status            TEXT        NOT NULL CHECK (status IN
                      ('PENDING', 'FUNDS_HELD', 'CAPTURING', 'COMPENSATING', 'COMPLETED', 'FAILED')),
  failure_reason    TEXT,
  psp_reference     TEXT,
  psp_attempts      INT         NOT NULL DEFAULT 0,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (payer_account_id <> payee_account_id)
);
-- The saga recovery job scans non-terminal payments by age.
CREATE INDEX payments_in_flight_idx ON payments (status, updated_at)
  WHERE status IN ('PENDING', 'FUNDS_HELD', 'CAPTURING', 'COMPENSATING');
CREATE INDEX payments_payer_idx ON payments (payer_account_id, created_at DESC);

-- Audit trail of every state transition.
CREATE TABLE payment_status_history (
  id           BIGSERIAL PRIMARY KEY,
  payment_id   UUID        NOT NULL REFERENCES payments(id),
  from_status  TEXT,
  to_status    TEXT        NOT NULL,
  reason       TEXT,
  at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX payment_status_history_payment_idx ON payment_status_history (payment_id, id);

-- Messaging: transactional outbox + idempotent-consumer inbox
CREATE TABLE outbox (
  id            BIGSERIAL PRIMARY KEY,
  message_id    UUID        NOT NULL UNIQUE,
  topic         TEXT        NOT NULL,
  message_key   TEXT        NOT NULL,
  event_type    TEXT        NOT NULL,
  payload       JSONB       NOT NULL,
  headers       JSONB       NOT NULL DEFAULT '{}'::jsonb,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at  TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE processed_messages (
  consumer      TEXT        NOT NULL,
  message_id    UUID        NOT NULL,
  processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, message_id)
);
