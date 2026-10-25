-- NOTIFICATIONS SERVICE SCHEMA
CREATE TABLE notifications (
  id                    UUID PRIMARY KEY,
  payment_id            UUID        NOT NULL,
  type                  TEXT        NOT NULL,
  channel               TEXT        NOT NULL,
  recipient_account_id  UUID        NOT NULL,
  message               TEXT        NOT NULL,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- business-level dedupe on top of the inbox: one notification of each type per payment
  UNIQUE (payment_id, type, recipient_account_id)
);

-- The service only consumes, but keeps the shared outbox table so the core module is uniform.
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

CREATE TABLE processed_messages (
  consumer      TEXT        NOT NULL,
  message_id    UUID        NOT NULL,
  processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, message_id)
);
