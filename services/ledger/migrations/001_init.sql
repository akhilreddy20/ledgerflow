-- =====================================================================================
-- LEDGER SERVICE SCHEMA - double-entry bookkeeping
--
-- Every movement of money is a `ledger_transaction` with >= 2 `journal_entries` whose
-- amounts SUM TO ZERO (debits negative, credits positive). Money is never created or
-- destroyed, only moved. Amounts are BIGINT minor units (paise/cents) - never floats.
-- =====================================================================================

CREATE TABLE accounts (
  id               UUID PRIMARY KEY,
  name             TEXT        NOT NULL,
  currency         CHAR(3)     NOT NULL,
  -- user/merchant: balance-checked + row-locked on every debit
  -- system:        internal accounts (world/funding, holds). Allowed to go negative and,
  --                to avoid a hot-row bottleneck, NOT row-locked; their balance is derived
  --                from journal_entries instead of the cached column.
  kind             TEXT        NOT NULL CHECK (kind IN ('user', 'merchant', 'system')),
  balance          BIGINT      NOT NULL DEFAULT 0,
  version          BIGINT      NOT NULL DEFAULT 0,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT non_system_balance_non_negative CHECK (kind = 'system' OR balance >= 0)
);

CREATE TABLE ledger_transactions (
  id               UUID PRIMARY KEY,
  -- Exactly-once guard: retries of the same logical operation carry the same key.
  idempotency_key  TEXT        NOT NULL UNIQUE,
  -- Hash of the postings: detects a key being re-used for a *different* operation.
  request_hash     TEXT        NOT NULL,
  type             TEXT        NOT NULL CHECK (type IN ('deposit', 'transfer', 'hold', 'capture', 'release')),
  metadata         JSONB       NOT NULL DEFAULT '{}'::jsonb,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE journal_entries (
  id               BIGSERIAL   PRIMARY KEY,
  transaction_id   UUID        NOT NULL REFERENCES ledger_transactions(id),
  account_id       UUID        NOT NULL REFERENCES accounts(id),
  amount           BIGINT      NOT NULL CHECK (amount <> 0),
  -- running balance of the account right after this entry (NULL for system accounts)
  balance_after    BIGINT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX journal_entries_account_idx ON journal_entries (account_id, id DESC);
CREATE INDEX journal_entries_tx_idx ON journal_entries (transaction_id);

-- The ledger is APPEND-ONLY. Corrections are new compensating transactions, never edits.
CREATE FUNCTION forbid_mutation() RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION 'journal_entries and ledger_transactions are append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER journal_entries_append_only
  BEFORE UPDATE OR DELETE ON journal_entries
  FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
CREATE TRIGGER ledger_transactions_append_only
  BEFORE UPDATE OR DELETE ON ledger_transactions
  FOR EACH ROW EXECUTE FUNCTION forbid_mutation();

-- Funds reserved for an in-flight payment (saga step 1). One hold per payment.
CREATE TABLE holds (
  id               UUID PRIMARY KEY,
  payment_id       UUID        NOT NULL UNIQUE,
  account_id       UUID        NOT NULL REFERENCES accounts(id),
  amount           BIGINT      NOT NULL CHECK (amount > 0),
  status           TEXT        NOT NULL CHECK (status IN ('HELD', 'CAPTURED', 'RELEASED')),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Well-known system accounts
INSERT INTO accounts (id, name, currency, kind) VALUES
  ('00000000-0000-0000-0000-000000000001', 'system:world (external funding source)', 'INR', 'system'),
  ('00000000-0000-0000-0000-000000000002', 'system:holds (funds reserved for in-flight payments)', 'INR', 'system');

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
