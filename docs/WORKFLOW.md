# Workflow: how a payment moves through LedgerFlow

This document follows one request through the system. Each step names the file and function that does the work, so you can read the code next to it.

---

## 0. Vocabulary

| Term | Meaning here |
|---|---|
| **Minor units** | All money is stored as integer **paise** (`BIGINT`). ₹250.00 = `25000`. Never use floats for money. |
| **Posting / journal entry** | One line of a ledger transaction: `(account, signed amount)`. Negative = money leaves, positive = money arrives. |
| **Ledger transaction** | A group of ≥ 2 postings that **sum to zero**. Money is moved, never created or destroyed. |
| **Hold** | Funds moved from the payer to the `system:holds` account while a payment is in flight. |
| **Command / event** | A command asks a service to do something (`HoldFunds`). An event states that something happened (`FundsHeld`). |
| **Outbox** | A table written in the same DB transaction as a business change. A relay publishes its rows to Kafka afterwards. |
| **Inbox** | The `processed_messages` table. It lets a consumer apply each message exactly once. |

---

## 1. Synchronous path: a wallet-to-wallet transfer

`POST /transfers` on the ledger. This is the simplest flow and it holds the core correctness logic.

```
Client ──POST /transfers + Idempotency-Key──▶ ledger
```

1. **Idempotency check** (`libs/common/src/idempotency.ts`, `IdempotencyInterceptor`)
   - Hash `method + path + body` to get the request fingerprint.
   - Redis `SET idem:ledger:<key> {in_progress} NX EX 30`.
     - If the key was **new**, execute the request.
     - If a request with the key is **in progress**, return `409` (a concurrent retry).
     - If it is **done** and the fingerprint matches, replay the stored response with the `Idempotent-Replayed: true` header.
     - If it is **done** but the fingerprint differs, return `422` (the key was reused for a different request).
2. **Validation**: `TransferDto` (class-validator). The amount must be an integer between 1 and 10¹².
3. **Post the transaction** (`services/ledger/src/ledger.service.ts`, `post()`), all inside one `BEGIN … COMMIT`:
   1. `INSERT INTO ledger_transactions … ON CONFLICT (idempotency_key) DO NOTHING`. This is the *database-level* idempotency guard, used if Redis lost the key. A concurrent duplicate waits on the unique index, then finds the first transaction and returns it.
   2. `SELECT … FROM accounts WHERE id = ANY($1) ORDER BY id FOR UPDATE`. This locks both accounts **in id order**, so A→B and B→A transfers can never deadlock.
   3. Check the balance against the **locked** row, so check-then-debit is race-free.
   4. Insert the journal entries (with `balance_after`) and update the cached balances.
   5. The DB enforces a backstop `CHECK (balance >= 0)`. Triggers make `journal_entries` and `ledger_transactions` **append-only**.
4. **Store the response** in Redis for 24 hours, then return `201`.

## 2. Asynchronous path: a payout through the saga

`POST /payments` on the payments service.

### Step 1: accept (payments service, ~ms)
`services/payments/src/payments.service.ts`, `create()`

In **one DB transaction**:
- `INSERT payments (status = PENDING)`, with a unique `idempotency_key`
- `INSERT payment_status_history`
- `INSERT outbox (topic = ledger.commands, key = payerAccountId, HoldFunds)`

The API returns `202 Accepted` straight away. The client polls `GET /payments/:id` for the outcome (or would receive a webhook in a real system).

> **Why the outbox?** Writing to Postgres and then calling `producer.send()` is a *dual write*. A crash between the two leaves a PENDING payment whose command was never sent. Here the command is committed atomically with the payment.

### Step 2: relay (payments service, background loop)
`libs/common/src/outbox.ts`, `OutboxRelay.publishBatch()`

```sql
SELECT … FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 200 FOR UPDATE SKIP LOCKED
```
→ `producer.sendBatch(acks = all, idempotent producer)` → `UPDATE outbox SET published_at = now()` → `COMMIT`.

- `SKIP LOCKED` lets every replica run a relay without double-publishing.
- A crash after `send` but before `COMMIT` means the batch is published again. Delivery is therefore **at-least-once**, and consumers must de-duplicate.

### Step 3: hold funds (ledger service)
`services/ledger/src/ledger.consumer.ts`, `onHoldFunds()`

Kafka delivers `HoldFunds`. The topic is partitioned by the **payer account id**, so commands for one account are processed in order. In **one DB transaction**:
1. `markProcessed(tx, 'ledger-service', message.id)`. If the row already exists, this is a duplicate: commit and return.
2. `holdFunds()`: validate the payee, then post `payer −amount / system:holds +amount` and insert a `holds` row (`HELD`).
3. `enqueue(ledger.events, FundsHeld)`.

If the payer has insufficient funds, a `BusinessError` is thrown. The transaction rolls back, and a **new** transaction records the inbox row and enqueues `HoldRejected`. Business outcomes are events, not errors.

### Step 4: call the PSP (payments service, saga orchestrator)
`services/payments/src/payment.saga.ts`, `onFundsHeld()`, then `executePayout()`

1. Guarded transition `PENDING → FUNDS_HELD`:
   ```sql
   UPDATE payments SET status='FUNDS_HELD' WHERE id=$1 AND status = ANY('{PENDING}') …
   ```
   Only one worker can win this, which makes the step idempotent.
2. `PspClient.payout()` (`services/payments/src/psp.client.ts`)
   - Each attempt has a 2 second **timeout**.
   - Transient failures (timeout, 5xx) are **retried** 3 times with **exponential backoff + full jitter**.
   - A **circuit breaker** wraps every call. After 5 failures in 10 seconds it opens for 15 seconds and fails fast. Declines and "still processing" responses do not count as failures.
   - The payment id is sent as the PSP **Idempotency-Key**. If a call timed out but the PSP actually paid, the retry gets the stored result, so the payee is **never paid twice**.
3. Outcome:
   - **succeeded**: in one transaction, `FUNDS_HELD → CAPTURING` + outbox `CaptureHold`.
   - **declined (402)**: in one transaction, `FUNDS_HELD → COMPENSATING` + outbox `ReleaseHold`. This is the compensation.
   - **transient** (timeout, 5xx, circuit open): log it and leave the payment in `FUNDS_HELD`. The event has done its job, since the database state is the source of truth. The **recovery job** owns PSP retries (step 6). The partition is not blocked and the DLQ is not flooded during an outage. The DLQ is reserved for messages that are really broken.

### Step 5: capture or release (ledger), then finish (payments)
- Ledger handles `CaptureHold`: `system:holds −amount / payee +amount`, sets the hold to `CAPTURED`, and emits `HoldCaptured`.
- Payments moves `CAPTURING → COMPLETED` and enqueues `PaymentCompleted` on `payments.events`.
- On the failure path, ledger handles `ReleaseHold`: `system:holds −amount / payer +amount`, emits `HoldReleased`. Payments moves `COMPENSATING → FAILED` and enqueues `PaymentFailed`.
- Notifications consumes `payments.events` and stores and logs an email for the payer (and the payee). It knows nothing about payments internals; it only reads the events.

### Step 6: recovery job (the saga's safety net)
`payment.saga.ts`, `recoverStuckPayments()`, runs every 10 seconds on every replica.

1. Claim up to 50 payments that are non-terminal and have not changed for 30 seconds, using `FOR UPDATE SKIP LOCKED` and an `updated_at` bump. Replicas never work on the same row.
2. For each claimed payment:
   - **PENDING**: re-send `HoldFunds`. The ledger de-duplicates per payment.
   - **CAPTURING / COMPENSATING**: re-send `CaptureHold` / `ReleaseHold`. Both are idempotent.
   - **FUNDS_HELD**: retry the PSP. After `SAGA_TIMEOUT_MS` or `SAGA_MAX_PSP_ATTEMPTS`, **reconcile first** with `GET /v1/payouts/:paymentId`:
     - If the PSP paid, capture.
     - If it did not, compensate.
     - If the PSP is unreachable, wait. The saga never compensates blindly.

## 3. Failure walkthroughs

| What breaks | What happens | Why money stays correct |
|---|---|---|
| payments crashes after `COMMIT`, before Kafka publish | The relay publishes the outbox row on restart. | Outbox: the command was committed with the payment. |
| Relay publishes, then crashes before marking the row published | The message is published twice. | Inbox: the second delivery is a no-op. |
| Ledger consumer crashes after DB commit, before the offset commit | Kafka redelivers the message. | Inbox row was committed with the effect, so it is skipped. |
| Client retries `POST /payments` (timeout on their side) | The same payment is returned. | Redis idempotency key, plus the DB unique constraint as a backstop. |
| PSP slow (response after our timeout) | We retry with the same key and the PSP returns the stored payout. | PSP idempotency key: one payout. |
| PSP down for minutes | In-call retries, then the circuit opens and calls fail fast; the payment stays `FUNDS_HELD`; the recovery job retries every pass; after the saga timeout, reconcile and then compensate. | Funds stay in `system:holds` (not lost, not spendable); the release returns them. |
| Malformed or unknown message | Sent straight to `<topic>.dlq` with error headers. | The partition keeps flowing; you inspect and replay with `npm run dlq:replay`. |
| Two transfers A→B and B→A at once | Both lock A, then B. | Deterministic lock order means no deadlock (0 deadlocks in the integration test). |
| 50 concurrent debits on a ₹1,000 wallet | Exactly 10 × ₹100 succeed. | Row lock + check against the locked balance + `CHECK (balance >= 0)`. |

## 4. Running the chaos scenario

Start payments with short saga timers so the outage scenario finishes in about 30 seconds:

```bash
# docker compose: add these under payments.environment and run `docker compose up -d payments`
SAGA_TIMEOUT_MS=20000
SAGA_STUCK_AFTER_MS=3000
SAGA_RECOVERY_INTERVAL_MS=2000

npm run e2e -- --chaos
```

Then watch it in the dashboards:
- **Grafana**: the circuit breaker goes to 2 (open), retries rise, and transitions show `FUNDS_HELD → COMPENSATING`.
- **Jaeger**: search for the `payments-service` traces. One trace spans HTTP → outbox → Kafka → ledger consumer, because the `traceparent` header travels with the message.
- **Payments logs**: `Payout … deferred to recovery`, then `Recovery pass handled …`, then the compensation.

## 5. Day-to-day development loop

1. `docker compose up -d postgres redis kafka jaeger` (infra only)
2. `npm run build:watch` in one terminal, and the services in others (see the README).
3. Change code, then restart that service.
4. `npm test` (unit), `npm run test:integration` (needs `TEST_DATABASE_URL`), then `npm run e2e`.
5. For a schema change, add `services/<svc>/migrations/00N_description.sql`. It is applied on the next boot. Never edit an applied migration.
6. Before committing, run `npm run build && npm test && npm run e2e`.
