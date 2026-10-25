# Build guide: phases, workflow and checkpoints

The finished code is all in this repo. This guide is the order to **work through it** so that you understand and own every part. Treat each phase as one or two evenings. Do not move on until the checkpoint passes. Commit at the end of each phase; a clean, incremental git history is itself evidence that you built it.

Suggested approach: for each phase, read the listed files and then **re-type or re-implement them yourself** in a fresh branch, using this repo as the reference. Then run the checkpoint against your version.

---

## Phase 0: environment (30 min)

- Install Node 20+, Docker Desktop, `jq`, k6, and (for phase 8) `kind` + `kubectl` + `helm`.
- `npm install`, then `docker compose up -d postgres redis kafka`.
- Read: `README.md`, `docs/WORKFLOW.md` §0.

**Checkpoint:** `docker compose ps` shows postgres, redis and kafka as healthy.

## Phase 1: the double-entry ledger (the core)

Build `services/ledger` **without Kafka**: accounts, deposits, transfers, entries.

- `services/ledger/migrations/001_init.sql`: tables, `CHECK (balance >= 0)`, append-only triggers
- `services/ledger/src/ledger.service.ts`, `post()`: balanced postings, `FOR UPDATE` in id order, funds check, entries, balance update
- `libs/common/src/db.ts`: pool, `withTransaction`, migration runner

**Checkpoint:**
```bash
docker compose exec postgres createdb -U ledgerflow ledger_test
TEST_DATABASE_URL=postgres://ledgerflow:ledgerflow@localhost:5432/ledger_test npm run test:integration
```
The double-spend, deadlock and invariant tests are green.

**Exercise:** remove the `.sort()` on `lockIds` and the `ORDER BY id`, then rerun the deadlock test. You should see `40P01` deadlocks, retried by `withTransaction`; check `pg_stat_database.deadlocks`. Put the sort back. You now *understand* lock ordering instead of having memorised it.

## Phase 2: idempotency keys

- `libs/common/src/idempotency.ts`: `IdempotencyStore`, `IdempotencyInterceptor`, `@Idempotent()`
- `ledger_transactions.idempotency_key` + `request_hash`

**Checkpoint:** `npm test` (idempotency spec), then by hand: send the same `POST /transfers` twice with the same key. The second response carries `Idempotent-Replayed: true` and the balance moves once. Then `redis-cli FLUSHALL` and send it again: the DB constraint still prevents a second transfer.

## Phase 3: Kafka plumbing (outbox, inbox, consumer)

- `libs/common/src/events.ts`: topics and the envelope
- `libs/common/src/kafka.ts`: idempotent producer, topic creation
- `libs/common/src/outbox.ts`: `enqueue()` + `OutboxRelay`
- `libs/common/src/inbox.ts`: `markProcessed()`
- `libs/common/src/consumer.ts`: retries, backoff, DLQ
- `libs/common/src/core.module.ts`: wires everything into Nest

**Checkpoint:** start the ledger, open Kafka UI (http://localhost:8080), and confirm the 6 topics exist (3 main + 3 `.dlq`).

## Phase 4: the payment saga

- `services/payments/migrations/001_init.sql`: the state machine in a CHECK constraint
- `services/payments/src/payments.service.ts`: `create()` (payment + outbox in one transaction), guarded `transition()`
- `services/ledger/src/ledger.consumer.ts`: HoldFunds / CaptureHold / ReleaseHold
- `services/payments/src/payment.saga.ts`: event handlers, `executePayout`, compensation

Temporarily stub the PSP as "always succeeds".

**Checkpoint:** create a payment; `GET /payments/:id` shows `PENDING → FUNDS_HELD → CAPTURING → COMPLETED`; the merchant balance went up; `/admin/invariants` is all ✔.

## Phase 5: the external dependency and resilience

- `services/psp-simulator`: magic amounts and the chaos endpoint
- `libs/common/src/retry.ts`, `libs/common/src/circuit-breaker.ts` (+ unit tests)
- `services/payments/src/psp.client.ts`: timeout, retry, breaker, PSP idempotency key
- `payment.saga.ts`, `recoverStuckPayments()`: recovery job, saga timeout, reconcile-before-compensate

**Checkpoint:** `npm run e2e` is all green. Then `npm run e2e -- --chaos` with the short saga timers from WORKFLOW.md §4.

## Phase 6: notifications and DLQ tooling

- `services/notifications`: a second, independent consumer group
- `scripts/dlq-replay.js`

**Checkpoint:** produce a poison message (see INTERVIEW_PREP.md §3), see it in the DLQ with `--list`, and replay it.

## Phase 7: observability and load

- `libs/common/src/otel.ts` (loaded with `--require`), `libs/common/src/tracing.ts` (Kafka header propagation)
- `libs/common/src/metrics.ts`, `infra/prometheus`, `infra/grafana`
- `load/transfers.js`, `load/payments.js`

**Checkpoint:** in Jaeger, one trace covers `POST /payments` through to the ledger consumer. In Grafana, the dashboard moves during a k6 run. **Write your k6 results into `docs/LOAD_TESTING.md` § Results**, with the machine spec.

## Phase 8: Kubernetes

- `Dockerfile` (one image per service via `--build-arg SERVICE`)
- `k8s/infra`, `k8s/base`, KEDA ScaledObjects

**Checkpoint:** run the steps in `docs/KUBERNETES.md`. Run `k6 run load/payments.js` against the port-forwarded service and watch `kubectl get hpa,pods -n ledgerflow -w` scale on lag.

## Phase 9: polish for the resume

1. Push to GitHub with a clear README (this one) and an architecture diagram (the Mermaid renders on GitHub).
2. Make sure GitHub Actions is green (`.github/workflows/ci.yml`).
3. Add a short screen recording or GIF: run `npm run e2e`, show a Jaeger trace and the Grafana panel during k6.
4. Replace the resume numbers with your own k6 results.
5. Practise the 60-second pitch and the drills in `docs/INTERVIEW_PREP.md`.

---

## Git workflow suggestion

```
main
 └─ feat/ledger-core        (phase 1)
 └─ feat/idempotency        (phase 2)
 └─ feat/messaging          (phase 3)
 └─ feat/payment-saga       (phase 4)
 └─ feat/psp-resilience     (phase 5)
 └─ feat/notifications-dlq  (phase 6)
 └─ feat/observability-load (phase 7)
 └─ feat/k8s                (phase 8)
```
Open a PR per phase, even for yourself, with a description of *why*. Interviewers sometimes look at PR history.

## Extension ideas (if you want more depth)

- **Refunds**: a new saga that moves money from merchant to payer after `COMPLETED`.
- **Webhooks**: notify merchants over HTTP with signed payloads (HMAC), retries and a delivery log.
- **Reconciliation job**: compare PSP payouts with ledger captures every night and flag mismatches.
- **Optimistic locking variant**: implement `post()` with the `version` column, then benchmark both under contention.
- **Hot-account sharding** for one heavy merchant, and benchmark before and after.
- **Debezium CDC** instead of the polling relay.
