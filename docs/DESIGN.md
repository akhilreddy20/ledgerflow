# Design decisions and trade-offs

Each section covers one decision: what was chosen, why, what it costs, and what changes at larger scale. These are the questions an SDE2 interview will dig into.

---

## 1. Service boundaries

| Service | Owns | Why separate |
|---|---|---|
| **ledger** | accounts, journal, holds | The **system of record for money**. It has a small API surface and strict invariants, and it changes rarely, so it is kept isolated so nothing else can corrupt it. |
| **payments** | payment lifecycle, saga state, PSP integration | Changes often (new PSPs, new flows) and has a different failure profile (external calls). |
| **notifications** | outbound messages | Purely downstream. It must never block or slow a payment. |

Each service has its **own database** (database-per-service). No service reads another's tables. They communicate only through APIs and Kafka.

**Cost:** there are no cross-service ACID transactions. This is why a saga is needed (section 5).

> Note the contrast with the HRMS modular monolith on the resume. That system's workflows were tightly coupled and low-volume, so one database and ACID transactions were the better trade. Here the ledger must be isolated and can scale independently. Being able to explain *when* each architecture fits is a classic SDE2 signal.

## 2. Double-entry ledger

- Every transaction has postings that sum to zero. Money moves between accounts; it is never created or destroyed. External money enters through the `system:world` account, which is allowed to go negative.
- Amounts are `BIGINT` minor units. Floats cannot represent 0.1 exactly.
- The journal is **append-only** and enforced by triggers. A mistake is corrected with a new, compensating transaction, which keeps a full audit trail.
- `accounts.balance` is a **cached** balance, updated in the same transaction as the entries. Invariant: `balance == SUM(entries)`, checked by `/admin/invariants` and by the load test.

**Alternative:** compute balances as `SUM(entries)` on every read. That is always consistent, but O(n) per read. Our approach is O(1) to read, at the price of updating a row on every write, which is what we lock anyway.

## 3. Concurrency control: pessimistic row locks

`SELECT … FOR UPDATE` on the accounts involved, **sorted by id**.

| Option | Verdict |
|---|---|
| **Pessimistic lock (chosen)** | Simple and correct. Contention is per account, and accounts are spread across users. Deterministic lock order prevents deadlocks. |
| Optimistic (`version` column + retry) | Good when conflicts are rare. Under a hot account it causes retry storms. The `version` column is kept so you can show this alternative. |
| `SERIALIZABLE` isolation | Correct, but it aborts more transactions under load and needs retry logic everywhere. `withTransaction` does retry `40001`/`40P01`, as a safety net. |
| Single-threaded per account (Kafka partition = actor) | Very fast and lock-free, but every write must go through Kafka. Mention it as the 100x design. |

**The hot-account problem.** System accounts (`system:holds`, `system:world`) take part in *every* payment. Locking them would serialize the whole system on one row. So system accounts are **not locked** and have no cached balance; their balance is derived from the journal. For a hot *merchant* account at scale, the answer is **sharded sub-accounts**: N rows per merchant, pick one at random for credits, and sum them for reads.

## 4. Idempotency at three layers

1. **API (Redis)**: the `Idempotency-Key` header plus a request fingerprint. A fast path that replays the original response for 24 hours. A short "in progress" lock stops two concurrent retries from both running.
2. **Database (unique constraints)**: `payments.idempotency_key` and `ledger_transactions.idempotency_key`. This is the **source of truth** when Redis evicts a key or fails over. `request_hash` catches a key being reused for a different operation.
3. **Messaging (inbox + guarded transitions)**: `processed_messages(consumer, message_id)` is written in the same transaction as the effect. Saga transitions are `UPDATE … WHERE status = <expected>`, so only one worker can win.

Because every step is idempotent, the whole pipeline can be at-least-once and still produce **exactly-once effects**. This is the most important idea in the project.

## 5. Saga: orchestration rather than choreography

The payments service owns an explicit state machine (`PENDING → FUNDS_HELD → CAPTURING → COMPLETED`, plus `COMPENSATING → FAILED`).

| | Orchestration (chosen) | Choreography |
|---|---|---|
| Where the flow lives | One place (`payment.saga.ts`) | Spread across services' event handlers |
| Visibility | `GET /payments/:id` shows the state and full history | Must be reconstructed from events |
| Coupling | Orchestrator knows the participants | Participants know each other's events |
| Good for | Money flows that need timeouts, compensation, audit | Simple, loosely related reactions (notifications) |

The project uses **both**: orchestration for the payment, choreography for notifications.

**Compensation rules**
- A hold is compensated by a release. A capture **cannot** be undone by a release; you would need a refund, which is a new forward transaction.
- **Never compensate blindly after a timeout.** The PSP may have paid. Reconcile with `GET /v1/payouts/:id` first. If the PSP is unreachable, keep waiting; the funds stay safely in `system:holds`.
- *Known edge:* if a payout request is still in flight at the exact moment the saga times out and reconciles, the PSP could pay after we release. Mitigations: make the saga timeout much longer than the PSP timeout (done: 5 min vs 2 s); in production, use a PSP "cancel/void" API and a daily **reconciliation job** that compares PSP settlement reports with the ledger.

## 6. Reliable messaging: outbox and Kafka

- **Transactional outbox** (rather than CDC with Debezium): no extra infrastructure, easy to explain. Cost: polling latency (around 50 ms) and outbox table churn, handled by a partial index and retention cleanup. At scale, Debezium tails the WAL instead of polling.
- **Idempotent producer** (`idempotent: true`, `acks: all`): Kafka de-duplicates retried sends within a producer session.
- **Partitioning by account id**: all commands for one account land on one partition, so they are processed **in order** by one consumer. Different accounts are processed in parallel across partitions. Partition count (6) caps consumer parallelism, so KEDA's `maxReplicaCount` is 6.
- **Ordering caveat:** with several relay replicas, `SKIP LOCKED` batches can publish out of order across replicas. This is safe here because every handler is state-guarded. If strict ordering mattered, run one relay per partition range or use CDC.
- **Retries inside the consumer block the partition** on purpose, to preserve per-key ordering. After `maxAttempts`, the message goes to `<topic>.dlq` with error headers so the partition can move on. Alternative: retry topics with delays (`topic.retry.5s`, `topic.retry.1m`). That gives better throughput but loses ordering.

## 7. Resilience to the external PSP

- **Timeout** (2 s): without one, a hung dependency exhausts connections and event-loop capacity.
- **Retry with full jitter**: `delay = random(0, min(cap, base·2^n))`. Plain exponential backoff synchronizes clients into waves; jitter spreads them out.
- **Only retry what is retryable**: a decline is an answer, not a failure. Retrying it is pointless, and counting it would trip the breaker for no reason.
- **Circuit breaker** (CLOSED → OPEN → HALF_OPEN): stops sending traffic to a dependency that is already failing, fails fast, and probes for recovery. State is per pod; a shared (Redis) breaker is possible but adds a dependency to the failure path.

## 8. Observability

- **Traces**: OpenTelemetry auto-instruments HTTP, Express, pg and ioredis. Kafka context is propagated **manually**. The `traceparent` header is stored in the outbox row and copied into the Kafka headers, so a single trace covers `POST /payments → outbox → Kafka → ledger consumer → Kafka → saga → PSP`.
- **Metrics** (Prometheus): RED metrics per route, consumer outcomes (ok / retry / dlq), outbox backlog, saga transitions, PSP outcomes, breaker state, idempotent replays.
- **What to alert on**: DLQ count > 0, outbox backlog growing, payments stuck in a non-terminal state for more than 5 minutes, breaker open, invariant check failing (page immediately).

## 9. What changes at 10x and 100x

| Pressure | Change |
|---|---|
| Ledger write throughput | Partition Postgres by account (Citus) or move to a purpose-built ledger (TigerBeetle). Batch postings per account. |
| Hot merchant accounts | Sharded sub-accounts, as in section 3. |
| Outbox polling | CDC with Debezium. |
| Read load on balances | Read replicas, or a cache with event-driven invalidation (careful: balances must not be stale for debits). |
| Multi-currency | Per-currency system accounts, an FX conversion account pair, and amounts tagged with currency. |
| Multi-region | Pin each account to a home region; a cross-region payment becomes a saga between regional ledgers. |
| Kafka | 3+ brokers, `replication.factor=3`, `min.insync.replicas=2`, more partitions (planned ahead, because repartitioning breaks key ordering). |
