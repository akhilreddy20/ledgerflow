# Interview prep: defending every resume bullet

**Rule: if you cannot explain a bullet in depth, remove it.** Before you put this project on your resume, read the code behind each bullet, run the tests, break things on purpose (see §3), and replace the numbers with ones from your own load tests.

---

## 1. Resume bullet → code → what to say

**"Built an event-driven payments platform as independent NestJS services (Payments, Ledger, Notifications) communicating over Kafka, with a double-entry ledger in PostgreSQL as the single source of truth for account balances."**
- Code: `services/*`, `libs/common/src/events.ts`, `services/ledger/migrations/001_init.sql`
- Say: database-per-service; the ledger only moves money through balanced postings; the journal is append-only (enforced by triggers); every balance can be rebuilt from the journal.

**"Prevented double-spends under concurrent transfers using row-level locking (SELECT … FOR UPDATE) with deterministic lock ordering to avoid deadlocks, and an append-only ledger whose debits and credits always net to zero."**
- Code: `LedgerService.post()` in `services/ledger/src/ledger.service.ts`
- Proof: `services/ledger/test/ledger.int-spec.ts`. With 50 concurrent ₹100 debits on a ₹1,000 wallet, exactly 10 succeed. 200 opposite-direction transfers produce 0 deadlocks (check with `SELECT deadlocks FROM pg_stat_database`).
- Say: the check happens on the *locked* row, so check-then-act is atomic. Locks are taken in id order, so the waits-for graph can't have a cycle. The DB `CHECK (balance >= 0)` is defence in depth.

**"Made the write API safe to retry with idempotency keys (Redis fast-path backed by a PostgreSQL unique constraint), so client and network retries never create duplicate charges."**
- Code: `libs/common/src/idempotency.ts`, `payments.service.ts`, `create()` (`ON CONFLICT DO NOTHING`)
- Say: Redis replays the original response and blocks concurrent duplicates. The DB unique key is the source of truth if Redis loses data. The fingerprint detects a key reused with a different body (422).

**"Guaranteed reliable event delivery with the transactional outbox pattern, and coordinated multi-step payment flows with an orchestrated saga that runs compensating transactions on partial failure."**
- Code: `libs/common/src/outbox.ts`, `libs/common/src/inbox.ts`, `services/payments/src/payment.saga.ts`
- Say: the dual-write problem; at-least-once delivery plus idempotent consumers gives exactly-once *effects*; the state machine; hold → release as compensation; never compensating blindly after a timeout.

**"Hardened calls to an external payment-provider simulator with timeouts, exponential backoff with jitter, a circuit breaker, and a dead-letter topic with replay tooling for failed events."**
- Code: `services/payments/src/psp.client.ts`, `libs/common/src/circuit-breaker.ts`, `libs/common/src/retry.ts`, `libs/common/src/consumer.ts`, `scripts/dlq-replay.js`
- Say: which errors are retryable; why jitter; the breaker's states; why a decline must not trip the breaker; why DLQ replay is safe (idempotent consumers).

**"Partitioned Kafka topics by account ID to preserve per-account ordering while scaling consumers horizontally; ran on Kubernetes with autoscaling on consumer lag."**
- Code: `enqueue(…, key = payerAccountId, …)`, `k8s/base/keda-scaledobjects.yaml`
- Say: same key → same partition → one consumer → ordered. Max useful replicas = partitions. Lag is a better scaling signal than CPU for I/O-bound consumers.

**"Instrumented services with OpenTelemetry distributed tracing and Prometheus/Grafana dashboards, and load-tested with k6 to sustain [N] TPS at P99 under [M] ms with zero balance drift."**
- Code: `libs/common/src/otel.ts`, `libs/common/src/tracing.ts`, `libs/common/src/metrics.ts`, `load/transfers.js`
- Say: manual trace propagation through Kafka headers (and why the kafkajs auto-instrumentation is disabled: it would stamp the outbox relay's context instead of the original request's); what "zero drift" means (the `/admin/invariants` checks). **Use your own measured N and M.**

---

## 2. Questions you should be able to answer

**Concurrency and data**
1. What happens if two requests debit the same account at the same time? Walk through the locks.
2. Why can't A→B and B→A deadlock? What would happen without the sort?
3. Why not `SERIALIZABLE`? Why not optimistic locking? When would you choose each?
4. The `system:holds` account takes part in every payment. Why doesn't it become a bottleneck? (It isn't locked and its balance is derived.) What do you do about a hot *merchant*? (Sharded sub-accounts.)
5. Why store money as `BIGINT` paise? What goes wrong with floats?
6. How would you fix a wrong ledger entry? (A compensating entry, never an UPDATE.)

**Messaging**
7. What is the dual-write problem? Why doesn't "commit to DB, then publish" work? Why doesn't "publish, then commit" work?
8. Outbox vs CDC (Debezium): trade-offs?
9. The relay published but crashed before marking the row. What happens downstream?
10. How do you get "exactly once"? (You don't, for delivery; you get exactly-once *effects* from at-least-once delivery plus idempotency.)
11. How is ordering preserved? When could it break? (Multiple relays, retry topics, repartitioning.)
12. Consumer group rebalance mid-message: what happens?
13. Why does the consumer retry in place instead of moving on? What's the cost? (Head-of-line blocking.)

**Saga**
14. Orchestration vs choreography: why orchestration for payments?
15. What is a compensating transaction? Can every step be compensated? (A capture can't be undone by a release; it needs a refund.)
16. The PSP call timed out. Did the payout happen? How do you find out, and why not just release the hold?
17. A payment is stuck in `CAPTURING`. Why, and what recovers it?
18. Two replicas both run the recovery job. Why don't they double-process? (`SKIP LOCKED` + lease + guarded transitions.)

**Resilience**
19. Circuit breaker states and transitions. Why count only some errors?
20. Why full jitter? What is the thundering-herd problem?
21. What goes to the DLQ, and how do you safely replay it?

**Scale and operations**
22. How does autoscaling work here? Why lag rather than CPU? Why `maxReplicaCount = 6`?
23. What are your SLOs, and what would you alert on?
24. How would you scale this 10x? 100x? (See DESIGN.md §9.)
25. How did you load test, and what was the bottleneck? (Answer from *your* runs: usually Postgres connections or lock contention on a few accounts.)

---

## 3. Drills: break it on purpose (do these before the interview)

| Drill | How | Expect |
|---|---|---|
| PSP outage | `curl -XPOST localhost:3004/admin/chaos -d '{"failureRate":1}' -H 'content-type: application/json'`, then create payments | Retries, then the breaker opens; payments stuck in `FUNDS_HELD`, then compensated after the saga timeout |
| Kill the ledger mid-load | `docker compose kill ledger` during `k6 run load/payments.js`, then `docker compose up -d ledger` | Lag grows, then drains; all payments finish; `npm run invariants` is clean |
| Duplicate storm | Send the same `POST /payments` 20 times concurrently with one key | 1 payment; the rest are 409 (in progress) or replays |
| Poison message | `docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic ledger.commands` and type `not json` | Lands in `ledger.commands.dlq`; the partition keeps flowing |
| DLQ replay | `npm run dlq:replay -- --topic ledger.events.dlq` | Replayed messages are no-ops if they were already applied |
| Trace a payment | Jaeger → service `payments-service` → `POST /payments` | One trace spanning both services and Kafka |

## 4. Your 60-second pitch

> "I built LedgerFlow to go deep on distributed-systems correctness in payments. A payout touches our ledger and an external PSP, which can't share one transaction, so I used an orchestrated saga: hold funds, call the PSP, then capture, or release as compensation. Every handoff goes through a transactional outbox to Kafka, and every consumer is idempotent, so delivery is at-least-once but the effects are exactly-once. Inside the ledger, double-spends are prevented with row locks taken in a deterministic order, so it can't deadlock. I load-tested it with k6 at [N] TPS, p99 [M] ms, and after every run an invariant check proves the books balance to the paisa. The part I'm proudest of is the timeout case: if the PSP doesn't answer, we don't know whether it paid, so the saga reconciles with the PSP before compensating instead of releasing funds blindly."
