# LedgerFlow: a distributed payments ledger

LedgerFlow is an event-driven payments platform built from NestJS services. It covers the hard parts of moving money between services:

- correctness when many requests run at once
- requests that are safe to retry
- reliable messaging
- multi-step workflows that can be undone
- graceful handling of failures in an external dependency
- observability

| | |
|---|---|
| **Stack** | TypeScript · NestJS · PostgreSQL · Apache Kafka · Redis · Docker · Kubernetes (+ KEDA) · OpenTelemetry · Prometheus · Grafana · k6 |
| **Services** | `ledger` (double-entry books) · `payments` (saga orchestrator) · `notifications` (downstream consumer) · `psp-simulator` (fake external bank/PSP that you can make fail on purpose) |
| **Patterns** | Double-entry ledger · `SELECT … FOR UPDATE` with deterministic lock ordering · idempotency keys (Redis + DB constraint) · transactional outbox · idempotent consumer (inbox) · orchestrated saga with compensation · retries with exponential backoff + jitter · circuit breaker · dead-letter topics with replay · Kafka partitioning by account · lag-based autoscaling |

---

## Architecture

```mermaid
flowchart LR
    client([Client / k6])

    subgraph payments-service
      PAPI[REST API<br/>POST /payments]
      SAGA[Saga orchestrator<br/>+ recovery job]
      PDB[(payments DB<br/>payments · outbox · inbox)]
    end

    subgraph ledger-service
      LAPI[REST API<br/>accounts · transfers]
      LCMD[Command consumer]
      LDB[(ledger DB<br/>accounts · journal_entries<br/>holds · outbox · inbox)]
    end

    subgraph notifications-service
      NCON[Event consumer]
      NDB[(notifications DB)]
    end

    PSP[[psp-simulator<br/>external payout API]]
    REDIS[(Redis<br/>idempotency keys)]

    subgraph Kafka
      T1{{ledger.commands<br/>key = accountId}}
      T2{{ledger.events}}
      T3{{payments.events}}
      DLQ{{*.dlq}}
    end

    client -->|Idempotency-Key| PAPI
    client -->|Idempotency-Key| LAPI
    PAPI --> REDIS
    LAPI --> REDIS
    PAPI -->|tx: payment + outbox| PDB
    PDB -. outbox relay .-> T1
    T1 --> LCMD
    LCMD -->|tx: inbox + ledger + outbox| LDB
    LDB -. outbox relay .-> T2
    T2 --> SAGA
    SAGA -->|timeout · retry · breaker| PSP
    SAGA --> PDB
    PDB -. outbox relay .-> T3
    T3 --> NCON --> NDB
    LCMD -. poison messages .-> DLQ
    SAGA -. poison messages .-> DLQ
```

### Payment workflow (the saga)

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant P as payments
    participant K as Kafka
    participant L as ledger
    participant S as PSP

    C->>P: POST /payments (Idempotency-Key)
    P->>P: TX { insert payment PENDING + outbox HoldFunds }
    P-->>C: 202 Accepted {status: PENDING}
    P--)K: relay publishes HoldFunds (key = payer account)
    K--)L: HoldFunds
    L->>L: TX { inbox, lock payer row, payer → holds account, outbox FundsHeld }
    L--)K: FundsHeld
    K--)P: FundsHeld
    P->>P: PENDING → FUNDS_HELD
    P->>S: POST /v1/payouts (Idempotency-Key = paymentId)
    alt PSP succeeded
        P->>P: TX { FUNDS_HELD → CAPTURING + outbox CaptureHold }
        K--)L: CaptureHold → holds account → payee
        L--)P: HoldCaptured
        P->>P: CAPTURING → COMPLETED (+ PaymentCompleted)
    else PSP declined, or saga timed out and PSP confirms no payout
        P->>P: TX { FUNDS_HELD → COMPENSATING + outbox ReleaseHold }
        K--)L: ReleaseHold → holds account → payer
        L--)P: HoldReleased
        P->>P: COMPENSATING → FAILED (+ PaymentFailed)
    end
    K--)C: notifications-service emails payer / payee
```

The step-by-step version, with the relevant code for each step, is in [docs/WORKFLOW.md](docs/WORKFLOW.md).

---

## Quick start (Docker)

Prerequisites: Docker Desktop (or Docker Engine with Compose v2), Node.js 20+, and optionally [k6](https://k6.io/docs/get-started/installation/).

```bash
git clone <your-repo-url> ledgerflow && cd ledgerflow
npm install                       # needed for the e2e / DLQ scripts on your host
docker compose up -d --build      # builds 4 service images + starts Postgres, Redis, Kafka, Jaeger, Prometheus, Grafana
docker compose ps                 # wait until ledger and payments show "healthy" (~30-60s on first run)
npm run e2e                       # end-to-end walkthrough: every check should be ✔
```

| URL | What |
|---|---|
| http://localhost:3001 | ledger API |
| http://localhost:3002 | payments API |
| http://localhost:3003 | notifications API |
| http://localhost:3004 | PSP simulator (`GET/POST /admin/chaos`) |
| http://localhost:16686 | Jaeger: distributed traces (pick `payments-service`) |
| http://localhost:3000 | Grafana: *LedgerFlow overview* dashboard (no login) |
| http://localhost:9090 | Prometheus |
| http://localhost:8080 | Kafka UI: topics, partitions, consumer lag, DLQ contents |

### Try it by hand

```bash
# 1. two accounts
PAYER=$(curl -s localhost:3001/accounts -H 'content-type: application/json' -d '{"name":"Akhil"}' | jq -r .id)
SHOP=$(curl -s localhost:3001/accounts -H 'content-type: application/json' -d '{"name":"Chai Point","kind":"merchant"}' | jq -r .id)

# 2. top up ₹1,000 (amounts are in paise)
curl -s localhost:3001/accounts/$PAYER/deposits -H 'content-type: application/json' \
     -H "Idempotency-Key: topup-$PAYER" -d '{"amount":100000}' | jq

# 3. pay ₹250 - run this twice: the second call returns the SAME payment (Idempotent-Replayed: true)
curl -si localhost:3002/payments -H 'content-type: application/json' -H 'Idempotency-Key: order-0001' \
     -d "{\"payerAccountId\":\"$PAYER\",\"payeeAccountId\":\"$SHOP\",\"amount\":25000}"

# 4. watch it move through the saga
curl -s localhost:3002/payments/<payment-id> | jq '.status, .history'

# 5. check the books
npm run invariants
```

The PSP simulator treats some amounts as test triggers, like Stripe's test cards:

| amount ends in | behaviour |
|---|---|
| `…402` (e.g. 1402) | always **declined** → compensation → `FAILED`, funds released |
| `…503` (e.g. 1503) | always **503** → retries → circuit breaker opens → saga timeout → reconcile → compensation |
| `…504` (e.g. 1504) | processed but **slow** (5s > 2s client timeout) → retry with the same key returns the stored result → paid **once** |

You can also inject random failures at runtime: `curl -XPOST localhost:3004/admin/chaos -H 'content-type: application/json' -d '{"failureRate":0.3}'`.

---

## Local development (services on your machine)

This is faster to iterate on, and you get breakpoints and logs in your terminal.

```bash
docker compose up -d postgres redis kafka jaeger prometheus grafana kafka-ui   # infra only
npm install
npm run build:watch               # terminal 1 - incremental TypeScript build

export KAFKA_BROKERS=localhost:29092
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318   # optional - enables tracing
OTEL_SERVICE_NAME=ledger-service        npm run start:ledger          # terminal 2
OTEL_SERVICE_NAME=payments-service      npm run start:payments        # terminal 3
OTEL_SERVICE_NAME=notifications-service npm run start:notifications   # terminal 4
npm run start:psp                                                     # terminal 5
```

Each service applies its own SQL migrations on boot (`services/*/migrations`) and creates the Kafka topics if they are missing.

## Tests

```bash
npm test                          # unit: circuit breaker, backoff/jitter, idempotency store
TEST_DATABASE_URL=postgres://ledgerflow:ledgerflow@localhost:5432/ledger_test npm run test:integration
#   (create it once: docker compose exec postgres createdb -U ledgerflow ledger_test)
#   proves: no double-spend with 50 concurrent debits, no deadlocks with 200 opposite transfers,
#   exactly-once idempotency under concurrency, append-only journal, zero drift
npm run e2e                       # full system, against the running stack
npm run e2e -- --chaos            # + PSP outage scenario (see docs/WORKFLOW.md for the env vars)
```

## Load testing

```bash
k6 run load/transfers.js                          # 1500 TPS for 60s against the ledger, then verifies invariants
k6 run -e RATE=500 -e DURATION=30s load/transfers.js
k6 run -e RATE=200 load/payments.js               # async saga throughput; then: npm run invariants
```

> **Record your own numbers.** Throughput depends on your hardware and Docker resources. Use what k6 prints (`http_reqs` rate, `p(99)`) in your resume and README, and keep the run's summary as evidence. [docs/LOAD_TESTING.md](docs/LOAD_TESTING.md) explains how to tune for the target and how to report results honestly.

## Kubernetes

```bash
kind create cluster --name ledgerflow                      # or: minikube start --cpus 4 --memory 8g
for s in ledger payments notifications psp-simulator; do
  docker build --build-arg SERVICE=$s -t ledgerflow/$s:local . && kind load docker-image ledgerflow/$s:local --name ledgerflow
done
helm repo add kedacore https://kedacore.github.io/charts && helm install keda kedacore/keda -n keda --create-namespace
kubectl apply -k k8s/infra                                  # dev Postgres, Redis, Kafka, Jaeger
kubectl -n ledgerflow rollout status statefulset/kafka
kubectl apply -k k8s/base                                   # the 4 services + KEDA ScaledObjects
kubectl -n ledgerflow port-forward svc/payments 3002 & kubectl -n ledgerflow port-forward svc/ledger 3001 &
```

Consumers autoscale on **Kafka consumer-group lag**, with CPU as a second trigger. Details are in [docs/KUBERNETES.md](docs/KUBERNETES.md).

## Operations

```bash
npm run dlq:replay -- --topic ledger.events.dlq --list     # what is parked and why
npm run dlq:replay -- --topic ledger.events.dlq            # replay after fixing the cause (safe: consumers are idempotent)
curl localhost:3002/admin/circuit-breaker                  # PSP breaker state
curl -XPOST localhost:3002/admin/saga/recover              # run a saga recovery pass now
curl localhost:3002/admin/stats                            # payments by status
```

## Verified behaviour

The full suite was run during development against PostgreSQL 16, Redis 7 and a Kafka-protocol-compatible broker:

- 13 unit tests and 10 PostgreSQL integration tests pass. These include 50 concurrent debits giving exactly 10 successes, and 200 opposite-direction transfers giving 0 deadlocks.
- `npm run e2e -- --chaos` passes all checks: happy path, idempotent replay, key reuse (422), PSP decline with compensation, insufficient funds, a slow PSP paid exactly once, and a PSP outage where the breaker opens, the saga times out, the payout is reconciled and the hold is compensated.
- Poison messages land in the DLQ, and `dlq:replay` lists and replays them.
- One OpenTelemetry trace covers `POST /payments` → ledger `HoldFunds` → saga `FundsHeld` → `psp.payout` → ledger `CaptureHold` → saga `HoldCaptured`.
- Ad-hoc load (2 shared vCPUs): about 400 transfers/s with 0 errors and zero balance drift. See [docs/LOAD_TESTING.md](docs/LOAD_TESTING.md). Run k6 on your machine for your own numbers.

## Repository layout

```
libs/common/            shared building blocks (read these first)
  src/outbox.ts         transactional outbox + relay (FOR UPDATE SKIP LOCKED)
  src/inbox.ts          idempotent consumer
  src/consumer.ts       Kafka consumer runner: retries, backoff+jitter, DLQ, tracing
  src/idempotency.ts    Idempotency-Key handling (Redis) + @Idempotent() decorator
  src/circuit-breaker.ts, src/retry.ts, src/db.ts, src/otel.ts, src/core.module.ts
services/ledger/        double-entry ledger: src/ledger.service.ts is the heart of the project
services/payments/      saga orchestrator: src/payment.saga.ts, src/psp.client.ts
services/notifications/ downstream consumer
services/psp-simulator/ controllable fake PSP
infra/                  Postgres init, Prometheus, Grafana provisioning + dashboard
k8s/base, k8s/infra     Kubernetes manifests (kustomize) + KEDA autoscaling
load/                   k6 scripts
scripts/                e2e walkthrough, DLQ replay, invariant checker
docs/                   WORKFLOW, DESIGN (trade-offs), BUILD_GUIDE, INTERVIEW_PREP, LOAD_TESTING, KUBERNETES
```

## Documentation

1. [docs/WORKFLOW.md](docs/WORKFLOW.md): how a request flows through the system, step by step, with the code for each step.
2. [docs/DESIGN.md](docs/DESIGN.md): design decisions, trade-offs, and what you would change at 10x scale.
3. [docs/BUILD_GUIDE.md](docs/BUILD_GUIDE.md): a phased plan to rebuild or extend this yourself, with a checkpoint per phase.
4. [docs/INTERVIEW_PREP.md](docs/INTERVIEW_PREP.md): each resume bullet mapped to code, plus likely interview questions and answers.
5. [docs/LOAD_TESTING.md](docs/LOAD_TESTING.md) and [docs/KUBERNETES.md](docs/KUBERNETES.md).
