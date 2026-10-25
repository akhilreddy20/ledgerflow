# Load testing

## What to measure

| Test | Command | What it proves |
|---|---|---|
| Ledger transfers (sync) | `k6 run load/transfers.js` | Raw ledger write throughput and latency under row-lock contention; **zero drift** (teardown runs the invariant check and fails the run if the books don't balance) |
| Payment saga (async) | `k6 run -e RATE=200 load/payments.js` | API acceptance latency; end-to-end saga throughput (watch the Grafana *Payment saga transitions* panel and `GET :3002/admin/stats`) |

## Procedure

1. Use a fresh stack: `docker compose down -v && docker compose up -d --build`.
2. Give Docker enough resources: at least 4 CPUs and 8 GB (Docker Desktop → Settings → Resources).
3. Warm up: `k6 run -e RATE=200 -e DURATION=20s load/transfers.js`.
4. Find the knee: run with `RATE=500, 1000, 1500, 2000…` and record the achieved `http_reqs` rate and `p(99)`. The knee is where p99 jumps or `dropped_iterations` appears.
5. Keep the best stable rate where p99 stays below your target and `http_req_failed` is under 1%.
6. After every run: `npm run invariants` must print *Books balance: zero drift.*

## Tuning levers (in order of impact)

1. **Run on your host, not in Docker Desktop's VM.** Start the infra with Docker and the services with `npm run start:*` (see the README).
2. **Run several ledger instances.** Node is single-threaded. In compose: `docker compose up -d --scale ledger=3` (remove the fixed host port first and put a load balancer in front), or run 3 instances on your host on ports 3001, 3011 and 3021.
3. **Connection pool:** `DB_POOL_SIZE` (default 30 for the ledger). Too small and requests queue; too large and Postgres thrashes. Keep the total across replicas under `max_connections` (300 in compose).
4. **More accounts:** `-e ACCOUNTS=1000` reduces lock contention. Testing with *fewer* accounts is the realistic hot-account worst case; report both.
5. **Postgres:** `synchronous_commit=off` is **not** acceptable for a ledger. Don't use it to inflate numbers.

## Reference baseline (from development, not k6)

These came from a quick Node load generator in a **2-vCPU cloud sandbox**, where Postgres, Redis, the Kafka-compatible broker, all four services *and* the load generator shared the same 2 cores. They are not resume numbers. They show that the system is correct under load and give you a floor to compare against:

| Concurrency | Achieved TPS | p50 | p95 | p99 | Errors | Invariants |
|---|---|---|---|---|---|---|
| 16 | ~405 | 38 ms | 62 ms | 75 ms | 0 | ✔ zero drift |
| 64 (CPU saturated) | ~380 | 161 ms | 248 ms | 314 ms | 0 | ✔ zero drift |

On a laptop with 8+ cores, with the services on the host and 2-3 ledger instances, expect several times this. **Measure it with k6 and record it below.**

## Results (fill in with YOUR runs)

| Date | Machine | Setup | Rate target | Achieved TPS | p95 | p99 | Errors | Drift |
|---|---|---|---|---|---|---|---|---|
| | e.g. M2 Pro, 32 GB | compose, 1 ledger | 1500 | | | | | ✔ |
| | | host, 3 ledgers | 1500 | | | | | ✔ |

Write the resume numbers from this table, not from the template. A defensible number such as "800 TPS at p99 95 ms on a laptop, with the bottleneck being lock contention on 200 accounts" is better than an inflated one you can't explain.

## Reading the k6 output

```
http_req_duration{name:transfer}...: avg=12ms  p(95)=38ms  p(99)=71ms
http_reqs..........................: 89912  1498.5/s     <- achieved TPS
dropped_iterations.................: 0                   <- >0 means the target rate was not reached
insufficient_funds.................: 41                  <- expected business rejections (422)
```
