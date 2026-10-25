/**
 * k6 load test: concurrent wallet-to-wallet transfers through the ledger.
 *
 *   k6 run load/transfers.js                         # defaults: 1500 TPS for 60s
 *   k6 run -e RATE=500 -e DURATION=30s load/transfers.js
 *
 * After the run, teardown() calls /admin/invariants and FAILS the test if the books do not balance.
 * Record the numbers k6 prints (http_reqs rate, p(99)) - those are what go on your resume.
 */
import http from 'k6/http';
import { check, fail } from 'k6';
import { Counter } from 'k6/metrics';
import exec from 'k6/execution';

const LEDGER = __ENV.LEDGER_URL || 'http://localhost:3001';
const RATE = Number(__ENV.RATE || 1500);
const ACCOUNTS = Number(__ENV.ACCOUNTS || 200);
const insufficient = new Counter('insufficient_funds');

export const options = {
  setupTimeout: '120s',
  scenarios: {
    transfers: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: Number(__ENV.VUS || 300),
      maxVUs: Number(__ENV.MAX_VUS || 1000),
    },
  },
  thresholds: {
    'http_req_duration{name:transfer}': ['p(99)<150'],
    'http_req_failed{name:transfer}': ['rate<0.01'],
    checks: ['rate>0.99'],
  },
};

const json = { headers: { 'Content-Type': 'application/json' } };

export function setup() {
  const ids = [];
  for (let i = 0; i < ACCOUNTS; i++) {
    const res = http.post(`${LEDGER}/accounts`, JSON.stringify({ name: `k6-${i}` }), json);
    if (res.status !== 201) fail(`account create failed: ${res.status} ${res.body}`);
    const id = res.json('id');
    const dep = http.post(`${LEDGER}/accounts/${id}/deposits`, JSON.stringify({ amount: 10_000_000 }), {
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `k6-seed-${id}` },
    });
    if (dep.status !== 201) fail(`deposit failed: ${dep.status} ${dep.body}`);
    ids.push(id);
  }
  return { ids };
}

export default function (data) {
  const ids = data.ids;
  const from = ids[Math.floor(Math.random() * ids.length)];
  let to = ids[Math.floor(Math.random() * ids.length)];
  if (to === from) to = ids[(ids.indexOf(from) + 1) % ids.length];
  const key = `k6-${exec.scenario.iterationInTest}-${Date.now()}`;
  const res = http.post(
    `${LEDGER}/transfers`,
    JSON.stringify({ fromAccountId: from, toAccountId: to, amount: 1 + Math.floor(Math.random() * 5000) }),
    { headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, tags: { name: 'transfer' } },
  );
  if (res.status === 422) insufficient.add(1);
  check(res, { 'transfer accepted (201) or business-rejected (422)': (r) => r.status === 201 || r.status === 422 });
}

export function teardown() {
  const res = http.get(`${LEDGER}/admin/invariants`);
  const report = res.json();
  console.log(`Invariants: ${JSON.stringify(report, null, 2)}`);
  if (!report.ok) fail('LEDGER INVARIANTS VIOLATED - balance drift detected');
}
