/**
 * k6 load test for the asynchronous payment saga (HTTP -> outbox -> Kafka -> ledger -> PSP -> ledger).
 *
 *   k6 run -e RATE=200 load/payments.js
 *
 * Measures API acceptance latency. Completion is asynchronous: after the run, use
 * `npm run invariants` and GET /admin/stats on the payments service to verify every payment
 * reached COMPLETED or FAILED and the books balance.
 */
import http from 'k6/http';
import { check, fail } from 'k6';
import exec from 'k6/execution';

const LEDGER = __ENV.LEDGER_URL || 'http://localhost:3001';
const PAYMENTS = __ENV.PAYMENTS_URL || 'http://localhost:3002';
const RATE = Number(__ENV.RATE || 200);

export const options = {
  setupTimeout: '120s',
  scenarios: {
    payments: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 100,
      maxVUs: 500,
    },
  },
  thresholds: {
    'http_req_duration{name:create_payment}': ['p(99)<150'],
    'http_req_failed{name:create_payment}': ['rate<0.01'],
  },
};

const headers = (key) => ({ 'Content-Type': 'application/json', 'Idempotency-Key': key });

export function setup() {
  const payers = [];
  for (let i = 0; i < 100; i++) {
    const id = http.post(`${LEDGER}/accounts`, JSON.stringify({ name: `payer-${i}` }), { headers: headers('x') }).json('id');
    http.post(`${LEDGER}/accounts/${id}/deposits`, JSON.stringify({ amount: 100_000_000 }), { headers: headers(`seed-${id}`) });
    payers.push(id);
  }
  const merchant = http.post(`${LEDGER}/accounts`, JSON.stringify({ name: 'merchant', kind: 'merchant' }), { headers: headers('x') });
  if (merchant.status !== 201) fail('merchant create failed');
  return { payers, merchant: merchant.json('id') };
}

export default function (data) {
  const payer = data.payers[Math.floor(Math.random() * data.payers.length)];
  const res = http.post(
    `${PAYMENTS}/payments`,
    JSON.stringify({ payerAccountId: payer, payeeAccountId: data.merchant, amount: 100 + Math.floor(Math.random() * 10_000) }),
    { headers: headers(`k6-pay-${exec.scenario.iterationInTest}-${Date.now()}`), tags: { name: 'create_payment' } },
  );
  check(res, { 'payment accepted (202)': (r) => r.status === 202 });
}
