#!/usr/bin/env node
/**
 * End-to-end walkthrough of the whole system. Run it with the stack up:
 *
 *   npm run e2e                 # happy path, decline, insufficient funds, idempotency, PSP timeout
 *   npm run e2e -- --chaos      # also: PSP outage -> circuit breaker -> saga timeout -> compensation
 *                               # (start payments with SAGA_TIMEOUT_MS=20000 SAGA_STUCK_AFTER_MS=3000
 *                               #  SAGA_RECOVERY_INTERVAL_MS=2000 so it finishes in ~30s)
 *
 * Exits non-zero if any expectation fails, so it doubles as a smoke test in CI.
 */
const { randomUUID } = require('crypto');

const LEDGER = process.env.LEDGER_URL ?? 'http://localhost:3001';
const PAYMENTS = process.env.PAYMENTS_URL ?? 'http://localhost:3002';
const NOTIFICATIONS = process.env.NOTIFICATIONS_URL ?? 'http://localhost:3003';
const CHAOS = process.argv.includes('--chaos');

const c = { green: '\x1b[32m', red: '\x1b[31m', dim: '\x1b[2m', bold: '\x1b[1m', reset: '\x1b[0m' };
let failures = 0;
const step = (title) => console.log(`\n${c.bold}▶ ${title}${c.reset}`);
const ok = (msg) => console.log(`  ${c.green}✔${c.reset} ${msg}`);
const bad = (msg) => {
  failures++;
  console.log(`  ${c.red}✘ ${msg}${c.reset}`);
};
const expect = (cond, msg) => (cond ? ok(msg) : bad(msg));
const rupees = (p) => `₹${(p / 100).toFixed(2)}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function call(method, url, body, key) {
  const res = await fetch(url, {
    method,
    headers: { 'content-type': 'application/json', ...(key ? { 'idempotency-key': key } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  return { status: res.status, headers: res.headers, body: text ? JSON.parse(text) : null };
}

async function waitFor(paymentId, statuses, timeoutMs = 60_000) {
  const start = Date.now();
  let p;
  while (Date.now() - start < timeoutMs) {
    p = (await call('GET', `${PAYMENTS}/payments/${paymentId}`)).body;
    if (statuses.includes(p.status)) return p;
    await sleep(200);
  }
  return p;
}

const balance = async (id) => (await call('GET', `${LEDGER}/accounts/${id}`)).body.balance;

async function pay(payer, payee, amount, key = randomUUID()) {
  const res = await call('POST', `${PAYMENTS}/payments`, { payerAccountId: payer, payeeAccountId: payee, amount }, key);
  if (res.status !== 202) throw new Error(`create payment -> ${res.status} ${JSON.stringify(res.body)}`);
  return { key, payment: res.body };
}

async function main() {
  step('Health checks');
  for (const [name, url] of [['ledger', LEDGER], ['payments', PAYMENTS], ['notifications', NOTIFICATIONS]]) {
    const r = await call('GET', `${url}/ready`).catch((e) => ({ status: e.message }));
    expect(r.status === 200, `${name} ready (${url})`);
  }
  if (failures) throw new Error('services not ready - is the stack running?');

  step('Create a customer wallet and a merchant, top up the wallet with ₹1,000');
  const payer = (await call('POST', `${LEDGER}/accounts`, { name: 'Akhil (customer)' })).body.id;
  const merchant = (await call('POST', `${LEDGER}/accounts`, { name: 'Chai Point (merchant)', kind: 'merchant' })).body.id;
  await call('POST', `${LEDGER}/accounts/${payer}/deposits`, { amount: 100_000 }, `topup-${payer}`);
  expect((await balance(payer)) === 100_000, `customer balance ${rupees(await balance(payer))}`);

  step('1. Happy path: pay ₹250 -> HoldFunds -> PSP payout -> CaptureHold -> COMPLETED');
  const a = await pay(payer, merchant, 25_000);
  ok(`accepted with 202, status ${a.payment.status} (id ${a.payment.id})`);
  const done = await waitFor(a.payment.id, ['COMPLETED', 'FAILED']);
  expect(done.status === 'COMPLETED', `final status ${done.status}`);
  console.log(`  ${c.dim}history: ${done.history.map((h) => h.to).join(' -> ')}${c.reset}`);

  step('2. Idempotency: retry the exact same request with the same Idempotency-Key');
  const replay = await call('POST', `${PAYMENTS}/payments`, { payerAccountId: payer, payeeAccountId: merchant, amount: 25_000 }, a.key);
  expect(replay.body.id === a.payment.id, 'same payment id returned - no second charge');
  expect(replay.headers.get('idempotent-replayed') === 'true', 'served from the idempotency store (Idempotent-Replayed: true)');
  const misuse = await call('POST', `${PAYMENTS}/payments`, { payerAccountId: payer, payeeAccountId: merchant, amount: 99 }, a.key);
  expect(misuse.status === 422, `same key + different body -> ${misuse.status} (rejected)`);

  step('3. PSP declines (magic amount ₹14.02) -> compensation releases the hold');
  const before3 = await balance(payer);
  const d = await pay(payer, merchant, 1_402);
  const declined = await waitFor(d.payment.id, ['COMPLETED', 'FAILED']);
  expect(declined.status === 'FAILED', `final status ${declined.status}: ${declined.failureReason}`);
  console.log(`  ${c.dim}history: ${declined.history.map((h) => h.to).join(' -> ')}${c.reset}`);
  expect((await balance(payer)) === before3, 'customer balance unchanged - held funds were returned');

  step('4. Insufficient funds -> ledger rejects the hold -> FAILED, nothing moves');
  const poor = await pay(payer, merchant, 10_000_000);
  const rejected = await waitFor(poor.payment.id, ['COMPLETED', 'FAILED']);
  expect(rejected.status === 'FAILED' && /INSUFFICIENT_FUNDS/.test(rejected.failureReason), `FAILED: ${rejected.failureReason}`);

  step('5. PSP is slow (magic amount ₹15.04): client times out, retries with the same key, paid exactly once');
  const s = await pay(payer, merchant, 1_504);
  const slow = await waitFor(s.payment.id, ['COMPLETED', 'FAILED'], 90_000);
  expect(slow.status === 'COMPLETED', `final status ${slow.status} after ${slow.pspAttempts} PSP attempt(s)`);

  if (CHAOS) {
    step('6. PSP outage (magic amount ₹15.03 always 503): retries -> circuit breaker -> saga timeout -> compensation');
    const before6 = await balance(payer);
    const o = await pay(payer, merchant, 1_503);
    const held = await waitFor(o.payment.id, ['FUNDS_HELD']);
    ok(`funds held, PSP failing... (status ${held.status})`);
    const breaker = await call('GET', `${PAYMENTS}/admin/circuit-breaker`);
    console.log(`  ${c.dim}circuit breaker: ${JSON.stringify(breaker.body)}${c.reset}`);
    const out = await waitFor(o.payment.id, ['COMPLETED', 'FAILED'], 120_000);
    expect(out.status === 'FAILED', `final status ${out.status}: ${out.failureReason}`);
    expect((await balance(payer)) === before6, 'customer made whole by the compensating ReleaseHold');
  }

  step('Final balances');
  const expectedMerchant = 25_000 + 1_504;
  const pb = await balance(payer);
  const mb = await balance(merchant);
  expect(pb === 100_000 - expectedMerchant, `customer ${rupees(pb)}`);
  expect(mb === expectedMerchant, `merchant ${rupees(mb)}`);

  step('Notifications (consumed from payments.events)');
  await sleep(1000);
  const notes = (await call('GET', `${NOTIFICATIONS}/notifications?paymentId=${a.payment.id}`)).body;
  expect(notes.length === 2, `${notes.length} notifications for payment 1`);
  for (const n of notes) console.log(`  ${c.dim}${n.message}${c.reset}`);

  step('Ledger invariants (double-entry books must balance)');
  const inv = (await call('GET', `${LEDGER}/admin/invariants`)).body;
  for (const chk of inv.checks) expect(chk.ok, chk.name);

  console.log(failures ? `\n${c.red}${failures} check(s) failed${c.reset}` : `\n${c.green}${c.bold}All checks passed${c.reset}`);
  process.exit(failures ? 1 : 0);
}

main().catch((err) => {
  console.error(`${c.red}${err.stack}${c.reset}`);
  process.exit(1);
});
