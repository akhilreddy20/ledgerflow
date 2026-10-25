/**
 * Integration tests against a real PostgreSQL. They prove the concurrency claims:
 * no double-spends, no deadlocks, exactly-once idempotency, and zero balance drift.
 *
 *   TEST_DATABASE_URL=postgres://ledgerflow:ledgerflow@localhost:5432/ledger_test npm run test:integration
 */
import { join } from 'path';
import { Pool } from 'pg';
import { BusinessError, createPool, runMigrations, withTransaction } from '@ledgerflow/common';
import { HOLDS_ACCOUNT_ID, LedgerService } from '../src/ledger.service';

const url = process.env.TEST_DATABASE_URL;
const describeDb = url ? describe : describe.skip;

describeDb('LedgerService (PostgreSQL)', () => {
  let pool: Pool;
  let ledger: LedgerService;

  beforeAll(async () => {
    pool = createPool(url!, 40);
    await pool.query('DROP SCHEMA public CASCADE; CREATE SCHEMA public;');
    await runMigrations(pool, join(__dirname, '..', 'migrations'));
    ledger = new LedgerService(pool);
  });

  afterAll(async () => {
    await pool.end();
  });

  const newAccount = async (balance = 0, name = 'acct') => {
    const a = await ledger.createAccount(name, 'INR', 'user');
    if (balance > 0) await ledger.deposit(a.id, balance, `seed-${a.id}`);
    return a.id as string;
  };
  const balanceOf = async (id: string) => (await ledger.getAccount(id)).balance as number;
  let seq = 0;
  const key = () => `k-${Date.now()}-${seq++}`;

  it('deposits and transfers with double-entry postings', async () => {
    const a = await newAccount(10_000);
    const b = await newAccount();
    const res = await ledger.transfer(a, b, 2_500, key());
    expect(res.duplicate).toBe(false);
    expect(await balanceOf(a)).toBe(7_500);
    expect(await balanceOf(b)).toBe(2_500);
    const tx = await ledger.getTransaction(res.transactionId);
    expect(tx.entries.reduce((s: number, e: any) => s + Number(e.amount), 0)).toBe(0);
  });

  it('rejects overdrafts', async () => {
    const a = await newAccount(100);
    const b = await newAccount();
    await expect(ledger.transfer(a, b, 101, key())).rejects.toMatchObject({ code: 'INSUFFICIENT_FUNDS' });
    expect(await balanceOf(a)).toBe(100);
  });

  it('prevents double-spends under 50 concurrent debits', async () => {
    const a = await newAccount(1_000);
    const b = await newAccount();
    const results = await Promise.allSettled(Array.from({ length: 50 }, () => ledger.transfer(a, b, 100, key())));
    const ok = results.filter((r) => r.status === 'fulfilled').length;
    const rejected = results.filter((r) => r.status === 'rejected') as PromiseRejectedResult[];
    expect(ok).toBe(10); // exactly 1000 / 100
    expect(rejected.every((r) => r.reason instanceof BusinessError && r.reason.code === 'INSUFFICIENT_FUNDS')).toBe(true);
    expect(await balanceOf(a)).toBe(0);
    expect(await balanceOf(b)).toBe(1_000);
  });

  it('never deadlocks on opposite-direction transfers (deterministic lock ordering)', async () => {
    const a = await newAccount(100_000);
    const b = await newAccount(100_000);
    const ops = Array.from({ length: 200 }, (_, i) => (i % 2 ? ledger.transfer(a, b, 7, key()) : ledger.transfer(b, a, 5, key())));
    const results = await Promise.allSettled(ops);
    const failures = results.filter((r) => r.status === 'rejected') as PromiseRejectedResult[];
    expect(failures.map((f) => f.reason?.message)).toEqual([]);
    expect((await balanceOf(a)) + (await balanceOf(b))).toBe(200_000);
  });

  it('applies the same idempotency key exactly once, even concurrently', async () => {
    const a = await newAccount(1_000);
    const b = await newAccount();
    const k = key();
    const results = await Promise.all(Array.from({ length: 10 }, () => ledger.transfer(a, b, 300, k)));
    expect(new Set(results.map((r) => r.transactionId)).size).toBe(1);
    expect(results.filter((r) => !r.duplicate)).toHaveLength(1);
    expect(await balanceOf(b)).toBe(300);
  });

  it('detects an idempotency key reused for a different operation', async () => {
    const a = await newAccount(1_000);
    const b = await newAccount();
    const k = key();
    await ledger.transfer(a, b, 100, k);
    await expect(ledger.transfer(a, b, 999, k)).rejects.toMatchObject({ code: 'IDEMPOTENCY_KEY_REUSED' });
  });

  it('holds, captures and releases funds for the payment saga', async () => {
    const payer = await newAccount(5_000);
    const payee = await newAccount();
    const p1 = '11111111-1111-4111-8111-' + String(Date.now()).padStart(12, '0').slice(-12);
    const p2 = '22222222-2222-4222-8222-' + String(Date.now()).padStart(12, '0').slice(-12);

    await withTransaction(pool, (tx) => ledger.holdFunds(tx, p1, payer, 1_000, payee));
    await withTransaction(pool, (tx) => ledger.holdFunds(tx, p1, payer, 1_000, payee)); // duplicate command
    await withTransaction(pool, (tx) => ledger.holdFunds(tx, p2, payer, 2_000, payee));
    expect(await balanceOf(payer)).toBe(2_000);

    await withTransaction(pool, (tx) => ledger.captureHold(tx, p1, payee));
    await withTransaction(pool, (tx) => ledger.captureHold(tx, p1, payee)); // duplicate command
    await withTransaction(pool, (tx) => ledger.releaseHold(tx, p2));
    expect(await balanceOf(payee)).toBe(1_000);
    expect(await balanceOf(payer)).toBe(4_000);
    await expect(withTransaction(pool, (tx) => ledger.releaseHold(tx, p1))).rejects.toMatchObject({
      code: 'HOLD_ALREADY_CAPTURED',
    });
    expect((await ledger.getAccount(HOLDS_ACCOUNT_ID)).balance).toBe(0);
  });

  it('rejects a hold for an unknown payee up-front', async () => {
    const payer = await newAccount(5_000);
    await expect(
      withTransaction(pool, (tx) =>
        ledger.holdFunds(tx, '33333333-3333-4333-8333-333333333333', payer, 10, '44444444-4444-4444-8444-444444444444'),
      ),
    ).rejects.toMatchObject({ code: 'PAYEE_NOT_FOUND' });
  });

  it('keeps the journal append-only', async () => {
    await expect(pool.query('UPDATE journal_entries SET amount = 1')).rejects.toThrow(/append-only/);
    await expect(pool.query('DELETE FROM ledger_transactions')).rejects.toThrow(/append-only/);
  });

  it('has zero balance drift after all of the above', async () => {
    const report = await ledger.checkInvariants();
    expect(report.checks.filter((c) => !c.ok)).toEqual([]);
    expect(report.ok).toBe(true);
  });
});
