import { Inject, Injectable } from '@nestjs/common';
import { BusinessError, PG_POOL, Tx, withTransaction } from '@ledgerflow/common';
import { createHash, randomUUID } from 'crypto';
import { Pool } from 'pg';
import { Counter } from 'prom-client';
import { registry } from '@ledgerflow/common';

export const WORLD_ACCOUNT_ID = '00000000-0000-0000-0000-000000000001';
export const HOLDS_ACCOUNT_ID = '00000000-0000-0000-0000-000000000002';

export type AccountKind = 'user' | 'merchant' | 'system';
export type TxType = 'deposit' | 'transfer' | 'hold' | 'capture' | 'release';

export interface Posting {
  accountId: string;
  /** Negative = debit (money leaves), positive = credit (money arrives). Minor units. */
  amount: number;
}

export interface PostRequest {
  idempotencyKey: string;
  type: TxType;
  postings: Posting[];
  metadata?: Record<string, unknown>;
}

export interface PostResult {
  transactionId: string;
  /** true when this idempotency key was already applied and nothing new was written. */
  duplicate: boolean;
}

export interface Hold {
  id: string;
  paymentId: string;
  accountId: string;
  amount: number;
  status: 'HELD' | 'CAPTURED' | 'RELEASED';
}

const ledgerTransactions = new Counter({
  name: 'ledger_transactions_total',
  help: 'Ledger transactions by type and outcome',
  labelNames: ['type', 'outcome'],
  registers: [registry],
});

@Injectable()
export class LedgerService {
  constructor(@Inject(PG_POOL) private readonly pool: Pool) {}

  // ------------------------------------------------------------------ accounts

  async createAccount(name: string, currency: string, kind: Exclude<AccountKind, 'system'>) {
    const { rows } = await this.pool.query(
      `INSERT INTO accounts (id, name, currency, kind) VALUES ($1, $2, $3, $4)
       RETURNING id, name, currency, kind, balance, created_at AS "createdAt"`,
      [randomUUID(), name, currency, kind],
    );
    return rows[0];
  }

  async getAccount(id: string) {
    const { rows } = await this.pool.query(
      `SELECT a.id, a.name, a.currency, a.kind,
              CASE WHEN a.kind = 'system'
                   THEN (SELECT COALESCE(SUM(amount), 0) FROM journal_entries WHERE account_id = a.id)::bigint
                   ELSE a.balance END AS balance,
              (SELECT COALESCE(SUM(amount), 0) FROM holds WHERE account_id = a.id AND status = 'HELD')::bigint AS "heldAmount",
              a.version, a.created_at AS "createdAt"
         FROM accounts a WHERE a.id = $1`,
      [id],
    );
    if (!rows[0]) throw new BusinessError('ACCOUNT_NOT_FOUND', `Account ${id} not found`, 404);
    return rows[0];
  }

  async listEntries(accountId: string, limit: number) {
    await this.getAccount(accountId);
    const { rows } = await this.pool.query(
      `SELECT e.id, e.transaction_id AS "transactionId", t.type, e.amount, e.balance_after AS "balanceAfter",
              t.metadata, e.created_at AS "createdAt"
         FROM journal_entries e JOIN ledger_transactions t ON t.id = e.transaction_id
        WHERE e.account_id = $1
        ORDER BY e.id DESC
        LIMIT $2`,
      [accountId, limit],
    );
    return rows;
  }

  async getTransaction(id: string) {
    const { rows } = await this.pool.query(
      `SELECT t.id, t.type, t.metadata, t.created_at AS "createdAt",
              json_agg(json_build_object('accountId', e.account_id, 'amount', e.amount) ORDER BY e.id) AS entries
         FROM ledger_transactions t JOIN journal_entries e ON e.transaction_id = t.id
        WHERE t.id = $1
        GROUP BY t.id`,
      [id],
    );
    if (!rows[0]) throw new BusinessError('TRANSACTION_NOT_FOUND', `Transaction ${id} not found`, 404);
    return rows[0];
  }

  // ------------------------------------------------------------------ the core: posting a balanced transaction

  /**
   * Atomically post a balanced, idempotent transaction. Must be called inside a DB transaction.
   *
   * Concurrency control (why double-spends are impossible):
   *  1. The idempotency key is inserted FIRST. A concurrent duplicate blocks on the unique index until
   *     the first one commits, then sees the conflict -> returns the original transaction.
   *  2. Every non-system account involved is locked with SELECT ... FOR UPDATE, ordered by id.
   *     Two transfers A->B and B->A both lock A then B, so they can never wait on each other in a cycle
   *     (no deadlocks). While we hold the lock, no one else can read-modify-write that balance.
   *  3. The balance check happens on the locked row, so "check then debit" is race-free.
   *  4. A CHECK constraint (balance >= 0) is the last line of defence if application logic were wrong.
   */
  async post(tx: Tx, req: PostRequest): Promise<PostResult> {
    this.validatePostings(req.postings);
    const requestHash = createHash('sha256')
      .update(JSON.stringify({ type: req.type, postings: req.postings }))
      .digest('hex');

    const transactionId = randomUUID();
    const inserted = await tx.query(
      `INSERT INTO ledger_transactions (id, idempotency_key, request_hash, type, metadata)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (idempotency_key) DO NOTHING`,
      [transactionId, req.idempotencyKey, requestHash, req.type, JSON.stringify(req.metadata ?? {})],
    );
    if (inserted.rowCount === 0) {
      const { rows } = await tx.query(
        'SELECT id, request_hash FROM ledger_transactions WHERE idempotency_key = $1',
        [req.idempotencyKey],
      );
      if (rows[0].request_hash !== requestHash) {
        throw new BusinessError('IDEMPOTENCY_KEY_REUSED', 'Idempotency key was already used for a different operation', 422);
      }
      ledgerTransactions.inc({ type: req.type, outcome: 'duplicate' });
      return { transactionId: rows[0].id, duplicate: true };
    }

    const ids = [...new Set(req.postings.map((p) => p.accountId))];
    const { rows: accounts } = await tx.query<{ id: string; kind: AccountKind; currency: string }>(
      'SELECT id, kind, currency FROM accounts WHERE id = ANY($1::uuid[])',
      [ids],
    );
    const byId = new Map(accounts.map((a) => [a.id, a]));
    for (const id of ids) {
      if (!byId.has(id)) throw new BusinessError('ACCOUNT_NOT_FOUND', `Account ${id} not found`, 404);
    }
    if (new Set(accounts.map((a) => a.currency)).size > 1) {
      throw new BusinessError('CURRENCY_MISMATCH', 'All accounts in a transaction must share one currency', 422);
    }

    // Lock user/merchant accounts in a deterministic order (deadlock-free).
    const lockIds = ids.filter((id) => byId.get(id)!.kind !== 'system').sort();
    const { rows: locked } = await tx.query<{ id: string; balance: number }>(
      'SELECT id, balance FROM accounts WHERE id = ANY($1::uuid[]) ORDER BY id FOR UPDATE',
      [lockIds],
    );
    const balances = new Map(locked.map((r) => [r.id, r.balance]));

    // Net effect per account, then the funds check against the locked balance.
    const net = new Map<string, number>();
    for (const p of req.postings) net.set(p.accountId, (net.get(p.accountId) ?? 0) + p.amount);
    for (const [id, delta] of net) {
      if (!balances.has(id)) continue; // system account
      if (balances.get(id)! + delta < 0) {
        ledgerTransactions.inc({ type: req.type, outcome: 'insufficient_funds' });
        throw new BusinessError(
          'INSUFFICIENT_FUNDS',
          `Account ${id} has ${balances.get(id)} available, needs ${-delta}`,
          422,
        );
      }
    }

    // Append journal entries with the running balance for user/merchant accounts.
    const running = new Map(balances);
    const entryAccount: string[] = [];
    const entryAmount: number[] = [];
    const entryBalanceAfter: (number | null)[] = [];
    for (const p of req.postings) {
      let after: number | null = null;
      if (running.has(p.accountId)) {
        after = running.get(p.accountId)! + p.amount;
        running.set(p.accountId, after);
      }
      entryAccount.push(p.accountId);
      entryAmount.push(p.amount);
      entryBalanceAfter.push(after);
    }
    await tx.query(
      `INSERT INTO journal_entries (transaction_id, account_id, amount, balance_after)
       SELECT $1, a, m, b FROM unnest($2::uuid[], $3::bigint[], $4::bigint[]) AS t(a, m, b)`,
      [transactionId, entryAccount, entryAmount, entryBalanceAfter],
    );

    const updIds = [...net.keys()].filter((id) => balances.has(id));
    if (updIds.length) {
      await tx.query(
        `UPDATE accounts a SET balance = a.balance + v.delta, version = a.version + 1
           FROM unnest($1::uuid[], $2::bigint[]) AS v(id, delta)
          WHERE a.id = v.id`,
        [updIds, updIds.map((id) => net.get(id)!)],
      );
    }
    ledgerTransactions.inc({ type: req.type, outcome: 'posted' });
    return { transactionId, duplicate: false };
  }

  private validatePostings(postings: Posting[]) {
    if (postings.length < 2) throw new BusinessError('INVALID_TRANSACTION', 'A transaction needs at least two postings');
    let sum = 0;
    for (const p of postings) {
      if (!Number.isSafeInteger(p.amount) || p.amount === 0) {
        throw new BusinessError('INVALID_AMOUNT', 'Posting amounts must be non-zero integers (minor units)');
      }
      sum += p.amount;
    }
    if (sum !== 0) throw new BusinessError('UNBALANCED_TRANSACTION', `Postings must sum to zero, got ${sum}`);
  }

  // ------------------------------------------------------------------ public operations

  deposit(accountId: string, amount: number, idempotencyKey: string) {
    return withTransaction(this.pool, (tx) =>
      this.post(tx, {
        idempotencyKey: `deposit:${idempotencyKey}`,
        type: 'deposit',
        postings: [
          { accountId: WORLD_ACCOUNT_ID, amount: -amount },
          { accountId, amount },
        ],
      }),
    );
  }

  transfer(fromAccountId: string, toAccountId: string, amount: number, idempotencyKey: string, description?: string) {
    if (fromAccountId === toAccountId) {
      throw new BusinessError('SAME_ACCOUNT', 'Cannot transfer to the same account');
    }
    return withTransaction(this.pool, (tx) =>
      this.post(tx, {
        idempotencyKey: `transfer:${idempotencyKey}`,
        type: 'transfer',
        postings: [
          { accountId: fromAccountId, amount: -amount },
          { accountId: toAccountId, amount },
        ],
        metadata: description ? { description } : {},
      }),
    );
  }

  // ------------------------------------------------------------------ holds (used by the payment saga)

  async getHold(paymentId: string, tx: Tx | Pool = this.pool, forUpdate = false): Promise<Hold | undefined> {
    const { rows } = await tx.query<Hold>(
      `SELECT id, payment_id AS "paymentId", account_id AS "accountId", amount, status
         FROM holds WHERE payment_id = $1 ${forUpdate ? 'FOR UPDATE' : ''}`,
      [paymentId],
    );
    return rows[0];
  }

  /** Saga step 1: move funds from the payer into the system holds account. Idempotent per payment. */
  async holdFunds(tx: Tx, paymentId: string, accountId: string, amount: number, payeeAccountId: string): Promise<Hold> {
    const existing = await this.getHold(paymentId, tx);
    if (existing) return existing;

    // Validate the payee up-front: failing later (at capture) would strand the payment.
    const { rows } = await tx.query<{ id: string; kind: AccountKind; currency: string }>(
      'SELECT id, kind, currency FROM accounts WHERE id = ANY($1::uuid[])',
      [[accountId, payeeAccountId]],
    );
    const payee = rows.find((r) => r.id === payeeAccountId);
    const payer = rows.find((r) => r.id === accountId);
    if (!payee || payee.kind === 'system') {
      throw new BusinessError('PAYEE_NOT_FOUND', `Payee account ${payeeAccountId} not found`, 404);
    }
    if (payer && payer.currency !== payee.currency) {
      throw new BusinessError('CURRENCY_MISMATCH', 'Payer and payee currencies differ', 422);
    }

    const posted = await this.post(tx, {
      idempotencyKey: `hold:${paymentId}`,
      type: 'hold',
      postings: [
        { accountId, amount: -amount },
        { accountId: HOLDS_ACCOUNT_ID, amount },
      ],
      metadata: { paymentId },
    });
    if (!posted.duplicate) {
      await tx.query(
        `INSERT INTO holds (id, payment_id, account_id, amount, status) VALUES ($1, $2, $3, $4, 'HELD')
         ON CONFLICT (payment_id) DO NOTHING`,
        [randomUUID(), paymentId, accountId, amount],
      );
    }
    return (await this.getHold(paymentId, tx))!;
  }

  /** Saga success: move held funds to the payee. */
  async captureHold(tx: Tx, paymentId: string, payeeAccountId: string) {
    const hold = await this.getHold(paymentId, tx, true);
    if (!hold) throw new BusinessError('HOLD_NOT_FOUND', `No hold for payment ${paymentId}`, 404);
    if (hold.status === 'RELEASED') throw new BusinessError('HOLD_ALREADY_RELEASED', 'Hold was already released');
    const posted = await this.post(tx, {
      idempotencyKey: `capture:${paymentId}`,
      type: 'capture',
      postings: [
        { accountId: HOLDS_ACCOUNT_ID, amount: -hold.amount },
        { accountId: payeeAccountId, amount: hold.amount },
      ],
      metadata: { paymentId },
    });
    if (hold.status === 'HELD') {
      await tx.query(`UPDATE holds SET status = 'CAPTURED', updated_at = now() WHERE id = $1`, [hold.id]);
    }
    return { hold: { ...hold, status: 'CAPTURED' as const }, transactionId: posted.transactionId };
  }

  /** Saga compensation: return held funds to the payer. */
  async releaseHold(tx: Tx, paymentId: string) {
    const hold = await this.getHold(paymentId, tx, true);
    if (!hold) throw new BusinessError('HOLD_NOT_FOUND', `No hold for payment ${paymentId}`, 404);
    if (hold.status === 'CAPTURED') throw new BusinessError('HOLD_ALREADY_CAPTURED', 'Hold was already captured');
    const posted = await this.post(tx, {
      idempotencyKey: `release:${paymentId}`,
      type: 'release',
      postings: [
        { accountId: HOLDS_ACCOUNT_ID, amount: -hold.amount },
        { accountId: hold.accountId, amount: hold.amount },
      ],
      metadata: { paymentId },
    });
    if (hold.status === 'HELD') {
      await tx.query(`UPDATE holds SET status = 'RELEASED', updated_at = now() WHERE id = $1`, [hold.id]);
    }
    return { hold: { ...hold, status: 'RELEASED' as const }, transactionId: posted.transactionId };
  }

  // ------------------------------------------------------------------ invariants ("zero balance drift")

  /**
   * Verifies the books. Run after load tests or on a schedule; any failure is a P0 bug.
   * Uses a REPEATABLE READ snapshot so all checks see the same consistent state.
   */
  async checkInvariants() {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
      const total = await client.query<{ s: number }>('SELECT COALESCE(SUM(amount), 0)::bigint AS s FROM journal_entries');
      const unbalanced = await client.query(
        'SELECT transaction_id FROM journal_entries GROUP BY transaction_id HAVING SUM(amount) <> 0 LIMIT 10',
      );
      const drift = await client.query(
        `SELECT a.id, a.balance AS cached, COALESCE(SUM(e.amount), 0)::bigint AS derived
           FROM accounts a LEFT JOIN journal_entries e ON e.account_id = a.id
          WHERE a.kind <> 'system'
          GROUP BY a.id
         HAVING a.balance <> COALESCE(SUM(e.amount), 0)
          LIMIT 10`,
      );
      const holds = await client.query<{ ledger: number; open: number }>(
        `SELECT (SELECT COALESCE(SUM(amount), 0) FROM journal_entries WHERE account_id = $1)::bigint AS ledger,
                (SELECT COALESCE(SUM(amount), 0) FROM holds WHERE status = 'HELD')::bigint AS open`,
        [HOLDS_ACCOUNT_ID],
      );
      const negative = await client.query(`SELECT id FROM accounts WHERE kind <> 'system' AND balance < 0 LIMIT 10`);
      const counts = await client.query(
        `SELECT (SELECT count(*) FROM ledger_transactions)::int AS transactions,
                (SELECT count(*) FROM journal_entries)::int AS entries,
                (SELECT count(*) FROM accounts)::int AS accounts`,
      );
      await client.query('COMMIT');

      const checks = [
        { name: 'all entries sum to zero', ok: total.rows[0].s === 0, detail: { sum: total.rows[0].s } },
        { name: 'every transaction balances', ok: unbalanced.rowCount === 0, detail: unbalanced.rows },
        { name: 'cached balance == sum(entries)', ok: drift.rowCount === 0, detail: drift.rows },
        {
          name: 'holds account == open holds',
          ok: holds.rows[0].ledger === holds.rows[0].open,
          detail: holds.rows[0],
        },
        { name: 'no negative user balances', ok: negative.rowCount === 0, detail: negative.rows },
      ];
      return { ok: checks.every((c) => c.ok), checks, stats: counts.rows[0] };
    } finally {
      client.release();
    }
  }
}
