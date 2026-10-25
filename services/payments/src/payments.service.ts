import { Inject, Injectable } from '@nestjs/common';
import {
  BusinessError,
  enqueue,
  envelope,
  EventTypes,
  HoldFunds,
  PG_POOL,
  registry,
  Topics,
  Tx,
  withTransaction,
} from '@ledgerflow/common';
import { createHash, randomUUID } from 'crypto';
import { Pool } from 'pg';
import { Counter } from 'prom-client';
import { CreatePaymentDto } from './dto';

export type PaymentStatus = 'PENDING' | 'FUNDS_HELD' | 'CAPTURING' | 'COMPENSATING' | 'COMPLETED' | 'FAILED';

export interface Payment {
  id: string;
  payerAccountId: string;
  payeeAccountId: string;
  amount: number;
  currency: string;
  description: string | null;
  status: PaymentStatus;
  failureReason: string | null;
  pspReference: string | null;
  pspAttempts: number;
  createdAt: string;
  updatedAt: string;
}

const SELECT = `SELECT id, payer_account_id AS "payerAccountId", payee_account_id AS "payeeAccountId", amount, currency,
  description, status, failure_reason AS "failureReason", psp_reference AS "pspReference", psp_attempts AS "pspAttempts",
  created_at AS "createdAt", updated_at AS "updatedAt" FROM payments`;

const transitions = new Counter({
  name: 'payment_transitions_total',
  help: 'Payment saga state transitions',
  labelNames: ['from', 'to'],
  registers: [registry],
});

@Injectable()
export class PaymentsService {
  constructor(@Inject(PG_POOL) private readonly pool: Pool) {}

  /**
   * Accept a payment: persist it as PENDING and enqueue the first saga command (HoldFunds) in the
   * SAME transaction (outbox). The HTTP call returns 202 immediately; the saga continues asynchronously.
   */
  async create(dto: CreatePaymentDto, idempotencyKey: string): Promise<Payment> {
    if (dto.payerAccountId === dto.payeeAccountId) {
      throw new BusinessError('SAME_ACCOUNT', 'Payer and payee must differ');
    }
    const requestHash = createHash('sha256').update(JSON.stringify(dto)).digest('hex');

    return withTransaction(this.pool, async (tx) => {
      const id = randomUUID();
      const inserted = await tx.query(
        `INSERT INTO payments (id, idempotency_key, request_hash, payer_account_id, payee_account_id, amount, currency, description, status)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'PENDING')
         ON CONFLICT (idempotency_key) DO NOTHING`,
        [id, idempotencyKey, requestHash, dto.payerAccountId, dto.payeeAccountId, dto.amount, dto.currency, dto.description ?? null],
      );
      if (inserted.rowCount === 0) {
        // Redis lost the key (or it expired) - the DB constraint is the source of truth.
        const { rows } = await tx.query('SELECT id, request_hash FROM payments WHERE idempotency_key = $1', [idempotencyKey]);
        if (rows[0].request_hash !== requestHash) {
          throw new BusinessError('IDEMPOTENCY_KEY_REUSED', 'Idempotency key was already used for a different payment');
        }
        return (await this.find(rows[0].id, tx))!;
      }
      await this.history(tx, id, null, 'PENDING', 'payment accepted');
      await enqueue(
        tx,
        Topics.LEDGER_COMMANDS,
        dto.payerAccountId, // partition key = account -> per-account ordering
        envelope<HoldFunds>(
          EventTypes.HOLD_FUNDS,
          'payments',
          {
            paymentId: id,
            accountId: dto.payerAccountId,
            payeeAccountId: dto.payeeAccountId,
            amount: dto.amount,
            currency: dto.currency,
          },
          id,
        ),
      );
      return (await this.find(id, tx))!;
    });
  }

  async find(id: string, db: Tx | Pool = this.pool): Promise<Payment | undefined> {
    const { rows } = await db.query<Payment>(`${SELECT} WHERE id = $1`, [id]);
    return rows[0];
  }

  async get(id: string) {
    const payment = await this.find(id);
    if (!payment) throw new BusinessError('PAYMENT_NOT_FOUND', `Payment ${id} not found`, 404);
    const { rows: history } = await this.pool.query(
      `SELECT from_status AS "from", to_status AS "to", reason, at FROM payment_status_history
        WHERE payment_id = $1 ORDER BY id`,
      [id],
    );
    return { ...payment, history };
  }

  async list(status: string | undefined, limit: number) {
    const { rows } = await this.pool.query<Payment>(
      `${SELECT} WHERE ($1::text IS NULL OR status = $1) ORDER BY created_at DESC LIMIT $2`,
      [status ?? null, limit],
    );
    return rows;
  }

  async stats() {
    const { rows } = await this.pool.query(
      `SELECT status, count(*)::int AS count FROM payments GROUP BY status ORDER BY status`,
    );
    return Object.fromEntries(rows.map((r) => [r.status, r.count]));
  }

  /**
   * Guarded state transition: only succeeds if the payment is currently in one of `from`.
   * This is what makes every saga step idempotent and race-safe: two workers handling the same
   * event (redelivery, recovery job) both try the UPDATE, exactly one gets a row back.
   * Returns the updated payment, or undefined if the transition did not apply.
   */
  async transition(
    tx: Tx,
    id: string,
    from: PaymentStatus[],
    to: PaymentStatus,
    reason: string,
    extra: { failureReason?: string; pspReference?: string } = {},
  ): Promise<Payment | undefined> {
    const { rows } = await tx.query<{ previous: PaymentStatus }>(
      `UPDATE payments p
          SET status = $3, updated_at = now(),
              failure_reason = COALESCE($4, p.failure_reason),
              psp_reference = COALESCE($5, p.psp_reference)
         FROM (SELECT id, status AS previous FROM payments WHERE id = $1 FOR UPDATE) old
        WHERE p.id = old.id AND p.status = ANY($2::text[])
        RETURNING old.previous`,
      [id, from, to, extra.failureReason ?? null, extra.pspReference ?? null],
    );
    if (!rows[0]) return undefined;
    await this.history(tx, id, rows[0].previous, to, reason);
    transitions.inc({ from: rows[0].previous, to });
    return this.find(id, tx);
  }

  private history(tx: Tx, id: string, from: PaymentStatus | null, to: PaymentStatus, reason: string) {
    return tx.query(
      'INSERT INTO payment_status_history (payment_id, from_status, to_status, reason) VALUES ($1, $2, $3, $4)',
      [id, from, to, reason],
    );
  }
}
