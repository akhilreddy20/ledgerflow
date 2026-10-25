import { Inject, Injectable, Logger, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';
import {
  CaptureHold,
  enqueue,
  Envelope,
  envelope,
  envInt,
  EventTypes,
  FundsHeld,
  HoldCaptured,
  HoldFunds,
  HoldRejected,
  HoldReleased,
  KAFKA,
  markProcessed,
  PaymentCompleted,
  PaymentFailed,
  PG_POOL,
  PRODUCER,
  ReleaseHold,
  runConsumer,
  Topics,
  TransientError,
  Tx,
  withTransaction,
} from '@ledgerflow/common';
import { Consumer, Kafka, Producer } from 'kafkajs';
import { Pool } from 'pg';
import { Payment, PaymentsService } from './payments.service';
import { PspClient, PspDeclinedError } from './psp.client';

export const SAGA_CONSUMER_GROUP = 'payments-saga';

/**
 * ORCHESTRATED SAGA for a payout.
 *
 * A payment spans two services (ledger, PSP) that cannot share one ACID transaction. Instead the saga
 * runs a sequence of local transactions, each one publishing the next step via the outbox, and defines
 * a COMPENSATING action for every step that can be undone:
 *
 *   step                         | on success          | on failure (compensation)
 *   -----------------------------|---------------------|-------------------------------
 *   1. HoldFunds   (ledger)      | FUNDS_HELD          | FAILED (nothing to undo)
 *   2. Payout      (external PSP)| CAPTURING           | ReleaseHold -> COMPENSATING -> FAILED
 *   3. CaptureHold (ledger)      | COMPLETED           | (retried until it succeeds - it cannot fail
 *                                                         for business reasons once funds are held)
 *
 * The orchestrator (this class) owns the state machine; participants just execute commands.
 */
@Injectable()
export class PaymentSaga implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger(PaymentSaga.name);
  private consumer?: Consumer;
  private recoveryTimer?: NodeJS.Timeout;

  private readonly sagaTimeoutMs = envInt('SAGA_TIMEOUT_MS', 5 * 60_000);
  private readonly maxPspAttempts = envInt('SAGA_MAX_PSP_ATTEMPTS', 8);
  private readonly stuckAfterMs = envInt('SAGA_STUCK_AFTER_MS', 30_000);

  constructor(
    private readonly payments: PaymentsService,
    private readonly psp: PspClient,
    @Inject(PG_POOL) private readonly pool: Pool,
    @Inject(KAFKA) private readonly kafka: Kafka,
    @Inject(PRODUCER) private readonly producer: Producer,
  ) {}

  async onApplicationBootstrap() {
    this.consumer = await runConsumer({
      kafka: this.kafka,
      producer: this.producer,
      groupId: SAGA_CONSUMER_GROUP,
      topics: [Topics.LEDGER_EVENTS],
      concurrency: envInt('CONSUMER_CONCURRENCY', 6),
      // In-consumer retries cover DB/Kafka blips. PSP outages are handled by the recovery job instead.
      maxAttempts: 3,
      handler: (msg) => this.handle(msg),
    });
    const every = envInt('SAGA_RECOVERY_INTERVAL_MS', 10_000);
    this.recoveryTimer = setInterval(() => this.recoverStuckPayments().catch((e) => this.logger.error(e.message)), every);
    this.logger.log(`Saga consuming ${Topics.LEDGER_EVENTS}; recovery every ${every}ms`);
  }

  async onApplicationShutdown() {
    clearInterval(this.recoveryTimer);
    await this.consumer?.disconnect();
  }

  async handle(msg: Envelope<any>) {
    switch (msg.type) {
      case EventTypes.FUNDS_HELD:
        return this.onFundsHeld(msg as Envelope<FundsHeld>);
      case EventTypes.HOLD_REJECTED:
        return this.onHoldRejected(msg as Envelope<HoldRejected>);
      case EventTypes.HOLD_CAPTURED:
        return this.onHoldCaptured(msg as Envelope<HoldCaptured>);
      case EventTypes.HOLD_RELEASED:
        return this.onHoldReleased(msg as Envelope<HoldReleased>);
      default:
        this.logger.debug(`Ignoring ${msg.type}`);
    }
  }

  // ------------------------------------------------------------------ step 1 result

  private async onFundsHeld(msg: Envelope<FundsHeld>) {
    const { paymentId } = msg.payload;
    // No inbox gate here on purpose: the next step (PSP call) happens AFTER the commit, so a redelivered
    // FundsHeld must be able to resume it. The guarded transition keeps it idempotent instead.
    await withTransaction(this.pool, (tx) =>
      this.payments.transition(tx, paymentId, ['PENDING'], 'FUNDS_HELD', 'ledger reserved funds'),
    );
    const payment = await this.payments.find(paymentId);
    if (payment?.status !== 'FUNDS_HELD') return;
    try {
      await this.executePayout(payment);
    } catch (err: any) {
      if (!(err instanceof TransientError)) throw err;
      // PSP slow/down/circuit open. The event has done its job (state is FUNDS_HELD in the DB, which is the
      // source of truth), so don't block the partition or flood the DLQ: the recovery job owns PSP retries.
      this.logger.warn(`Payout ${paymentId} deferred to recovery: ${err.message}`);
    }
  }

  private async onHoldRejected(msg: Envelope<HoldRejected>) {
    const { paymentId, code, reason } = msg.payload;
    await this.inTx(msg, async (tx) => {
      const p = await this.payments.transition(tx, paymentId, ['PENDING'], 'FAILED', `hold rejected: ${code}`, {
        failureReason: `${code}: ${reason}`,
      });
      if (p) await this.publishFailed(tx, p);
    });
  }

  // ------------------------------------------------------------------ step 2: external PSP

  /**
   * Call the PSP. Outcomes:
   *  - succeeded -> FUNDS_HELD -> CAPTURING, send CaptureHold
   *  - declined  -> FUNDS_HELD -> COMPENSATING, send ReleaseHold
   *  - transient (timeout, 5xx, circuit open) -> throw; the caller leaves the payment in FUNDS_HELD and the
   *    recovery job keeps retrying until the saga timeout, after which we reconcile with the PSP and compensate.
   */
  async executePayout(payment: Payment) {
    await this.pool.query('UPDATE payments SET psp_attempts = psp_attempts + 1, updated_at = now() WHERE id = $1', [
      payment.id,
    ]);
    try {
      const result = await this.psp.payout({
        paymentId: payment.id,
        amount: payment.amount,
        currency: payment.currency,
        destinationAccountId: payment.payeeAccountId,
      });
      await this.startCapture(payment, result.pspReference);
    } catch (err) {
      if (err instanceof PspDeclinedError) {
        await this.startCompensation(payment, `PSP_DECLINED: ${err.message}`);
        return;
      }
      throw err;
    }
  }

  private startCapture(payment: Payment, pspReference: string) {
    return withTransaction(this.pool, async (tx) => {
      const p = await this.payments.transition(tx, payment.id, ['FUNDS_HELD'], 'CAPTURING', 'PSP payout succeeded', {
        pspReference,
      });
      if (p) await this.sendCapture(tx, p);
    });
  }

  private startCompensation(payment: Payment, reason: string) {
    return withTransaction(this.pool, async (tx) => {
      const p = await this.payments.transition(tx, payment.id, ['FUNDS_HELD'], 'COMPENSATING', reason, {
        failureReason: reason,
      });
      if (p) await this.sendRelease(tx, p, reason);
    });
  }

  // ------------------------------------------------------------------ step 3 results

  private async onHoldCaptured(msg: Envelope<HoldCaptured>) {
    await this.inTx(msg, async (tx) => {
      const p = await this.payments.transition(tx, msg.payload.paymentId, ['CAPTURING'], 'COMPLETED', 'funds captured to payee');
      if (!p) return;
      await enqueue(
        tx,
        Topics.PAYMENT_EVENTS,
        p.id,
        envelope<PaymentCompleted>(
          EventTypes.PAYMENT_COMPLETED,
          'payments',
          {
            paymentId: p.id,
            payerAccountId: p.payerAccountId,
            payeeAccountId: p.payeeAccountId,
            amount: p.amount,
            currency: p.currency,
            pspReference: p.pspReference ?? '',
          },
          p.id,
        ),
      );
    });
  }

  private async onHoldReleased(msg: Envelope<HoldReleased>) {
    await this.inTx(msg, async (tx) => {
      const p = await this.payments.transition(tx, msg.payload.paymentId, ['COMPENSATING'], 'FAILED', 'funds released to payer');
      if (p) await this.publishFailed(tx, p);
    });
  }

  // ------------------------------------------------------------------ recovery (the saga's safety net)

  /**
   * Finds payments stuck in a non-terminal state (lost message, PSP outage, message parked in the DLQ)
   * and pushes them forward. Rows are claimed with FOR UPDATE SKIP LOCKED + an updated_at bump, so
   * several replicas can run this concurrently without working on the same payment.
   */
  async recoverStuckPayments(): Promise<number> {
    const claimed = await withTransaction(this.pool, async (tx) => {
      const { rows } = await tx.query<{ id: string }>(
        `SELECT id FROM payments
          WHERE status IN ('PENDING', 'FUNDS_HELD', 'CAPTURING', 'COMPENSATING')
            AND updated_at < now() - make_interval(secs => $1::double precision / 1000)
          ORDER BY updated_at
          LIMIT 50
          FOR UPDATE SKIP LOCKED`,
        [this.stuckAfterMs],
      );
      if (rows.length) {
        await tx.query('UPDATE payments SET updated_at = now() WHERE id = ANY($1::uuid[])', [rows.map((r) => r.id)]);
      }
      return rows.map((r) => r.id);
    });

    for (const id of claimed) {
      const p = await this.payments.find(id);
      if (!p) continue;
      try {
        await this.recoverOne(p);
      } catch (err: any) {
        this.logger.warn(`Recovery of ${p.id} (${p.status}) failed: ${err.message}`);
      }
    }
    if (claimed.length) this.logger.log(`Recovery pass handled ${claimed.length} stuck payment(s)`);
    return claimed.length;
  }

  private async recoverOne(p: Payment) {
    switch (p.status) {
      case 'PENDING':
        // HoldFunds may have been parked in the DLQ - re-send it (the ledger dedupes per payment).
        return withTransaction(this.pool, (tx) => this.sendHold(tx, p));
      case 'CAPTURING':
        return withTransaction(this.pool, (tx) => this.sendCapture(tx, p));
      case 'COMPENSATING':
        return withTransaction(this.pool, (tx) => this.sendRelease(tx, p, p.failureReason ?? 'compensation retry'));
      case 'FUNDS_HELD': {
        const expired = Date.now() - new Date(p.createdAt).getTime() > this.sagaTimeoutMs;
        if (!expired && p.pspAttempts < this.maxPspAttempts) return this.executePayout(p);
        // Saga timed out. NEVER compensate blindly: the PSP may have paid out even though we never saw
        // the response. Reconcile first.
        const lookup = await this.psp.lookup(p.id);
        if (lookup.status === 'succeeded') return this.startCapture(p, lookup.pspReference);
        return this.startCompensation(p, `SAGA_TIMEOUT after ${p.pspAttempts} PSP attempts`);
      }
    }
  }

  // ------------------------------------------------------------------ helpers

  private inTx(msg: Envelope, fn: (tx: Tx) => Promise<void>) {
    return withTransaction(this.pool, async (tx) => {
      if (!(await markProcessed(tx, SAGA_CONSUMER_GROUP, msg.id))) return;
      await fn(tx);
    });
  }

  private sendHold(tx: Tx, p: Payment) {
    return enqueue(
      tx,
      Topics.LEDGER_COMMANDS,
      p.payerAccountId,
      envelope<HoldFunds>(
        EventTypes.HOLD_FUNDS,
        'payments',
        { paymentId: p.id, accountId: p.payerAccountId, payeeAccountId: p.payeeAccountId, amount: p.amount, currency: p.currency },
        p.id,
      ),
    );
  }

  private sendCapture(tx: Tx, p: Payment) {
    return enqueue(
      tx,
      Topics.LEDGER_COMMANDS,
      p.payerAccountId,
      envelope<CaptureHold>(EventTypes.CAPTURE_HOLD, 'payments', { paymentId: p.id, payeeAccountId: p.payeeAccountId }, p.id),
    );
  }

  private sendRelease(tx: Tx, p: Payment, reason: string) {
    return enqueue(
      tx,
      Topics.LEDGER_COMMANDS,
      p.payerAccountId,
      envelope<ReleaseHold>(EventTypes.RELEASE_HOLD, 'payments', { paymentId: p.id, reason }, p.id),
    );
  }

  private publishFailed(tx: Tx, p: Payment) {
    return enqueue(
      tx,
      Topics.PAYMENT_EVENTS,
      p.id,
      envelope<PaymentFailed>(
        EventTypes.PAYMENT_FAILED,
        'payments',
        { paymentId: p.id, payerAccountId: p.payerAccountId, amount: p.amount, currency: p.currency, reason: p.failureReason ?? 'unknown' },
        p.id,
      ),
    );
  }
}
