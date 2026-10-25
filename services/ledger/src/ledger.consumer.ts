import { Inject, Injectable, Logger, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';
import {
  BusinessError,
  CaptureHold,
  enqueue,
  Envelope,
  envelope,
  EventTypes,
  HoldFunds,
  KAFKA,
  markProcessed,
  NonRetryableError,
  PG_POOL,
  PRODUCER,
  ReleaseHold,
  runConsumer,
  Topics,
  Tx,
  withTransaction,
} from '@ledgerflow/common';
import { Consumer, Kafka, Producer } from 'kafkajs';
import { Pool } from 'pg';
import { LedgerService } from './ledger.service';

export const LEDGER_CONSUMER_GROUP = 'ledger-service';

/**
 * Executes the payment saga's commands against the ledger.
 *
 * For every command, in ONE database transaction:
 *   1. inbox check (exactly-once effect even if Kafka redelivers)
 *   2. apply the ledger change
 *   3. write the resulting event to the outbox
 * Business rejections (insufficient funds...) are not errors - they become a HoldRejected event.
 */
@Injectable()
export class LedgerCommandConsumer implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger(LedgerCommandConsumer.name);
  private consumer?: Consumer;

  constructor(
    private readonly ledger: LedgerService,
    @Inject(PG_POOL) private readonly pool: Pool,
    @Inject(KAFKA) private readonly kafka: Kafka,
    @Inject(PRODUCER) private readonly producer: Producer,
  ) {}

  async onApplicationBootstrap() {
    this.consumer = await runConsumer({
      kafka: this.kafka,
      producer: this.producer,
      groupId: LEDGER_CONSUMER_GROUP,
      topics: [Topics.LEDGER_COMMANDS],
      concurrency: Number(process.env.CONSUMER_CONCURRENCY ?? 6),
      handler: (msg) => this.handle(msg),
    });
    this.logger.log(`Consuming ${Topics.LEDGER_COMMANDS}`);
  }

  async onApplicationShutdown() {
    await this.consumer?.disconnect();
  }

  async handle(msg: Envelope<any>) {
    switch (msg.type) {
      case EventTypes.HOLD_FUNDS:
        return this.onHoldFunds(msg as Envelope<HoldFunds>);
      case EventTypes.CAPTURE_HOLD:
        return this.inTx(msg, (tx) => this.onCapture(tx, msg as Envelope<CaptureHold>));
      case EventTypes.RELEASE_HOLD:
        return this.inTx(msg, (tx) => this.onRelease(tx, msg as Envelope<ReleaseHold>));
      default:
        throw new NonRetryableError(`Unknown command type ${msg.type}`);
    }
  }

  /** Inbox + effect + outbox, atomically. */
  private inTx(msg: Envelope, fn: (tx: Tx) => Promise<void>) {
    return withTransaction(this.pool, async (tx) => {
      if (!(await markProcessed(tx, LEDGER_CONSUMER_GROUP, msg.id))) {
        this.logger.debug(`Duplicate ${msg.type} ${msg.id} ignored`);
        return;
      }
      await fn(tx);
    });
  }

  private async onHoldFunds(msg: Envelope<HoldFunds>) {
    const p = msg.payload;
    try {
      await this.inTx(msg, async (tx) => {
        const hold = await this.ledger.holdFunds(tx, p.paymentId, p.accountId, p.amount, p.payeeAccountId);
        await enqueue(
          tx,
          Topics.LEDGER_EVENTS,
          p.accountId,
          envelope(EventTypes.FUNDS_HELD, 'ledger', { paymentId: p.paymentId, holdId: hold.id, accountId: p.accountId, amount: hold.amount }, p.paymentId),
        );
      });
    } catch (err) {
      if (!(err instanceof BusinessError)) throw err; // transient -> retried by the consumer runner
      // Expected business outcome: tell the saga the hold was rejected (in a fresh transaction,
      // because the failed one was rolled back).
      await this.inTx(msg, (tx) =>
        enqueue(
          tx,
          Topics.LEDGER_EVENTS,
          p.accountId,
          envelope(EventTypes.HOLD_REJECTED, 'ledger', { paymentId: p.paymentId, code: err.code, reason: err.message }, p.paymentId),
        ),
      );
    }
  }

  private async onCapture(tx: Tx, msg: Envelope<CaptureHold>) {
    const { paymentId, payeeAccountId } = msg.payload;
    const { hold, transactionId } = await this.ledger.captureHold(tx, paymentId, payeeAccountId);
    await enqueue(
      tx,
      Topics.LEDGER_EVENTS,
      hold.accountId,
      envelope(EventTypes.HOLD_CAPTURED, 'ledger', { paymentId, holdId: hold.id, transactionId }, paymentId),
    );
  }

  private async onRelease(tx: Tx, msg: Envelope<ReleaseHold>) {
    const { paymentId } = msg.payload;
    const { hold, transactionId } = await this.ledger.releaseHold(tx, paymentId);
    await enqueue(
      tx,
      Topics.LEDGER_EVENTS,
      hold.accountId,
      envelope(EventTypes.HOLD_RELEASED, 'ledger', { paymentId, holdId: hold.id, transactionId }, paymentId),
    );
  }
}
