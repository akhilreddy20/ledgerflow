import { Inject, Injectable, Logger, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';
import {
  Envelope,
  EventTypes,
  KAFKA,
  markProcessed,
  NonRetryableError,
  PaymentCompleted,
  PaymentFailed,
  PG_POOL,
  PRODUCER,
  runConsumer,
  Topics,
  withTransaction,
} from '@ledgerflow/common';
import { randomUUID } from 'crypto';
import { Consumer, Kafka, Producer } from 'kafkajs';
import { Pool } from 'pg';

export const NOTIFICATIONS_GROUP = 'notifications-service';

const rupees = (paise: number) => `₹${(paise / 100).toFixed(2)}`;

/**
 * Downstream consumer of payment lifecycle events. Shows the "many independent consumers" benefit of
 * an event-driven design: payments knows nothing about notifications. A real system would call an
 * email/SMS/push provider here; we record the message in a table and log it.
 */
@Injectable()
export class NotificationsConsumer implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger('Notifications');
  private consumer?: Consumer;

  constructor(
    @Inject(PG_POOL) private readonly pool: Pool,
    @Inject(KAFKA) private readonly kafka: Kafka,
    @Inject(PRODUCER) private readonly producer: Producer,
  ) {}

  async onApplicationBootstrap() {
    this.consumer = await runConsumer({
      kafka: this.kafka,
      producer: this.producer,
      groupId: NOTIFICATIONS_GROUP,
      topics: [Topics.PAYMENT_EVENTS],
      handler: (msg) => this.handle(msg),
    });
  }

  async onApplicationShutdown() {
    await this.consumer?.disconnect();
  }

  async handle(msg: Envelope<any>) {
    const messages = this.render(msg);
    await withTransaction(this.pool, async (tx) => {
      if (!(await markProcessed(tx, NOTIFICATIONS_GROUP, msg.id))) return;
      for (const m of messages) {
        await tx.query(
          `INSERT INTO notifications (id, payment_id, type, channel, recipient_account_id, message)
           VALUES ($1, $2, $3, 'email', $4, $5)
           ON CONFLICT (payment_id, type, recipient_account_id) DO NOTHING`,
          [randomUUID(), m.paymentId, msg.type, m.recipient, m.text],
        );
      }
    });
    for (const m of messages) this.logger.log(`[email -> ${m.recipient}] ${m.text}`);
  }

  private render(msg: Envelope<any>) {
    if (msg.type === EventTypes.PAYMENT_COMPLETED) {
      const p = msg.payload as PaymentCompleted;
      return [
        { paymentId: p.paymentId, recipient: p.payerAccountId, text: `Your payment of ${rupees(p.amount)} was sent (ref ${p.pspReference}).` },
        { paymentId: p.paymentId, recipient: p.payeeAccountId, text: `You received ${rupees(p.amount)}.` },
      ];
    }
    if (msg.type === EventTypes.PAYMENT_FAILED) {
      const p = msg.payload as PaymentFailed;
      return [{ paymentId: p.paymentId, recipient: p.payerAccountId, text: `Your payment of ${rupees(p.amount)} failed: ${p.reason}. No money was taken.` }];
    }
    throw new NonRetryableError(`Unexpected event type ${msg.type}`);
  }
}
