import { Logger } from '@nestjs/common';
import { Producer } from 'kafkajs';
import { Pool } from 'pg';
import { Envelope } from './events';
import { Tx } from './db';
import { outboxBacklog, outboxPublished } from './metrics';
import { currentTraceHeaders } from './tracing';
import { sleep } from './retry';

/**
 * TRANSACTIONAL OUTBOX
 *
 * Problem: "update the database AND publish to Kafka" is a dual write. If the process crashes between
 * the two, the DB and Kafka disagree forever (a payment that is FUNDS_HELD but no command ever sent).
 *
 * Solution: write the message into an `outbox` table in the SAME database transaction as the business
 * change. Either both commit or neither does. A separate relay (below) publishes committed rows to
 * Kafka and marks them published. Delivery is at-least-once, so consumers must be idempotent (inbox.ts).
 */
export async function enqueue(tx: Tx, topic: string, key: string, message: Envelope): Promise<void> {
  await tx.query(
    `INSERT INTO outbox (message_id, topic, message_key, event_type, payload, headers)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [message.id, topic, key, message.type, JSON.stringify(message), JSON.stringify(currentTraceHeaders())],
  );
}

interface OutboxRow {
  id: number;
  message_id: string;
  topic: string;
  message_key: string;
  event_type: string;
  payload: Envelope;
  headers: Record<string, string>;
}

export interface OutboxRelayOptions {
  batchSize?: number;
  idleDelayMs?: number;
  /** Published rows older than this are deleted to keep the table small. */
  retentionHours?: number;
}

/**
 * Polls the outbox and publishes to Kafka.
 *
 * `FOR UPDATE SKIP LOCKED` lets several replicas run the relay concurrently: each grabs a different
 * batch instead of blocking on (or double-publishing) the same rows. Rows are only marked published
 * after Kafka acknowledged them; a crash in between means the batch is published again (duplicates,
 * never loss) - which the consumers' inbox absorbs.
 */
export class OutboxRelay {
  private readonly logger = new Logger(OutboxRelay.name);
  private running = false;
  private loop?: Promise<void>;
  private lastCleanup = 0;

  constructor(
    private readonly pool: Pool,
    private readonly producer: Producer,
    private readonly opts: OutboxRelayOptions = {},
  ) {}

  start() {
    if (this.running) return;
    this.running = true;
    this.loop = this.run();
  }

  async stop() {
    this.running = false;
    await this.loop;
  }

  /** Publish one batch. Returns the number of rows published. Public for tests. */
  async publishBatch(): Promise<number> {
    const batchSize = this.opts.batchSize ?? 200;
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query<OutboxRow>(
        `SELECT id, message_id, topic, message_key, event_type, payload, headers
           FROM outbox
          WHERE published_at IS NULL
          ORDER BY id
          LIMIT $1
          FOR UPDATE SKIP LOCKED`,
        [batchSize],
      );
      if (rows.length === 0) {
        await client.query('COMMIT');
        return 0;
      }
      const byTopic = new Map<string, OutboxRow[]>();
      for (const row of rows) byTopic.set(row.topic, [...(byTopic.get(row.topic) ?? []), row]);

      await this.producer.sendBatch({
        acks: -1,
        topicMessages: [...byTopic.entries()].map(([topic, msgs]) => ({
          topic,
          messages: msgs.map((m) => ({
            key: m.message_key,
            value: JSON.stringify(m.payload),
            headers: { ...m.headers, 'message-id': m.message_id, 'event-type': m.event_type },
          })),
        })),
      });

      await client.query('UPDATE outbox SET published_at = now() WHERE id = ANY($1::bigint[])', [rows.map((r) => r.id)]);
      await client.query('COMMIT');
      outboxPublished.inc(rows.length);
      return rows.length;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }
  }

  private async run() {
    const idle = this.opts.idleDelayMs ?? 50;
    while (this.running) {
      try {
        const published = await this.publishBatch();
        await this.housekeeping();
        if (published === 0) await sleep(idle);
      } catch (err: any) {
        this.logger.error(`Outbox relay error: ${err?.message}`);
        await sleep(1000);
      }
    }
  }

  private async housekeeping() {
    const now = Date.now();
    if (now - this.lastCleanup < 10_000) return;
    this.lastCleanup = now;
    const { rows } = await this.pool.query<{ n: number }>(
      'SELECT count(*)::int AS n FROM outbox WHERE published_at IS NULL',
    );
    outboxBacklog.set(rows[0].n);
    await this.pool.query(
      `DELETE FROM outbox WHERE published_at < now() - make_interval(hours => $1)`,
      [this.opts.retentionHours ?? 24],
    );
  }
}
