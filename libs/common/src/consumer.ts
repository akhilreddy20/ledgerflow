import { Logger } from '@nestjs/common';
import { Consumer, IHeaders, Kafka, Producer } from 'kafkajs';
import { dlqTopic, Envelope } from './events';
import { isRetryable, NonRetryableError } from './errors';
import { dlqMessages, messageHandlingDuration, messagesProcessed } from './metrics';
import { backoffDelay, sleep } from './retry';
import { contextFromHeaders, SpanKind, withSpan } from './tracing';

export interface MessageMeta {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
  attempt: number;
}

export type MessageHandler = (message: Envelope<any>, meta: MessageMeta) => Promise<void>;

export interface ConsumerOptions {
  kafka: Kafka;
  /** Used to send poison messages to `<topic>.dlq`. */
  producer: Producer;
  groupId: string;
  topics: string[];
  handler: MessageHandler;
  /** Partitions processed in parallel. Messages *within* a partition are always processed in order. */
  concurrency?: number;
  maxAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
}

function headersToStrings(headers: IHeaders | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(headers ?? {})) {
    if (v === undefined) continue;
    out[k] = Array.isArray(v) ? v.map(String).join(',') : v.toString();
  }
  return out;
}

/**
 * Runs a consumer group with:
 *  - per-message retries with exponential backoff + jitter (blocking the partition on purpose so
 *    per-key ordering is preserved - a later message for the same account never overtakes an earlier one)
 *  - dead-letter routing after maxAttempts, or immediately for non-retryable errors
 *  - trace context continued from the producer (traceparent header)
 *  - Prometheus metrics per consumer / topic / outcome
 *
 * Offsets are committed by kafkajs after eachMessage resolves, i.e. AFTER the handler committed its DB
 * transaction. Crash in between -> redelivery -> the inbox makes it a no-op.
 */
export async function runConsumer(opts: ConsumerOptions): Promise<Consumer> {
  const logger = new Logger(`Consumer:${opts.groupId}`);
  const maxAttempts = opts.maxAttempts ?? 5;
  const consumer = opts.kafka.consumer({
    groupId: opts.groupId,
    sessionTimeout: 30_000,
    heartbeatInterval: 3_000,
  });
  await consumer.connect();
  for (const topic of opts.topics) await consumer.subscribe({ topic, fromBeginning: true });

  await consumer.run({
    partitionsConsumedConcurrently: opts.concurrency ?? 3,
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      const headers = headersToStrings(message.headers);
      const stopTimer = messageHandlingDuration.startTimer({ consumer: opts.groupId, topic });
      const raw = message.value?.toString() ?? '';
      let lastError: any;

      for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
          let parsed: Envelope;
          try {
            parsed = JSON.parse(raw);
            if (!parsed?.id || !parsed?.type) throw new Error('missing id/type');
          } catch (e: any) {
            throw new NonRetryableError(`Malformed message: ${e.message}`);
          }
          await withSpan(
            `consume ${topic} ${parsed.type}`,
            {
              'messaging.system': 'kafka',
              'messaging.destination': topic,
              'messaging.kafka.partition': partition,
              'messaging.message_id': parsed.id,
              'ledgerflow.attempt': attempt,
            },
            () =>
              opts.handler(parsed, {
                topic,
                partition,
                offset: message.offset,
                key: message.key?.toString() ?? null,
                attempt,
              }),
            { parent: contextFromHeaders(headers), kind: SpanKind.CONSUMER },
          );
          messagesProcessed.inc({ consumer: opts.groupId, topic, outcome: 'ok' });
          stopTimer();
          return;
        } catch (err: any) {
          lastError = err;
          if (!isRetryable(err) || attempt === maxAttempts) break;
          const delay = backoffDelay(attempt, opts.baseDelayMs ?? 200, opts.maxDelayMs ?? 5_000);
          logger.warn(`${topic}[${partition}]@${message.offset} attempt ${attempt} failed: ${err.message}; retrying in ${delay}ms`);
          messagesProcessed.inc({ consumer: opts.groupId, topic, outcome: 'retry' });
          await sleep(delay);
          await heartbeat(); // keep our partition assignment while we back off
        }
      }

      // Give up: park the message in the DLQ so the partition can move on.
      logger.error(`${topic}[${partition}]@${message.offset} -> DLQ: ${lastError?.message}`);
      await opts.producer.send({
        topic: dlqTopic(topic),
        acks: -1,
        messages: [
          {
            key: message.key,
            value: message.value,
            headers: {
              ...headers,
              'x-original-topic': topic,
              'x-original-partition': String(partition),
              'x-original-offset': message.offset,
              'x-consumer-group': opts.groupId,
              'x-error': String(lastError?.message ?? 'unknown').slice(0, 500),
              'x-error-type': String(lastError?.name ?? 'Error'),
              'x-failed-at': new Date().toISOString(),
            },
          },
        ],
      });
      dlqMessages.inc({ consumer: opts.groupId, topic });
      messagesProcessed.inc({ consumer: opts.groupId, topic, outcome: 'dlq' });
      stopTimer();
    },
  });

  return consumer;
}
