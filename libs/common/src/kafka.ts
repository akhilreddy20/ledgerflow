import { Kafka, logLevel, Producer, Partitioners } from 'kafkajs';
import { ALL_TOPICS } from './events';

export function createKafka(clientId: string, brokers: string[]): Kafka {
  return new Kafka({
    clientId,
    brokers,
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 300, retries: 10 },
  });
}

/**
 * Idempotent producer: the broker de-duplicates retried sends (producer id + sequence numbers),
 * so a network blip between us and Kafka cannot create duplicates *inside Kafka*.
 * acks = all (-1) means the write is on every in-sync replica before we mark the outbox row published.
 */
export async function createProducer(kafka: Kafka): Promise<Producer> {
  // Disable only for Kafka-compatible brokers that lack InitProducerId (some emulators).
  // The outbox + inbox already make duplicates harmless; idempotence is an extra layer.
  const idempotent = (process.env.KAFKA_IDEMPOTENT_PRODUCER ?? 'true') !== 'false';
  const producer = kafka.producer({
    idempotent,
    maxInFlightRequests: idempotent ? 1 : 5,
    createPartitioner: Partitioners.DefaultPartitioner,
    // An idempotent producer must not give up early (kafkajs warns otherwise). The relay is a
    // background loop, so retrying for a long time during a broker outage is the right behaviour.
    retry: idempotent ? { retries: Number.MAX_SAFE_INTEGER, initialRetryTime: 300, maxRetryTime: 30_000 } : undefined,
  });
  await producer.connect();
  return producer;
}

/** Create topics if missing. In production you would manage topics with Strimzi/Terraform instead. */
export async function ensureTopics(kafka: Kafka, numPartitions: number, topics: string[] = ALL_TOPICS): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const existing = new Set(await admin.listTopics());
    const missing = topics.filter((t) => !existing.has(t));
    if (missing.length) {
      await admin.createTopics({
        waitForLeaders: true,
        topics: missing.map((topic) => ({
          topic,
          // DLQs are low-volume; one partition keeps replay ordering simple.
          numPartitions: topic.endsWith('.dlq') ? 1 : numPartitions,
          replicationFactor: Number(process.env.KAFKA_REPLICATION_FACTOR ?? 1),
        })),
      });
    }
  } finally {
    await admin.disconnect();
  }
}
