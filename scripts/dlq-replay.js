#!/usr/bin/env node
/**
 * Dead-letter queue tooling.
 *
 *   npm run dlq:replay -- --topic ledger.commands.dlq --list      # inspect what is parked and why
 *   npm run dlq:replay -- --topic ledger.commands.dlq             # re-publish everything to the original topic
 *
 * Replay is safe because every consumer is idempotent (inbox + guarded state transitions):
 * a message that was actually processed before it landed in the DLQ becomes a no-op.
 * Fix the root cause first (deploy the bug fix, bring the dependency back), then replay.
 */
const { Kafka, logLevel } = require('kafkajs');

const args = process.argv.slice(2);
const arg = (name, def) => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 ? args[i + 1] : def;
};
const topic = arg('topic');
const listOnly = args.includes('--list');
if (!topic || !topic.endsWith('.dlq')) {
  console.error('usage: dlq-replay --topic <name>.dlq [--list]');
  process.exit(2);
}

const kafka = new Kafka({
  clientId: 'dlq-replay',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:29092').split(','),
  logLevel: logLevel.WARN,
});

async function main() {
  const admin = kafka.admin();
  await admin.connect();
  const offsets = await admin.fetchTopicOffsets(topic);
  await admin.disconnect();
  const total = offsets.reduce((s, o) => s + (Number(o.high) - Number(o.low)), 0);
  console.log(`${topic}: ${total} message(s) retained`);
  if (total === 0) return;

  // A dedicated group so replays resume where the previous replay stopped.
  const groupId = listOnly ? `dlq-inspect-${Date.now()}` : 'dlq-replayer';
  const consumer = kafka.consumer({ groupId });
  const idempotent = (process.env.KAFKA_IDEMPOTENT_PRODUCER ?? 'true') !== 'false';
  const producer = kafka.producer({ idempotent, maxInFlightRequests: idempotent ? 1 : 5 });
  await Promise.all([consumer.connect(), producer.connect()]);
  await consumer.subscribe({ topic, fromBeginning: true });

  let seen = 0;
  const finished = new Promise((resolve) => {
    let idle = setTimeout(resolve, 5000);
    consumer.run({
      eachMessage: async ({ message }) => {
        clearTimeout(idle);
        seen++;
        const h = Object.fromEntries(Object.entries(message.headers ?? {}).map(([k, v]) => [k, v?.toString()]));
        console.log(`#${seen} ${h['event-type'] ?? '?'} from ${h['x-original-topic']} ` +
          `[${h['x-error-type']}] ${h['x-error']} (failed at ${h['x-failed-at']})`);
        if (!listOnly) {
          const { ['x-error']: _e, ['x-error-type']: _t, ['x-failed-at']: _f, ...rest } = message.headers ?? {};
          await producer.send({
            topic: h['x-original-topic'],
            acks: -1,
            messages: [{ key: message.key, value: message.value, headers: { ...rest, 'x-replayed-at': new Date().toISOString() } }],
          });
        }
        idle = setTimeout(resolve, 3000);
      },
    });
  });
  await finished;
  await consumer.disconnect();
  await producer.disconnect();
  console.log(listOnly ? `Listed ${seen} message(s).` : `Replayed ${seen} message(s).`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
