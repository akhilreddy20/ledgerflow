import { Tx } from './db';

/**
 * IDEMPOTENT CONSUMER ("inbox")
 *
 * Kafka + the outbox give at-least-once delivery, so the same message can arrive twice
 * (relay crash, consumer rebalance before offset commit, DLQ replay...).
 *
 * Call this as the FIRST statement in the same DB transaction that applies the message's effects.
 * The (consumer, message_id) primary key makes the second delivery a no-op:
 *   - returns true  -> first time we see it, go ahead
 *   - returns false -> already processed, skip (and just commit the offset)
 *
 * Because the inbox row and the business change commit atomically, the effect happens exactly once.
 */
export async function markProcessed(tx: Tx, consumer: string, messageId: string): Promise<boolean> {
  const res = await tx.query(
    `INSERT INTO processed_messages (consumer, message_id) VALUES ($1, $2)
     ON CONFLICT DO NOTHING`,
    [consumer, messageId],
  );
  return res.rowCount === 1;
}
