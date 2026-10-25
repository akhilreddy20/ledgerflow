import { readdirSync, readFileSync } from 'fs';
import { join } from 'path';
import { Pool, PoolClient, types } from 'pg';

// BIGINT (int8) columns come back from pg as strings by default. All money in this system is stored
// in minor units (paise/cents) and validated to stay within Number.MAX_SAFE_INTEGER, so parse to number.
types.setTypeParser(20, (value) => {
  const n = Number(value);
  if (!Number.isSafeInteger(n)) throw new Error(`int8 value ${value} exceeds Number.MAX_SAFE_INTEGER`);
  return n;
});

export type Db = Pool;
export type Tx = PoolClient;

export function createPool(connectionString: string, max = 20): Pool {
  return new Pool({
    connectionString,
    max,
    idleTimeoutMillis: 30_000,
    // Fail fast instead of queueing forever when the pool is exhausted.
    connectionTimeoutMillis: 5_000,
    application_name: process.env.OTEL_SERVICE_NAME ?? 'ledgerflow',
  });
}

/** Postgres error codes we react to. */
export const PgErrors = {
  UNIQUE_VIOLATION: '23505',
  SERIALIZATION_FAILURE: '40001',
  DEADLOCK_DETECTED: '40P01',
} as const;

/**
 * Run fn inside BEGIN/COMMIT. Rolls back on any error.
 * Serialization failures and deadlocks are retried a few times because they are
 * safe to replay - the whole transaction was rolled back.
 */
export async function withTransaction<T>(pool: Pool, fn: (tx: Tx) => Promise<T>, maxAttempts = 3): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const result = await fn(client);
      await client.query('COMMIT');
      return result;
    } catch (err: any) {
      await client.query('ROLLBACK').catch(() => undefined);
      const retryable = err?.code === PgErrors.SERIALIZATION_FAILURE || err?.code === PgErrors.DEADLOCK_DETECTED;
      if (!retryable || attempt >= maxAttempts) throw err;
    } finally {
      client.release();
    }
  }
}

/**
 * Minimal forward-only migration runner.
 * Applies every *.sql file in `dir` (sorted by name) exactly once, recorded in schema_migrations.
 * A transaction-level advisory lock makes it safe when several replicas start at the same time.
 */
export async function runMigrations(pool: Pool, dir: string): Promise<string[]> {
  const files = readdirSync(dir)
    .filter((f) => f.endsWith('.sql'))
    .sort();
  const applied: string[] = [];
  await withTransaction(pool, async (tx) => {
    await tx.query('SELECT pg_advisory_xact_lock(727274)');
    await tx.query(`CREATE TABLE IF NOT EXISTS schema_migrations (
      name TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
    const { rows } = await tx.query<{ name: string }>('SELECT name FROM schema_migrations');
    const done = new Set(rows.map((r) => r.name));
    for (const file of files) {
      if (done.has(file)) continue;
      await tx.query(readFileSync(join(dir, file), 'utf8'));
      await tx.query('INSERT INTO schema_migrations(name) VALUES ($1)', [file]);
      applied.push(file);
    }
  });
  return applied;
}
