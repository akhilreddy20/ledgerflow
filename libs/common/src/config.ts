/** Tiny typed env reader. Every setting has a sensible local-dev default. */
export function env(name: string, fallback?: string): string {
  const value = process.env[name] ?? fallback;
  if (value === undefined) throw new Error(`Missing required environment variable ${name}`);
  return value;
}

export function envInt(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n)) throw new Error(`Environment variable ${name} must be a number, got "${raw}"`);
  return n;
}

export function envBool(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(raw.toLowerCase());
}

export const commonConfig = () => ({
  kafkaBrokers: env('KAFKA_BROKERS', 'localhost:29092').split(','),
  redisUrl: env('REDIS_URL', 'redis://localhost:6379'),
  kafkaPartitions: envInt('KAFKA_PARTITIONS', 6),
});
