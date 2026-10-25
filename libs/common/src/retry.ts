import { isRetryable } from './errors';

export interface RetryOptions {
  /** Total attempts including the first one. */
  attempts: number;
  /** Base delay in ms for exponential backoff. */
  baseDelayMs: number;
  /** Upper bound for a single delay. */
  maxDelayMs: number;
  /** Decide whether an error is worth retrying. Defaults to the shared error taxonomy. */
  shouldRetry?: (err: unknown) => boolean;
  onRetry?: (err: unknown, attempt: number, delayMs: number) => void;
  /** Injected for tests. */
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
}

export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * "Full jitter" exponential backoff (AWS Architecture Blog):
 *   delay = random(0, min(maxDelay, base * 2^attempt))
 * Jitter spreads retries out so that many clients failing at the same moment
 * do not hammer the dependency again in lock-step (thundering herd).
 */
export function backoffDelay(attempt: number, baseDelayMs: number, maxDelayMs: number, random = Math.random): number {
  const ceiling = Math.min(maxDelayMs, baseDelayMs * 2 ** attempt);
  return Math.floor(random() * ceiling);
}

export async function retryWithBackoff<T>(fn: (attempt: number) => Promise<T>, opts: RetryOptions): Promise<T> {
  const shouldRetry = opts.shouldRetry ?? isRetryable;
  const doSleep = opts.sleep ?? sleep;
  let lastError: unknown;
  for (let attempt = 0; attempt < opts.attempts; attempt++) {
    try {
      return await fn(attempt);
    } catch (err) {
      lastError = err;
      const isLast = attempt === opts.attempts - 1;
      if (isLast || !shouldRetry(err)) throw err;
      const delay = backoffDelay(attempt, opts.baseDelayMs, opts.maxDelayMs, opts.random);
      opts.onRetry?.(err, attempt + 1, delay);
      await doSleep(delay);
    }
  }
  throw lastError;
}
