/**
 * Error taxonomy used across services.
 *
 *  - BusinessError:      an expected domain outcome (insufficient funds, unknown account).
 *                        Never retried. HTTP layer maps it to a 4xx.
 *  - NonRetryableError:  a message that can never succeed (malformed payload). Sent straight to the DLQ.
 *  - TransientError:     something that may succeed later (network, PSP 5xx, open circuit). Retried with backoff.
 */
export class BusinessError extends Error {
  constructor(
    public readonly code: string,
    message: string,
    public readonly httpStatus = 422,
  ) {
    super(message);
    this.name = 'BusinessError';
  }
}

export class NonRetryableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'NonRetryableError';
  }
}

export class TransientError extends Error {
  constructor(message: string, public readonly cause?: unknown) {
    super(message);
    this.name = 'TransientError';
  }
}

export function isRetryable(err: unknown): boolean {
  return !(err instanceof BusinessError) && !(err instanceof NonRetryableError);
}
