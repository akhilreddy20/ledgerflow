import { Injectable, Logger } from '@nestjs/common';
import {
  BusinessError,
  CircuitBreaker,
  CircuitOpenError,
  CircuitState,
  env,
  envInt,
  isRetryable,
  registry,
  retryWithBackoff,
  TransientError,
  withSpan,
} from '@ledgerflow/common';
import { Counter, Gauge } from 'prom-client';

/** The PSP answered "no" - a definitive business outcome, not an outage. */
export class PspDeclinedError extends BusinessError {
  constructor(reason: string) {
    super('PSP_DECLINED', reason, 422);
  }
}

/** The PSP is still processing an earlier attempt with the same key - healthy, just not done yet. */
export class PspInProgressError extends TransientError {
  constructor() {
    super('PSP is still processing this payout');
    this.name = 'PspInProgressError';
  }
}

export interface PayoutRequest {
  paymentId: string;
  amount: number;
  currency: string;
  destinationAccountId: string;
}

export interface PayoutResult {
  pspReference: string;
  status: 'succeeded';
}

export type PayoutLookup = { status: 'succeeded'; pspReference: string } | { status: 'declined' | 'not_found' };

const STATE_VALUE: Record<CircuitState, number> = { CLOSED: 0, HALF_OPEN: 1, OPEN: 2 };

const breakerState = new Gauge({
  name: 'circuit_breaker_state',
  help: 'Circuit breaker state (0=closed, 1=half-open, 2=open)',
  labelNames: ['name'],
  registers: [registry],
});
const pspCalls = new Counter({
  name: 'psp_calls_total',
  help: 'Calls to the external payment service provider',
  labelNames: ['outcome'],
  registers: [registry],
});

/**
 * Resilient client for the external Payment Service Provider (the simulator in this repo).
 *
 *  - timeout on every call (a hung dependency must not hang us)
 *  - retries with exponential backoff + full jitter for transient failures only
 *  - circuit breaker: after repeated failures we stop calling for a while and fail fast,
 *    giving the PSP room to recover; declines do NOT count as failures
 *  - the payment id is sent as the PSP Idempotency-Key, so retries (ours or after a timeout where the PSP
 *    actually processed the request) can never pay out twice
 */
@Injectable()
export class PspClient {
  private readonly logger = new Logger(PspClient.name);
  private readonly baseUrl = env('PSP_URL', 'http://localhost:3004');
  private readonly timeoutMs = envInt('PSP_TIMEOUT_MS', 2000);

  readonly breaker = new CircuitBreaker({
    name: 'psp',
    failureThreshold: envInt('PSP_BREAKER_FAILURES', 5),
    windowMs: envInt('PSP_BREAKER_WINDOW_MS', 10_000),
    openDurationMs: envInt('PSP_BREAKER_OPEN_MS', 15_000),
    halfOpenMaxCalls: 1,
    // Declines and "still processing" are healthy answers, not PSP failures.
    isFailure: (err) => !(err instanceof PspDeclinedError) && !(err instanceof PspInProgressError),
    onStateChange: (from, to) => {
      this.logger.warn(`PSP circuit ${from} -> ${to}`);
      breakerState.set({ name: 'psp' }, STATE_VALUE[to]);
    },
  });

  constructor() {
    breakerState.set({ name: 'psp' }, 0);
  }

  payout(req: PayoutRequest): Promise<PayoutResult> {
    return withSpan('psp.payout', { 'payment.id': req.paymentId, 'payment.amount': req.amount }, () =>
      retryWithBackoff(() => this.breaker.execute(() => this.doPayout(req)), {
        attempts: envInt('PSP_RETRY_ATTEMPTS', 3),
        baseDelayMs: 200,
        maxDelayMs: 2000,
        // Don't hammer an open circuit; let the saga retry later.
        shouldRetry: (err) => isRetryable(err) && !(err instanceof CircuitOpenError),
        onRetry: (err: any, attempt, delay) =>
          this.logger.warn(`PSP payout ${req.paymentId} attempt ${attempt} failed (${err.message}); retry in ${delay}ms`),
      }),
    );
  }

  /** Ask the PSP what happened to a payout - used before compensating a timed-out saga. */
  async lookup(paymentId: string): Promise<PayoutLookup> {
    return this.breaker.execute(async () => {
      const res = await this.request('GET', `/v1/payouts/${paymentId}`);
      if (res.status === 404) return { status: 'not_found' };
      if (res.status >= 500) throw new TransientError(`PSP lookup failed with ${res.status}`);
      const body = (await res.json()) as any;
      return body.status === 'succeeded' ? { status: 'succeeded', pspReference: body.id } : { status: 'declined' };
    });
  }

  private async doPayout(req: PayoutRequest): Promise<PayoutResult> {
    const res = await this.request(
      'POST',
      '/v1/payouts',
      { amount: req.amount, currency: req.currency, destination: req.destinationAccountId, reference: req.paymentId },
      req.paymentId,
    );
    const body = (await res.json().catch(() => ({}))) as any;
    if (res.status === 200 || res.status === 201) {
      pspCalls.inc({ outcome: 'succeeded' });
      return { pspReference: body.id, status: 'succeeded' };
    }
    if (res.status === 402) {
      pspCalls.inc({ outcome: 'declined' });
      throw new PspDeclinedError(body.reason ?? 'declined');
    }
    if (res.status === 409) {
      pspCalls.inc({ outcome: 'in_progress' });
      throw new PspInProgressError();
    }
    pspCalls.inc({ outcome: 'error' });
    throw new TransientError(`PSP responded ${res.status}: ${body.error ?? ''}`);
  }

  private async request(method: string, path: string, body?: unknown, idempotencyKey?: string): Promise<Response> {
    try {
      return await fetch(`${this.baseUrl}${path}`, {
        method,
        headers: {
          'content-type': 'application/json',
          ...(idempotencyKey ? { 'idempotency-key': idempotencyKey } : {}),
        },
        body: body ? JSON.stringify(body) : undefined,
        signal: AbortSignal.timeout(this.timeoutMs),
      });
    } catch (err: any) {
      pspCalls.inc({ outcome: err?.name === 'TimeoutError' ? 'timeout' : 'network_error' });
      throw new TransientError(`PSP ${method} ${path} failed: ${err?.name === 'TimeoutError' ? 'timeout' : err?.message}`, err);
    }
  }
}
