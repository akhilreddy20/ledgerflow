import { TransientError } from './errors';

export type CircuitState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

export interface CircuitBreakerOptions {
  name: string;
  /** Number of failures inside the rolling window that trips the breaker. */
  failureThreshold: number;
  /** Rolling window for counting failures. */
  windowMs: number;
  /** How long the breaker stays OPEN before letting a probe request through. */
  openDurationMs: number;
  /** Number of probe calls allowed while HALF_OPEN. */
  halfOpenMaxCalls: number;
  /** Errors that should NOT count as failures (e.g. a business decline is a healthy response). */
  isFailure?: (err: unknown) => boolean;
  onStateChange?: (from: CircuitState, to: CircuitState) => void;
  now?: () => number;
}

export class CircuitOpenError extends TransientError {
  constructor(name: string) {
    super(`Circuit "${name}" is OPEN - failing fast`);
    this.name = 'CircuitOpenError';
  }
}

/**
 * Classic three-state circuit breaker.
 *
 *  CLOSED    -> calls flow; failures are counted in a rolling window.
 *  OPEN      -> calls fail immediately (no load on the struggling dependency).
 *  HALF_OPEN -> after openDurationMs a limited number of probes are allowed.
 *               One success closes the circuit, one failure re-opens it.
 */
export class CircuitBreaker {
  private state: CircuitState = 'CLOSED';
  private failures: number[] = [];
  private openedAt = 0;
  private halfOpenInFlight = 0;
  private readonly now: () => number;

  constructor(private readonly opts: CircuitBreakerOptions) {
    this.now = opts.now ?? Date.now;
  }

  get currentState(): CircuitState {
    if (this.state === 'OPEN' && this.now() - this.openedAt >= this.opts.openDurationMs) {
      this.transition('HALF_OPEN');
    }
    return this.state;
  }

  snapshot() {
    return {
      name: this.opts.name,
      state: this.currentState,
      recentFailures: this.recentFailures(),
      openedAt: this.openedAt ? new Date(this.openedAt).toISOString() : null,
    };
  }

  async execute<T>(fn: () => Promise<T>): Promise<T> {
    const state = this.currentState;
    if (state === 'OPEN') throw new CircuitOpenError(this.opts.name);
    if (state === 'HALF_OPEN') {
      if (this.halfOpenInFlight >= this.opts.halfOpenMaxCalls) throw new CircuitOpenError(this.opts.name);
      this.halfOpenInFlight++;
    }
    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (err) {
      const counts = this.opts.isFailure ? this.opts.isFailure(err) : true;
      if (counts) this.onFailure();
      else this.onSuccess();
      throw err;
    } finally {
      if (state === 'HALF_OPEN') this.halfOpenInFlight--;
    }
  }

  private onSuccess() {
    if (this.state === 'HALF_OPEN') this.transition('CLOSED');
  }

  private onFailure() {
    if (this.state === 'HALF_OPEN') {
      this.trip();
      return;
    }
    this.failures.push(this.now());
    if (this.recentFailures() >= this.opts.failureThreshold) this.trip();
  }

  private recentFailures(): number {
    const cutoff = this.now() - this.opts.windowMs;
    this.failures = this.failures.filter((t) => t > cutoff);
    return this.failures.length;
  }

  private trip() {
    this.openedAt = this.now();
    this.transition('OPEN');
  }

  private transition(to: CircuitState) {
    if (this.state === to) return;
    const from = this.state;
    this.state = to;
    if (to === 'CLOSED') {
      this.failures = [];
      this.openedAt = 0;
    }
    this.opts.onStateChange?.(from, to);
  }
}
