import { Body, Controller, Get, Header, Headers, HttpException, Param, Post, Res } from '@nestjs/common';
import { registry } from '@ledgerflow/common';
import type { Response } from 'express';
import { randomUUID } from 'crypto';
import { Counter } from 'prom-client';

interface Payout {
  id: string;
  idempotencyKey: string;
  amount: number;
  currency: string;
  destination: string;
  status: 'succeeded' | 'declined';
  reason?: string;
  createdAt: string;
}

interface Chaos {
  /** Probability of a 503 (transient, not recorded - a retry can succeed). */
  failureRate: number;
  /** Probability of a 402 decline (recorded - retries get the same decline). */
  declineRate: number;
  /** Probability of a very slow response (longer than the client's timeout). The payout IS processed. */
  slowRate: number;
  slowMs: number;
  minLatencyMs: number;
  maxLatencyMs: number;
}

const requests = new Counter({
  name: 'psp_sim_requests_total',
  help: 'PSP simulator responses',
  labelNames: ['outcome'],
  registers: [registry],
});

const num = (v: string | undefined, d: number) => (v === undefined || v === '' ? d : Number(v));
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * A fake external Payment Service Provider (think Razorpay/Stripe payouts) with controllable failures.
 *
 * Deterministic "magic amounts" (like Stripe test cards) make demos and tests reproducible:
 *   amount ending in ...402  -> always declined (402)
 *   amount ending in ...503  -> always 503 (drives retries, the circuit breaker and saga recovery)
 *   amount ending in ...504  -> processed, but responds after slowMs (client times out, then the
 *                               retry with the same Idempotency-Key returns the stored result)
 */
@Controller()
export class PspController {
  private readonly payouts = new Map<string, Payout>();
  private readonly inFlight = new Set<string>();
  private chaos: Chaos = {
    failureRate: num(process.env.PSP_FAILURE_RATE, 0),
    declineRate: num(process.env.PSP_DECLINE_RATE, 0),
    slowRate: num(process.env.PSP_SLOW_RATE, 0),
    slowMs: num(process.env.PSP_SLOW_MS, 5000),
    minLatencyMs: num(process.env.PSP_MIN_LATENCY_MS, 10),
    maxLatencyMs: num(process.env.PSP_MAX_LATENCY_MS, 60),
  };

  @Post('v1/payouts')
  async create(
    @Headers('idempotency-key') key: string | undefined,
    @Body() body: { amount: number; currency: string; destination: string; reference: string },
    @Res() res: Response,
  ) {
    if (!key) throw new HttpException({ error: 'missing Idempotency-Key' }, 400);

    const existing = this.payouts.get(key);
    if (existing) {
      requests.inc({ outcome: 'replay' });
      return res.status(existing.status === 'succeeded' ? 200 : 402).json(existing);
    }
    if (this.inFlight.has(key)) {
      requests.inc({ outcome: 'conflict' });
      return res.status(409).json({ error: 'request with this key in progress' });
    }

    const tail = body.amount % 1000;
    const r = Math.random();
    await sleep(this.chaos.minLatencyMs + Math.random() * (this.chaos.maxLatencyMs - this.chaos.minLatencyMs));

    if (tail === 503 || r < this.chaos.failureRate) {
      requests.inc({ outcome: 'unavailable' });
      return res.status(503).json({ error: 'upstream bank unavailable' });
    }

    this.inFlight.add(key);
    try {
      if (tail === 504 || Math.random() < this.chaos.slowRate) await sleep(this.chaos.slowMs);
      const declined = tail === 402 || Math.random() < this.chaos.declineRate;
      const payout: Payout = {
        id: `po_${randomUUID().replace(/-/g, '').slice(0, 20)}`,
        idempotencyKey: key,
        amount: body.amount,
        currency: body.currency,
        destination: body.destination,
        status: declined ? 'declined' : 'succeeded',
        reason: declined ? 'beneficiary bank rejected the transfer' : undefined,
        createdAt: new Date().toISOString(),
      };
      this.payouts.set(key, payout);
      requests.inc({ outcome: payout.status });
      return res.status(declined ? 402 : 201).json(payout);
    } finally {
      this.inFlight.delete(key);
    }
  }

  @Get('v1/payouts/:key')
  get(@Param('key') key: string) {
    const p = this.payouts.get(key);
    if (!p) throw new HttpException({ error: 'not found' }, 404);
    return p;
  }

  @Get('admin/chaos')
  getChaos() {
    return { ...this.chaos, storedPayouts: this.payouts.size };
  }

  /** Change failure injection at runtime, e.g. {"failureRate": 1} to simulate a full outage. */
  @Post('admin/chaos')
  setChaos(@Body() patch: Partial<Chaos>) {
    for (const [k, v] of Object.entries(patch)) {
      if (k in this.chaos && typeof v === 'number') (this.chaos as any)[k] = v;
    }
    return this.chaos;
  }

  @Get('health')
  health() {
    return { status: 'ok' };
  }

  @Get('ready')
  ready() {
    return { status: 'ready' };
  }

  @Get('metrics')
  @Header('Content-Type', registry.contentType)
  metrics() {
    return registry.metrics();
  }
}
