import {
  BadRequestException,
  CallHandler,
  ConflictException,
  ExecutionContext,
  Inject,
  Injectable,
  NestInterceptor,
  UnprocessableEntityException,
  UseInterceptors,
} from '@nestjs/common';
import { createHash } from 'crypto';
import type { Request, Response } from 'express';
import type Redis from 'ioredis';
import { from, Observable, of, throwError } from 'rxjs';
import { catchError, mergeMap } from 'rxjs/operators';
import { toErrorResponse } from './http';
import { idempotentReplays } from './metrics';
import { REDIS } from './tokens';

export interface StoredResponse {
  status: number;
  body: unknown;
}

type Record_ = { state: 'in_progress'; fp: string } | { state: 'done'; fp: string; response: StoredResponse };

export type BeginResult =
  | { kind: 'new' }
  | { kind: 'replay'; response: StoredResponse }
  | { kind: 'in_progress' }
  | { kind: 'mismatch' };

/**
 * IDEMPOTENCY KEYS - two layers:
 *
 *  1. Redis (this class) - the fast path. Stores the first response for a key for 24h and replays it
 *     for retries, plus a short "in progress" lock so two concurrent retries do not both execute.
 *  2. PostgreSQL unique constraint on idempotency_key - the source of truth. If Redis lost the key
 *     (eviction, failover), the insert hits the constraint and the service returns the existing row.
 *
 * The request fingerprint (hash of method + path + body) catches clients that re-use a key for a
 * *different* request, which is a client bug and gets a 422.
 */
export class IdempotencyStore {
  constructor(
    private readonly redis: Redis,
    private readonly scope: string,
    private readonly ttlSeconds = 24 * 3600,
    private readonly lockSeconds = 30,
  ) {}

  private k(key: string) {
    return `idem:${this.scope}:${key}`;
  }

  async begin(key: string, fingerprint: string): Promise<BeginResult> {
    const lock: Record_ = { state: 'in_progress', fp: fingerprint };
    const acquired = await this.redis.set(this.k(key), JSON.stringify(lock), 'EX', this.lockSeconds, 'NX');
    if (acquired === 'OK') return { kind: 'new' };

    const raw = await this.redis.get(this.k(key));
    if (!raw) return this.begin(key, fingerprint); // expired between SET and GET - try again
    const rec = JSON.parse(raw) as Record_;
    if (rec.fp !== fingerprint) return { kind: 'mismatch' };
    if (rec.state === 'in_progress') return { kind: 'in_progress' };
    return { kind: 'replay', response: rec.response };
  }

  async complete(key: string, fingerprint: string, response: StoredResponse): Promise<void> {
    const rec: Record_ = { state: 'done', fp: fingerprint, response };
    await this.redis.set(this.k(key), JSON.stringify(rec), 'EX', this.ttlSeconds);
  }

  /** Release the lock after an unexpected (5xx) failure so the client may retry. */
  async abort(key: string): Promise<void> {
    await this.redis.del(this.k(key));
  }
}

export function requestFingerprint(req: Request): string {
  return createHash('sha256')
    .update(`${req.method}\n${req.baseUrl}${req.path}\n${JSON.stringify(req.body ?? {})}`)
    .digest('hex');
}

const KEY_PATTERN = /^[A-Za-z0-9_\-:.]{8,128}$/;

/**
 * Apply with @UseInterceptors(new IdempotencyInterceptor(store)) on POST handlers.
 * Requires an `Idempotency-Key` header. Successful and 4xx responses are stored and replayed;
 * 5xx responses release the key so the client can retry safely.
 */
export class IdempotencyInterceptor implements NestInterceptor {
  constructor(private readonly store: IdempotencyStore, private readonly scope: string) {}

  intercept(ctx: ExecutionContext, next: CallHandler): Observable<unknown> {
    const req = ctx.switchToHttp().getRequest<Request>();
    const res = ctx.switchToHttp().getResponse<Response>();
    const key = req.header('idempotency-key');
    if (!key || !KEY_PATTERN.test(key)) {
      return throwError(
        () => new BadRequestException('Idempotency-Key header is required (8-128 chars of [A-Za-z0-9_-:.])'),
      );
    }
    const fp = requestFingerprint(req);

    return from(this.store.begin(key, fp)).pipe(
      mergeMap((begin) => {
        switch (begin.kind) {
          case 'mismatch':
            return throwError(
              () => new UnprocessableEntityException('Idempotency-Key was already used with a different request body'),
            );
          case 'in_progress':
            return throwError(() => new ConflictException('A request with this Idempotency-Key is still being processed'));
          case 'replay':
            idempotentReplays.inc({ scope: this.scope });
            res.status(begin.response.status);
            res.setHeader('Idempotent-Replayed', 'true');
            return of(begin.response.body);
          case 'new':
            return next.handle().pipe(
              mergeMap(async (body) => {
                await this.store.complete(key, fp, { status: res.statusCode, body });
                return body;
              }),
              catchError((err) =>
                from(
                  (async () => {
                    const { status, body } = toErrorResponse(err);
                    if (status < 500) await this.store.complete(key, fp, { status, body });
                    else await this.store.abort(key);
                    throw err;
                  })(),
                ),
              ),
            );
        }
      }),
    );
  }
}

/**
 * Method decorator: `@Idempotent('payments')` on a POST handler.
 * Builds an injectable interceptor bound to the shared Redis client and a key namespace (scope).
 */
export function Idempotent(scope: string) {
  @Injectable()
  class ScopedIdempotencyInterceptor extends IdempotencyInterceptor {
    constructor(@Inject(REDIS) redis: Redis) {
      super(new IdempotencyStore(redis, scope), scope);
    }
  }
  return UseInterceptors(ScopedIdempotencyInterceptor);
}
