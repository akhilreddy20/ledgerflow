import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';
import type { NextFunction, Request, Response } from 'express';

/** One Prometheus registry per process, exposed on GET /metrics by every service. */
export const registry = new Registry();
collectDefaultMetrics({ register: registry });

export const httpDuration = new Histogram({
  name: 'http_request_duration_seconds',
  help: 'HTTP request latency',
  labelNames: ['method', 'route', 'status'],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.15, 0.25, 0.5, 1, 2.5, 5],
  registers: [registry],
});

export const messagesProcessed = new Counter({
  name: 'kafka_messages_processed_total',
  help: 'Kafka messages handled by consumers',
  labelNames: ['consumer', 'topic', 'outcome'],
  registers: [registry],
});

export const messageHandlingDuration = new Histogram({
  name: 'kafka_message_handling_seconds',
  help: 'Time spent handling one Kafka message (including retries)',
  labelNames: ['consumer', 'topic'],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
  registers: [registry],
});

export const dlqMessages = new Counter({
  name: 'kafka_dlq_messages_total',
  help: 'Messages routed to a dead-letter topic',
  labelNames: ['consumer', 'topic'],
  registers: [registry],
});

export const outboxBacklog = new Gauge({
  name: 'outbox_unpublished_messages',
  help: 'Rows in the outbox not yet published to Kafka',
  registers: [registry],
});

export const outboxPublished = new Counter({
  name: 'outbox_published_total',
  help: 'Outbox rows published to Kafka',
  registers: [registry],
});

export const idempotentReplays = new Counter({
  name: 'idempotent_replays_total',
  help: 'Requests answered from a stored idempotent response instead of being re-executed',
  labelNames: ['scope'],
  registers: [registry],
});

/** Express middleware recording latency per *route template* (not raw URL, to keep cardinality low). */
export function httpMetricsMiddleware(req: Request, res: Response, next: NextFunction) {
  const end = httpDuration.startTimer();
  res.on('finish', () => {
    const route = (req.route?.path as string | undefined) ?? (res.statusCode === 404 ? 'unmatched' : req.path);
    end({ method: req.method, route: `${req.baseUrl ?? ''}${route}`, status: String(res.statusCode) });
  });
  next();
}
