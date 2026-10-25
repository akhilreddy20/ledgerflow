/**
 * OpenTelemetry bootstrap. Loaded BEFORE the application with:
 *
 *   node --require ./libs/common/dist/otel.js services/<name>/dist/main.js
 *
 * It must run first so the auto-instrumentations can patch `http`, `express`, `pg` and `ioredis`
 * before any application code imports them. Kafka context propagation is done manually
 * (see tracing.ts) because the outbox decouples the producer from the request.
 *
 * Tracing is enabled only when OTEL_EXPORTER_OTLP_ENDPOINT is set (e.g. http://jaeger:4318).
 */
import { NodeSDK } from '@opentelemetry/sdk-node';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';

const endpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT;

if (endpoint) {
  const sdk = new NodeSDK({
    serviceName: process.env.OTEL_SERVICE_NAME ?? 'ledgerflow',
    traceExporter: new OTLPTraceExporter({ url: `${endpoint.replace(/\/$/, '')}/v1/traces` }),
    instrumentations: [
      getNodeAutoInstrumentations({
        // Very noisy and not useful here.
        '@opentelemetry/instrumentation-fs': { enabled: false },
        '@opentelemetry/instrumentation-net': { enabled: false },
        '@opentelemetry/instrumentation-dns': { enabled: false },
        // We propagate trace context through Kafka ourselves: the outbox stores the traceparent of the
        // request that created the message, and the relay must publish THAT context - not its own.
        // The kafkajs auto-instrumentation would overwrite it with the relay loop's context.
        '@opentelemetry/instrumentation-kafkajs': { enabled: false },
      }),
    ],
  });
  sdk.start();
  const shutdown = () => sdk.shutdown().catch(() => undefined);
  process.once('SIGTERM', shutdown);
  process.once('SIGINT', shutdown);
  // eslint-disable-next-line no-console
  console.log(`[otel] tracing enabled -> ${endpoint}`);
}
