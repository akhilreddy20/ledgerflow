import { context, propagation, SpanKind, SpanStatusCode, trace, Context } from '@opentelemetry/api';

const tracer = trace.getTracer('ledgerflow');

/** Serialize the active trace context (W3C traceparent) so it can travel inside Kafka headers. */
export function currentTraceHeaders(): Record<string, string> {
  const carrier: Record<string, string> = {};
  propagation.inject(context.active(), carrier);
  return carrier;
}

/** Rebuild a trace context from Kafka headers. */
export function contextFromHeaders(headers: Record<string, string | undefined>): Context {
  return propagation.extract(context.active(), headers);
}

/** Run fn inside a span. Works as a no-op when tracing is disabled. */
export async function withSpan<T>(
  name: string,
  attributes: Record<string, string | number | boolean>,
  fn: () => Promise<T>,
  options: { parent?: Context; kind?: SpanKind } = {},
): Promise<T> {
  const parent = options.parent ?? context.active();
  const span = tracer.startSpan(name, { kind: options.kind ?? SpanKind.INTERNAL, attributes }, parent);
  return context.with(trace.setSpan(parent, span), async () => {
    try {
      const result = await fn();
      span.setStatus({ code: SpanStatusCode.OK });
      return result;
    } catch (err: any) {
      span.recordException(err);
      span.setStatus({ code: SpanStatusCode.ERROR, message: err?.message });
      throw err;
    } finally {
      span.end();
    }
  });
}

export { SpanKind };
