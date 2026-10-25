import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
  INestApplication,
  Logger,
  ValidationPipe,
} from '@nestjs/common';
import type { Response } from 'express';
import { BusinessError } from './errors';
import { httpMetricsMiddleware } from './metrics';

export interface ErrorResponse {
  status: number;
  body: { error: string; message: string | string[] };
}

/** Single place that turns any thrown error into an HTTP status + JSON body. */
export function toErrorResponse(err: unknown): ErrorResponse {
  if (err instanceof BusinessError) {
    return { status: err.httpStatus, body: { error: err.code, message: err.message } };
  }
  if (err instanceof HttpException) {
    const res = err.getResponse() as any;
    return {
      status: err.getStatus(),
      body: {
        error: typeof res === 'object' ? (res.error ?? err.name) : err.name,
        message: typeof res === 'object' ? res.message : String(res),
      },
    };
  }
  return { status: HttpStatus.INTERNAL_SERVER_ERROR, body: { error: 'INTERNAL_ERROR', message: 'Internal server error' } };
}

@Catch()
export class AllExceptionsFilter implements ExceptionFilter {
  private readonly logger = new Logger('HTTP');

  catch(exception: unknown, host: ArgumentsHost) {
    const res = host.switchToHttp().getResponse<Response>();
    const { status, body } = toErrorResponse(exception);
    if (status >= 500) this.logger.error((exception as any)?.stack ?? String(exception));
    res.status(status).json(body);
  }
}

/** Common bootstrap: validation, error mapping, metrics middleware, graceful shutdown. */
export function configureApp(app: INestApplication) {
  app.use(httpMetricsMiddleware);
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true, transform: true }));
  app.useGlobalFilters(new AllExceptionsFilter());
  app.enableShutdownHooks();
}
