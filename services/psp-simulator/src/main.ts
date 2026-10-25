import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { envInt, httpMetricsMiddleware } from '@ledgerflow/common';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { logger: ['warn', 'error'] });
  app.use(httpMetricsMiddleware);
  app.enableShutdownHooks();
  const port = envInt('PORT', 3004);
  await app.listen(port);
  console.log(`psp-simulator listening on :${port}`);
}

bootstrap();
