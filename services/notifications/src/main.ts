import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { configureApp, envInt } from '@ledgerflow/common';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { logger: ['log', 'warn', 'error'] });
  configureApp(app);
  const port = envInt('PORT', 3003);
  await app.listen(port);
  console.log(`notifications-service listening on :${port}`);
}

bootstrap();
