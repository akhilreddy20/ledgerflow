import { Module } from '@nestjs/common';
import { CoreModule, env } from '@ledgerflow/common';
import { join } from 'path';
import { PaymentSaga } from './payment.saga';
import { PaymentsController } from './payments.controller';
import { PaymentsService } from './payments.service';
import { PspClient } from './psp.client';

@Module({
  imports: [
    CoreModule.forRoot({
      serviceName: 'payments-service',
      databaseUrl: env('DATABASE_URL', 'postgres://ledgerflow:ledgerflow@localhost:5432/payments'),
      migrationsDir: join(__dirname, '..', 'migrations'),
    }),
  ],
  controllers: [PaymentsController],
  providers: [PaymentsService, PspClient, PaymentSaga],
})
export class AppModule {}
