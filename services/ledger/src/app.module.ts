import { Module } from '@nestjs/common';
import { CoreModule, env } from '@ledgerflow/common';
import { join } from 'path';
import { LedgerCommandConsumer } from './ledger.consumer';
import { LedgerController } from './ledger.controller';
import { LedgerService } from './ledger.service';

@Module({
  imports: [
    CoreModule.forRoot({
      serviceName: 'ledger-service',
      databaseUrl: env('DATABASE_URL', 'postgres://ledgerflow:ledgerflow@localhost:5432/ledger'),
      migrationsDir: join(__dirname, '..', 'migrations'),
      dbPoolSize: Number(process.env.DB_POOL_SIZE ?? 30),
    }),
  ],
  controllers: [LedgerController],
  providers: [LedgerService, LedgerCommandConsumer],
})
export class AppModule {}
