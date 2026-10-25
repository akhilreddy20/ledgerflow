import { Module } from '@nestjs/common';
import { CoreModule, env } from '@ledgerflow/common';
import { join } from 'path';
import { NotificationsConsumer } from './notifications.consumer';
import { NotificationsController } from './notifications.controller';

@Module({
  imports: [
    CoreModule.forRoot({
      serviceName: 'notifications-service',
      databaseUrl: env('DATABASE_URL', 'postgres://ledgerflow:ledgerflow@localhost:5432/notifications'),
      migrationsDir: join(__dirname, '..', 'migrations'),
      outboxRelay: false,
      dbPoolSize: 10,
    }),
  ],
  controllers: [NotificationsController],
  providers: [NotificationsConsumer],
})
export class AppModule {}
