import {
  Controller,
  DynamicModule,
  Get,
  Global,
  Header,
  Inject,
  Logger,
  Module,
  OnApplicationShutdown,
  OnModuleInit,
  ServiceUnavailableException,
} from '@nestjs/common';
import Redis from 'ioredis';
import { Kafka, Producer } from 'kafkajs';
import { Pool } from 'pg';
import { commonConfig } from './config';
import { createPool, runMigrations } from './db';
import { createKafka, createProducer, ensureTopics } from './kafka';
import { registry } from './metrics';
import { OutboxRelay } from './outbox';

import { KAFKA, PG_POOL, PRODUCER, REDIS, SERVICE_NAME } from './tokens';

export interface CoreModuleOptions {
  serviceName: string;
  databaseUrl: string;
  migrationsDir: string;
  /** Publish this service's outbox to Kafka (true for every service that writes events). */
  outboxRelay?: boolean;
  dbPoolSize?: number;
}

@Controller()
class OpsController {
  constructor(@Inject(PG_POOL) private readonly pool: Pool, @Inject(REDIS) private readonly redis: Redis) {}

  /** Liveness: the process is up. */
  @Get('health')
  health() {
    return { status: 'ok' };
  }

  /** Readiness: dependencies reachable - Kubernetes only routes traffic when this passes. */
  @Get('ready')
  async ready() {
    try {
      await this.pool.query('SELECT 1');
      await this.redis.ping();
      return { status: 'ready' };
    } catch (err: any) {
      throw new ServiceUnavailableException(`not ready: ${err.message}`);
    }
  }

  @Get('metrics')
  @Header('Content-Type', registry.contentType)
  metrics() {
    return registry.metrics();
  }
}

class CoreLifecycle implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger('Core');
  private relay?: OutboxRelay;

  constructor(
    private readonly opts: CoreModuleOptions,
    private readonly pool: Pool,
    private readonly redis: Redis,
    private readonly kafka: Kafka,
    private readonly producer: Producer,
  ) {}

  async onModuleInit() {
    const applied = await runMigrations(this.pool, this.opts.migrationsDir);
    if (applied.length) this.logger.log(`Applied migrations: ${applied.join(', ')}`);
    await ensureTopics(this.kafka, commonConfig().kafkaPartitions);
    if (this.opts.outboxRelay !== false) {
      this.relay = new OutboxRelay(this.pool, this.producer);
      this.relay.start();
      this.logger.log('Outbox relay started');
    }
  }

  async onApplicationShutdown() {
    await this.relay?.stop();
    await this.producer.disconnect().catch(() => undefined);
    this.redis.disconnect();
    await this.pool.end().catch(() => undefined);
  }
}

/**
 * Shared infrastructure for every service: PostgreSQL pool, Redis, Kafka client + idempotent producer,
 * migrations on boot, topic creation, outbox relay, and /health /ready /metrics endpoints.
 */
@Global()
@Module({})
export class CoreModule {
  static forRoot(opts: CoreModuleOptions): DynamicModule {
    const cfg = commonConfig();
    return {
      module: CoreModule,
      controllers: [OpsController],
      providers: [
        { provide: SERVICE_NAME, useValue: opts.serviceName },
        { provide: PG_POOL, useFactory: () => createPool(opts.databaseUrl, opts.dbPoolSize ?? 20) },
        { provide: REDIS, useFactory: () => new Redis(cfg.redisUrl, { maxRetriesPerRequest: 3 }) },
        { provide: KAFKA, useFactory: () => createKafka(opts.serviceName, cfg.kafkaBrokers) },
        { provide: PRODUCER, inject: [KAFKA], useFactory: (kafka: Kafka) => createProducer(kafka) },
        {
          provide: CoreLifecycle,
          inject: [PG_POOL, REDIS, KAFKA, PRODUCER],
          useFactory: (pool: Pool, redis: Redis, kafka: Kafka, producer: Producer) =>
            new CoreLifecycle(opts, pool, redis, kafka, producer),
        },
      ],
      exports: [SERVICE_NAME, PG_POOL, REDIS, KAFKA, PRODUCER],
    };
  }
}
