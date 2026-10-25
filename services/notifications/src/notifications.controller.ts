import { Controller, DefaultValuePipe, Get, Inject, ParseIntPipe, Query } from '@nestjs/common';
import { PG_POOL } from '@ledgerflow/common';
import { Pool } from 'pg';

@Controller('notifications')
export class NotificationsController {
  constructor(@Inject(PG_POOL) private readonly pool: Pool) {}

  @Get()
  async list(
    @Query('paymentId') paymentId: string | undefined,
    @Query('limit', new DefaultValuePipe(50), ParseIntPipe) limit: number,
  ) {
    const { rows } = await this.pool.query(
      `SELECT id, payment_id AS "paymentId", type, channel, recipient_account_id AS "recipientAccountId", message,
              created_at AS "createdAt"
         FROM notifications
        WHERE ($1::uuid IS NULL OR payment_id = $1)
        ORDER BY created_at DESC LIMIT $2`,
      [paymentId ?? null, Math.min(Math.max(limit, 1), 500)],
    );
    return rows;
  }
}
