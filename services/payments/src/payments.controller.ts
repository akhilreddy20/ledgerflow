import {
  Body,
  Controller,
  DefaultValuePipe,
  Get,
  HttpCode,
  Param,
  ParseIntPipe,
  ParseUUIDPipe,
  Post,
  Query,
  Req,
} from '@nestjs/common';
import { Idempotent } from '@ledgerflow/common';
import type { Request } from 'express';
import { CreatePaymentDto } from './dto';
import { PaymentSaga } from './payment.saga';
import { PaymentsService } from './payments.service';
import { PspClient } from './psp.client';

@Controller()
export class PaymentsController {
  constructor(
    private readonly payments: PaymentsService,
    private readonly saga: PaymentSaga,
    private readonly psp: PspClient,
  ) {}

  /**
   * Start a payout. Returns 202 Accepted with status PENDING; poll GET /payments/:id for the outcome.
   * Requires an Idempotency-Key header - retrying with the same key never creates a second payment.
   */
  @Post('payments')
  @HttpCode(202)
  @Idempotent('payments')
  create(@Body() dto: CreatePaymentDto, @Req() req: Request) {
    return this.payments.create(dto, req.header('idempotency-key')!);
  }

  @Get('payments/:id')
  get(@Param('id', ParseUUIDPipe) id: string) {
    return this.payments.get(id);
  }

  @Get('payments')
  list(@Query('status') status: string | undefined, @Query('limit', new DefaultValuePipe(50), ParseIntPipe) limit: number) {
    return this.payments.list(status, Math.min(Math.max(limit, 1), 500));
  }

  @Get('admin/stats')
  stats() {
    return this.payments.stats();
  }

  @Get('admin/circuit-breaker')
  breaker() {
    return this.psp.breaker.snapshot();
  }

  /** Run one saga recovery pass now (normally runs on a timer). */
  @Post('admin/saga/recover')
  @HttpCode(200)
  async recover() {
    return { recovered: await this.saga.recoverStuckPayments() };
  }
}
