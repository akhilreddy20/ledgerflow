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
import { CreateAccountDto, DepositDto, TransferDto } from './dto';
import { LedgerService } from './ledger.service';

@Controller()
export class LedgerController {
  constructor(private readonly ledger: LedgerService) {}

  @Post('accounts')
  createAccount(@Body() dto: CreateAccountDto) {
    return this.ledger.createAccount(dto.name, dto.currency, dto.kind);
  }

  @Get('accounts/:id')
  getAccount(@Param('id', ParseUUIDPipe) id: string) {
    return this.ledger.getAccount(id);
  }

  @Get('accounts/:id/entries')
  entries(
    @Param('id', ParseUUIDPipe) id: string,
    @Query('limit', new DefaultValuePipe(50), ParseIntPipe) limit: number,
  ) {
    return this.ledger.listEntries(id, Math.min(Math.max(limit, 1), 500));
  }

  /** Fund an account from the outside world (e.g. a top-up). Requires Idempotency-Key. */
  @Post('accounts/:id/deposits')
  @Idempotent('ledger')
  async deposit(@Param('id', ParseUUIDPipe) id: string, @Body() dto: DepositDto, @Req() req: Request) {
    const res = await this.ledger.deposit(id, dto.amount, req.header('idempotency-key')!);
    return { ...res, account: await this.ledger.getAccount(id) };
  }

  /** Synchronous wallet-to-wallet transfer. Requires Idempotency-Key. */
  @Post('transfers')
  @Idempotent('ledger')
  async transfer(@Body() dto: TransferDto, @Req() req: Request) {
    const res = await this.ledger.transfer(
      dto.fromAccountId,
      dto.toAccountId,
      dto.amount,
      req.header('idempotency-key')!,
      dto.description,
    );
    return { ...res, fromAccountId: dto.fromAccountId, toAccountId: dto.toAccountId, amount: dto.amount };
  }

  @Get('transactions/:id')
  getTransaction(@Param('id', ParseUUIDPipe) id: string) {
    return this.ledger.getTransaction(id);
  }

  @Get('holds/:paymentId')
  async getHold(@Param('paymentId', ParseUUIDPipe) paymentId: string) {
    return (await this.ledger.getHold(paymentId)) ?? { paymentId, status: 'NONE' };
  }

  /** Books check: double-entry invariants. Used by load tests to prove "zero balance drift". */
  @Get('admin/invariants')
  @HttpCode(200)
  invariants() {
    return this.ledger.checkInvariants();
  }
}
