import { IsIn, IsInt, IsOptional, IsString, IsUUID, Length, Max, MaxLength, Min } from 'class-validator';

/** 10^12 minor units = 10 billion rupees. Comfortably inside Number.MAX_SAFE_INTEGER. */
const MAX_AMOUNT = 1_000_000_000_000;

export class CreateAccountDto {
  @IsString()
  @Length(1, 120)
  name: string;

  @IsOptional()
  @IsIn(['INR'], { message: 'This ledger is single-currency (INR). See docs/DESIGN.md for multi-currency.' })
  currency: string = 'INR';

  @IsOptional()
  @IsIn(['user', 'merchant'])
  kind: 'user' | 'merchant' = 'user';
}

export class DepositDto {
  @IsInt()
  @Min(1)
  @Max(MAX_AMOUNT)
  amount: number;
}

export class TransferDto {
  @IsUUID()
  fromAccountId: string;

  @IsUUID()
  toAccountId: string;

  @IsInt()
  @Min(1)
  @Max(MAX_AMOUNT)
  amount: number;

  @IsOptional()
  @IsString()
  @MaxLength(200)
  description?: string;
}
