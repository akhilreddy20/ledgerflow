import { IsIn, IsInt, IsOptional, IsString, IsUUID, Max, MaxLength, Min } from 'class-validator';

export class CreatePaymentDto {
  @IsUUID()
  payerAccountId: string;

  @IsUUID()
  payeeAccountId: string;

  /** Minor units (paise). */
  @IsInt()
  @Min(1)
  @Max(1_000_000_000_000)
  amount: number;

  @IsOptional()
  @IsIn(['INR'])
  currency: string = 'INR';

  @IsOptional()
  @IsString()
  @MaxLength(200)
  description?: string;
}
