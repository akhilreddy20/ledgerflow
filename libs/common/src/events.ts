import { randomUUID } from 'crypto';

/** Kafka topics. Every topic has a matching dead-letter topic: `<topic>.dlq`. */
export const Topics = {
  /** Commands sent by the payments saga to the ledger. Keyed by the payer account id. */
  LEDGER_COMMANDS: 'ledger.commands',
  /** Facts published by the ledger. Keyed by the payer account id. */
  LEDGER_EVENTS: 'ledger.events',
  /** Payment lifecycle facts consumed by notifications (and anyone else). Keyed by payment id. */
  PAYMENT_EVENTS: 'payments.events',
} as const;

export const dlqTopic = (topic: string) => `${topic}.dlq`;
export const ALL_TOPICS = Object.values(Topics).flatMap((t) => [t, dlqTopic(t)]);

/** Every message on the wire is wrapped in this envelope. `id` is what consumers de-duplicate on. */
export interface Envelope<T = unknown> {
  id: string;
  type: string;
  occurredAt: string;
  source: string;
  /** Business correlation id (the payment id for saga traffic). */
  correlationId?: string;
  payload: T;
}

export function envelope<T>(type: string, source: string, payload: T, correlationId?: string): Envelope<T> {
  return { id: randomUUID(), type, occurredAt: new Date().toISOString(), source, correlationId, payload };
}

// ---------------------------------------------------------------- ledger commands
export interface HoldFunds {
  paymentId: string;
  accountId: string;
  /** Validated at hold time so a payment can never get stuck on an unknown payee at capture time. */
  payeeAccountId: string;
  amount: number;
  currency: string;
}
export interface CaptureHold {
  paymentId: string;
  payeeAccountId: string;
}
export interface ReleaseHold {
  paymentId: string;
  reason: string;
}

// ---------------------------------------------------------------- ledger events
export interface FundsHeld {
  paymentId: string;
  holdId: string;
  accountId: string;
  amount: number;
}
export interface HoldRejected {
  paymentId: string;
  code: string;
  reason: string;
}
export interface HoldCaptured {
  paymentId: string;
  holdId: string;
  transactionId: string;
}
export interface HoldReleased {
  paymentId: string;
  holdId: string;
  transactionId: string;
}

// ---------------------------------------------------------------- payment events
export interface PaymentCompleted {
  paymentId: string;
  payerAccountId: string;
  payeeAccountId: string;
  amount: number;
  currency: string;
  pspReference: string;
}
export interface PaymentFailed {
  paymentId: string;
  payerAccountId: string;
  amount: number;
  currency: string;
  reason: string;
}

export const EventTypes = {
  HOLD_FUNDS: 'HoldFunds',
  CAPTURE_HOLD: 'CaptureHold',
  RELEASE_HOLD: 'ReleaseHold',
  FUNDS_HELD: 'FundsHeld',
  HOLD_REJECTED: 'HoldRejected',
  HOLD_CAPTURED: 'HoldCaptured',
  HOLD_RELEASED: 'HoldReleased',
  PAYMENT_COMPLETED: 'PaymentCompleted',
  PAYMENT_FAILED: 'PaymentFailed',
} as const;
