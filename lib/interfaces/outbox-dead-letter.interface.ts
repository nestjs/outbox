import type { OutboxHeaders } from './outbox-message.interface.js';

/** One failed publish attempt, kept on the message and carried into the dead-letter table. */
export interface OutboxAttempt {
  attempt: number;
  /** Epoch ms. */
  at: number;
  error: string;
  transport?: string;
}

export type OutboxDeadLetterReason =
  /** The retry budget (`retry.attempts`) ran out. */
  | 'exhausted'
  /** `retryIf` returned false, or the error was a `NonRetryableMessageError`. */
  | 'rejected';

export interface OutboxDeadLetter<P = unknown> {
  readonly id: string;
  readonly topic: string;
  readonly payload: P;
  readonly headers: OutboxHeaders;
  readonly key: string | null;
  readonly createdAt: number;
  readonly attempts: number;
  readonly lastError: string | null;
  readonly reason: OutboxDeadLetterReason;
  /** Epoch ms. */
  readonly failedAt: number;
  /** Every failed attempt, oldest first (survives requeue → fail again). */
  readonly history: OutboxAttempt[];
}

/** Which dead letters a bulk operation applies to. An empty filter is rejected; use `{ all: true }`. */
export interface OutboxDeadLetterFilter {
  ids?: string[];
  topic?: string;
  key?: string;
  /** Epoch ms or Date: only dead letters that failed before this. */
  failedBefore?: number | Date;
  all?: boolean;
}

export interface OutboxDeadLetterQuery {
  topic?: string;
  key?: string;
  /** Default 50. */
  limit?: number;
  offset?: number;
}

export type OutboxDeadLetterTarget = string | string[] | OutboxDeadLetterFilter;
