import type { OutboxMessage } from '../interfaces/outbox-message.interface.js';
import type { OutboxDeadLetterReason } from '../interfaces/outbox-dead-letter.interface.js';

/** Channel `nestjs:outbox:published`: a transport accepted the message. */
export interface OutboxPublishedEvent {
  type: 'published';
  message: OutboxMessage;
  transport: string;
  durationMs: number;
}

/** Channel `nestjs:outbox:retry-scheduled`: a publish failed and will be retried after `delayMs`. */
export interface OutboxRetryScheduledEvent {
  type: 'retry-scheduled';
  message: OutboxMessage;
  /** Undefined when `route` itself threw. */
  transport: string | undefined;
  error: unknown;
  /** The attempt that just failed (1-based). */
  attempt: number;
  delayMs: number;
}

/** Channel `nestjs:outbox:dead-lettered`: the message moved to the dead-letter table. */
export interface OutboxDeadLetteredEvent {
  type: 'dead-lettered';
  message: OutboxMessage;
  transport: string | undefined;
  error: unknown;
  attempt: number;
  reason: OutboxDeadLetterReason;
}

/**
 * Channel `nestjs:outbox:lease-lost`: another relay took the message over while this one
 * was publishing it, so it may be published more than once (consumers dedupe by id).
 */
export interface OutboxLeaseLostEvent {
  type: 'lease-lost';
  message: OutboxMessage;
}

export type OutboxEvent =
  | OutboxPublishedEvent
  | OutboxRetryScheduledEvent
  | OutboxDeadLetteredEvent
  | OutboxLeaseLostEvent;
