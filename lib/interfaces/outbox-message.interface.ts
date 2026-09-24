import type { Duration } from '../utils/duration.util.js';

export type OutboxHeaders = Record<string, string>;

/** A message as stored in the outbox and handed to transports and handlers. */
export interface OutboxMessage<P = unknown> {
  /** A UUIDv7 unless `add()` got one. Consumers deduplicate by it; it doesn't decide order. */
  readonly id: string;
  readonly topic: string;
  /** JSON snapshot taken when the message was added. */
  readonly payload: P;
  readonly headers: OutboxHeaders;
  /**
   * Ordering key. Messages sharing a key are published one at a time, in the order they
   * were added (the store's insertion order), whatever their topic or transport.
   */
  readonly key: string | null;
  /** Epoch ms. */
  readonly createdAt: number;
  /** Epoch ms. Not claimable before this (delayed messages, retry backoff). */
  readonly availableAt: number;
  /** Failed publish attempts so far (0 on the first delivery). */
  readonly attempts: number;
  readonly lastError: string | null;
}

/** Input to `Outbox.add()`. */
export interface NewOutboxMessage<P = unknown> {
  topic: string;
  payload: P;
  headers?: OutboxHeaders;
  key?: string | null;
  /** Supply your own id (must be unique); defaults to a fresh UUIDv7. */
  id?: string;
  /** Publish no earlier than this point in time. */
  availableAt?: Date | number;
  /** Publish no earlier than this long from now (`'10m'`, or ms). Not together with `availableAt`. */
  delay?: Duration;
}
