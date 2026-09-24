import type { Awaitable } from './awaitable.interface.js';

/**
 * Consumer-side deduplication: which consumer processed which message. What `OutboxInbox`
 * and the `@OnOutboxMessage()` handlers use. A service that only consumes (an
 * `@EventPattern()` receiving `ClientProxyTransport` envelopes) implements this contract
 * alone and registers it with `OutboxStorage.registerSource({ inbox: this })`; a service
 * that also produces usually implements both in one class. Its method names don't clash with
 * `OutboxStore`'s, so one class can implement both.
 *
 * `@nestjs/outbox/testing` exports `outboxInboxStoreContract()`, its test suite.
 */
export interface OutboxInboxStore<Tx = unknown> {
  /**
   * Records that `consumer` processed `messageId`, and returns `false` if it already had.
   * With a `tx`, the record is written through it and commits or rolls back with the
   * consumer's own writes (refuse a handle that is not a transaction with
   * `OutboxTransactionRequiredError`); without one, it is written at once, on the store's
   * own connection. Insert-if-absent in one statement on a unique `(consumer, messageId)`
   * key (`ON CONFLICT DO NOTHING`), so a concurrent delivery of the same message waits for
   * this transaction and then sees the record: never check first, then insert.
   */
  recordInbox(tx: Tx | undefined, consumer: string, messageId: string, now: number): Awaitable<boolean>;

  hasInbox(consumer: string, messageId: string): Awaitable<boolean>;

  /** Deletes records processed before `before` (epoch ms). Returns how many. */
  pruneInbox(before: number): Awaitable<number>;
}
