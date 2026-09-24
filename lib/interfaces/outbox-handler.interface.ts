import type { OutboxInboxResult } from './outbox-inbox-result.interface.js';
import type { OutboxMessage } from './outbox-message.interface.js';
import type { Awaitable } from './awaitable.interface.js';

export interface OnOutboxMessageOptions {
  /**
   * The handler's inbox identity: the inbox records which message ids this consumer
   * has processed. Keep it stable; a new name makes every past message look new.
   */
  consumer: string;
  /**
   * Skip messages this consumer already processed, and record each one after the
   * handler succeeds. Default `true`. With `false`, every delivery runs the handler.
   */
  inbox?: boolean;
}

export interface OutboxHandlerMetadata extends OnOutboxMessageOptions {
  topics: string[];
}

/** Second argument of an `@OnOutboxMessage()` handler. */
export interface OutboxHandlerContext<Tx = unknown> {
  /** The message: `id`, `topic`, `key`, `headers`, `createdAt`... (`payload` is the first argument). */
  readonly message: OutboxMessage;
  readonly consumer: string;
  /**
   * 1, plus the failed attempts recorded so far: 1 on the first delivery, 2 on the first
   * retry. A redelivery after a crash or a lost lease repeats the number.
   */
  readonly attempt: number;
  /**
   * Aborts when the relay stops waiting for this delivery (`relay.publishTimeout`) and
   * counts it as a failed attempt. Pass it to the calls the handler makes, such as
   * `fetch(url, { signal })`, so the handler stops too.
   */
  readonly signal: AbortSignal;
  /**
   * Makes the handler's effects exactly-once. Call it inside your own transaction with
   * the transaction handle and the work to do: it records the message for this consumer
   * through `tx`, then runs `work` only if the consumer hasn't processed the message yet
   * (otherwise it returns `{ duplicate: true }` without calling `work`). The record
   * commits or rolls back with `work`'s writes.
   *
   * Synchronous for a synchronous store and `work`; a promise (await it before
   * COMMIT) for an asynchronous store.
   */
  processInTransaction<T>(tx: Tx, work: () => T): Awaitable<OutboxInboxResult<Awaited<T>>>;
}
