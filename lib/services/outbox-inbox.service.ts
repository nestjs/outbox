import { Injectable } from '@nestjs/common';
import { toMs, type Duration } from '../utils/duration.util.js';
import { OutboxTransactionRequiredError } from '../errors/outbox-transaction-required.error.js';
import type { Awaitable } from '../interfaces/awaitable.interface.js';
import { OutboxStorage } from '../storage/outbox.storage.js';
import type { OutboxInboxResult } from '../interfaces/outbox-inbox-result.interface.js';

/**
 * Consumer-side deduplication by message id, per consumer. `@OnOutboxMessage()`
 * handlers use it automatically; remote consumers (an `@EventPattern()` receiving a
 * `ClientProxyTransport` envelope) call it directly.
 */
@Injectable()
export class OutboxInbox {
  /** Deliveries running in this process, by consumer and message id. */
  private readonly running = new Map<string, Promise<unknown>>();

  constructor(private readonly storage: OutboxStorage) {}

  private get store() {
    return this.storage.inbox;
  }

  /**
   * Runs `work` unless `consumer` already processed `messageId`, then records it.
   *
   * A second delivery of the same message to the same consumer that arrives while the
   * first is still running in this process waits for it, then skips `work` if the first
   * succeeded. Across processes, and after a crash (or a failed record) between `work`
   * and the record, `work` can run twice. Use `processInTransaction()` when the side
   * effects live in the same database.
   */
  async process<T>(
    consumer: string,
    messageId: string,
    work: () => T | Promise<T>,
  ): Promise<OutboxInboxResult<Awaited<T>>> {
    const key = `${consumer}\u0000${messageId}`;
    for (let earlier = this.running.get(key); earlier; earlier = this.running.get(key)) {
      await earlier.then(noop, noop);
    }

    const delivery = this.checkRunRecord(consumer, messageId, work);
    this.running.set(key, delivery);

    try {
      return await delivery;
    } finally {
      if (this.running.get(key) === delivery) {
        this.running.delete(key);
      }
    }
  }

  /**
   * Records `messageId` for `consumer` through `tx`, then runs `work` only if this is
   * the first time. Call it inside your transaction: the record commits or rolls back
   * with `work`'s writes, which makes its effects exactly-once.
   *
   * The duplicate check and the call to `work` happen in here, so an un-awaited result
   * can't turn into a skipped check. With a synchronous store (the in-memory one, or yours
   * on a synchronous driver) and a synchronous `work`, the result is synchronous too. With
   * an asynchronous store it is a promise: await it before you commit.
   */
  processInTransaction<Tx, T>(
    tx: Tx,
    consumer: string,
    messageId: string,
    work: () => T,
  ): Awaitable<OutboxInboxResult<Awaited<T>>> {
    if (tx === undefined || tx === null) {
      throw new OutboxTransactionRequiredError('processInTransaction() got no transaction handle.');
    }

    const recorded = this.store.recordInbox(tx, consumer, messageId, Date.now());
    return isPromise<boolean>(recorded)
      ? recorded.then((first) => runIfFirst(first, work))
      : runIfFirst(recorded, work);
  }

  /** Deletes inbox entries older than `olderThan` (`'30d'`). Keep it longer than any redelivery, requeues included. */
  prune(olderThan: Duration): Awaitable<number> {
    return this.store.pruneInbox(Date.now() - toMs(olderThan));
  }

  private async checkRunRecord<T>(
    consumer: string,
    messageId: string,
    work: () => T | Promise<T>,
  ): Promise<OutboxInboxResult<Awaited<T>>> {
    if (await this.store.hasInbox(consumer, messageId)) {
      return { duplicate: true };
    }

    const result = await work();
    await this.store.recordInbox(undefined, consumer, messageId, Date.now());
    return { duplicate: false, result };
  }
}

const noop = () => {};

function runIfFirst<T>(first: boolean, work: () => T): Awaitable<OutboxInboxResult<Awaited<T>>> {
  if (typeof first !== 'boolean') {
    throw new TypeError('OutboxInboxStore.recordInbox() must return (or resolve to) a boolean');
  }
  if (!first) {
    return { duplicate: true };
  }

  const result = work();
  return isPromise<Awaited<T>>(result)
    ? result.then((value) => ({ duplicate: false, result: value }))
    : { duplicate: false, result: result as Awaited<T> };
}

function isPromise<T>(value: unknown): value is Promise<T> {
  return typeof (value as Promise<T> | undefined)?.then === 'function';
}
