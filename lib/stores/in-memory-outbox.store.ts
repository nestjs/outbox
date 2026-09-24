import { Logger } from '@nestjs/common';
import { OutboxTransactionRequiredError } from '../errors/outbox-transaction-required.error.js';
import type { OutboxMessage } from '../interfaces/outbox-message.interface.js';
import type {
  OutboxClaimRequest,
  OutboxDeadLetterUpdate,
  OutboxRescheduleUpdate,
  OutboxStoreStats,
  OutboxStore,
} from '../interfaces/outbox-store.interface.js';
import type {
  OutboxAttempt,
  OutboxDeadLetter,
  OutboxDeadLetterFilter,
  OutboxDeadLetterQuery,
} from '../interfaces/outbox-dead-letter.interface.js';
import type { OutboxInboxStore } from '../interfaces/outbox-inbox-store.interface.js';

interface Row {
  seq: number;
  message: OutboxMessage;
  history: OutboxAttempt[];
  leaseOwner: string | null;
  leaseUntil: number | null;
}

interface DeadRow {
  seq: number;
  deadLetter: OutboxDeadLetter;
}

/** A transaction of `InMemoryOutboxStore.transaction()`: its writes, applied at commit. */
class InMemoryTransaction {
  open = true;
  readonly messages: OutboxMessage[] = [];
  readonly inbox = new Map<string, number>();
  /** Settles when the transaction ends: `true` when it committed. */
  readonly ended: Promise<boolean>;
  finish!: (committed: boolean) => void;

  constructor() {
    this.ended = new Promise((resolve) => (this.finish = resolve));
  }
}

/**
 * The default store, used when no source is registered, and the test double. Everything
 * lives in this process: lost on restart, not shared between instances.
 *
 * It implements both contracts, `OutboxStore` and `OutboxInboxStore`. It can't join your
 * database's transactions: `add(tx)` and `recordInbox(tx)` with any handle other than one
 * from `store.transaction()` write at once, so a rollback doesn't take the message back, and
 * the first such handle logs a warning (once per store). `transaction(work)` gives a handle
 * whose writes apply when `work` resolves and are dropped when it throws, for tests that
 * exercise rollbacks.
 *
 * Methods answer synchronously, except a `recordInbox()` that meets the same record
 * pending in another open transaction: it waits for that one to end, as a unique key would.
 */
export class InMemoryOutboxStore implements OutboxStore<unknown>, OutboxInboxStore<unknown> {
  private static readonly logger = new Logger('OutboxModule');
  private warnedAboutTransactions = false;
  private rows: Row[] = [];
  private readonly ids = new Map<string, Row>();
  private readonly dead = new Map<string, DeadRow>();
  private readonly inbox = new Map<string, number>();
  /** Inbox records written in transactions that are still open. */
  private readonly pendingInbox = new Map<string, InMemoryTransaction>();
  private seq = 0;

  /**
   * Runs `work` with a transaction handle for `add()` and `recordInbox()`. Its writes
   * apply, in one step, when `work` resolves, and are dropped when it throws.
   */
  async transaction<T>(work: (tx: unknown) => T | Promise<T>): Promise<T> {
    const tx = new InMemoryTransaction();
    let result: T;
    try {
      result = await work(tx);
    } catch (error) {
      this.end(tx, false);
      throw error;
    }

    try {
      this.apply(tx.messages);
    } catch (error) {
      this.end(tx, false);
      throw error;
    }

    for (const [key, at] of tx.inbox) {
      this.inbox.set(key, at);
    }

    this.end(tx, true);
    return result;
  }

  // ---------------------------------------------------------------- producer

  add(tx: unknown, messages: readonly OutboxMessage[]): void {
    const own = this.own(tx, 'add');
    const copies = messages.map((m) => clone(m));
    if (own) {
      own.messages.push(...copies);
    } else {
      this.apply(copies);
    }
  }

  // ---------------------------------------------------------------- relay

  claim({ owner, now, leaseMs, limit }: OutboxClaimRequest): OutboxMessage[] {
    const batch: OutboxMessage[] = [];
    for (const row of this.claimable(now)) {
      if (batch.length >= limit) {
        break;
      }
      row.leaseOwner = owner;
      row.leaseUntil = now + leaseMs;
      batch.push(clone(row.message));
    }

    return batch;
  }

  markPublished(id: string, owner: string): boolean {
    const row = this.leased(id, owner);
    if (!row) {
      return false;
    }
    this.remove(row);
    return true;
  }

  reschedule(id: string, owner: string, update: OutboxRescheduleUpdate): boolean {
    const row = this.leased(id, owner);
    if (!row) {
      return false;
    }

    row.message = { ...row.message, attempts: update.attempts, availableAt: update.availableAt, lastError: update.error.error };
    row.history.push(clone(update.error));
    row.leaseOwner = null;
    row.leaseUntil = null;
    return true;
  }

  deadLetter(id: string, owner: string, update: OutboxDeadLetterUpdate): boolean {
    const row = this.leased(id, owner);
    if (!row) {
      return false;
    }

    this.remove(row);
    const { availableAt: _, ...message } = row.message;
    this.dead.set(id, {
      seq: row.seq,
      deadLetter: {
        ...message,
        attempts: update.attempts,
        lastError: update.error.error,
        reason: update.reason,
        failedAt: update.failedAt,
        history: [...row.history, clone(update.error)],
      },
    });

    return true;
  }

  release(ids: readonly string[], owner: string): number {
    let released = 0;
    for (const id of new Set(ids)) {
      const row = this.leased(id, owner);
      if (!row) {
        continue;
      }
      row.leaseOwner = null;
      row.leaseUntil = null;
      released++;
    }

    return released;
  }

  stats(now: number): OutboxStoreStats {
    let leased = 0;
    let oldestDueAt: number | null = null;
    for (const { message, leaseUntil } of this.rows) {
      if (leaseUntil !== null && leaseUntil > now) {
        leased++;
      }
      const since = message.attempts > 0 ? message.createdAt : message.availableAt <= now ? message.availableAt : null;
      if (since !== null && (oldestDueAt === null || since < oldestDueAt)) {
        oldestDueAt = since;
      }
    }

    return {
      pending: this.rows.length,
      ready: [...this.claimable(now)].length,
      leased,
      deadLetters: this.dead.size,
      oldestDueAt,
    };
  }

  // ---------------------------------------------------------------- dead letters

  listDeadLetters({ topic, key, limit = 50, offset = 0 }: OutboxDeadLetterQuery): OutboxDeadLetter[] {
    return [...this.dead.values()]
      .map((row) => row.deadLetter)
      .filter((d) => (topic === undefined || d.topic === topic) && (key === undefined || d.key === key))
      .sort((a, b) => b.failedAt - a.failedAt || (a.id < b.id ? 1 : a.id > b.id ? -1 : 0))
      .slice(offset, offset + limit)
      .map((d) => clone(d));
  }

  getDeadLetter(id: string): OutboxDeadLetter | undefined {
    const row = this.dead.get(id);
    return row ? clone(row.deadLetter) : undefined;
  }

  requeueDeadLetters(filter: OutboxDeadLetterFilter, now: number): number {
    const matching = this.matching(filter);
    for (const row of matching) {
      if (this.ids.has(row.deadLetter.id)) {
        throw new Error(`Can't requeue dead letter ${row.deadLetter.id}: a message with that id is pending`);
      }
    }

    for (const { seq, deadLetter } of matching) {
      const { reason: _r, failedAt: _f, history, ...message } = deadLetter;
      this.dead.delete(deadLetter.id);
      this.insert({ seq, message: { ...message, attempts: 0, availableAt: now }, history, leaseOwner: null, leaseUntil: null });
    }

    return matching.length;
  }

  purgeDeadLetters(filter: OutboxDeadLetterFilter): number {
    const matching = this.matching(filter);
    for (const { deadLetter } of matching) {
      this.dead.delete(deadLetter.id);
    }
    return matching.length;
  }

  // ---------------------------------------------------------------- inbox

  recordInbox(tx: unknown, consumer: string, messageId: string, now: number): boolean | Promise<boolean> {
    const own = tx === undefined ? undefined : this.own(tx, 'recordInbox');
    const key = `${consumer}\u0000${messageId}`;
    const pending = this.pendingInbox.get(key);
    if (pending && pending !== own) {
      // Another open transaction has this record: wait for it to end, then look again.
      return pending.ended.then(() => this.recordInbox(tx, consumer, messageId, now));
    }

    if (this.inbox.has(key) || own?.inbox.has(key)) {
      return false;
    }

    if (own) {
      own.inbox.set(key, now);
      this.pendingInbox.set(key, own);
    } else {
      this.inbox.set(key, now);
    }

    return true;
  }

  hasInbox(consumer: string, messageId: string): boolean {
    return this.inbox.has(`${consumer}\u0000${messageId}`);
  }

  pruneInbox(before: number): number {
    let pruned = 0;
    for (const [key, at] of this.inbox) {
      if (at < before) {
        this.inbox.delete(key);
        pruned++;
      }
    }

    return pruned;
  }

  // ---------------------------------------------------------------- helpers

  /** This store's open transaction, or `undefined` for a handle from elsewhere (written at once). */
  private own(tx: unknown, method: string): InMemoryTransaction | undefined {
    if (tx === undefined || tx === null) {
      throw new OutboxTransactionRequiredError(`InMemoryOutboxStore.${method}() got no transaction handle.`);
    }

    if (!(tx instanceof InMemoryTransaction)) {
      if (!this.warnedAboutTransactions) {
        this.warnedAboutTransactions = true;
        InMemoryOutboxStore.logger.warn(
          `InMemoryOutboxStore.${method}() received your transaction handle, and can't join it: the write applies ` +
            "at once, so a rolled-back transaction won't undo it. Register a store for your database with " +
            'OutboxStorage.registerSource(). (Logged once.)',
        );
      }
      return undefined;
    }

    if (!tx.open) {
      throw new OutboxTransactionRequiredError(`InMemoryOutboxStore.${method}() got a transaction that has ended.`);
    }

    return tx;
  }

  private end(tx: InMemoryTransaction, committed: boolean) {
    tx.open = false;
    for (const key of tx.inbox.keys()) {
      if (this.pendingInbox.get(key) === tx) {
        this.pendingInbox.delete(key);
      }
    }
    tx.finish(committed);
  }

  /** Inserts messages with the next sequence numbers: all or nothing. */
  private apply(messages: readonly OutboxMessage[]) {
    const seen = new Set<string>();
    for (const m of messages) {
      if (this.ids.has(m.id) || seen.has(m.id)) {
        throw new Error(`Duplicate outbox message id ${m.id}`);
      }
      seen.add(m.id);
    }

    for (const message of messages) {
      this.insert({ seq: ++this.seq, message, history: [], leaseOwner: null, leaseUntil: null });
    }
  }

  private insert(row: Row) {
    this.ids.set(row.message.id, row);
    // Usually the end; a requeued dead letter goes back to its place.
    let i = this.rows.length;
    while (i > 0 && this.rows[i - 1]!.seq > row.seq) {
      i--;
    }
    this.rows.splice(i, 0, row);
  }

  private remove(row: Row) {
    this.ids.delete(row.message.id);
    this.rows.splice(this.rows.indexOf(row), 1);
  }

  private leased(id: string, owner: string): Row | undefined {
    const row = this.ids.get(id);
    return row && row.leaseOwner === owner ? row : undefined;
  }

  /** Claimable rows in `seq` order: due, unleased, and not behind a delayed or leased row of their key. */
  private *claimable(now: number): Generator<Row> {
    const blocked = new Set<string>();
    for (const row of this.rows) {
      const { key, availableAt } = row.message;
      if (key !== null && blocked.has(key)) {
        continue;
      }

      const held = availableAt > now || (row.leaseUntil !== null && row.leaseUntil > now);
      if (held) {
        if (key !== null) {
          blocked.add(key);
        }
        continue;
      }
      yield row;
    }
  }

  private matching(filter: OutboxDeadLetterFilter): DeadRow[] {
    const { ids, topic, key, failedBefore, all } = filter;
    if (!all && ids === undefined && topic === undefined && key === undefined && failedBefore === undefined) {
      throw new Error('Refusing an empty dead-letter filter; pass { all: true }');
    }

    const idSet = ids && new Set(ids);
    return [...this.dead.values()].filter(
      ({ deadLetter: d }) =>
        (!idSet || idSet.has(d.id)) &&
        (topic === undefined || d.topic === topic) &&
        (key === undefined || d.key === key) &&
        (failedBefore === undefined || d.failedAt < +failedBefore),
    );
  }
}

/** A deep copy through JSON, as a database stores it. */
function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}
