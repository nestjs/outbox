import { Injectable } from '@nestjs/common';
import { toMs } from './utils/duration.util.js';
import { OutboxTransactionRequiredError } from './errors/outbox-transaction-required.error.js';
import type { NewOutboxMessage, OutboxMessage } from './interfaces/outbox-message.interface.js';
import type { Awaitable } from './interfaces/awaitable.interface.js';
import { OutboxRelay } from './services/outbox-relay.service.js';
import type { OutboxStore } from './interfaces/outbox-store.interface.js';
import { OutboxStorage } from './storage/outbox.storage.js';
import { uuidv7 } from './utils/uuid.util.js';

/**
 * Producer API. `Tx` is your data layer's transaction handle; inject as
 * `Outbox<Transaction>` (Drizzle's `tx`), `Outbox<EntityManager>`,
 * `Outbox<Prisma.TransactionClient>`...
 */
@Injectable()
export class Outbox<Tx = unknown> {
  constructor(
    private readonly storage: OutboxStorage,
    private readonly relay: OutboxRelay,
  ) {}

  /**
   * Writes the message(s) through `tx`, your open transaction. Nothing is published
   * until the transaction commits and the relay picks the rows up.
   *
   * The store call happens before `add()` returns, so with a synchronous store (the
   * in-memory one, or yours on a synchronous driver) the result is not a promise. With an
   * async store, await it like any other query, before the transaction commits.
   */
  add<P>(tx: Tx, message: NewOutboxMessage<P>): Awaitable<OutboxMessage<P>>;
  add<P>(tx: Tx, messages: readonly NewOutboxMessage<P>[]): Awaitable<OutboxMessage<P>[]>;
  add<P>(
    tx: Tx,
    input: NewOutboxMessage<P> | readonly NewOutboxMessage<P>[],
  ): Awaitable<OutboxMessage<P> | OutboxMessage<P>[]> {
    if (tx === undefined || tx === null) {
      throw new OutboxTransactionRequiredError('Outbox.add() got no transaction handle.');
    }

    const now = Date.now();
    const batch = isBatch(input);
    const messages = (batch ? input : [input]).map((m) => build(m, now));
    const output = batch ? messages : messages[0]!;

    const written = (this.storage.messages as OutboxStore<Tx>).add(tx, messages);
    return isPromise(written) ? written.then(() => output) : output;
  }

  /** Poll now. Call after your transaction commits to skip the poll interval. */
  notify(): void {
    this.relay.notify();
  }
}

function build<P>(input: NewOutboxMessage<P>, now: number): OutboxMessage<P> {
  if (typeof input?.topic !== 'string' || input.topic === '') {
    throw new TypeError('Outbox message needs a topic (a non-empty string)');
  }
  if (input.key !== undefined && input.key !== null && typeof input.key !== 'string') {
    throw new TypeError(`Outbox message: \`key\` must be a string or null (got ${typeof input.key})`);
  }
  if (input.delay !== undefined && input.availableAt !== undefined) {
    throw new TypeError('Outbox message: pass either `delay` or `availableAt`, not both');
  }

  // Snapshot now; fails fast (inside the caller's transaction) on BigInt or cycles.
  const json = JSON.stringify(input.payload);
  if (json === undefined) {
    throw new TypeError('Outbox payload must be JSON-serializable');
  }

  const availableAt =
    input.availableAt !== undefined ? +input.availableAt : now + toMs(input.delay ?? 0);
  if (!Number.isFinite(availableAt)) {
    throw new TypeError(
      `Outbox message: \`availableAt\` must be a valid Date or epoch milliseconds (got ${availableAt})`,
    );
  }

  return {
    id: input.id ?? uuidv7(),
    topic: input.topic,
    payload: JSON.parse(json),
    headers: { ...input.headers },
    key: input.key ?? null,
    createdAt: now,
    availableAt,
    attempts: 0,
    lastError: null,
  };
}

function isBatch<P>(
  input: NewOutboxMessage<P> | readonly NewOutboxMessage<P>[],
): input is readonly NewOutboxMessage<P>[] {
  return Array.isArray(input);
}

function isPromise<T>(value: unknown): value is Promise<T> {
  return typeof (value as Promise<T> | undefined)?.then === 'function';
}
