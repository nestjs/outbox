import type { OutboxMessage } from './outbox-message.interface.js';
import type {
  OutboxDeadLetter,
  OutboxDeadLetterFilter,
  OutboxDeadLetterQuery,
  OutboxAttempt,
  OutboxDeadLetterReason,
} from './outbox-dead-letter.interface.js';
import type { Awaitable } from './awaitable.interface.js';

/**
 * Where the outbox keeps its messages and dead letters: what `Outbox.add()`, the relay and
 * `OutboxDeadLetters` use. The application implements it in a provider with whatever it
 * already uses (a Drizzle database, a TypeORM data source, Prisma, a driver) and registers
 * that provider in its constructor, usually together with the inbox:
 * `OutboxStorage.registerSource({ messages: this, inbox: this })`. Without one, the module
 * uses `InMemoryOutboxStore`.
 *
 * `Tx` is your data layer's transaction handle, passed through untouched: Drizzle's `tx`, a
 * TypeORM `EntityManager`, a Prisma transaction client, a `pg` `PoolClient`. The package
 * never looks inside it.
 *
 * Every method may return its result or a promise of it. Methods other than `add` run on
 * the store's own connection. `@nestjs/outbox/testing` exports `outboxStoreContract()`, the
 * test suite every implementation must pass; the README's "Implementing a store" walks
 * through each method and the race each rule prevents.
 *
 * Order within a key is the store's own sequence (an identity or autoincrement column,
 * called `seq` below), assigned in commit order: never the `id` or `createdAt`, which come
 * from the producers' clocks.
 */
export interface OutboxStore<Tx = unknown> {
  // ---------------------------------------------------------------- producer

  /**
   * Inserts `messages` through `tx`, the caller's open transaction, so they commit or roll
   * back with the caller's writes. Refuse a handle that is not a transaction (the database
   * itself) with `OutboxTransactionRequiredError`. An empty array writes nothing.
   *
   * Commit order: on a database that runs writers concurrently, two transactions adding
   * messages with the same key must commit in the order their rows are numbered. Take a
   * lock per key on `tx` (PostgreSQL: `pg_advisory_xact_lock`), held until it ends, before
   * inserting, in a fixed order. Messages without a key need no lock.
   */
  add(tx: Tx, messages: readonly OutboxMessage[]): Awaitable<void>;

  // ---------------------------------------------------------------- relay

  /**
   * Leases up to `limit` messages to `owner` until `now + leaseMs` and returns them in
   * `seq` order. A message is claimable when it is due (`availableAt <= now`), unleased or
   * its lease expired (`leaseUntil <= now`), and no older message with its key is delayed
   * (`availableAt > now`) or leased (`leaseUntil > now`). Within a key, that is a
   * contiguous run from the oldest pending message. Concurrent claims must never return the
   * same message, nor split a key's run between two owners. Nothing claimable: `[]`.
   */
  claim(request: OutboxClaimRequest): Awaitable<OutboxMessage[]>;

  /** Deletes the message if `owner` still holds its lease. `true` if it did. */
  markPublished(id: string, owner: string): Awaitable<boolean>;

  /**
   * If `owner` still holds the lease: sets `attempts`, `availableAt` and `lastError`
   * (`update.error.error`), appends `update.error` to the message's failure history, and
   * clears the lease, in one conditional write. `true` if it did.
   */
  reschedule(id: string, owner: string, update: OutboxRescheduleUpdate): Awaitable<boolean>;

  /**
   * If `owner` still holds the lease: moves the message to the dead letters, atomically
   * (never both, never neither). The dead letter keeps the message's `seq`, takes
   * `attempts`, `reason`, `failedAt` and `lastError` from `update`, and its history gets
   * `update.error` appended. It replaces an earlier dead letter with the same id. `true`
   * if it moved.
   */
  deadLetter(id: string, owner: string, update: OutboxDeadLetterUpdate): Awaitable<boolean>;

  /**
   * Clears the leases `owner` holds on these messages without counting an attempt
   * (shutdown, a lease too short to start another publish). Returns how many it cleared.
   */
  release(ids: readonly string[], owner: string): Awaitable<number>;

  /** Counts for `OutboxRelay.stats()`. See `OutboxStoreStats` for each field. */
  stats(now: number): Awaitable<OutboxStoreStats>;

  // ---------------------------------------------------------------- dead letters

  /** Newest first (`failedAt` descending, then `id` descending), `limit` 50 by default. */
  listDeadLetters(query: OutboxDeadLetterQuery): Awaitable<OutboxDeadLetter[]>;

  getDeadLetter(id: string): Awaitable<OutboxDeadLetter | undefined>;

  /**
   * Moves the matching dead letters back to the messages, atomically: same `id`, same
   * `seq` (so it goes back ahead of its key's later messages), `attempts` 0,
   * `availableAt` = `now`, `lastError` and history kept. Returns how many moved. The
   * fields of a filter combine with AND; refuse a filter with none of them unless `all` is
   * set, and match nothing for `ids: []`.
   */
  requeueDeadLetters(filter: OutboxDeadLetterFilter, now: number): Awaitable<number>;

  /** Deletes the matching dead letters (same filter rules). Returns how many. */
  purgeDeadLetters(filter: OutboxDeadLetterFilter): Awaitable<number>;
}

/** `OutboxStore.claim()` input. */
export interface OutboxClaimRequest {
  /** Fencing token for this claim. Every later write for these messages must present it. */
  owner: string;
  /** Epoch ms, from the relay's clock. */
  now: number;
  /** How long the claim is exclusive. */
  leaseMs: number;
  limit: number;
}

/** `OutboxStore.reschedule()` input. */
export interface OutboxRescheduleUpdate {
  attempts: number;
  availableAt: number;
  error: OutboxAttempt;
}

/** `OutboxStore.deadLetter()` input. */
export interface OutboxDeadLetterUpdate {
  attempts: number;
  reason: OutboxDeadLetterReason;
  failedAt: number;
  error: OutboxAttempt;
}

export interface OutboxStoreStats {
  /** Messages not yet published (ready, delayed or leased). */
  pending: number;
  /**
   * Claimable right now: due, unleased, and not held back by an older message with
   * the same key that is delayed (retrying) or leased.
   */
  ready: number;
  /** Under an unexpired lease. */
  leased: number;
  deadLetters: number;
  /**
   * Epoch ms since which the longest-waiting message has waited, or null when none is:
   * the earliest `availableAt` among due messages that haven't failed yet, and the
   * earliest `createdAt` among messages that have. A message scheduled for later
   * doesn't count until it is due.
   */
  oldestDueAt: number | null;
}
