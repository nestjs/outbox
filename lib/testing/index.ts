/**
 * `@nestjs/outbox/testing`: the `OutboxStore` and `OutboxInboxStore` contracts as
 * runner-agnostic test suites, one per contract.
 *
 * ```ts
 * import { outboxInboxStoreContract, outboxStoreContract } from '@nestjs/outbox/testing';
 *
 * describe('DrizzleOutboxStore', () => {
 *   const harness = async () => ({ store, transaction: (work) => db.transaction(work) });
 *   for (const c of outboxStoreContract(harness, { concurrent: true })) it(c.name, c.run);
 *   for (const c of outboxInboxStoreContract(harness, { concurrent: true })) it(c.name, c.run);
 * });
 * ```
 *
 * Each case creates its own harness (so give each one empty tables, or a fresh database),
 * runs, and closes it. A case throws (an `AssertionError`) when the store breaks the rule
 * its name states.
 */
import assert from 'node:assert/strict';
import type { OutboxMessage } from '../interfaces/outbox-message.interface.js';
import type { OutboxAttempt } from '../interfaces/outbox-dead-letter.interface.js';
import type { Awaitable } from '../interfaces/awaitable.interface.js';
import { OutboxTransactionRequiredError } from '../errors/outbox-transaction-required.error.js';
import type { OutboxStore } from '../interfaces/outbox-store.interface.js';
import type { OutboxInboxStore } from '../interfaces/outbox-inbox-store.interface.js';
import { uuidv7 } from '../utils/uuid.util.js';

/** What an `outboxStoreContract()` case runs against. */
export interface OutboxStoreHarness<Tx = unknown> {
  /** The store, on empty tables. */
  store: OutboxStore<Tx>;
  /**
   * Runs `work` in a transaction of the application's own, on the database the store
   * uses, and passes its handle: commits when `work` resolves, rolls back when it throws.
   * With `concurrent`, several may be open at once, so each needs its own connection.
   */
  transaction<T>(work: (tx: Tx) => Promise<T>): Promise<T>;
  /**
   * A handle the store must refuse as a transaction, typically the database itself: `add()`
   * must throw `OutboxTransactionRequiredError` for it. Leave it out when the store can't tell.
   */
  notATransaction?: unknown;
  /** Called after the case, pass or fail. */
  close?(): Awaitable<void>;
}

/** What an `outboxInboxStoreContract()` case runs against: the same, with an inbox store. */
export interface OutboxInboxStoreHarness<Tx = unknown> extends Omit<OutboxStoreHarness<Tx>, 'store'> {
  /** The store, on an empty inbox table. `notATransaction`: `recordInbox()` must refuse it. */
  store: OutboxInboxStore<Tx>;
}

export interface OutboxStoreContractOptions {
  /**
   * Adds the concurrency cases: producers whose transactions overlap, relays claiming side
   * by side (`outboxStoreContract()`); two deliveries of a message in overlapping
   * transactions (`outboxInboxStoreContract()`). They are what a naive
   * implementation fails. On a database with one connection (PGlite) they pass serialized;
   * run them against a server too, where the transactions really overlap. Default `false`.
   */
  concurrent?: boolean;
}

export interface OutboxStoreContractCase {
  name: string;
  run(): Promise<void>;
}

/** The contract every `OutboxStore` (messages and dead letters) passes, as cases for any test runner. */
export function outboxStoreContract<Tx>(
  createStore: () => Awaitable<OutboxStoreHarness<Tx>>,
  options: OutboxStoreContractOptions = {},
): OutboxStoreContractCase[] {
  return toCases([...CASES, ...(options.concurrent ? CONCURRENT : [])], async () => helpers(await createStore()));
}

/** The contract every `OutboxInboxStore` passes, as cases for any test runner. */
export function outboxInboxStoreContract<Tx>(
  createStore: () => Awaitable<OutboxInboxStoreHarness<Tx>>,
  options: OutboxStoreContractOptions = {},
): OutboxStoreContractCase[] {
  return toCases([...INBOX_CASES, ...(options.concurrent ? INBOX_CONCURRENT : [])], createStore);
}

function toCases<H extends { close?(): Awaitable<void> }>(
  cases: [string, (h: H) => Promise<void>][],
  create: () => Awaitable<H>,
): OutboxStoreContractCase[] {
  return cases.map(([name, body]) => ({
    name,
    async run() {
      const harness = await create();
      try {
        await body(harness);
      } finally {
        await harness.close?.();
      }
    },
  }));
}

// ------------------------------------------------------------------ helpers

type Harness<Tx> = OutboxStoreHarness<Tx> & {
  add(...messages: OutboxMessage[]): Promise<void>;
  claimed(owner: string, now?: number, limit?: number, leaseMs?: number): Promise<OutboxMessage[]>;
  claim(owner: string, now?: number, limit?: number, leaseMs?: number): Promise<string[]>;
};

function helpers<Tx>(h: OutboxStoreHarness<Tx>): Harness<Tx> {
  const claimed = async (owner: string, now = 10, limit = 100, leaseMs = 1_000) =>
    await h.store.claim({ owner, now, leaseMs, limit });

  return {
    ...h,
    store: h.store,
    transaction: (work) => h.transaction(work),
    add: (...messages) => h.transaction(async (tx) => void (await h.store.add(tx, messages))),
    claimed,
    claim: async (owner, now, limit, leaseMs) => (await claimed(owner, now, limit, leaseMs)).map((m) => m.topic),
  };
}

function message(topic: string, key: string | null = null, availableAt = 0): OutboxMessage {
  return {
    id: uuidv7(),
    topic,
    // Quotes, non-ASCII, and text that looks like placeholders of every dialect.
    payload: { topic, text: "O'Reilly ü   ? $1 :name" },
    headers: { 'x-source': 'test' },
    key,
    createdAt: 0,
    availableAt,
    attempts: 0,
    lastError: null,
  };
}

const failure = (attempt: number): OutboxAttempt => ({ attempt, at: 1, error: `boom ${attempt}` });

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((res) => (resolve = res));
  return { promise, resolve };
}

function pick<T extends object>(value: T | undefined, keys: (keyof T)[]): Partial<T> | undefined {
  return value && (Object.fromEntries(keys.map((key) => [key, value[key]])) as Partial<T>);
}

/** Awaits a store call that must fail, whether it throws or rejects. */
async function rejects(call: () => unknown, expected: RegExp | (new (...args: any[]) => Error), message: string) {
  await assert.rejects(async () => call(), expected as RegExp, message);
}

// ------------------------------------------------------------------ cases

const CASES: [string, (h: Harness<any>) => Promise<void>][] = [
  [
    "add() writes through the caller's transaction: a rollback leaves nothing behind",
    async (h) => {
      await rejects(
        () =>
          h.transaction(async (tx) => {
            await h.store.add(tx, [message('order.placed')]);
            throw new Error('payment declined');
          }),
        /payment declined/,
        'the transaction should fail with its own error',
      );
      assert.deepEqual(await h.claim('r1'), [], 'a rolled-back message must not be claimable');

      await h.add(message('order.placed'));
      assert.deepEqual(await h.claim('r1'), ['order.placed']);
    },
  ],
  [
    'add() of an empty batch writes nothing',
    async (h) => {
      await h.transaction(async (tx) => void (await h.store.add(tx, [])));
      assert.equal((await h.store.stats(10)).pending, 0);
    },
  ],
  [
    'refuses a handle that is not a transaction',
    async (h) => {
      if (h.notATransaction === undefined) {
        return;
      }
      await rejects(() => h.store.add(h.notATransaction, [message('x')]), OutboxTransactionRequiredError, 'add()');
      assert.deepEqual(await h.claim('r1'), [], 'nothing may be written');
    },
  ],
  [
    'round-trips every message field, a null payload included',
    async (h) => {
      const original = { ...message('order.placed', 'customer-1'), createdAt: 1_790_000_000_123, availableAt: 5 };
      const empty = { ...message('cache.flushed'), payload: null, headers: {} };
      await h.add(original, empty);
      assert.deepEqual(await h.claimed('r1', 10, 2), [original, empty]);
    },
  ],
  [
    'answers "nothing" when nothing matches',
    async (h) => {
      assert.deepEqual(await h.claimed('r1'), []);
      assert.equal(await h.store.markPublished('missing', 'r1'), false);
      assert.equal(await h.store.reschedule('missing', 'r1', { attempts: 1, availableAt: 0, error: failure(1) }), false);
      assert.equal(
        await h.store.deadLetter('missing', 'r1', { attempts: 1, reason: 'rejected', failedAt: 1, error: failure(1) }),
        false,
      );
      assert.equal(await h.store.release([], 'r1'), 0);
      assert.equal(await h.store.release(['missing'], 'r1'), 0);
      assert.deepEqual(await h.store.listDeadLetters({}), []);
      assert.equal(await h.store.getDeadLetter('missing'), undefined);
      assert.equal(await h.store.requeueDeadLetters({ ids: ['missing'] }, 10), 0);
      assert.equal(await h.store.requeueDeadLetters({ all: true }, 10), 0);
      assert.equal(await h.store.purgeDeadLetters({ all: true }), 0);
      assert.deepEqual(await h.store.stats(10), { pending: 0, ready: 0, leased: 0, deadLetters: 0, oldestDueAt: null });
    },
  ],
  [
    'leases claimed messages until the lease expires',
    async (h) => {
      await h.add(message('a'), message('b'));
      assert.deepEqual(await h.claim('r1', 10, 1), ['a']);
      assert.deepEqual(await h.claim('r2', 10), ['b']);
      assert.deepEqual(await h.claim('r3', 10), []);
      // r1's lease (until 1010) expired: anyone may take the message over.
      assert.deepEqual(await h.claim('r3', 1_010), ['a', 'b']);
    },
  ],
  [
    'does not hand out delayed messages early',
    async (h) => {
      await h.add(message('later', null, 500));
      assert.deepEqual(await h.claim('r1', 10), []);
      assert.deepEqual(await h.claim('r1', 500), ['later']);
    },
  ],
  [
    'claims a contiguous run per key and never skips ahead of a blocked message',
    async (h) => {
      await h.add(message('a1', 'A'), message('b1', 'B'), message('a2', 'A'), message('n1'), message('a3', 'A'));
      // r1 takes the oldest two: a1 (head of A) and b1. A is now blocked by r1's lease.
      assert.deepEqual(await h.claim('r1', 10, 2), ['a1', 'b1']);
      assert.deepEqual(await h.claim('r2', 10), ['n1']);
    },
  ],
  [
    'keeps a key blocked while its head waits to retry, and unblocks it on dead-letter',
    async (h) => {
      const a1 = message('a1', 'A');
      await h.add(a1, message('a2', 'A'), message('b1', 'B'));
      assert.deepEqual(await h.claim('r1', 10, 1), ['a1']);

      assert.equal(await h.store.reschedule(a1.id, 'r1', { attempts: 1, availableAt: 100, error: failure(1) }), true);
      assert.deepEqual(await h.claim('r2', 10), ['b1']);
      assert.deepEqual(await h.claim('r3', 100, 1), ['a1']);

      const moved = await h.store.deadLetter(a1.id, 'r3', { attempts: 2, reason: 'exhausted', failedAt: 150, error: failure(2) });
      assert.equal(moved, true);
      assert.deepEqual(await h.claim('r4', 150), ['a2']);

      const [dead] = await h.store.listDeadLetters({});
      assert.deepEqual(pick(dead, ['id', 'topic', 'key', 'attempts', 'reason', 'lastError', 'failedAt', 'payload', 'headers', 'createdAt']), {
        id: a1.id,
        topic: 'a1',
        key: 'A',
        attempts: 2,
        reason: 'exhausted',
        lastError: 'boom 2',
        failedAt: 150,
        payload: a1.payload,
        headers: a1.headers,
        createdAt: a1.createdAt,
      });
      assert.deepEqual(dead!.history, [failure(1), failure(2)], 'the history of every failed attempt');
      assert.deepEqual(await h.store.getDeadLetter(a1.id), dead);
    },
  ],
  [
    'fences every write by the claim owner',
    async (h) => {
      const m = message('a');
      await h.add(m);
      await h.claim('r1', 10);
      await h.claim('r2', 2_000); // r1's lease expired and r2 took over

      assert.equal(await h.store.markPublished(m.id, 'r1'), false, 'markPublished');
      assert.equal(await h.store.reschedule(m.id, 'r1', { attempts: 1, availableAt: 0, error: failure(1) }), false, 'reschedule');
      assert.equal(
        await h.store.deadLetter(m.id, 'r1', { attempts: 1, reason: 'rejected', failedAt: 1, error: failure(1) }),
        false,
        'deadLetter',
      );
      assert.equal(await h.store.release([m.id], 'r1'), 0, 'release');

      assert.equal((await h.store.stats(2_000)).deadLetters, 0);
      assert.equal(await h.store.markPublished(m.id, 'r2'), true);
      assert.equal((await h.store.stats(2_000)).pending, 0);
    },
  ],
  [
    'releases leases so the messages are claimable again at once',
    async (h) => {
      const m = message('a', 'A');
      await h.add(m, message('a2', 'A'));
      await h.claim('r1', 10);
      assert.equal(await h.store.release([m.id], 'r1'), 1);
      // a2 is still leased by r1, a1 is free again: a1 is the only claimable one.
      assert.deepEqual(await h.claim('r2', 10), ['a']);
    },
  ],
  [
    'requeues and purges dead letters by id or filter, and refuses an empty filter',
    async (h) => {
      const [a, b, c] = [message('a', 'K'), message('b'), message('b')];
      await h.add(a, b, c);
      await h.claim('r1', 10, 10);
      for (const m of [a, b, c]) {
        await h.store.deadLetter(m.id, 'r1', { attempts: 3, reason: 'exhausted', failedAt: 20, error: failure(3) });
      }

      const stats = await h.store.stats(30);
      assert.deepEqual([stats.pending, stats.deadLetters], [0, 3]);
      assert.equal((await h.store.listDeadLetters({ topic: 'b' })).length, 2);
      assert.equal(await h.store.requeueDeadLetters({ ids: [] }, 40), 0, 'ids: [] matches nothing');
      assert.equal(await h.store.purgeDeadLetters({ ids: [] }), 0, 'ids: [] matches nothing');

      assert.equal(await h.store.requeueDeadLetters({ ids: [a.id] }, 40), 1);
      const [requeued] = await h.claimed('r2', 40, 10);
      assert.deepEqual(pick(requeued, ['id', 'attempts', 'availableAt', 'lastError']), {
        id: a.id,
        attempts: 0,
        availableAt: 40,
        lastError: 'boom 3',
      });

      await rejects(() => h.store.purgeDeadLetters({}), /all: true/, 'purge with an empty filter');
      await rejects(() => h.store.requeueDeadLetters({}, 50), /all: true/, 'requeue with an empty filter');

      assert.equal(await h.store.purgeDeadLetters({ topic: 'b', failedBefore: 21 }), 2);
      assert.equal((await h.store.stats(50)).deadLetters, 0);
    },
  ],
  [
    'requeues a dead letter with its failure history, and lists by key, newest first, in pages',
    async (h) => {
      const ms = [message('k1', 'K'), message('k2', 'K'), message('j1', 'J')];
      await h.add(...ms);
      await h.claim('r1', 10, 10);
      for (const [i, m] of ms.entries()) {
        await h.store.deadLetter(m.id, 'r1', { attempts: 1, reason: 'rejected', failedAt: 100 + i, error: failure(1) });
      }

      assert.deepEqual((await h.store.listDeadLetters({ key: 'K' })).map((d) => d.topic), ['k2', 'k1']);
      assert.deepEqual((await h.store.listDeadLetters({ limit: 1, offset: 1 })).map((d) => d.topic), ['k2']);
      assert.equal(await h.store.requeueDeadLetters({ key: 'K' }, 200), 2);
      assert.equal(await h.store.requeueDeadLetters({ all: true }, 200), 1);

      const [k1] = await h.claimed('r2', 200, 1);
      assert.equal(
        await h.store.deadLetter(k1!.id, 'r2', { attempts: 1, reason: 'rejected', failedAt: 300, error: failure(2) }),
        true,
      );
      assert.deepEqual((await h.store.getDeadLetter(k1!.id))!.history, [failure(1), failure(2)]);
    },
  ],
  [
    'replaces an earlier dead letter when a message with the same id is dead-lettered again',
    async (h) => {
      const first = message('first');
      await h.add(first);
      await h.claim('r1', 10);
      await h.store.deadLetter(first.id, 'r1', { attempts: 1, reason: 'rejected', failedAt: 20, error: failure(1) });

      // The producer chose the id (`add({ id })`), and used it again.
      const again = { ...message('again'), id: first.id };
      await h.add(again);
      await h.claim('r2', 30);

      assert.equal(
        await h.store.deadLetter(again.id, 'r2', { attempts: 1, reason: 'exhausted', failedAt: 40, error: failure(1) }),
        true,
      );

      const list = await h.store.listDeadLetters({});
      assert.deepEqual(
        list.map((d) => [d.id, d.topic, d.reason]),
        [[first.id, 'again', 'exhausted']],
      );
    },
  ],
  [
    'publishes a key in the order its messages were added, whatever their ids',
    async (h) => {
      // Two API instances whose clocks are a few milliseconds apart: the cancellation is added
      // after the placement committed, but its UUIDv7 comes from the clock that is behind.
      const placed = { ...message('placed', 'order-1'), id: '0199a000-0000-7000-8000-000000000002' };
      const cancelled = { ...message('cancelled', 'order-1'), id: '0199a000-0000-7000-8000-000000000001' };
      await h.add(placed);
      await h.add(cancelled);
      assert.deepEqual(await h.claim('r1'), ['placed', 'cancelled']);
    },
  ],
  [
    'puts a requeued dead letter back at its place in its key',
    async (h) => {
      const first = { ...message('first', 'K'), id: 'zz-first' };
      const second = { ...message('second', 'K'), id: 'aa-second' };
      await h.add(first, second);
      assert.deepEqual(await h.claim('r1', 10, 1), ['first']);
      await h.store.deadLetter(first.id, 'r1', { attempts: 1, reason: 'rejected', failedAt: 20, error: failure(1) });

      assert.equal(await h.store.requeueDeadLetters({ ids: [first.id] }, 30), 1);
      assert.deepEqual(await h.claim('r2', 30), ['first', 'second']);
    },
  ],
  [
    'reports pending, ready, leased, and since when the longest-waiting message has been due',
    async (h) => {
      await h.add(
        { ...message('a'), createdAt: 5, availableAt: 5 },
        { ...message('b'), createdAt: 7, availableAt: 7 },
        { ...message('c', null, 1_000), createdAt: 0 }, // scheduled for later: not waiting yet
      );
      await h.claim('r1', 10, 1);
      assert.deepEqual(await h.store.stats(10), { pending: 3, ready: 1, leased: 1, deadLetters: 0, oldestDueAt: 5 });
    },
  ],
  [
    'does not count a message scheduled for later as waiting, and a retrying one waits since it was added',
    async (h) => {
      await h.add({ ...message('reminder', null, 86_400_000), createdAt: 0 });
      const before = await h.store.stats(1_000);
      assert.deepEqual([before.pending, before.oldestDueAt], [1, null]);
      assert.equal((await h.store.stats(86_400_000)).oldestDueAt, 86_400_000);

      const retried = { ...message('retried'), createdAt: 50, availableAt: 50 };
      await h.add(retried);
      await h.claim('r1', 100);
      await h.store.reschedule(retried.id, 'r1', { attempts: 1, availableAt: 5_000, error: failure(1) });
      assert.equal((await h.store.stats(1_000)).oldestDueAt, 50);
    },
  ],
  [
    'counts as ready only what a claim could take: a delayed or leased message holds back its key',
    async (h) => {
      await h.add(message('k1', 'K', 1_000), message('k2', 'K'), message('j1', 'J'), message('j2', 'J'), message('free'));
      assert.equal((await h.store.stats(10)).ready, 3, 'j1, j2, free: k2 waits behind the scheduled k1');

      await h.claim('r1', 10, 1); // leases j1 (the oldest claimable)
      const stats = await h.store.stats(10);
      assert.deepEqual([stats.pending, stats.leased, stats.ready], [5, 1, 1], 'only free is ready');
      assert.deepEqual(await h.claim('r2', 10), ['free']);
      assert.equal((await h.store.stats(10)).ready, 0);
    },
  ],
];

const CONCURRENT: [string, (h: Harness<any>) => Promise<void>][] = [
  [
    'publishes a key in commit order when producers overlap',
    async (h) => {
      // T1 adds a1 and keeps its transaction open; T2 adds the key's next message meanwhile.
      // Commit order is the order the two transactions finish in. A store that numbers rows
      // at insert without serializing the key's producers lets T2 commit first and still
      // publishes a1 first.
      const [a1, a2, b1] = [message('a1', 'A'), message('a2', 'A'), message('b1', 'B')];
      const committed: string[] = [];
      const added = deferred();
      const finish = deferred();

      const t1 = h
        .transaction(async (tx) => {
          await h.store.add(tx, [a1]);
          added.resolve();
          await finish.promise;
        })
        .then(() => committed.push('a1'));
      await added.promise;
      const t2 = h.transaction((tx) => Promise.resolve(h.store.add(tx, [b1, a2]))).then(() => committed.push('a2'));
      await sleep(150);
      finish.resolve();
      await Promise.all([t1, t2]);

      const order = (await h.claim('r1')).filter((topic) => topic.startsWith('a'));
      assert.deepEqual(order, committed, `key A must be published in commit order (${committed.join(', ')})`);
    },
  ],
  [
    'never hands a message to two relays, nor a key to two relays at once',
    async (h) => {
      const messages = Array.from({ length: 60 }, (_, i) => message(`m${i}`, i % 3 === 0 ? null : `key-${i % 5}`));
      await h.add(...messages);

      const seen = new Map<string, string>();
      const holder = new Map<string, string>(); // key -> relay holding unpublished messages of it
      const published: string[] = [];
      const relay = async (owner: string) => {
        for (;;) {
          const batch = await h.store.claim({ owner, now: 10, leaseMs: 60_000, limit: 7 });
          if (batch.length === 0) {
            return;
          }

          for (const m of batch) {
            assert.equal(seen.get(m.id), undefined, `${m.topic} was handed to ${seen.get(m.id)} and ${owner}`);
            seen.set(m.id, owner);
            if (m.key !== null) {
              assert.equal(holder.get(m.key) ?? owner, owner, `key ${m.key} split between relays`);
              holder.set(m.key, owner);
            }
          }

          await sleep(5);
          // Publish in order, then drop the row. A key is free once its last message in the
          // batch is deleted; forget the holder just before, since another relay may claim the
          // key's next message as soon as the delete commits.
          for (const [i, m] of batch.entries()) {
            published.push(m.topic);
            if (m.key !== null && !batch.slice(i + 1).some((next) => next.key === m.key)) {
              holder.delete(m.key);
            }
            assert.equal(await h.store.markPublished(m.id, owner), true);
          }
        }
      };

      await Promise.all(['r1', 'r2', 'r3', 'r4'].map(relay));

      assert.equal(seen.size, 60, 'every message published');
      for (let k = 0; k < 5; k++) {
        const order = messages.filter((m) => m.key === `key-${k}`).map((m) => m.topic);
        assert.deepEqual(
          published.filter((topic) => order.includes(topic)),
          order,
          `key-${k} published in the order it was added`,
        );
      }
      assert.equal((await h.store.stats(10)).pending, 0);
    },
  ],
  [
    'keeps each key in order while producers and relays race',
    async (h) => {
      const published = new Map<string, number[]>();
      let producing = true;

      // Four producers add numbered messages for three keys, each in its own transaction.
      const producer = async (p: number) => {
        for (let i = 0; i < 15; i++) {
          const key = `order-${(p + i) % 3}`;
          await h.add({ ...message(key, key), payload: { p, i } });
        }
      };

      const relay = async (owner: string) => {
        while (producing || (await h.store.stats(10)).pending > 0) {
          const batch = await h.store.claim({ owner, now: 10, leaseMs: 60_000, limit: 5 });
          for (const m of batch) {
            const { p, i } = m.payload as { p: number; i: number };
            published.set(m.key!, [...(published.get(m.key!) ?? []), p * 100 + i]);
            assert.equal(await h.store.markPublished(m.id, owner), true);
          }
          if (batch.length === 0) {
            await sleep(2);
          }
        }
      };

      const relays = ['r1', 'r2', 'r3'].map(relay);
      await Promise.all([0, 1, 2, 3].map(producer));
      producing = false;
      await Promise.all(relays);

      assert.equal([...published.values()].flat().length, 60, 'every message published once');
      // Per producer, a key's messages come out in the order that producer added them.
      for (const [key, numbers] of published) {
        for (let p = 0; p < 4; p++) {
          const mine = numbers.filter((n) => Math.floor(n / 100) === p);
          assert.deepEqual(mine, [...mine].sort((a, b) => a - b), `${key}: producer ${p}'s messages out of order`);
        }
      }
    },
  ],
  [
    "a stale relay's fenced writes race a takeover: never both",
    async (h) => {
      // r1's lease (until 110) has expired when r2 claims at 1000. Whatever r1 writes at that
      // moment must lose to r2's claim or pre-empt it, never both: a store that reads the row,
      // then writes by id alone, deletes or clears a message r2 is publishing.
      const writes: [string, (id: string) => unknown][] = [
        ['markPublished', (id) => h.store.markPublished(id, 'r1')],
        ['reschedule', (id) => h.store.reschedule(id, 'r1', { attempts: 1, availableAt: 5_000, error: failure(1) })],
        ['deadLetter', (id) => h.store.deadLetter(id, 'r1', { attempts: 1, reason: 'rejected', failedAt: 1, error: failure(1) })],
        ['release', async (id) => (await h.store.release([id], 'r1')) === 1],
      ];

      for (const [name, write] of writes) {
        for (let round = 0; round < 10; round++) {
          const m = message(`${name}-${round}`);
          await h.add(m);
          assert.equal((await h.claimed('r1', 10, 100, 100)).length, 1);

          // Staggered by 0 to 4 ms, so on a server some rounds land r2's claim inside r1's write.
          const [wrote, batch] = await Promise.all([
            sleep(round % 5).then(() => write(m.id)),
            h.store.claim({ owner: 'r2', now: 1_000, leaseMs: 1_000, limit: 100 }),
          ]);
          const taken = batch.some((b) => b.id === m.id);
          if (name !== 'release') {
            assert.ok(!(wrote && taken), `${name} by r1 and a claim by r2 both succeeded`);
          }
          if (taken) {
            assert.equal(await h.store.markPublished(m.id, 'r2'), true, `r2's lease survives r1's ${name}`);
          }

          // Nothing left for the next round (a release can make r2's claim skip the row it is writing).
          for (const rest of await h.store.claim({ owner: 'sweep', now: 1_000_000, leaseMs: 1, limit: 1_000 })) {
            await h.store.markPublished(rest.id, 'sweep');
          }
          await h.store.purgeDeadLetters({ all: true });
        }
      }
    },
  ],
];

// ------------------------------------------------------------------ inbox cases

const INBOX_CASES: [string, (h: OutboxInboxStoreHarness<any>) => Promise<void>][] = [
  [
    'refuses a handle that is not a transaction',
    async (h) => {
      if (h.notATransaction === undefined) {
        return;
      }

      await rejects(
        () => h.store.recordInbox(h.notATransaction, 'billing', 'm1', 1),
        OutboxTransactionRequiredError,
        'recordInbox()',
      );
      assert.equal(await h.store.hasInbox('billing', 'm1'), false, 'nothing may be written');
    },
  ],
  [
    'answers "nothing" when nothing matches',
    async (h) => {
      assert.equal(await h.store.hasInbox('billing', 'missing'), false);
      assert.equal(await h.store.pruneInbox(Date.now()), 0);
    },
  ],
  [
    'records inbox entries once per consumer, inside a transaction when given one',
    async (h) => {
      assert.equal(await h.store.recordInbox(undefined, 'billing', 'm1', 1), true);
      assert.equal(await h.store.recordInbox(undefined, 'billing', 'm1', 2), false);
      assert.equal(await h.store.recordInbox(undefined, 'shipping', 'm1', 2), true);
      assert.equal(await h.store.recordInbox(undefined, 'Billing', 'm1', 2), true, 'names are case-sensitive');

      await rejects(
        () =>
          h.transaction(async (tx) => {
            assert.equal(await h.store.recordInbox(tx, 'billing', 'm2', 3), true);
            assert.equal(await h.store.recordInbox(tx, 'billing', 'm2', 3), false, 'seen in the same transaction');
            throw new Error('handler failed');
          }),
        /handler failed/,
        'the transaction should fail with its own error',
      );
      assert.equal(await h.store.hasInbox('billing', 'm2'), false, 'rolled back with the transaction');
      await h.transaction(async (tx) => void (await h.store.recordInbox(tx, 'billing', 'm2', 3)));
      assert.equal(await h.store.hasInbox('billing', 'm2'), true);

      assert.equal(await h.store.pruneInbox(2), 1);
      assert.equal(await h.store.hasInbox('billing', 'm1'), false);
      assert.equal(await h.store.hasInbox('shipping', 'm1'), true);
    },
  ],
];

const INBOX_CONCURRENT: [string, (h: OutboxInboxStoreHarness<any>) => Promise<void>][] = [
  [
    'makes two deliveries of a message meet at the inbox key',
    async (h) => {
      for (const outcome of ['commit', 'rollback'] as const) {
        const id = `m-${outcome}`;
        const recorded = deferred();
        const finish = deferred();

        const first = h.transaction(async (tx) => {
          assert.equal(await h.store.recordInbox(tx, 'billing', id, 1), true);
          recorded.resolve();
          await finish.promise;
          if (outcome === 'rollback') {
            throw new Error('handler failed');
          }
        });

        await recorded.promise;
        let second: boolean | undefined;
        const other = h.transaction(async (tx) => void (second = await h.store.recordInbox(tx, 'billing', id, 2)));
        await sleep(150);
        finish.resolve();
        await first.catch(() => undefined);
        await other;

        // After a commit the second delivery is a duplicate; after a rollback it runs.
        assert.equal(second, outcome === 'rollback', `after a ${outcome}`);
      }
    },
  ],
];
