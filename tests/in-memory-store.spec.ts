import { Logger } from '@nestjs/common';
import { InMemoryOutboxStore, Outbox, OutboxTransactionRequiredError, type OutboxRelay } from '../lib/index.js';
import { outboxInboxStoreContract, outboxStoreContract } from '../lib/testing/index.js';
import { message, storageWith } from './helpers.js';

describe('InMemoryOutboxStore: the store contracts', () => {
  const harness = () => {
    const store = new InMemoryOutboxStore();
    return { store, transaction: <T>(work: (tx: unknown) => Promise<T>) => store.transaction(work) };
  };

  describe('OutboxStore', () => {
    for (const c of outboxStoreContract(harness, { concurrent: true })) {
      it(c.name, c.run);
    }
  });

  describe('OutboxInboxStore', () => {
    for (const c of outboxInboxStoreContract(harness, { concurrent: true })) {
      it(c.name, c.run);
    }
  });
});

describe('InMemoryOutboxStore', () => {
  const claim = (store: InMemoryOutboxStore) => store.claim({ owner: 'r', now: 10, leaseMs: 1_000, limit: 10 }).map((m) => m.topic);

  it('answers synchronously: Outbox.add() returns the messages, not a promise, inside the transaction', async () => {
    const store = new InMemoryOutboxStore();
    const outbox = new Outbox(storageWith(store), {} as OutboxRelay);

    await expect(
      store.transaction((tx) => {
        const written = outbox.add(tx, { topic: 'order.placed', payload: {} });
        expect(written).not.toBeInstanceOf(Promise); // nothing to await before the commit
        expect(written).toMatchObject({ topic: 'order.placed', attempts: 0 });
        expect(store.add(tx, [message('direct')])).toBeUndefined();
        throw new Error('payment declined');
      }),
    ).rejects.toThrow('payment declined');

    expect(claim(store)).toEqual([]); // the rollback took both back
  });

  it('writes at once through a handle it does not own: it cannot join a database transaction', () => {
    const store = new InMemoryOutboxStore();
    const drizzleTx = { some: 'transaction of your ORM' };
    store.add(drizzleTx, [message('a')]);
    expect(claim(store)).toEqual(['a']);
    expect(store.recordInbox(drizzleTx, 'billing', 'm1', 1)).toBe(true);
    expect(store.hasInbox('billing', 'm1')).toBe(true);
  });

  it('warns once, the first time it gets a transaction handle it cannot join', async () => {
    const warn = vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => {});
    try {
      const store = new InMemoryOutboxStore();
      await store.transaction(async (tx) => store.add(tx, [message('own')])); // its own handle: no warning
      store.recordInbox(undefined, 'billing', 'm0', 1); // no handle: nothing to join
      expect(warn).not.toHaveBeenCalled();

      store.add({ drizzle: 'tx' }, [message('a')]);
      store.recordInbox({ drizzle: 'tx' }, 'billing', 'm1', 1);
      store.add({ drizzle: 'tx2' }, [message('b')]);

      expect(warn).toHaveBeenCalledTimes(1);
      expect(warn).toHaveBeenCalledWith(
        "InMemoryOutboxStore.add() received your transaction handle, and can't join it: the write applies at once, " +
          "so a rolled-back transaction won't undo it. Register a store for your database with " +
          'OutboxStorage.registerSource(). (Logged once.)',
      );

      // Once per store: another store warns again, here from recordInbox().
      new InMemoryOutboxStore().recordInbox({ drizzle: 'tx' }, 'billing', 'm1', 1);
      expect(warn).toHaveBeenCalledTimes(2);
      expect(String(warn.mock.calls[1]![0])).toMatch(/^InMemoryOutboxStore\.recordInbox\(\) received your transaction handle/);
    } finally {
      warn.mockRestore();
    }
  });

  it('refuses a missing handle and one whose transaction ended', async () => {
    const store = new InMemoryOutboxStore();
    expect(() => store.add(undefined, [message('a')])).toThrow(OutboxTransactionRequiredError);
    let leaked: unknown;
    await store.transaction(async (tx) => void (leaked = tx));
    expect(() => store.add(leaked, [message('a')])).toThrow(/a transaction that has ended/);
  });

  it('applies a transaction all or nothing when a message id is taken', async () => {
    const store = new InMemoryOutboxStore();
    const taken = message('taken');
    await store.transaction(async (tx) => store.add(tx, [taken]));
    await expect(store.transaction(async (tx) => store.add(tx, [message('new'), { ...taken }]))).rejects.toThrow(
      `Duplicate outbox message id ${taken.id}`,
    );
    expect(claim(store)).toEqual(['taken']);
  });

  it('keeps its own copies: changing a returned message changes nothing stored', () => {
    const store = new InMemoryOutboxStore();
    const original = message('a');
    store.add({}, [original]);
    (original.payload as { topic: string }).topic = 'changed';
    const [claimed] = store.claim({ owner: 'r', now: 10, leaseMs: 1_000, limit: 1 });
    (claimed!.payload as { topic: string }).topic = 'changed again';
    expect(store.claim({ owner: 'r2', now: 2_000, leaseMs: 1_000, limit: 1 })[0]!.payload).toEqual({ topic: 'a' });
  });
});

describe('InMemoryOutboxStore: edge cases', () => {
  const failure = { attempt: 1, at: 20, error: 'boom' };

  it('refuses to requeue a dead letter whose id is pending again, and moves none of the batch', async () => {
    const store = new InMemoryOutboxStore();
    const [a, b] = [message('a'), message('b')];
    await store.transaction(async (tx) => store.add(tx, [a, b]));
    store.claim({ owner: 'r', now: 10, leaseMs: 1_000, limit: 10 });
    store.deadLetter(a.id, 'r', { attempts: 1, reason: 'rejected', failedAt: 20, error: failure });
    store.deadLetter(b.id, 'r', { attempts: 1, reason: 'rejected', failedAt: 20, error: failure });

    // The producer chose the id and used it again.
    await store.transaction(async (tx) => store.add(tx, [{ ...message('a again'), id: a.id }]));

    expect(() => store.requeueDeadLetters({ all: true }, 30)).toThrow(
      `Can't requeue dead letter ${a.id}: a message with that id is pending`,
    );
    expect(store.stats(30)).toMatchObject({ pending: 1, deadLetters: 2 });
  });

  it('drops the inbox records of a transaction that fails to commit, and lets a waiting delivery through', async () => {
    const store = new InMemoryOutboxStore();
    const taken = message('taken');
    await store.transaction(async (tx) => store.add(tx, [taken]));

    let waiting: boolean | Promise<boolean> | undefined;
    await expect(
      store.transaction(async (tx) => {
        expect(store.recordInbox(tx, 'billing', 'm1', 1)).toBe(true);
        waiting = store.recordInbox(undefined, 'billing', 'm1', 2); // another delivery meets the pending record
        store.add(tx, [{ ...message('dup'), id: taken.id }]); // fails at commit
      }),
    ).rejects.toThrow(`Duplicate outbox message id ${taken.id}`);

    expect(waiting).toBeInstanceOf(Promise);
    expect(await waiting).toBe(true);
    expect(store.hasInbox('billing', 'm1')).toBe(true);
  });

  it('releases each id once, and only the leases the owner holds', () => {
    const store = new InMemoryOutboxStore();
    const [a, b] = [message('a'), message('b')];
    store.add({}, [a, b]);
    store.claim({ owner: 'r1', now: 10, leaseMs: 1_000, limit: 1 });
    store.claim({ owner: 'r2', now: 10, leaseMs: 1_000, limit: 1 });

    expect(store.release([a.id, a.id, b.id, 'missing'], 'r1')).toBe(1);
    expect(store.stats(10)).toMatchObject({ leased: 1, ready: 1 });
  });

  it('counts a lease as expired at its end, not a millisecond later', () => {
    const store = new InMemoryOutboxStore();
    store.add({}, [message('a')]);
    store.claim({ owner: 'r1', now: 10, leaseMs: 1_000, limit: 1 });

    expect(store.stats(1_009)).toMatchObject({ leased: 1, ready: 0 });
    expect(store.stats(1_010)).toMatchObject({ leased: 0, ready: 1 });
    expect(store.claim({ owner: 'r2', now: 1_010, leaseMs: 1_000, limit: 1 })).toHaveLength(1);
  });

  it('lists dead letters that failed at the same time by id, descending', () => {
    const store = new InMemoryOutboxStore();
    const messages = ['b', 'c', 'a'].map((id) => ({ ...message(id), id }));
    store.add({}, messages);
    store.claim({ owner: 'r', now: 10, leaseMs: 1_000, limit: 10 });
    for (const m of messages) {
      store.deadLetter(m.id, 'r', { attempts: 1, reason: 'rejected', failedAt: 20, error: failure });
    }

    expect(store.listDeadLetters({}).map((d) => d.id)).toEqual(['c', 'b', 'a']);
  });

  it('prunes inbox entries strictly older than the cutoff', () => {
    const store = new InMemoryOutboxStore();
    store.recordInbox(undefined, 'billing', 'old', 99);
    store.recordInbox(undefined, 'billing', 'edge', 100);

    expect(store.pruneInbox(100)).toBe(1);
    expect([store.hasInbox('billing', 'old'), store.hasInbox('billing', 'edge')]).toEqual([false, true]);
  });
});
