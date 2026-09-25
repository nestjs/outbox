/**
 * The producer API (`Outbox.add()`, `notify()`) and the operator API (`OutboxDeadLetters`),
 * on a registry built without Nest.
 */
import {
  InMemoryOutboxStore,
  Outbox,
  OutboxDeadLetters,
  OutboxTransactionRequiredError,
  type OutboxMessage,
  type OutboxRelay,
  type OutboxStore,
} from '../lib/index.js';
import { message, storageWith } from './helpers.js';

const UUID_V7 = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

describe('Outbox.add()', () => {
  let store: InMemoryOutboxStore;
  let notified: number;
  let outbox: Outbox;

  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['Date'], now: 1_700_000_000_000 });
    store = new InMemoryOutboxStore();
    notified = 0;
    outbox = new Outbox(storageWith(store), { notify: () => void notified++ } as OutboxRelay);
  });
  afterEach(() => vi.useRealTimers());

  const inTransaction = <T>(work: (tx: unknown) => T) => store.transaction(async (tx) => work(tx));
  const pending = () => store.claim({ owner: 'check', now: Number.MAX_SAFE_INTEGER, leaseMs: 1, limit: 100 });

  it('fills in the stored fields: a UUIDv7 id, timestamps, no attempts, a null key, copied headers', async () => {
    const headers = { 'x-trace': 'abc' };
    const added = await inTransaction((tx) => outbox.add(tx, { topic: 'order.placed', payload: { orderId: 1 }, headers }));
    headers['x-trace'] = 'changed';

    expect(added).toEqual({
      id: expect.stringMatching(UUID_V7),
      topic: 'order.placed',
      payload: { orderId: 1 },
      headers: { 'x-trace': 'abc' },
      key: null,
      createdAt: 1_700_000_000_000,
      availableAt: 1_700_000_000_000,
      attempts: 0,
      lastError: null,
    });
    expect(pending()).toEqual([added]);
  });

  it('keeps an id the producer chose, and a key', async () => {
    const added = await inTransaction((tx) =>
      outbox.add(tx, { id: 'order-1-placed', topic: 'order.placed', payload: {}, key: 'order-1' }),
    );
    expect(added).toMatchObject({ id: 'order-1-placed', key: 'order-1' });
  });

  it('snapshots the payload as JSON when added: later changes and non-JSON values do not leak in', async () => {
    const payload = { total: 10, at: new Date(0), skip: undefined as unknown, items: ['a'] };
    const added = await inTransaction((tx) => outbox.add(tx, { topic: 'order.placed', payload }));
    payload.total = 99;
    payload.items.push('b');

    expect(added.payload).toEqual({ total: 10, at: '1970-01-01T00:00:00.000Z', items: ['a'] });
    expect(pending()[0]!.payload).toEqual(added.payload);
  });

  it('schedules with availableAt, as a Date or epoch milliseconds, or with a delay as a duration', async () => {
    const [atDate, atNumber, delayed, inPast] = await inTransaction((tx) =>
      outbox.add(tx, [
        { topic: 'a', payload: {}, availableAt: new Date(1_700_000_060_000) },
        { topic: 'b', payload: {}, availableAt: 1_700_000_120_000 },
        { topic: 'c', payload: {}, delay: '15m' },
        { topic: 'd', payload: {}, availableAt: 0 },
      ]),
    );

    expect([atDate, atNumber, delayed, inPast].map((m) => m!.availableAt)).toEqual([
      1_700_000_060_000,
      1_700_000_120_000,
      1_700_000_000_000 + 900_000,
      0,
    ]);
    expect(delayed!.createdAt).toBe(1_700_000_000_000);
  });

  it('adds a batch in one store call, in order, and returns the messages as an array', async () => {
    const add = vi.spyOn(store, 'add');
    const added = await inTransaction((tx) =>
      outbox.add(tx, [
        { topic: 'first', payload: 1 },
        { topic: 'second', payload: 2 },
      ]),
    );

    expect(add).toHaveBeenCalledTimes(1);
    expect(added.map((m) => m.topic)).toEqual(['first', 'second']);
    expect(new Set(added.map((m) => m.id)).size).toBe(2);
    expect(pending().map((m) => m.topic)).toEqual(['first', 'second']);
  });

  it('refuses invalid messages before writing any of the batch', async () => {
    const cases: [unknown, RegExp][] = [
      [{ payload: {} }, /needs a topic \(a non-empty string\)/],
      [{ topic: '', payload: {} }, /needs a topic/],
      [{ topic: 42, payload: {} }, /needs a topic/],
      [{ topic: 'a', payload: {}, key: 7 }, /`key` must be a string or null \(got number\)/],
      [{ topic: 'a', payload: undefined }, /payload must be JSON-serializable/],
      [{ topic: 'a', payload: () => 1 }, /payload must be JSON-serializable/],
      [{ topic: 'a', payload: { total: 10n } }, /BigInt/],
      [{ topic: 'a', payload: {}, availableAt: new Date('not a date') }, /`availableAt` must be a valid Date or epoch milliseconds \(got NaN\)/],
      [{ topic: 'a', payload: {}, delay: '10 minutes' }, /Invalid duration "10 minutes"/],
    ];

    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    cases.push([{ topic: 'a', payload: cyclic }, /circular/i]);

    for (const [invalid, error] of cases) {
      await expect(
        inTransaction((tx) => outbox.add(tx, [{ topic: 'valid', payload: {} }, invalid as OutboxMessage])),
      ).rejects.toThrow(error);
    }
    expect(pending()).toEqual([]);
  });

  it('requires a transaction handle: null and undefined are refused before the store is called', () => {
    const add = vi.spyOn(store, 'add');
    for (const tx of [null, undefined]) {
      expect(() => outbox.add(tx, { topic: 'a', payload: {} })).toThrow(OutboxTransactionRequiredError);
    }
    expect(add).not.toHaveBeenCalled();
  });

  it('returns a promise of the messages when the store is asynchronous, resolved after the write', async () => {
    const written: OutboxMessage[] = [];
    let finish!: () => void;
    const asyncStore = Object.assign(new InMemoryOutboxStore(), {
      add: (_tx: unknown, messages: readonly OutboxMessage[]) =>
        new Promise<void>((resolve) => {
          finish = () => {
            written.push(...messages);
            resolve();
          };
        }),
    }) as unknown as OutboxStore & InMemoryOutboxStore;
    const producer = new Outbox(storageWith(asyncStore), {} as OutboxRelay);

    const result = producer.add({ tx: true }, { topic: 'order.placed', payload: {} });
    expect(result).toBeInstanceOf(Promise);

    let resolved: OutboxMessage | undefined;
    void (result as Promise<OutboxMessage>).then((m) => (resolved = m));
    await Promise.resolve();
    expect(resolved).toBeUndefined();

    finish();
    await result;
    expect(resolved).toEqual(written[0]);
  });

  it("propagates the store's error: the caller's transaction rolls back", async () => {
    vi.spyOn(store, 'add').mockImplementation(() => {
      throw new Error('unique violation');
    });
    await expect(inTransaction((tx) => outbox.add(tx, { topic: 'a', payload: {} }))).rejects.toThrow('unique violation');
  });

  it('notify() asks the relay to poll now', () => {
    outbox.notify();
    outbox.notify();
    expect(notified).toBe(2);
  });
});

describe('OutboxDeadLetters', () => {
  let store: InMemoryOutboxStore;
  let notified: number;
  let deadLetters: OutboxDeadLetters;

  beforeEach(() => {
    store = new InMemoryOutboxStore();
    notified = 0;
    deadLetters = new OutboxDeadLetters(storageWith(store), { notify: () => void notified++ } as OutboxRelay);
  });

  /** Adds the messages and dead-letters them, failing at `failedAt` 100, 101, ... */
  const kill = async (...messages: OutboxMessage[]) => {
    await store.transaction(async (tx) => store.add(tx, messages));
    store.claim({ owner: 'r', now: 10, leaseMs: 1_000, limit: 100 });
    for (const [i, m] of messages.entries()) {
      store.deadLetter(m.id, 'r', {
        attempts: 1,
        reason: 'rejected',
        failedAt: 100 + i,
        error: { attempt: 1, at: 100 + i, error: 'boom' },
      });
    }
    return messages;
  };

  it('targets one id, a list of ids, or a filter', async () => {
    const [a, b, c, d] = await kill(message('a'), message('b'), message('c'), message('d', 'K'));

    expect(await deadLetters.requeue(a!.id)).toBe(1);
    expect(await deadLetters.purge([b!.id, c!.id, 'missing'])).toBe(2);
    expect(await deadLetters.requeue({ key: 'K' })).toBe(1);

    expect(await deadLetters.list()).toEqual([]);
    expect(store.claim({ owner: 'r2', now: Date.now(), leaseMs: 1_000, limit: 10 }).map((m) => m.id)).toEqual([
      a!.id,
      d!.id,
    ]);
  });

  it('refuses an empty filter before the store sees it, and accepts { all: true }', async () => {
    await kill(message('a'), message('b'));
    const requeue = vi.spyOn(store, 'requeueDeadLetters');
    const purge = vi.spyOn(store, 'purgeDeadLetters');

    const empty = 'Refusing an empty dead-letter filter; pass { all: true } to target every dead letter';
    await expect(deadLetters.requeue({})).rejects.toThrow(empty);
    await expect(deadLetters.purge({ ids: undefined, all: false })).rejects.toThrow(empty);
    expect(requeue).not.toHaveBeenCalled();
    expect(purge).not.toHaveBeenCalled();

    expect(await deadLetters.purge({ all: true })).toBe(2);
  });

  it('wakes the relay after a requeue that moved something, and only then', async () => {
    const [a] = await kill(message('a'));

    expect(await deadLetters.requeue('missing')).toBe(0);
    expect(notified).toBe(0);

    expect(await deadLetters.requeue(a!.id)).toBe(1);
    expect(notified).toBe(1);

    await kill(message('b'));
    expect(await deadLetters.purge({ all: true })).toBe(1);
    expect(notified).toBe(1);
  });

  it('lists newest first with the query passed through, and gets one by id', async () => {
    const [a, , c] = await kill(message('orders', 'K'), message('orders', 'J'), message('orders', 'K'));

    expect((await deadLetters.list({ key: 'K' })).map((d) => d.id)).toEqual([c!.id, a!.id]);
    expect((await deadLetters.list({ topic: 'orders', limit: 1, offset: 2 })).map((d) => d.id)).toEqual([a!.id]);
    expect(await deadLetters.get(a!.id)).toMatchObject({ id: a!.id, reason: 'rejected', failedAt: 100 });
    expect(await deadLetters.get('missing')).toBeUndefined();
  });

  it('purges by failedBefore given as a Date', async () => {
    await kill(message('a'), message('b'), message('c'));
    expect(await deadLetters.purge({ failedBefore: new Date(102) })).toBe(2);
    expect((await deadLetters.list()).map((d) => d.topic)).toEqual(['c']);
  });
});
