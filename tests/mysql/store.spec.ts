/**
 * What MySqlOutboxStore does beyond the contract, on MySQL: a large batch in a few statements of one JSON parameter each,
 * lock rows for keyed messages only, times with a fraction of a millisecond, the inputs a column would refuse (a key,
 * id, topic, consumer or owner longer than its column fails clearly, before any statement: in `OutboxInbox.process()`,
 * before the handler runs), keys, ids, topics and
 * consumers that differ only in case, accents or a trailing space kept apart, payloads round-tripping, each value bound to
 * its own placeholder (MySQL binds `?` by position), a requeue that meets a pending message with the same id, dead
 * letters of one moment in byte order, `stats()` and a claim on a key of 20,000 messages, and registering itself for
 * both contracts.
 */
import { randomUUID } from 'node:crypto';
import { OutboxInbox, OutboxStorage } from '../../lib/index.js';
import { MySqlOutboxStore } from '../../lib/mysql/index.js';
import { silentLogger } from '../helpers.js';
import { message, mysql2Client, onMysql, recording, rows, testDatabase, truncate, type Client } from './support.js';

const { database, reason } = await testDatabase('store');

describe(`MySqlOutboxStore through ${mysql2Client.name}`, () => {
  onMysql(reason);
  let client: Client;
  let recorder: ReturnType<typeof recording>;
  let store: MySqlOutboxStore;

  beforeAll(async () => {
    if (reason) {
      return;
    }

    client = await mysql2Client.open(database!.url);
    recorder = recording(client.executor);
    store = new MySqlOutboxStore({ executor: recorder.executor });
    await store.onModuleInit();
  });
  afterAll(async () => {
    await client?.close();
  });
  beforeEach(async () => {
    if (!reason) {
      await truncate(client.executor, 'nest_outbox');
      recorder.statements.length = 0;
    }
  });

  const claimAll = (owner: string = randomUUID()) => store.claim({ owner, now: 10, leaseMs: 1_000, limit: 100_000 });
  const inserts = () => recorder.statements.filter(({ text }) => text.startsWith('INSERT INTO `nest_outbox_messages`'));
  const locks = () => recorder.statements.filter(({ text }) => text.includes('`nest_outbox_locks`'));

  it('adds a batch of 12,000 messages in a few statements of one JSON parameter each, in the batch order', async () => {
    const batch = Array.from({ length: 12_000 }, (_, i) => message(`m${i}`));
    await client.transaction((tx) => store.add(tx, batch));

    expect(inserts().length).toBeGreaterThan(1);
    expect(inserts().length).toBeLessThanOrEqual(3);
    expect(inserts().every(({ params }) => params?.length === 1)).toBe(true);
    expect((await claimAll()).map((m) => m.topic)).toEqual(batch.map((m) => m.topic));
  });

  it("locks the keys' buckets in two statements, and takes no lock for messages without a key", async () => {
    await client.transaction((tx) => store.add(tx, [message('a'), message('b')]));
    expect(locks()).toEqual([]);

    await client.transaction((tx) => store.add(tx, [message('c', 'K'), message('d', 'L'), message('e', 'K')]));
    expect(locks()).toHaveLength(2);
    expect(locks()[0]!.text).toMatch(/^INSERT INTO `nest_outbox_locks` \(id\) VALUES \(\?\), \(\?\) ON DUPLICATE KEY UPDATE id = id$/);
  });

  it('passes over a key its leased head holds back, so a batch fills with the messages it can take', async () => {
    const key = [message('head', 'K'), ...Array.from({ length: 10 }, (_, i) => message(`k-${i}`, 'K'))];
    const free = Array.from({ length: 5 }, (_, i) => message(`free-${i}`));
    await client.transaction((tx) => store.add(tx, [...key, ...free]));
    expect((await store.claim({ owner: 'r1', now: 10, leaseMs: 1_000, limit: 1 })).map((m) => m.topic)).toEqual(['head']);

    // Not the key's next five, which a claim can't take while its head is leased.
    expect((await store.claim({ owner: 'r2', now: 10, leaseMs: 1_000, limit: 5 })).map((m) => m.topic)).toEqual(free.map((m) => m.topic));
  });

  it('takes times with a fraction of a millisecond (a numeric delay or lease), in whole milliseconds', async () => {
    const delayed = message('delayed', null, { createdAt: 1.5, availableAt: 100.7 });
    await client.transaction((tx) => store.add(tx, [delayed]));
    expect(await store.claim({ owner: 'r1', now: 99.9, leaseMs: 1_000.5, limit: 10 })).toEqual([]);

    const [claimed] = await store.claim({ owner: 'r1', now: 100.2, leaseMs: 0.5, limit: 10 });
    expect(claimed).toMatchObject({ createdAt: 1, availableAt: 100 });
    expect(await store.reschedule(delayed.id, 'r1', { attempts: 1, availableAt: 250.9, error: { attempt: 1, at: 100.5, error: 'boom' } })).toBe(true);
    expect(await store.stats(249.5)).toMatchObject({ ready: 0, oldestDueAt: 1 });
    expect(await store.stats(250.1)).toMatchObject({ ready: 1 });
  });

  it('refuses a failedBefore that is no time, and a page that is no whole number, before any statement', async () => {
    await expect(store.purgeDeadLetters({ failedBefore: new Date('not a date') })).rejects.toThrow(
      'Dead-letter filter: `failedBefore` must be a valid Date or epoch milliseconds (got Invalid Date)',
    );
    await expect(store.listDeadLetters({ limit: 2.5 })).rejects.toThrow('listDeadLetters(): `limit` must be a whole number of at least 0 (got 2.5)');
    await expect(store.listDeadLetters({ offset: -1 })).rejects.toThrow('`offset` must be a whole number of at least 0 (got -1)');
    expect(recorder.statements).toEqual([]);
  });

  it('refuses a key, id, topic, consumer or owner longer than its 255-character column, before any statement, counting characters as MySQL does', async () => {
    const long = 'k'.repeat(256);
    const refusals: Array<[() => Promise<unknown>, string]> = [
      [() => client.transaction((tx) => store.add(tx, [message('order.placed', long)])), 'MySqlOutboxStore.add(): a key is at most 255 characters on MySQL'],
      [() => client.transaction((tx) => store.add(tx, [{ ...message('order.placed'), id: long }])), 'MySqlOutboxStore.add(): a message id is at most 255'],
      [() => client.transaction((tx) => store.add(tx, [message(long)])), 'MySqlOutboxStore.add(): a topic is at most 255'],
      [() => store.recordInbox(undefined, long, 'm-1', 1), 'MySqlOutboxStore.recordInbox(): a consumer name is at most 255'],
      [() => client.transaction((tx) => store.recordInbox(tx, 'billing', long, 1)), 'MySqlOutboxStore.recordInbox(): a message id is at most 255'],
      [() => store.hasInbox(long, 'm-1'), 'MySqlOutboxStore.hasInbox(): a consumer name is at most 255'],
      [() => store.hasInbox('billing', long), 'MySqlOutboxStore.hasInbox(): a message id is at most 255'],
      [() => store.claim({ owner: long, now: 10, leaseMs: 1_000, limit: 10 }), 'MySqlOutboxStore.claim(): a lease owner is at most 255'],
    ];
    for (const [call, text] of refusals) {
      const error = await call().catch((e: unknown) => e);
      expect(error).toBeInstanceOf(RangeError);
      expect((error as Error).message).toContain(text);
      expect((error as Error).message).toContain(`"${'k'.repeat(40)}..." has 256.`);
    }
    expect(recorder.statements).toEqual([]);

    // 255 characters fit, four-byte ones included (510 UTF-16 units).
    const [ascii, emoji] = ['k'.repeat(255), '😀'.repeat(255)];
    await client.transaction((tx) => store.add(tx, [message('a', ascii), message('b', emoji), { ...message(emoji), id: emoji }]));
    expect((await claimAll()).map((m) => m.key)).toEqual([ascii, emoji, null]);
    expect(await store.recordInbox(undefined, emoji, emoji, 1)).toBe(true);
    expect(await store.hasInbox(emoji, emoji)).toBe(true);
  });

  it("refuses a message id longer than its column in OutboxInbox.process() before the handler runs, which would otherwise run at every redelivery", async () => {
    const storage = new OutboxStorage();
    Object.assign(storage, { logger: silentLogger });
    storage.registerSource({ messages: store, inbox: store });
    const inbox = new OutboxInbox(storage);
    let runs = 0;
    const handle = () => ++runs;

    // hasInbox() refuses it, before process() runs the handler: recordInbox() would refuse it after.
    for (let delivery = 1; delivery <= 2; delivery++) {
      await expect(inbox.process('webhooks:payments', 'm'.repeat(256), handle)).rejects.toThrow(
        'MySqlOutboxStore.hasInbox(): a message id is at most 255 characters on MySQL',
      );
    }
    expect(runs).toBe(0);

    // 255 characters are processed once, then found.
    expect(await inbox.process('webhooks:payments', 'm'.repeat(255), handle)).toEqual({ duplicate: false, result: 1 });
    expect(await inbox.process('webhooks:payments', 'm'.repeat(255), handle)).toEqual({ duplicate: true });
  });

  it('keeps keys, ids, topics and consumers that differ only in case, accents or a trailing space apart', async () => {
    const keys = ['order-a', 'Order-A', 'órder-a', 'order-a '];
    await client.transaction((tx) => store.add(tx, keys.flatMap((key) => [message(`${key}#1`, key), message(`${key}#2`, key)])));
    // One key's head leased: the others, and only they, go on.
    expect((await store.claim({ owner: 'r1', now: 10, leaseMs: 1_000, limit: 1 })).map((m) => m.topic)).toEqual(['order-a#1']);
    expect((await store.claim({ owner: 'r2', now: 10, leaseMs: 1_000, limit: 100 })).map((m) => m.topic)).toEqual([
      'Order-A#1',
      'Order-A#2',
      'órder-a#1',
      'órder-a#2',
      'order-a #1',
      'order-a #2',
    ]);
    expect((await store.stats(10)).ready).toBe(0);

    // Ids in one unique index, topics and keys in the dead letters' filters.
    const ids = ['abc', 'ABC', 'ábc', 'abc '];
    await client.transaction((tx) => store.add(tx, ids.map((id, i) => ({ ...message(['order.placed', 'Order.Placed', 'order.placéd', 'order.placed '][i]!, keys[i]!), id }))));
    const leased = await store.claim({ owner: 'r3', now: 5_000, leaseMs: 1_000, limit: 100 });
    for (const m of leased.filter((m) => ids.includes(m.id))) {
      expect(await store.deadLetter(m.id, 'r3', { attempts: 1, reason: 'rejected', failedAt: 1, error: { attempt: 1, at: 1, error: 'boom' } })).toBe(true);
    }
    expect((await store.listDeadLetters({})).map((d) => d.id).sort()).toEqual([...ids].sort());
    expect((await store.listDeadLetters({ topic: 'Order.Placed' })).map((d) => d.id)).toEqual(['ABC']);
    expect((await store.listDeadLetters({ key: 'order-a ' })).map((d) => d.id)).toEqual(['abc ']);
    expect(await store.getDeadLetter('ábc')).toMatchObject({ id: 'ábc', topic: 'order.placéd', key: 'órder-a' });
    expect(await store.purgeDeadLetters({ key: 'Order-A' })).toBe(1);
    expect(await store.requeueDeadLetters({ topic: 'order.placed' }, 6_000)).toBe(1);

    for (const consumer of ['billing', 'Billing', 'bílling', 'billing ']) {
      expect(await store.recordInbox(undefined, consumer, 'm-1', 1)).toBe(true);
    }
    expect(await store.recordInbox(undefined, 'billing', 'M-1', 1)).toBe(true);
    expect(await store.recordInbox(undefined, 'billing', 'm-1', 2)).toBe(false);
  });

  it('round-trips payloads, headers and failure histories: quotes, non-ASCII, fractions, large whole numbers, nesting, and a null payload as SQL NULL', async () => {
    const payload = {
      text: "O'Reilly \"quoted\" \\ ü 😀 ? $1 :name",
      numbers: [0.1 + 0.2, 1 / 3, 1e-7, 123456789.123, 2 ** 53 - 1, -0.5, 1e21],
      // Doubles MySQL 8's JSON text parser reads 1 ulp off.
      doubles: [0.9999999999999999, 7e-30, 0.12274816974613123, 1.2345678901234567e300, { deep: 0.49355803101514717 }],
      nested: { empty: {}, list: [], deep: [{ a: null }] },
    };
    const [full, empty] = [{ ...message('full'), payload, headers: { 'x-tenant': 'ü-7', 'X-Tenant': 'other' } }, { ...message('empty'), payload: null }];
    await client.transaction((tx) => store.add(tx, [full, empty]));
    expect(await rows(database!.admin, 'SELECT id, payload IS NULL AS missing FROM nest_outbox_messages ORDER BY seq')).toEqual([
      { id: full.id, missing: 0 },
      { id: empty.id, missing: 1 },
    ]);

    const claimed = await claimAll('r1');
    expect(claimed).toEqual([full, empty]);
    const failure = { attempt: 1, at: 1.5e12, error: payload.text, transport: 'kafka' };
    expect(await store.deadLetter(full.id, 'r1', { attempts: 1, reason: 'exhausted', failedAt: 2, error: failure })).toBe(true);
    expect(await store.getDeadLetter(full.id)).toMatchObject({ payload, headers: full.headers, history: [failure], lastError: payload.text });
  });

  it('purges dead letters that failed before a moment with a fraction of a millisecond', async () => {
    const m = message('m');
    await client.transaction((tx) => store.add(tx, [m]));
    await claimAll('r1');
    await store.deadLetter(m.id, 'r1', { attempts: 1, reason: 'rejected', failedAt: 20, error: { attempt: 1, at: 20, error: 'boom' } });

    expect(await store.purgeDeadLetters({ failedBefore: 20 })).toBe(0);
    expect(await store.purgeDeadLetters({ failedBefore: 20.1 })).toBe(1);
  });

  it('refuses to requeue a dead letter whose id a pending message has, moving nothing', async () => {
    const first = message('first');
    await client.transaction((tx) => store.add(tx, [first]));
    await claimAll('r1');
    await store.deadLetter(first.id, 'r1', { attempts: 1, reason: 'rejected', failedAt: 20, error: { attempt: 1, at: 20, error: 'boom' } });

    // The producer chose the id (`add({ id })`) and used it again.
    await client.transaction((tx) => store.add(tx, [{ ...message('again'), id: first.id }]));
    await expect(store.requeueDeadLetters({ all: true }, 30)).rejects.toThrow(
      `MySqlOutboxStore: can't requeue dead letter "${first.id}": a message with that id is pending.`,
    );
    expect(await store.stats(30)).toMatchObject({ pending: 1, deadLetters: 1 });
  });

  it('binds each value to its own placeholder: filters that combine every condition, each dead letter missing one', async () => {
    const target = { ...message('order.placed', 'order-1'), id: 'target' };
    const others = [
      { ...message('order.shipped', 'order-1'), id: 'other-topic' },
      { ...message('order.placed', 'order-2'), id: 'other-key' },
      { ...message('order.placed', 'order-1'), id: 'failed-later' },
      { ...message('order.placed', 'order-1'), id: 'not-named' },
    ];
    await client.transaction((tx) => store.add(tx, [target, ...others]));
    await claimAll('r1');
    for (const m of [target, ...others]) {
      const failedAt = m.id === 'failed-later' ? 90 : 10;
      await store.deadLetter(m.id, 'r1', { attempts: 2, reason: 'rejected', failedAt, error: { attempt: 2, at: failedAt, error: `boom ${m.id}` } });
    }

    expect((await store.listDeadLetters({ topic: 'order.placed', key: 'order-1', limit: 2, offset: 1 })).map((d) => d.id)).toEqual(['target', 'not-named']);
    const filter = { ids: ['target', 'other-topic', 'other-key', 'failed-later'], topic: 'order.placed', key: 'order-1' };
    expect(await store.requeueDeadLetters({ ...filter, failedBefore: 50 }, 60)).toBe(1);
    const [requeued] = await store.claim({ owner: 'r2', now: 60, leaseMs: 1_000, limit: 10 });
    expect(requeued).toMatchObject({ id: 'target', topic: 'order.placed', key: 'order-1', attempts: 0, availableAt: 60, lastError: 'boom target' });
    expect(await store.purgeDeadLetters({ ...filter, failedBefore: 100 })).toBe(1);
    expect((await store.listDeadLetters({})).map((d) => d.id).sort()).toEqual(['not-named', 'other-key', 'other-topic']);
  });

  it('lists dead letters that failed at the same moment by id in byte order', async () => {
    const ms = ['b', 'B', 'a', 'A', 'a-1', 'a1'].map((id) => ({ ...message(`topic-${id}`), id }));
    await client.transaction((tx) => store.add(tx, ms));
    await claimAll('r1');
    for (const m of ms) {
      await store.deadLetter(m.id, 'r1', { attempts: 1, reason: 'rejected', failedAt: 50, error: { attempt: 1, at: 50, error: 'boom' } });
    }

    const byteOrder = ms.map((m) => m.id).sort().reverse();
    expect((await store.listDeadLetters({})).map((d) => d.id)).toEqual(byteOrder);
  });

  describe('on a key of 20,000 messages', () => {
    beforeEach(async () => {
      if (reason) {
        return;
      }
      await client.executor.execute(
        `INSERT INTO nest_outbox_messages (id, topic, headers, \`key\`, created_at, available_at)
SELECT CONCAT('m-', n.i), 'order.placed', JSON_OBJECT(), 'order-1', 0, 0 FROM JSON_TABLE(CAST(? AS JSON), '$[*]' COLUMNS (i int PATH '$')) AS n`,
        [JSON.stringify(Array.from({ length: 20_000 }, (_, i) => i + 1))],
      );
    });

    it("counts the key's backlog as ready in one pass while no relay runs", async () => {
      const started = performance.now();
      expect(await store.stats(10)).toMatchObject({ pending: 20_000, ready: 20_000, leased: 0 });
      // A running count takes milliseconds; a MAX() over the rows before each one took 26 seconds.
      expect(performance.now() - started).toBeLessThan(5_000);

      await store.claim({ owner: 'r1', now: 10, leaseMs: 1_000, limit: 1 });
      expect(await store.stats(10)).toMatchObject({ pending: 20_000, ready: 0, leased: 1 });
    });

    it("claims a batch at the key's head without reading the whole key for each message, and passes over the rest once it's leased", async () => {
      const started = performance.now();
      const batch = await store.claim({ owner: 'r1', now: 10, leaseMs: 1_000, limit: 200 });
      // About 20,000 index reads (each message's older ones); reading the key for each would be 4,000,000.
      expect(performance.now() - started).toBeLessThan(1_000);
      expect(batch.map((m) => m.id)).toEqual(Array.from({ length: 200 }, (_, i) => `m-${i + 1}`));

      expect(await store.claim({ owner: 'r2', now: 10, leaseMs: 1_000, limit: 200 })).toEqual([]);
    });
  });

  it('registers itself for both contracts, given the registry', () => {
    const storage = new OutboxStorage();
    Object.assign(storage, { logger: silentLogger });
    const registered = new MySqlOutboxStore({ executor: client.executor, migrate: false }, storage);
    expect(storage.messages).toBe(registered);
    expect(storage.inbox).toBe(registered);
  });
});
