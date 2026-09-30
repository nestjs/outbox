/**
 * What PostgresOutboxStore does beyond the contract, on PGlite and on PostgreSQL: a batch past PostgreSQL's parameter
 * limit, no lock for messages without a key, times with a fraction of a millisecond, the inputs a column would refuse,
 * a requeue that meets a pending message with the same id, dead letters of one moment in byte order whatever the
 * database's collation, and registering itself for both contracts.
 */
import { randomUUID } from 'node:crypto';
import { OutboxStorage } from '../../lib/index.js';
import { PostgresOutboxStore, type SqlExecutor, type SqlTransaction } from '../../lib/postgres/index.js';
import { silentLogger } from '../helpers.js';
import { message, openPglite, pgClient, testDatabase, truncate, type Client } from './support.js';

const { database, reason } = await testDatabase('store_behaviour');

/** `executor`, recording the statements it runs, its transactions' and the application's included. */
function recording(executor: SqlExecutor): { executor: SqlExecutor; statements: string[] } {
  const statements: string[] = [];
  const record = (tx: SqlTransaction): SqlTransaction => ({
    query: (text, params) => {
      statements.push(text);
      return tx.query(text, params);
    },
  });
  return {
    statements,
    executor: {
      dialect: executor.dialect,
      query: (text, params) => {
        statements.push(text);
        return executor.query(text, params);
      },
      transaction: (work, options) => executor.transaction((tx) => work(record(tx)), options),
      wrapTransaction: (transaction) => record(executor.wrapTransaction(transaction)),
    },
  };
}

const targets = [
  { name: 'fromDrizzle (PGlite)', open: openPglite, skip: undefined },
  { name: `${pgClient.name} on PostgreSQL`, open: () => pgClient.open(database!.url), skip: reason },
];

describe.each(targets)('PostgresOutboxStore through $name', ({ open, skip }) => {
  let client: Client;
  let recorder: ReturnType<typeof recording>;
  let store: PostgresOutboxStore;

  beforeAll(async () => {
    if (skip) {
      return;
    }

    client = await open();
    recorder = recording(client.executor);
    store = new PostgresOutboxStore({ executor: recorder.executor });
    await store.onModuleInit();
  });
  afterAll(async () => {
    await client?.close();
  });

  beforeEach((context) => {
    if (skip) {
      context.skip(skip);
    }
  });
  beforeEach(async () => {
    if (!skip) {
      await truncate(client.executor, 'nest_outbox');
      recorder.statements.length = 0;
    }
  });

  const claimAll = (owner: string = randomUUID()) => store.claim({ owner, now: 10, leaseMs: 1_000, limit: 100_000 });

  it("adds a batch past PostgreSQL's 65,535 parameters, in one statement, in the batch's order", async () => {
    const batch = Array.from({ length: 12_000 }, (_, i) => message(`m${i}`));
    await client.transaction((tx) => store.add(tx, batch));

    expect(recorder.statements.filter((text) => text.startsWith('INSERT'))).toHaveLength(1);
    expect((await claimAll()).map((m) => m.topic)).toEqual(batch.map((m) => m.topic));
  });

  it('takes a lock per key, in one statement, and none for messages without a key', async () => {
    await client.transaction((tx) => store.add(tx, [message('a'), message('b')]));
    expect(recorder.statements.filter((text) => text.includes('pg_advisory_xact_lock'))).toEqual([]);

    await client.transaction((tx) => store.add(tx, [message('c', 'K'), message('d', 'L'), message('e', 'K')]));
    expect(recorder.statements.filter((text) => text.includes('pg_advisory_xact_lock'))).toHaveLength(1);
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
      `PostgresOutboxStore: can't requeue dead letter "${first.id}": a message with that id is pending.`,
    );
    expect(await store.stats(30)).toMatchObject({ pending: 1, deadLetters: 1 });
  });

  it("lists dead letters that failed at the same moment by id in byte order, whatever the database's collation", async () => {
    const ms = ['b', 'B', 'a', 'A', 'a-1', 'a1'].map((id) => ({ ...message(`topic-${id}`), id }));
    await client.transaction((tx) => store.add(tx, ms));
    await claimAll('r1');
    for (const m of ms) {
      await store.deadLetter(m.id, 'r1', { attempts: 1, reason: 'rejected', failedAt: 50, error: { attempt: 1, at: 50, error: 'boom' } });
    }

    const byteOrder = ms.map((m) => m.id).sort().reverse();
    expect((await store.listDeadLetters({})).map((d) => d.id)).toEqual(byteOrder);
  });

  it("counts a key's backlog as ready in one pass: 20,000 messages of one key while no relay runs", async () => {
    await client.executor.query(`INSERT INTO nest_outbox.messages (id, topic, headers, key, created_at, available_at)
      SELECT 'm-' || i, 'order.placed', '{}', 'order-1', 0, 0 FROM generate_series(1, 20000) AS i`);
    const started = performance.now();
    expect(await store.stats(10)).toMatchObject({ pending: 20_000, ready: 20_000, leased: 0 });
    // One pass takes milliseconds; checking each message's older ones takes minutes.
    expect(performance.now() - started).toBeLessThan(5_000);

    await store.claim({ owner: 'r1', now: 10, leaseMs: 1_000, limit: 1 });
    expect(await store.stats(10)).toMatchObject({ pending: 20_000, ready: 0, leased: 1 });
  });

  it('registers itself for both contracts, given the registry', () => {
    const storage = new OutboxStorage();
    Object.assign(storage, { logger: silentLogger });
    const registered = new PostgresOutboxStore({ executor: client.executor, migrate: false }, storage);
    expect(storage.messages).toBe(registered);
    expect(storage.inbox).toBe(registered);
  });
});
