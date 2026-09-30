/**
 * MySqlOutboxStore's races on MySQL, beyond the contract suites': many producers of one key whose transactions overlap,
 * through every client, at REPEATABLE READ and READ COMMITTED, published in the order they commit; concurrent claims
 * through every client taking every claimable message between them, a key's run with one owner; a producer's
 * rollback, which burns an AUTO_INCREMENT value the relay never waits for; a claim that meets a key whose head another
 * connection is writing; keys that share a lock bucket (their producers take turns: the false contention, measured),
 * ordered all the same; the lock table bounded by the buckets while producers churn through many more keys; and a
 * deadlock in the application's transaction, which reaches the application as MySQL's error 1213.
 */
import { randomUUID } from 'node:crypto';
import mysql from 'mysql2/promise';
import { mysqlErrorCode } from '@nestjs/store-kit/mysql';
import { fromMysql2, MySqlOutboxStore } from '../../lib/mysql/index.js';
import { KEY_BUCKETS, keyBucket } from '../../lib/mysql/mysql-outbox-keys.util.js';
import { clients, message, mysql2Client, onMysql, POOL_SIZE, rows, testDatabase, truncate, type Client, type ClientFactory } from './support.js';

const { database, reason } = await testDatabase('races');

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((res) => (resolve = res));
  return { promise, resolve };
}

/** Opens `factory` on this file's database, with a store migrated under `schema`, for the tests of a describe. */
function useStore(factory: ClientFactory, schema: string) {
  const opened = {} as { client: Client; store: MySqlOutboxStore };
  beforeAll(async () => {
    if (reason) {
      return;
    }

    opened.client = await factory.open(database!.url);
    opened.store = new MySqlOutboxStore({ executor: opened.client.executor, schema });
    await opened.store.onModuleInit();
  });
  afterAll(async () => {
    await opened.client?.close();
  });
  beforeEach(async () => {
    if (!reason) {
      await truncate(opened.client.executor, schema);
    }
  });
  return opened;
}

/** The seq each message of `ids` has now. */
async function seqs(schema: string, ids: string[]): Promise<Map<string, number>> {
  const found = await rows<{ id: string; seq: number }>(database!.admin, `SELECT id, seq FROM ${schema}_messages WHERE id IN (?)`, [ids]);
  return new Map(found.map((row) => [row.id, Number(row.seq)]));
}

/** A key whose bucket is `key`'s, and one whose isn't. */
function collidingKey(key: string): string {
  for (let i = 0; ; i++) {
    const candidate = `${key}~${i}`;
    if (keyBucket(candidate) === keyBucket(key)) {
      return candidate;
    }
  }
}

describe.each(clients.map((factory, i) => ({ factory, schema: `race_${i}` })))('$factory.name', ({ factory, schema }) => {
  onMysql(reason);
  const opened = useStore(factory, schema);

  // add() doesn't rely on the application's isolation level: the bucket's lock row and AUTO_INCREMENT keep commit order.
  it.each(['repeatable read', 'read committed'] as const)('publishes a key in the order its producers commit, however their transactions overlap (%s)', async (isolation) => {
    const { client, store } = opened;
    const producers = 12;
    // The seq of each message as the relay publishes it: a producer that committed later must never have a smaller one.
    const published: number[] = [];
    let producing = true;

    // The relay on a connection of its own, as another instance's, so it claims between the producers' commits rather
    // than waiting for their connections.
    const relayPool = mysql.createPool({ uri: database!.url, connectionLimit: 1 });
    const relayStore = new MySqlOutboxStore({ executor: fromMysql2(relayPool), schema, migrate: false });
    const relay = (async () => {
      while (producing || (await relayStore.stats(10)).pending > 0) {
        const owner = randomUUID();
        const batch = await relayStore.claim({ owner, now: 10, leaseMs: 60_000, limit: 5 });
        for (const m of batch) {
          published.push((await seqs(schema, [m.id])).get(m.id)!);
          expect(await relayStore.markPublished(m.id, owner)).toBe(true);
        }
        if (batch.length === 0) {
          await sleep(2);
        }
      }
    })().finally(() => relayPool.end());

    await Promise.all(
      Array.from({ length: producers }, (_, i) =>
        client.transaction(async (tx) => {
          await store.add(tx, [message(`step-${i}`, 'order-1')]);
          // The earlier producers hold their transactions longest: without the key's lock, later inserts commit first.
          await sleep((producers - i) * 3 + Math.random() * 5);
        }, isolation),
      ),
    );
    producing = false;
    await relay;

    expect(published).toHaveLength(producers);
    expect(published).toEqual([...published].sort((a, b) => a - b));
  });

  it('lets concurrent claims take every claimable message between them: none twice, none left out, each key with one owner', async () => {
    const { client, store } = opened;
    // 20 messages without a key, then four keys of 5 messages each: four claims of 10 can take all 40.
    const keys = ['a', 'b', 'c', 'd'];
    const messages = [
      ...Array.from({ length: 20 }, (_, i) => message(`free-${i}`)),
      ...keys.flatMap((key) => Array.from({ length: 5 }, (_, i) => message(`${key}-${i}`, key))),
    ];
    await client.transaction((tx) => store.add(tx, messages));

    const batches = await Promise.all(['r1', 'r2', 'r3', 'r4'].map((owner) => store.claim({ owner, now: 10, leaseMs: 60_000, limit: 10 })));
    expect(batches.map((batch) => batch.length)).toEqual([10, 10, 10, 10]);
    expect(batches.flat().map((m) => m.id).sort()).toEqual(messages.map((m) => m.id).sort());
    for (const key of keys) {
      const holders = batches.filter((batch) => batch.some((m) => m.key === key));
      expect(holders).toHaveLength(1);
      expect(holders[0]!.filter((m) => m.key === key).map((m) => m.topic)).toEqual([0, 1, 2, 3, 4].map((i) => `${key}-${i}`));
    }
  });
});

describe(`${mysql2Client.name}: a producer that rolls back`, () => {
  onMysql(reason);
  const opened = useStore(mysql2Client, 'race_rollback');

  it('burns a seq the relay never waits for: the next message of the key is published at once', async () => {
    const { client, store } = opened;
    const first = message('first', 'order-1');
    await client.transaction((tx) => store.add(tx, [first]));
    await expect(
      client.transaction(async (tx) => {
        await store.add(tx, [message('rolled-back', 'order-1')]);
        throw new Error('payment declined');
      }),
    ).rejects.toThrow('payment declined');
    const third = message('third', 'order-1');
    await client.transaction((tx) => store.add(tx, [third]));

    const numbered = await seqs('race_rollback', [first.id, third.id]);
    expect(numbered.get(third.id)! - numbered.get(first.id)!).toBeGreaterThan(1);
    expect((await store.claim({ owner: 'r1', now: 10, leaseMs: 1_000, limit: 10 })).map((m) => m.topic)).toEqual(['first', 'third']);
  });

  it('lets the next producer of the key go once it rolls back, and the relay publishes that one without waiting for the gap', async () => {
    const { client, store } = opened;
    const added = deferred();
    const finish = deferred();
    const t1 = client
      .transaction(async (tx) => {
        await store.add(tx, [message('rolled-back', 'order-1')]);
        added.resolve();
        await finish.promise;
        throw new Error('payment declined');
      })
      .catch(() => undefined);
    await added.promise;

    let committed = false;
    const t2 = client.transaction((tx) => store.add(tx, [message('next', 'order-1')])).then(() => (committed = true));
    await sleep(200);
    expect(committed).toBe(false); // waiting for the key's lock
    expect(await store.claim({ owner: 'r1', now: 10, leaseMs: 1_000, limit: 10 })).toEqual([]);

    finish.resolve();
    await Promise.all([t1, t2]);
    expect((await store.claim({ owner: 'r2', now: 10, leaseMs: 1_000, limit: 10 })).map((m) => m.topic)).toEqual(['next']);
  });
});

describe(`${mysql2Client.name}: a claim and a write in flight`, () => {
  onMysql(reason);
  const opened = useStore(mysql2Client, 'race_claim');

  it('leaves a key whole while another connection writes its head, whose lease expired: the rest waits behind it', async () => {
    const { client, store } = opened;
    const [head, tail, other] = [message('head', 'K'), message('tail', 'K'), message('other', 'L')];
    await client.transaction((tx) => store.add(tx, [head, tail, other]));
    // r1 took the head with a lease that expired at 20: a stale relay, now writing it (a reschedule, a delete).
    expect((await store.claim({ owner: 'r1', now: 10, leaseMs: 10, limit: 1 })).map((m) => m.topic)).toEqual(['head']);

    const writer = await database!.admin.getConnection();
    try {
      await writer.beginTransaction();
      await writer.query('SELECT 1 FROM race_claim_messages WHERE id = ? FOR UPDATE', [head.id]);
      const batch = await store.claim({ owner: 'r2', now: 1_000, leaseMs: 1_000, limit: 10 });
      expect(batch.map((m) => m.topic)).toEqual(['other']);
    } finally {
      await writer.rollback();
      writer.release();
    }

    expect((await store.claim({ owner: 'r3', now: 1_000, leaseMs: 1_000, limit: 10 })).map((m) => m.topic)).toEqual(['head', 'tail']);
  });
});

describe(`${mysql2Client.name}: keys that share a lock bucket`, () => {
  onMysql(reason);
  const opened = useStore(mysql2Client, 'race_buckets');

  /** How long a producer of `key` waits while another transaction holds a message of `held` for `holdMs`. */
  async function waitBehind(held: string, key: string, holdMs: number): Promise<number> {
    const { client, store } = opened;
    const locked = deferred();
    const holder = client.transaction(async (tx) => {
      await store.add(tx, [message('held', held)]);
      locked.resolve();
      await sleep(holdMs);
    });
    await locked.promise;
    const started = performance.now();
    await client.transaction((tx) => store.add(tx, [message('waiting', key)]));
    const waited = performance.now() - started;
    await holder;
    return waited;
  }

  it('makes the producer of one wait for a transaction that added the other (the false contention), and no other key', async () => {
    const shared = collidingKey('order-a');
    expect(keyBucket(shared)).toBe(keyBucket('order-a'));
    const apart = ['order-b', 'order-c'].find((key) => keyBucket(key) !== keyBucket('order-a'))!;

    expect(await waitBehind('order-a', shared, 500)).toBeGreaterThan(350);
    expect(await waitBehind('order-a', apart, 500)).toBeLessThan(250);
    // Two keys share a bucket with a chance of 1 in 16,384: with c producers at once, a producer waits for an unrelated
    // one with a chance of about (c - 1) / 16,384 (0.3% for 50), for the rest of that one's transaction.
    expect(KEY_BUCKETS).toBe(16_384);
  });

  it('publishes each of two keys of one bucket in commit order while their producers race', async () => {
    const { client, store } = opened;
    const keys = ['order-a', collidingKey('order-a')];
    await Promise.all(
      Array.from({ length: 16 }, (_, i) =>
        client.transaction(async (tx) => {
          await store.add(tx, [message(`${keys[i % 2]}#${i}`, keys[i % 2]!)]);
          await sleep((16 - i) * 2);
        }),
      ),
    );

    const batch = await store.claim({ owner: 'r1', now: 10, leaseMs: 1_000, limit: 100 });
    const numbered = await seqs('race_buckets', batch.map((m) => m.id));
    for (const key of keys) {
      const order = batch.filter((m) => m.key === key).map((m) => numbered.get(m.id)!);
      expect(order).toHaveLength(8);
      expect(order).toEqual([...order].sort((a, b) => a - b));
    }
  });
});

describe(`${mysql2Client.name}: the lock rows`, () => {
  onMysql(reason);
  const opened = useStore(mysql2Client, 'race_bounded');

  it('stay one per bucket while producers churn through many more keys, taking the buckets in one order', async () => {
    const { client, store } = opened;
    const keys = Array.from({ length: 40_000 }, (_, i) => `customer-${i}`);
    // Four producers at once, each adding 1,000 keys a transaction: overlapping buckets, taken in the lock rows' order.
    const producers = Array.from({ length: 4 }, async (_, p) => {
      for (let start = p * 1_000; start < keys.length; start += 4_000) {
        await client.transaction((tx) => store.add(tx, keys.slice(start, start + 1_000).map((key) => message('customer.updated', key))));
      }
    });
    await Promise.all(producers);
    await store.claim({ owner: 'r1', now: 10, leaseMs: 1_000, limit: 1 });

    const buckets = new Set(keys.map((key) => keyBucket(key))).size;
    const [{ n }] = await rows<{ n: number }>(database!.admin, 'SELECT COUNT(*) AS n FROM race_bounded_locks');
    // A row per bucket the keys fell in, and the claim's.
    expect(n).toBe(buckets + 1);
    expect(n).toBeLessThanOrEqual(KEY_BUCKETS + 1);
    expect(n).toBeLessThan(keys.length / 2);
    expect((await store.stats(10)).pending).toBe(keys.length);
  });
});

describe(`${mysql2Client.name}: a deadlock in the application's transaction`, () => {
  onMysql(reason);
  const opened = useStore(mysql2Client, 'race_deadlock');

  /**
   * How many of the transactions of these MySQL connections wait for a lock. InnoDB serves INNODB_TRX from a cache it
   * refreshes only once it hasn't been read for 0.1 s: ask less often than that.
   */
  async function waiting(threads: number[]): Promise<number> {
    await sleep(200);
    const [{ n }] = await rows<{ n: number }>(
      database!.admin,
      "SELECT COUNT(*) AS n FROM information_schema.INNODB_TRX WHERE trx_state = 'LOCK WAIT' AND trx_mysql_thread_id IN (?)",
      [threads],
    );
    return n;
  }

  it("reaches the application as MySQL's error 1213: its transaction was rolled back, and running it again adds the message", async () => {
    const { client, store } = opened;
    // A bucket's lock row doesn't exist before its first message. The first producer creates it and rolls back while
    // others wait for it: MySQL breaks the lock waits it leaves behind by rolling some of them back.
    const key = `fresh-${randomUUID()}`;
    const pool = client.root as mysql.Pool;
    const connections = await Promise.all(Array.from({ length: POOL_SIZE }, () => pool.getConnection()));
    let settled: Array<string | number>;
    try {
      const threads = await Promise.all(connections.map(async (c) => ((await c.query('SELECT CONNECTION_ID() AS id'))[0] as Array<{ id: number }>)[0]!.id));
      const [creator, ...others] = connections;
      await creator!.beginTransaction();
      await store.add(creator, [message('rolled-back', key)]);
      const outcomes = others.map(async (connection, i) => {
        await connection.beginTransaction();
        try {
          await store.add(connection, [message(`waiter-${i}`, key)]);
          await connection.commit();
          return 'committed';
        } catch (error) {
          await connection.rollback();
          return mysqlErrorCode(error) ?? (error as Error).message;
        }
      });
      while ((await waiting(threads.slice(1))) < others.length) {
        // The others are on their way to the bucket's row.
      }
      await creator!.rollback();
      settled = await Promise.all(outcomes);
    } finally {
      for (const connection of connections) {
        connection.release();
      }
    }

    expect(settled.every((outcome) => outcome === 'committed' || outcome === 1213)).toBe(true);
    expect(settled).toContain(1213);
    // What the application does with a deadlock: runs its transaction again.
    for (const [i, outcome] of settled.entries()) {
      if (outcome === 1213) {
        await client.transaction((tx) => store.add(tx, [message(`waiter-${i}`, key)]));
      }
    }
    const claimed = await store.claim({ owner: 'r1', now: 10, leaseMs: 1_000, limit: 10 });
    expect(claimed.map((m) => m.topic).sort()).toEqual(Array.from({ length: POOL_SIZE - 1 }, (_, i) => `waiter-${i}`));
  });
});
