/**
 * PostgresOutboxStore's races on PostgreSQL, beyond the contract suites': many producers of one key whose transactions
 * overlap, through every client, published in the order they commit; a claim that meets a key whose head another
 * connection is writing, which leaves the rest of the key behind it; and producers whose keys share a lock number,
 * forced into the turns that deadlock when locks are taken in the keys' order.
 */
import { randomUUID } from 'node:crypto';
import type pg from 'pg';
import { PostgresOutboxStore } from '../../lib/postgres/index.js';
import { clients, message, pgClient, testDatabase, truncate, type Client } from './support.js';

const { database, reason } = await testDatabase('store_races');

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((res) => (resolve = res));
  return { promise, resolve };
}

const onPostgres = () =>
  beforeEach((context) => {
    if (reason) {
      context.skip(reason);
    }
  });

/** Opens `factory` on this file's database, with a store migrated in `schema`, for the tests of a describe. */
function useStore(factory: typeof pgClient, schema: string) {
  const opened = {} as { client: Client; store: PostgresOutboxStore };
  beforeAll(async () => {
    if (reason) {
      return;
    }

    opened.client = await factory.open(database!.url);
    opened.store = new PostgresOutboxStore({ executor: opened.client.executor, schema });
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

describe.each(clients.map((factory, i) => ({ factory, schema: `race_${i}` })))('$factory.name', ({ factory, schema }) => {
  onPostgres();
  const opened = useStore(factory, schema);

  it('publishes a key in the order its producers commit, however their transactions overlap', async () => {
    const { client, store } = opened;
    const producers = 12;
    // The seq of each message as the relay publishes it: a producer that committed later must never have a smaller one.
    const published: number[] = [];
    let producing = true;

    const relay = (async () => {
      while (producing || (await store.stats(10)).pending > 0) {
        const owner = randomUUID();
        const batch = await store.claim({ owner, now: 10, leaseMs: 60_000, limit: 5 });
        for (const m of batch) {
          const { rows } = await database!.admin.query(`SELECT seq::text AS seq FROM "${schema}".messages WHERE id = $1`, [m.id]);
          published.push(Number(rows[0].seq));
          expect(await store.markPublished(m.id, owner)).toBe(true);
        }
        if (batch.length === 0) {
          await sleep(2);
        }
      }
    })();

    await Promise.all(
      Array.from({ length: producers }, (_, i) =>
        client.transaction(async (tx) => {
          await store.add(tx, [message(`step-${i}`, 'order-1')]);
          // The earlier producers hold their transactions longest: without the key's lock, later inserts commit first.
          await sleep((producers - i) * 3 + Math.random() * 5);
        }),
      ),
    );
    producing = false;
    await relay;

    expect(published).toHaveLength(producers);
    expect(published).toEqual([...published].sort((a, b) => a - b));
  });
});

describe(`${pgClient.name}: a claim and a write in flight`, () => {
  onPostgres();
  const opened = useStore(pgClient, 'race_claim');

  it("leaves a key whole while another connection writes its head, whose lease expired: the rest waits behind it", async () => {
    const { client, store } = opened;
    const [head, tail, other] = [message('head', 'K'), message('tail', 'K'), message('other', 'L')];
    await client.transaction((tx) => store.add(tx, [head, tail, other]));
    // r1 took the head with a lease that expired at 20: a stale relay, now writing it (a reschedule, a delete).
    expect((await store.claim({ owner: 'r1', now: 10, leaseMs: 10, limit: 1 })).map((m) => m.topic)).toEqual(['head']);

    const writer = await database!.admin.connect();
    try {
      await writer.query('BEGIN');
      await writer.query('SELECT 1 FROM race_claim.messages WHERE id = $1 FOR UPDATE', [head.id]);
      const batch = await store.claim({ owner: 'r2', now: 1_000, leaseMs: 1_000, limit: 10 });
      expect(batch.map((m) => m.topic)).toEqual(['other']);
    } finally {
      await writer.query('ROLLBACK');
      writer.release();
    }

    expect((await store.claim({ owner: 'r3', now: 1_000, leaseMs: 1_000, limit: 10 })).map((m) => m.topic)).toEqual(['head', 'tail']);
  });
});

describe(`${pgClient.name}: keys that share a lock number`, () => {
  onPostgres();
  const opened = useStore(pgClient, 'race_locks');

  /** Two keys whose lock numbers (`hashtext()`) collide, in JavaScript's order. */
  async function collidingKeys(): Promise<[string, string]> {
    const { rows } = await database!.admin.query<{ a: string; b: string }>(
      `SELECT min(k) AS a, max(k) AS b FROM (SELECT 'order-' || i AS k FROM generate_series(1, 500000) AS i) AS keys
       GROUP BY hashtext(k) HAVING count(*) > 1 LIMIT 1`,
    );
    const { rows: check } = await database!.admin.query('SELECT hashtext($1) = hashtext($2) AS same', [rows[0]!.a, rows[0]!.b]);
    expect(check[0].same).toBe(true);
    return [rows[0]!.a, rows[0]!.b].sort() as [string, string];
  }

  /** The application's client as fromPg() takes it, pausing after its first advisory lock until `gate` opens. */
  function pausing(connection: pg.PoolClient, gate: Promise<void>, locked: () => void) {
    let paused = false;
    return {
      connect: connection.connect.bind(connection),
      async query(text: string, params?: unknown[]) {
        const result = await connection.query(text, params);
        if (!paused && text.includes('pg_advisory_xact_lock')) {
          paused = true;
          locked();
          await gate;
        }
        return result;
      },
    };
  }

  it('never deadlocks two producers that lock them in the turns that would deadlock in the keys\' order', async () => {
    const { client, store } = opened;
    const [x, z] = await collidingKeys();
    // In the keys' order, T1 would lock x then y, and T2 y then z: T1 holds x's number and waits for y, T2 holds y and
    // waits for z's number, which is x's.
    const y = `${x}!`;
    expect([x, y, z]).toEqual([x, y, z].sort());

    const pool = client.root as pg.Pool;
    const produce = async (keys: string[], gate: Promise<void>, locked: () => void) => {
      const connection = await pool.connect();
      try {
        await connection.query('BEGIN');
        await store.add(
          pausing(connection, gate, locked),
          keys.map((key) => message(key, key)),
        );
        await connection.query('COMMIT');
      } catch (error) {
        await connection.query('ROLLBACK');
        throw error;
      } finally {
        connection.release();
      }
    };

    const [t1Locked, t2Locked, t1Gate, t2Gate] = [deferred(), deferred(), deferred(), deferred()];
    const t1 = produce([x, y], t1Gate.promise, t1Locked.resolve);
    await Promise.race([t1Locked.promise, t1]);
    const t2 = produce([y, z], t2Gate.promise, t2Locked.resolve);
    // T2 holds its first lock, or (locks taken in their numbers' order) waits for T1's.
    await Promise.race([t2Locked.promise, sleep(300)]);
    t1Gate.resolve();
    t2Gate.resolve();
    await Promise.all([t1, t2]);

    const batch = await store.claim({ owner: 'r1', now: 10, leaseMs: 1_000, limit: 10 });
    expect(batch.map((m) => m.topic).sort()).toEqual([x, y, y, z].sort());
  });
});
