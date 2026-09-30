/**
 * `add()` and `recordInbox()` through PostgresOutboxStore with each client's own transaction object (node-postgres's
 * client after BEGIN, Drizzle's `tx`, TypeORM's `EntityManager`, Prisma's transaction client, Kysely's `trx`): messages
 * and inbox records commit with the application's row, unseen by other connections until then, or roll back with it.
 * The database, the pool or the client passed as the transaction is refused with OutboxTransactionRequiredError (the
 * kit's TypeError as its cause), writing nothing; `recordInbox()` without a transaction writes at once.
 */
import { randomUUID } from 'node:crypto';
import type pg from 'pg';
import { OutboxTransactionRequiredError } from '../../lib/index.js';
import { PostgresOutboxStore } from '../../lib/postgres/index.js';
import { clients, message, openPglite, pgClient, testDatabase, truncate, type Client } from './support.js';

const { database, reason } = await testDatabase('store_transactions');

const targets = [
  ...clients.map((factory, i) => ({
    name: `${factory.name} on PostgreSQL`,
    schema: `tx_${i}`,
    open: () => factory.open(database!.url),
    postgres: true,
    skip: reason,
  })),
  { name: 'fromDrizzle (PGlite)', schema: 'tx_pglite', open: openPglite, postgres: false, skip: undefined },
];

describe.each(targets)('$name', ({ schema, open, postgres, skip }) => {
  let client: Client;
  let store: PostgresOutboxStore;

  beforeAll(async () => {
    if (skip) {
      return;
    }

    client = await open();
    store = new PostgresOutboxStore({ executor: client.executor, schema });
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
      await truncate(client.executor, schema);
      await client.executor.query('DELETE FROM orders');
    }
  });

  const orders = async (id: string) => (await client.executor.query<{ id: string }>('SELECT id::text AS id FROM orders WHERE id = $1::uuid', [id])).length;
  const pending = async () => (await store.stats(1)).pending;
  const claimed = async () => (await store.claim({ owner: randomUUID(), now: 1, leaseMs: 1_000, limit: 100 })).map((m) => m.id);

  it("add() in the application's transaction commits with its row, and no other connection sees it before", async () => {
    const id = randomUUID();
    const added = message('order.placed', `order-${id}`);
    await client.transaction(async (tx) => {
      await client.insertOrder(tx, id);
      await store.add(tx, [added]);
      if (postgres) {
        expect(await database!.admin.query(`SELECT id FROM "${schema}".messages`)).toMatchObject({ rowCount: 0 });
        expect(await pending()).toBe(0);
      }
    });

    expect(await orders(id)).toBe(1);
    expect(await claimed()).toEqual([added.id]);
  });

  it("a rolled-back add() leaves neither the application's row nor its messages", async () => {
    const id = randomUUID();
    await expect(
      client.transaction(async (tx) => {
        await client.insertOrder(tx, id);
        await store.add(tx, [message('order.placed', `order-${id}`), message('order.audited')]);
        throw new Error('payment declined');
      }),
    ).rejects.toThrow('payment declined');

    expect(await orders(id)).toBe(0);
    expect(await pending()).toBe(0);
    expect(await claimed()).toEqual([]);
  });

  it('add() in a REPEATABLE READ transaction commits with its row, and rolls back with it', async () => {
    const [committed, rolledBack] = [randomUUID(), randomUUID()];
    const added = message('order.placed', `order-${committed}`);
    await client.transaction(async (tx) => {
      await client.insertOrder(tx, committed);
      await store.add(tx, [added]);
    }, 'repeatable read');
    await expect(
      client.transaction(async (tx) => {
        await client.insertOrder(tx, rolledBack);
        await store.add(tx, [message('order.placed', `order-${rolledBack}`)]);
        throw new Error('payment declined');
      }, 'repeatable read'),
    ).rejects.toThrow('payment declined');

    expect([await orders(committed), await orders(rolledBack)]).toEqual([1, 0]);
    expect(await claimed()).toEqual([added.id]);
  });

  it("recordInbox() in the application's transaction commits with its row; a later delivery is a duplicate", async () => {
    const id = randomUUID();
    await client.transaction(async (tx) => {
      await client.insertOrder(tx, id);
      expect(await store.recordInbox(tx, 'billing', 'm-1', 1)).toBe(true);
      expect(await store.recordInbox(tx, 'billing', 'm-1', 1)).toBe(false);
      if (postgres) {
        expect(await store.hasInbox('billing', 'm-1')).toBe(false);
      }
    });

    expect(await orders(id)).toBe(1);
    expect(await store.hasInbox('billing', 'm-1')).toBe(true);
    expect(await client.transaction((tx) => store.recordInbox(tx, 'billing', 'm-1', 2))).toBe(false);
  });

  it("a rolled-back recordInbox() leaves neither the application's row nor the record, so the redelivery runs", async () => {
    const id = randomUUID();
    await expect(
      client.transaction(async (tx) => {
        await client.insertOrder(tx, id);
        expect(await store.recordInbox(tx, 'billing', 'm-2', 1)).toBe(true);
        throw new Error('handler failed');
      }),
    ).rejects.toThrow('handler failed');

    expect(await orders(id)).toBe(0);
    expect(await store.hasInbox('billing', 'm-2')).toBe(false);
    expect(await client.transaction((tx) => store.recordInbox(tx, 'billing', 'm-2', 2))).toBe(true);
  });

  it("recordInbox() without a transaction writes at once, on the store's own connection", async () => {
    expect(await store.recordInbox(undefined, 'billing', 'm-3', 1)).toBe(true);
    expect(await store.hasInbox('billing', 'm-3')).toBe(true);
    expect(await store.recordInbox(undefined, 'billing', 'm-3', 2)).toBe(false);
  });

  it('refuses the database, the pool or the client as the transaction, writing nothing, the reason in the error', async () => {
    const error = await store.add(client.root, [message('order.placed')]).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(OutboxTransactionRequiredError);
    const { message: text, cause } = error as OutboxTransactionRequiredError;
    expect(cause).toBeInstanceOf(TypeError);
    expect(text).toBe(
      "This call must run inside the caller's transaction: pass the transaction handle as the first argument, between BEGIN and COMMIT. " +
        `PostgresOutboxStore.add(): ${(cause as TypeError).message}`,
    );

    await expect(store.add(client.root, [])).rejects.toThrow(OutboxTransactionRequiredError);
    await expect(store.recordInbox(client.root, 'billing', 'm-4', 1)).rejects.toThrow(OutboxTransactionRequiredError);
    await expect(store.recordInbox(null, 'billing', 'm-4', 1)).rejects.toThrow('PostgresOutboxStore.recordInbox(): ');
    expect(await pending()).toBe(0);
    expect(await store.hasInbox('billing', 'm-4')).toBe(false);
  });
});

describe(`${pgClient.name} on PostgreSQL: a client before BEGIN`, () => {
  let client: Client;
  let store: PostgresOutboxStore;

  beforeAll(async () => {
    if (!reason) {
      client = await pgClient.open(database!.url);
      store = new PostgresOutboxStore({ executor: client.executor, schema: 'tx_begin' });
      await store.onModuleInit();
    }
  });
  afterAll(async () => {
    await client?.close();
  });
  beforeEach((context) => {
    if (reason) {
      context.skip(reason);
    }
  });

  it('is refused: each statement would commit on its own', async () => {
    const connection = await (client.root as pg.Pool).connect();
    try {
      const error = await store.add(connection, [message('order.placed')]).catch((e: unknown) => e);
      expect(error).toBeInstanceOf(OutboxTransactionRequiredError);
      expect((error as Error).message).toContain("PostgresOutboxStore.add(): The node-postgres client isn't in a transaction: send BEGIN on it first");
    } finally {
      connection.release();
    }
    expect((await store.stats(1)).pending).toBe(0);
  });
});
