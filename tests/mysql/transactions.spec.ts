/**
 * `add()` and `recordInbox()` through MySqlOutboxStore with each client's own transaction object (a mysql2 connection
 * after beginTransaction(), Drizzle's `tx`, TypeORM's `EntityManager`, Prisma's transaction client, Kysely's `trx`), at
 * REPEATABLE READ (MySQL's default) and READ COMMITTED: messages and inbox records commit with the application's row,
 * unseen by other connections until then, or roll back with it. The database, the pool or the client passed as the
 * transaction is refused with OutboxTransactionRequiredError (the kit's TypeError as its cause), writing nothing, and so
 * is a mysql2 connection outside a transaction; `recordInbox()` without a transaction writes at once.
 */
import { randomUUID } from 'node:crypto';
import type mysql from 'mysql2/promise';
import { OutboxTransactionRequiredError } from '../../lib/index.js';
import { MySqlOutboxStore } from '../../lib/mysql/index.js';
import { clients, message, mysql2Client, onMysql, rows, testDatabase, truncate, type Client } from './support.js';

const { database, reason } = await testDatabase('transactions');

const isolations = ['repeatable read', 'read committed'] as const;

describe.each(clients.map((factory, i) => ({ factory, schema: `tx_${i}` })))('$factory.name on MySQL', ({ factory, schema }) => {
  onMysql(reason);
  let client: Client;
  let store: MySqlOutboxStore;

  beforeAll(async () => {
    if (reason) {
      return;
    }

    client = await factory.open(database!.url);
    store = new MySqlOutboxStore({ executor: client.executor, schema });
    await store.onModuleInit();
  });
  afterAll(async () => {
    await client?.close();
  });
  beforeEach(async () => {
    if (!reason) {
      await truncate(client.executor, schema);
      await client.executor.execute('DELETE FROM orders');
    }
  });

  const orders = async (id: string) => (await rows(database!.admin, 'SELECT id FROM orders WHERE id = ?', [id])).length;
  const stored = async () => (await rows<{ n: number }>(database!.admin, `SELECT COUNT(*) AS n FROM ${schema}_messages`))[0]!.n;
  const pending = async () => (await store.stats(1)).pending;
  const claimed = async () => (await store.claim({ owner: randomUUID(), now: 1, leaseMs: 1_000, limit: 100 })).map((m) => m.id);

  it.each(isolations)("add() in the application's transaction (%s) commits with its row, and no other connection sees it before", async (isolation) => {
    const id = randomUUID();
    const added = message('order.placed', `order-${id}`);
    await client.transaction(async (tx) => {
      await client.insertOrder(tx, id);
      await store.add(tx, [added]);
      expect(await stored()).toBe(0);
      expect(await pending()).toBe(0);
    }, isolation);

    expect(await orders(id)).toBe(1);
    expect(await claimed()).toEqual([added.id]);
  });

  it.each(isolations)("a rolled-back add() (%s) leaves neither the application's row nor its messages", async (isolation) => {
    const id = randomUUID();
    await expect(
      client.transaction(async (tx) => {
        await client.insertOrder(tx, id);
        await store.add(tx, [message('order.placed', `order-${id}`), message('order.audited')]);
        throw new Error('payment declined');
      }, isolation),
    ).rejects.toThrow('payment declined');

    expect(await orders(id)).toBe(0);
    expect(await stored()).toBe(0);
    expect(await claimed()).toEqual([]);
  });

  it.each(isolations)("recordInbox() in the application's transaction (%s) commits with its row; a later delivery is a duplicate", async (isolation) => {
    const id = randomUUID();
    await client.transaction(async (tx) => {
      await client.insertOrder(tx, id);
      expect(await store.recordInbox(tx, 'billing', 'm-1', 1)).toBe(true);
      expect(await store.recordInbox(tx, 'billing', 'm-1', 1)).toBe(false);
      expect(await store.hasInbox('billing', 'm-1')).toBe(false);
    }, isolation);

    expect(await orders(id)).toBe(1);
    expect(await store.hasInbox('billing', 'm-1')).toBe(true);
    expect(await client.transaction((tx) => store.recordInbox(tx, 'billing', 'm-1', 2), isolation)).toBe(false);
  });

  it.each(isolations)("a rolled-back recordInbox() (%s) leaves neither the application's row nor the record, so the redelivery runs", async (isolation) => {
    const id = randomUUID();
    await expect(
      client.transaction(async (tx) => {
        await client.insertOrder(tx, id);
        expect(await store.recordInbox(tx, 'billing', 'm-2', 1)).toBe(true);
        throw new Error('handler failed');
      }, isolation),
    ).rejects.toThrow('handler failed');

    expect(await orders(id)).toBe(0);
    expect(await store.hasInbox('billing', 'm-2')).toBe(false);
    expect(await client.transaction((tx) => store.recordInbox(tx, 'billing', 'm-2', 2), isolation)).toBe(true);
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
        `MySqlOutboxStore.add(): ${(cause as TypeError).message}`,
    );

    await expect(store.add(client.root, [])).rejects.toThrow(OutboxTransactionRequiredError);
    await expect(store.recordInbox(client.root, 'billing', 'm-4', 1)).rejects.toThrow(OutboxTransactionRequiredError);
    await expect(store.recordInbox(null, 'billing', 'm-4', 1)).rejects.toThrow('MySqlOutboxStore.recordInbox(): ');
    expect(await stored()).toBe(0);
    expect(await store.hasInbox('billing', 'm-4')).toBe(false);
  });
});

describe(`${mysql2Client.name} on MySQL: a connection outside a transaction`, () => {
  onMysql(reason);
  let client: Client;
  let store: MySqlOutboxStore;

  beforeAll(async () => {
    if (!reason) {
      client = await mysql2Client.open(database!.url);
      store = new MySqlOutboxStore({ executor: client.executor, schema: 'tx_begin' });
      await store.onModuleInit();
    }
  });
  afterAll(async () => {
    await client?.close();
  });

  it('is refused at its first statement, before anything is written: each statement would commit on its own', async () => {
    const connection = await (client.root as mysql.Pool).getConnection();
    try {
      for (const call of [() => store.add(connection, [message('order.placed', 'order-1')]), () => store.recordInbox(connection, 'billing', 'm-1', 1)]) {
        const error = await call().catch((e: unknown) => e);
        expect(error).toBeInstanceOf(OutboxTransactionRequiredError);
        expect((error as Error).message).toMatch(/MySqlOutboxStore\.(add|recordInbox)\(\): The mysql2 connection isn't in a transaction: call beginTransaction\(\) on it first/);
      }
    } finally {
      connection.release();
    }
    expect(await rows(database!.admin, 'SELECT id FROM tx_begin_messages')).toEqual([]);
    expect(await store.hasInbox('billing', 'm-1')).toBe(false);
  });
});
