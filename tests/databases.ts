/**
 * The databases the package's tests run on, behind one interface:
 *
 * - the in-memory store, the default and the test double: synchronous, in this process. Its
 *   `transaction()` holds the writes until the work resolves, so rollbacks can be tested,
 *   but it can't join a real database's transaction;
 * - the outbox tutorial's `DrizzleOutboxStore` (the docs' recipe: an app registers a store on
 *   its own database) with the tutorial's drizzle-kit migrations, on PGlite (PostgreSQL in
 *   this process, one connection: always runs) and on PostgreSQL (`SQL_TEST_PG_URL`, else a
 *   throwaway cluster; every `store()` has its own pool, as another app instance would, so
 *   transactions really overlap).
 *
 * Each has the application's own business tables, orders and invoices, written in the same
 * transaction as the outbox, so a test can check what commits together.
 */
import { PGlite } from '@electric-sql/pglite';
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { drizzle as drizzlePglite } from 'drizzle-orm/pglite';
import { migrate as migratePglite } from 'drizzle-orm/pglite/migrator';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { DrizzleOutboxStore } from './fixtures/database/drizzle-outbox.store.js';
import type { Database, Transaction } from './fixtures/database/drizzle.js';
import * as schema from './fixtures/database/schema.js';
import type { TestPostgres } from './support/postgres.js';
import { InMemoryOutboxStore, OutboxStorage, type OutboxInboxStore, type OutboxStore } from '../lib/index.js';
import type { Awaitable } from '../lib/interfaces/awaitable.interface.js';

export type TestStore<Tx = any> = OutboxStore<Tx> & OutboxInboxStore<Tx>;

export interface TestDatabase<Tx = any> {
  readonly name: string;
  /** The store an application instance registers. On PostgreSQL each call opens its own pool. */
  store(): TestStore<Tx>;
  /** A transaction of the application's: commits when `work` resolves, rolls back when it throws. */
  transaction<T>(work: (tx: Tx) => Awaitable<T>): Promise<T>;
  insertOrder(tx: Tx, id: number): Awaitable<void>;
  insertInvoice(tx: Tx, orderId: number, consumer: string): Awaitable<void>;
  count(table: 'orders' | 'invoices', consumer?: string): Promise<number>;
  /** A handle `add()` and `recordInbox()` must refuse (the database itself); undefined when the store can't tell. */
  readonly notATransaction: Tx | undefined;
  /** Empties every table, and closes the pools of the stores opened since the last reset. */
  reset(): Promise<void>;
  close(): Promise<void>;
}

/** A Drizzle one also hands out the database, for a provider that injects it (`@InjectDrizzle()`). */
export interface DrizzleTestDatabase extends TestDatabase<Transaction> {
  readonly db: Database;
}

// ------------------------------------------------------------------ in memory

export function inMemoryDatabase(): TestDatabase<unknown> {
  let store = new InMemoryOutboxStore();
  let orders: number[] = [];
  let invoices: { orderId: number; consumer: string }[] = [];

  /** The business writes of each open transaction, applied when the store commits it. */
  const pending = new WeakMap<object, Array<() => void>>();
  const writesOf = (tx: unknown) => {
    const writes = typeof tx === 'object' && tx !== null ? pending.get(tx) : undefined;
    if (!writes) {
      throw new Error('Not an open transaction of this database');
    }
    return writes;
  };

  return {
    name: 'InMemoryOutboxStore',
    store: () => store,
    async transaction(work) {
      const writes: Array<() => void> = [];
      const result = await store.transaction(async (tx) => {
        pending.set(tx as object, writes);
        return work(tx);
      });

      for (const write of writes) {
        write();
      }
      return result;
    },
    insertOrder: (tx, id) => void writesOf(tx).push(() => orders.push(id)),
    insertInvoice: (tx, orderId, consumer) => void writesOf(tx).push(() => invoices.push({ orderId, consumer })),
    async count(table, consumer) {
      if (table === 'orders') {
        return orders.length;
      }
      return invoices.filter((invoice) => consumer === undefined || invoice.consumer === consumer).length;
    },
    // It writes through any other handle at once (with a warning): it can't tell.
    notATransaction: undefined,
    async reset() {
      store = new InMemoryOutboxStore();
      orders = [];
      invoices = [];
    },
    async close() {},
  };
}

// ------------------------------------------------------------------ Drizzle

const migrationsFolder = fileURLToPath(new URL('./fixtures/drizzle', import.meta.url));

async function createTables(db: Database) {
  await db.execute(sql`CREATE TABLE test_orders (id integer PRIMARY KEY, total integer NOT NULL)`);
  await db.execute(sql`CREATE TABLE test_invoices (order_id integer NOT NULL, consumer text NOT NULL)`);
}

function drizzleDatabase(
  name: string,
  db: Database,
  options: { store(): TestStore<Transaction>; reset?(): Promise<void>; close(): Promise<void> },
): DrizzleTestDatabase {
  return {
    name,
    db,
    store: options.store,
    transaction: (work) => db.transaction(async (tx) => work(tx)),
    async insertOrder(tx, id) {
      await tx.execute(sql`INSERT INTO test_orders (id, total) VALUES (${id}, 100)`);
    },
    async insertInvoice(tx, orderId, consumer) {
      await tx.execute(sql`INSERT INTO test_invoices (order_id, consumer) VALUES (${orderId}, ${consumer})`);
    },
    async count(table, consumer) {
      const from = table === 'orders' ? sql`test_orders` : sql`test_invoices`;
      const where = consumer === undefined ? sql`` : sql` WHERE consumer = ${consumer}`;
      const { rows } = await db.execute<{ n: number }>(sql`SELECT count(*)::int AS n FROM ${from}${where}`);
      return rows[0]!.n;
    },
    notATransaction: db as unknown as Transaction,
    async reset() {
      await options.reset?.();
      await db.execute(
        sql`TRUNCATE outbox_messages, outbox_dead_letters, outbox_inbox, test_orders, test_invoices RESTART IDENTITY`,
      );
    },
    close: options.close,
  };
}

/** `DrizzleOutboxStore` on PGlite: one connection, so transactions take turns. */
export async function pgliteDatabase(): Promise<DrizzleTestDatabase> {
  const client = new PGlite();
  const db = drizzlePglite(client, { schema }) as unknown as Database;
  await migratePglite(db as never, { migrationsFolder });
  await createTables(db);

  return drizzleDatabase('DrizzleOutboxStore on PGlite', db, {
    store: () => new DrizzleOutboxStore(db, new OutboxStorage()),
    close: () => client.close(),
  });
}

/** `DrizzleOutboxStore` on PostgreSQL: each `store()` on a pool of its own, like another app instance. */
export async function postgresDatabase(postgres: TestPostgres, database: string): Promise<DrizzleTestDatabase> {
  const url = await postgres.createDatabase(database);
  const main = new pg.Pool({ connectionString: url, max: 8 });
  const db = drizzle(main, { schema });
  await migrate(db, { migrationsFolder });
  await createTables(db);

  let instances: pg.Pool[] = [];
  const endInstances = async () => {
    const pools = instances;
    instances = [];
    await Promise.all(pools.map((pool) => pool.end()));
  };

  return drizzleDatabase('DrizzleOutboxStore on PostgreSQL', db, {
    store() {
      const pool = new pg.Pool({ connectionString: url, max: 4 });
      instances.push(pool);
      return new DrizzleOutboxStore(drizzle(pool, { schema }), new OutboxStorage());
    },
    reset: endInstances,
    async close() {
      await endInstances();
      await main.end();
    },
  });
}

/**
 * `store` for an application instance that can crash: after `crash()`, every call fails, as
 * it would once the process is gone. What the relay had in flight stops at its next write.
 */
export function crashable<S extends object>(store: S): { store: S; crash(): void } {
  let crashed = false;
  const proxy = new Proxy(store, {
    get(target, property, receiver) {
      const value = Reflect.get(target, property, receiver);
      if (typeof value !== 'function') {
        return value;
      }

      return (...args: unknown[]) => {
        if (crashed) {
          throw new Error('The instance crashed: its connection is gone');
        }
        return value.apply(target, args);
      };
    },
  });

  return { store: proxy, crash: () => void (crashed = true) };
}
