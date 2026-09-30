/**
 * PostgresOutboxStore's tests: the database clients an application may hand it (node-postgres, Drizzle on node-postgres
 * and on PGlite, TypeORM, Prisma, Kysely), each with the ORM's own way of running a transaction, and a database per test
 * file on PostgreSQL (`SQL_TEST_PG_URL`, else a throwaway cluster, else those tests are skipped with the reason),
 * through tests/support/postgres.ts, which names (`obx_...`) and sweeps them.
 */
import { PGlite } from '@electric-sql/pglite';
import { PrismaPg } from '@prisma/adapter-pg';
import { drizzle as drizzlePg } from 'drizzle-orm/node-postgres';
import { integer, jsonb, pgTable, text, uuid } from 'drizzle-orm/pg-core';
import { drizzle as drizzlePglite } from 'drizzle-orm/pglite';
import { Kysely, PostgresDialect } from 'kysely';
import pg from 'pg';
import { Column, DataSource, Entity, PrimaryColumn } from 'typeorm';
import type { OutboxMessage } from '../../lib/index.js';
import { fromDrizzle, fromKysely, fromPg, fromPrisma, fromTypeOrm, PostgresOutboxStore, type SqlExecutor } from '../../lib/postgres/index.js';
import { outboxInboxStoreContract, outboxStoreContract, type OutboxStoreHarness } from '../../lib/testing/index.js';
import { uuidv7 } from '../../lib/utils/uuid.util.js';
import { PrismaClient } from '../fixtures/prisma/generated/client.js';
import { endPool, startPostgres } from '../support/postgres.js';

/** The application's table (the Prisma fixture's `Order` model), written in the same transactions as its messages. */
export const ORDERS_DDL = `CREATE TABLE IF NOT EXISTS orders (
  id uuid PRIMARY KEY,
  user_id text NOT NULL,
  items jsonb NOT NULL,
  total integer NOT NULL,
  status text NOT NULL
)`;

const ORDER = { userId: 'customer-1', items: [{ productId: 'salmon-kibble-2kg', quantity: 1 }], total: 2499, status: 'placed' };

export type Isolation = 'read committed' | 'repeatable read';

/** A database client as an application holds one. */
export interface Client {
  name: string;
  executor: SqlExecutor;
  /** A transaction as the application runs one with this client: `work` gets the ORM's own transaction object. */
  transaction<T>(work: (tx: unknown) => Promise<T>, isolation?: Isolation): Promise<T>;
  /** What an application might pass by mistake instead of its transaction: the pool, the database, the client. */
  root: unknown;
  /** The application's write, through its transaction object. */
  insertOrder(tx: unknown, id: string): Promise<void>;
  close(): Promise<void>;
}

export interface ClientFactory {
  name: string;
  open(url: string): Promise<Client>;
}

const drizzleOrders = pgTable('orders', {
  id: uuid('id').primaryKey(),
  userId: text('user_id').notNull(),
  items: jsonb('items').notNull(),
  total: integer('total').notNull(),
  status: text('status').notNull(),
});

@Entity('orders')
class OrderEntity {
  @PrimaryColumn('uuid')
  id!: string;

  @Column('text', { name: 'user_id' })
  userId!: string;

  @Column('jsonb')
  items!: unknown;

  @Column('integer')
  total!: number;

  @Column('text')
  status!: string;
}

interface KyselyDatabase {
  orders: { id: string; user_id: string; items: string; total: number; status: string };
}

export const pgClient: ClientFactory = {
  name: 'fromPg (node-postgres Pool)',
  async open(url) {
    const pool = new pg.Pool({ connectionString: url, max: 10 });
    return {
      name: this.name,
      executor: fromPg(pool),
      root: pool,
      async transaction(work, isolation) {
        const client = await pool.connect();
        try {
          await client.query(isolation ? `BEGIN ISOLATION LEVEL ${isolation.toUpperCase()}` : 'BEGIN');
          const result = await work(client);
          await client.query('COMMIT');
          return result;
        } catch (error) {
          await client.query('ROLLBACK');
          throw error;
        } finally {
          client.release();
        }
      },
      insertOrder: async (tx, id) => {
        await (tx as pg.PoolClient).query('INSERT INTO orders (id, user_id, items, total, status) VALUES ($1, $2, $3, $4, $5)', [
          id,
          ORDER.userId,
          JSON.stringify(ORDER.items),
          ORDER.total,
          ORDER.status,
        ]);
      },
      close: () => endPool(pool),
    };
  },
};

export const drizzleClient: ClientFactory = {
  name: 'fromDrizzle (node-postgres)',
  async open(url) {
    const pool = new pg.Pool({ connectionString: url, max: 10 });
    const db = drizzlePg(pool);
    return {
      name: this.name,
      executor: fromDrizzle(db),
      root: db,
      transaction: (work, isolation) => db.transaction(work, isolation ? { isolationLevel: isolation } : undefined),
      insertOrder: async (tx, id) => {
        await (tx as typeof db).insert(drizzleOrders).values({ id, ...ORDER });
      },
      close: () => endPool(pool),
    };
  },
};

export const typeOrmClient: ClientFactory = {
  name: 'fromTypeOrm',
  async open(url) {
    const dataSource = await new DataSource({ type: 'postgres', url, entities: [OrderEntity], poolSize: 10 }).initialize();
    return {
      name: this.name,
      executor: fromTypeOrm(dataSource),
      root: dataSource.manager,
      transaction: (work, isolation) =>
        isolation ? dataSource.transaction(isolation.toUpperCase() as 'REPEATABLE READ', work) : dataSource.transaction(work),
      insertOrder: async (tx, id) => {
        await (tx as DataSource['manager']).insert(OrderEntity, { id, ...ORDER });
      },
      close: () => dataSource.destroy(),
    };
  },
};

export const prismaClient: ClientFactory = {
  name: 'fromPrisma (@prisma/adapter-pg)',
  async open(url) {
    const prisma = new PrismaClient({ adapter: new PrismaPg({ connectionString: url, max: 10 }) });
    return {
      name: this.name,
      executor: fromPrisma(prisma),
      root: prisma,
      // The application's own limits: Prisma's default maxWait (2 s) is shorter than the wait for a connection gets on a
      // busy machine when the contract's producers and relays share a pool of 10.
      transaction: (work, isolation) =>
        prisma.$transaction((tx) => work(tx), {
          maxWait: 10_000,
          timeout: 30_000,
          ...(isolation ? { isolationLevel: isolation === 'repeatable read' ? 'RepeatableRead' : 'ReadCommitted' } : {}),
        }),
      insertOrder: async (tx, id) => {
        await (tx as PrismaClient).order.create({ data: { id, ...ORDER } });
      },
      close: () => prisma.$disconnect(),
    };
  },
};

export const kyselyClient: ClientFactory = {
  name: 'fromKysely',
  async open(url) {
    const db = new Kysely<KyselyDatabase>({ dialect: new PostgresDialect({ pool: new pg.Pool({ connectionString: url, max: 10 }) }) });
    return {
      name: this.name,
      executor: fromKysely(db),
      root: db,
      transaction: (work, isolation) =>
        isolation ? db.transaction().setIsolationLevel(isolation).execute(work) : db.transaction().execute(work),
      insertOrder: async (tx, id) => {
        await (tx as typeof db)
          .insertInto('orders')
          .values({ id, user_id: ORDER.userId, items: JSON.stringify(ORDER.items), total: ORDER.total, status: ORDER.status })
          .execute();
      },
      close: () => db.destroy(),
    };
  },
};

/** Every client on PostgreSQL. */
export const clients = [pgClient, drizzleClient, typeOrmClient, prismaClient, kyselyClient];

/** Drizzle on PGlite: PostgreSQL in-process, one connection, so every transaction waits for the one before it. */
export async function openPglite(): Promise<Client & { pglite: PGlite }> {
  const pglite = new PGlite();
  const db = drizzlePglite(pglite);
  await db.execute(ORDERS_DDL);
  return {
    name: 'fromDrizzle (PGlite)',
    pglite,
    executor: fromDrizzle(db),
    root: db,
    transaction: (work, isolation) => db.transaction(work, isolation ? { isolationLevel: isolation } : undefined),
    insertOrder: async (tx, id) => {
      await (tx as typeof db).insert(drizzleOrders).values({ id, ...ORDER });
    },
    close: () => pglite.close(),
  };
}

export interface TestDatabase {
  url: string;
  /** A client for looking at the database from outside the store. */
  admin: pg.Pool;
}

/**
 * A database of this test file on PostgreSQL (with `orders`), dropped after the file; `null`, with the reason, where
 * there's no PostgreSQL. Tests that need it skip with the reason.
 */
export async function testDatabase(name: string): Promise<{ database: TestDatabase; reason?: undefined } | { database: null; reason: string }> {
  const { postgres, reason } = await startPostgres();
  if (!postgres) {
    return { database: null, reason: `no PostgreSQL: ${reason}` };
  }

  const url = await postgres.createDatabase(name);
  const admin = new pg.Pool({ connectionString: url, max: 3 });
  await admin.query(ORDERS_DDL);
  afterAll(async () => {
    await endPool(admin);
    await postgres.stop();
  });
  return { database: { url, admin } };
}

/** Empties the store's tables in `schema`, as the contract wants a store on empty tables. */
export async function truncate(executor: SqlExecutor, schema: string): Promise<void> {
  const tables = ['messages', 'dead_letters', 'inbox'].map((table) => `"${schema}".${table}`);
  await executor.query(`TRUNCATE ${tables.join(', ')} RESTART IDENTITY`);
}

/** A message as `Outbox.add()` builds one, due at once. */
export function message(topic: string, key: string | null = null, extra: Partial<OutboxMessage> = {}): OutboxMessage {
  return {
    id: uuidv7(),
    topic,
    payload: { topic },
    headers: {},
    key,
    createdAt: 0,
    availableAt: 0,
    attempts: 0,
    lastError: null,
    ...extra,
  };
}

/**
 * The `OutboxStore` and `OutboxInboxStore` contracts on PostgresOutboxStore through `client`, in a schema of its own,
 * with the concurrency cases, the ORM's own transactions, and its root object as what the store must refuse.
 */
export function describeContract(label: string, client: () => Promise<Client | null>, schema: string, skipReason?: string): void {
  describe(label, () => {
    let opened: Client | null = null;
    beforeAll(async () => {
      opened = await client();
      if (opened) {
        await new PostgresOutboxStore({ executor: opened.executor, schema }).migrate();
      }
    });
    afterAll(() => opened?.close());
    if (skipReason) {
      beforeEach((context) => context.skip(skipReason));
    }

    const harness = async (): Promise<OutboxStoreHarness & { store: PostgresOutboxStore }> => {
      await truncate(opened!.executor, schema);
      return {
        store: new PostgresOutboxStore({ executor: opened!.executor, schema, migrate: false }),
        transaction: (work) => opened!.transaction(work),
        notATransaction: opened!.root,
      };
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
}
