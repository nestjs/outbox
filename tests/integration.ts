/**
 * The store recipes the docs document (copied into tests/fixtures), as the database of a
 * real Nest application instance, for the `*.integration.spec.ts` suites:
 *
 * - `DrizzleOutboxStore` through `@nestjs/drizzle` on PGlite (PostgreSQL in this process,
 *   one connection shared by every instance) and on PostgreSQL;
 * - `TypeOrmOutboxStore` through `@nestjs/typeorm` on PostgreSQL;
 * - `PrismaOutboxStore` on PostgreSQL (needs the client `npm run generate:prisma` writes into
 *   tests/fixtures/prisma/generated, which the vitest global setup runs; skipped, with the
 *   reason, without it);
 * - the first-party `PostgresOutboxStore` (`@nestjs/outbox/postgres`), registered as the docs
 *   show it, by a factory provider that injects the database and `OutboxStorage`: through
 *   `fromDrizzle()` on PGlite and on PostgreSQL, `fromTypeOrm()` and `fromPrisma()`;
 * - the first-party `MySqlOutboxStore` (`@nestjs/outbox/mysql`), registered the same way, on
 *   MySQL: through `fromDrizzle()` (`@nestjs/drizzle` with drizzle-orm/mysql2), `fromTypeOrm()`
 *   and `fromPrisma()` (its MariaDB adapter).
 *
 * Each vitest project runs the suites on one family (`integrationRecipes()`: its `sqlDialect`, see
 * vitest.config.ts): PostgreSQL and PGlite in `outbox`, MySQL in `outbox:mysql-store`.
 *
 * On PostgreSQL (`SQL_TEST_PG_URL`, else a throwaway cluster from local binaries, else skipped
 * with the reason) and MySQL (`SQL_TEST_MYSQL_URL`, else skipped with the reason) every
 * application instance opens a pool of its own, so instances race over real connections (two
 * connections each on MySQL, a shared server). Each database is migrated with the recipe's own
 * migrations (the first-party store's, as `npx nest-outbox migrate` applies them on deploy) and
 * gets two business tables of the application's, `it_orders` and `it_invoices`, written in the
 * same transactions as the outbox and the inbox.
 */
import { PGlite } from '@electric-sql/pglite';
import { Global, Inject, Injectable, Module, type DynamicModule, type InjectionToken, type Provider, type Type } from '@nestjs/common';
import { DrizzleModule, getDrizzleToken, InjectDrizzle } from '@nestjs/drizzle';
import { TypeOrmModule } from '@nestjs/typeorm';
import { sql } from 'drizzle-orm';
import { drizzle as drizzleMysql } from 'drizzle-orm/mysql2';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { drizzle as drizzlePglite } from 'drizzle-orm/pglite';
import { migrate as migratePglite } from 'drizzle-orm/pglite/migrator';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import mysql from 'mysql2/promise';
import pg from 'pg';
import { DataSource, type EntityManager } from 'typeorm';
import { inject } from 'vitest';
import { DrizzleInboxStore } from './fixtures/analytics/database/drizzle-inbox.store.js';
import * as analyticsSchema from './fixtures/analytics/database/schema.js';
import { DrizzleOutboxStore } from './fixtures/database/drizzle-outbox.store.js';
import type { Database, Transaction } from './fixtures/database/drizzle.js';
import * as schema from './fixtures/database/schema.js';
import { dataSourceOptions } from './fixtures/typeorm/data-source.js';
import { TypeOrmOutboxStore } from './fixtures/typeorm/typeorm-outbox.store.js';
import { startMysql, type TestMysql } from './support/mysql.js';
import { endPool, startPostgres, type TestPostgres } from './support/postgres.js';
import { OutboxStorage, type OutboxInboxStore, type OutboxStore } from '../lib/index.js';
import * as mysqlStore from '../lib/mysql/index.js';
import { fromDrizzle, fromPg, fromPrisma, fromTypeOrm, PostgresOutboxStore, type SqlExecutor } from '../lib/postgres/index.js';

declare module 'vitest' {
  export interface ProvidedContext {
    /** The database family a project runs the integration suites on (vitest.config.ts). */
    sqlDialect: 'postgres' | 'mysql';
  }
}

const fixture = (path: string) => fileURLToPath(new URL(`./fixtures/${path}`, import.meta.url));

/** The application's own data access, as its services and handlers use it. */
export abstract class AppDatabase<Tx = any> {
  /** The ORM's transaction: commits when `work` resolves, rolls back when it throws. */
  abstract transaction<T>(work: (tx: Tx) => Promise<T>): Promise<T>;
  abstract insertOrder(tx: Tx, id: number): Promise<void>;
  abstract insertInvoice(tx: Tx, orderId: number, consumer: string): Promise<void>;
  /** The database itself, not a transaction. */
  abstract readonly root: Tx;
}

export interface RecipeDatabase {
  /**
   * One application instance's database access, global in its application: its connection
   * (a pool of its own on a server), the recipe's store (registered in its constructor) and
   * `AppDatabase`.
   */
  module(): DynamicModule;
  /** The store class the module registers, to reach the instance with `app.get()`. */
  readonly storeClass: Type<OutboxStore<any> | OutboxInboxStore<any>>;
  query<T = Record<string, any>>(text: string, params?: unknown[]): Promise<T[]>;
  count(table: 'it_orders' | 'it_invoices' | 'outbox_inbox', consumer?: string): Promise<number>;
  reset(): Promise<void>;
  close(): Promise<void>;
}

export interface Recipe {
  readonly name: string;
  /** Why the recipe can't run here, or undefined. */
  readonly skip: string | undefined;
  /** Whether the store can tell the root handle from a transaction (the Prisma recipe's can't). */
  readonly refusesRoot: boolean;
  /** The order API's database: the recipe's outbox and inbox tables. */
  open(name: string): Promise<RecipeDatabase>;
  /**
   * A consumer-only service's database. The Drizzle recipes use the analytics service's
   * `DrizzleInboxStore` (tests/fixtures/analytics), registered as `{ inbox: this }`; the others
   * the full store, `PostgresOutboxStore` included (its tables for messages stay empty).
   */
  openConsumer(name: string): Promise<RecipeDatabase>;
}

const BUSINESS_TABLES = [
  'CREATE TABLE it_orders (id integer PRIMARY KEY)',
  'CREATE TABLE it_invoices (order_id integer NOT NULL, consumer text NOT NULL)',
];

function globalModule(imports: DynamicModule['imports'], providers: DynamicModule['providers'], database: Type<AppDatabase>): DynamicModule {
  @Global()
  @Module({})
  class RecipeModule {}

  return {
    module: RecipeModule,
    imports,
    providers: [...(providers ?? []), database, { provide: AppDatabase, useExisting: database }],
    exports: [AppDatabase],
  };
}

function withQueries(
  query: RecipeDatabase['query'],
  tables: string[],
  rest: Pick<RecipeDatabase, 'module' | 'storeClass' | 'close'>,
  inboxTable = 'outbox_inbox',
): RecipeDatabase {
  return {
    ...rest,
    query,
    async count(table, consumer) {
      const from = table === 'outbox_inbox' ? inboxTable : table;
      const where = consumer === undefined ? '' : ' WHERE consumer = $1';
      const rows = await query<{ n: number }>(`SELECT count(*)::int AS n FROM ${from}${where}`, consumer === undefined ? [] : [consumer]);
      return rows[0]!.n;
    },
    async reset() {
      await query(`TRUNCATE ${tables.join(', ')} RESTART IDENTITY`);
    },
  };
}

// ------------------------------------------------------------------ Drizzle

@Injectable()
class DrizzleAppDatabase extends AppDatabase<Transaction> {
  constructor(@InjectDrizzle() private readonly db: Database) {
    super();
  }

  get root() {
    return this.db as unknown as Transaction;
  }

  transaction<T>(work: (tx: Transaction) => Promise<T>) {
    return this.db.transaction(work);
  }

  async insertOrder(tx: Transaction, id: number) {
    await tx.execute(sql`INSERT INTO it_orders (id) VALUES (${id})`);
  }

  async insertInvoice(tx: Transaction, orderId: number, consumer: string) {
    await tx.execute(sql`INSERT INTO it_invoices (order_id, consumer) VALUES (${orderId}, ${consumer})`);
  }
}

const OUTBOX_TABLES = ['outbox_messages', 'outbox_dead_letters', 'outbox_inbox', 'it_orders', 'it_invoices'];
const INBOX_TABLES = ['outbox_inbox', 'it_orders', 'it_invoices'];

async function drizzlePgliteDatabase(inboxOnly: boolean): Promise<RecipeDatabase> {
  const client = new PGlite();
  const tables = inboxOnly ? analyticsSchema : schema;
  const db = drizzlePglite(client, { schema: tables });
  await migratePglite(db, { migrationsFolder: fixture(inboxOnly ? 'analytics/drizzle' : 'drizzle') });
  for (const statement of BUSINESS_TABLES) {
    await client.query(statement);
  }

  const storeClass = inboxOnly ? DrizzleInboxStore : DrizzleOutboxStore;
  const query: RecipeDatabase['query'] = async (text, params) => (await client.query(text, params)).rows as never;

  return withQueries(query, inboxOnly ? INBOX_TABLES : OUTBOX_TABLES, {
    storeClass,
    // Every instance shares the one connection PGlite has; the test closes it, not the apps.
    module: () => globalModule([DrizzleModule.forRoot({ db, autoCloseConnection: false })], [storeClass], DrizzleAppDatabase),
    close: () => client.close(),
  });
}

async function drizzlePostgresDatabase(postgres: TestPostgres, name: string, inboxOnly: boolean): Promise<RecipeDatabase> {
  const url = await postgres.createDatabase(name);
  const pool = new pg.Pool({ connectionString: url, max: 4 });
  await migrate(drizzle(pool), { migrationsFolder: fixture(inboxOnly ? 'analytics/drizzle' : 'drizzle') });
  for (const statement of BUSINESS_TABLES) {
    await pool.query(statement);
  }

  const storeClass = inboxOnly ? DrizzleInboxStore : DrizzleOutboxStore;
  const query: RecipeDatabase['query'] = async (text, params) => (await pool.query(text, params)).rows;

  return withQueries(query, inboxOnly ? INBOX_TABLES : OUTBOX_TABLES, {
    storeClass,
    // DrizzleModule opens the instance's pool and ends it in onApplicationShutdown.
    module: () =>
      globalModule(
        [DrizzleModule.forRoot({ drizzle, connection: { connectionString: url, max: 5 }, schema: inboxOnly ? analyticsSchema : schema })],
        [storeClass],
        DrizzleAppDatabase,
      ),
    close: () => pool.end(),
  });
}

// ------------------------------------------------------------------ TypeORM

@Injectable()
class TypeOrmAppDatabase extends AppDatabase<EntityManager> {
  constructor(private readonly dataSource: DataSource) {
    super();
  }

  get root() {
    return this.dataSource.manager;
  }

  transaction<T>(work: (tx: EntityManager) => Promise<T>) {
    return this.dataSource.transaction(work);
  }

  async insertOrder(tx: EntityManager, id: number) {
    await tx.query('INSERT INTO it_orders (id) VALUES ($1)', [id]);
  }

  async insertInvoice(tx: EntityManager, orderId: number, consumer: string) {
    await tx.query('INSERT INTO it_invoices (order_id, consumer) VALUES ($1, $2)', [orderId, consumer]);
  }
}

async function typeOrmDatabase(postgres: TestPostgres, name: string): Promise<RecipeDatabase> {
  const url = await postgres.createDatabase(name);
  // What `typeorm migration:run` does on deploy.
  const migrator = await new DataSource({ ...dataSourceOptions, url }).initialize();
  await migrator.runMigrations();
  for (const statement of BUSINESS_TABLES) {
    await migrator.query(statement);
  }

  const query: RecipeDatabase['query'] = (text, params) => migrator.query(text, params);

  return withQueries(query, OUTBOX_TABLES, {
    storeClass: TypeOrmOutboxStore,
    module: () =>
      globalModule(
        [TypeOrmModule.forRoot({ ...dataSourceOptions, url, poolSize: 5, retryAttempts: 0 })],
        [TypeOrmOutboxStore],
        TypeOrmAppDatabase,
      ),
    close: () => migrator.destroy(),
  });
}

// ------------------------------------------------------------------ Prisma

const prismaClient = fixture('prisma/generated/client.ts');
const prismaSkip = existsSync(prismaClient)
  ? undefined
  : 'the Prisma client is not generated (run `npm run generate:prisma`)';

/**
 * Loaded by a computed path: the generated client is gitignored, so a static import would
 * fail the whole suite (and the type check) on a checkout that hasn't generated it.
 */
async function loadPrisma(): Promise<{ PrismaService: Type<any>; PrismaOutboxStore: Type<OutboxStore<any>> }> {
  const service = fixture('prisma/prisma.service.ts');
  const store = fixture('prisma/prisma-outbox.store.ts');
  const [{ PrismaService }, { PrismaOutboxStore }] = await Promise.all([import(service), import(store)]);
  return { PrismaService, PrismaOutboxStore };
}

/**
 * The application's data access through Prisma, and the `PrismaService` it injects, on `url`: raw statements with the
 * dialect's placeholders (`$1` on PostgreSQL, `?` on MySQL).
 */
function prismaApplication(
  PrismaService: Type<any>,
  url: string,
  placeholder: (n: number) => string = (n) => `$${n}`,
): { database: Type<AppDatabase>; service: Provider } {
  @Injectable()
  class PrismaAppDatabase extends AppDatabase {
    constructor(@Inject(PrismaService) private readonly prismaService: any) {
      super();
    }

    get root() {
      return this.prismaService;
    }

    transaction<T>(work: (tx: any) => Promise<T>): Promise<T> {
      return this.prismaService.$transaction(work);
    }

    async insertOrder(tx: any, id: number) {
      await tx.$executeRawUnsafe(`INSERT INTO it_orders (id) VALUES (${placeholder(1)})`, id);
    }

    async insertInvoice(tx: any, orderId: number, consumer: string) {
      await tx.$executeRawUnsafe(`INSERT INTO it_invoices (order_id, consumer) VALUES (${placeholder(1)}, ${placeholder(2)})`, orderId, consumer);
    }
  }

  return {
    database: PrismaAppDatabase,
    service: {
      // PrismaService reads DATABASE_URL when constructed; it disconnects in onApplicationShutdown.
      provide: PrismaService,
      useFactory: () => {
        const previous = process.env.DATABASE_URL;
        process.env.DATABASE_URL = url;
        try {
          return new PrismaService();
        } finally {
          if (previous === undefined) {
            delete process.env.DATABASE_URL;
          } else {
            process.env.DATABASE_URL = previous;
          }
        }
      },
    },
  };
}

async function prismaDatabase(postgres: TestPostgres, name: string): Promise<RecipeDatabase> {
  const url = await postgres.createDatabase(name);
  const pool = new pg.Pool({ connectionString: url, max: 4 });

  // What `prisma migrate deploy` applies, in order.
  const migrations = fixture('prisma/migrations');
  for (const dir of readdirSync(migrations).filter((entry) => !entry.endsWith('.toml')).sort()) {
    await pool.query(readFileSync(`${migrations}/${dir}/migration.sql`, 'utf8'));
  }
  for (const statement of BUSINESS_TABLES) {
    await pool.query(statement);
  }

  const { PrismaService, PrismaOutboxStore } = await loadPrisma();
  const application = prismaApplication(PrismaService, url);
  const query: RecipeDatabase['query'] = async (text, params) => (await pool.query(text, params)).rows;

  return withQueries(query, OUTBOX_TABLES, {
    storeClass: PrismaOutboxStore,
    module: () => globalModule([], [application.service, PrismaOutboxStore], application.database),
    close: () => pool.end(),
  });
}

// ------------------------------------------------------------------ PostgresOutboxStore

/** The first-party store's tables, in its schema (`nest_outbox`), and the application's. */
const FIRST_PARTY_TABLES = ['nest_outbox.messages', 'nest_outbox.dead_letters', 'nest_outbox.inbox', 'it_orders', 'it_invoices'];

/**
 * `PostgresOutboxStore` as an application registers it: a factory provider that injects the database (`token`) and
 * the registry, with which the store registers itself for both contracts.
 */
function postgresOutboxStore(token: InjectionToken, executor: (db: any) => SqlExecutor): Provider {
  return {
    provide: PostgresOutboxStore,
    inject: [token, OutboxStorage],
    useFactory: (db: unknown, storage: OutboxStorage) => new PostgresOutboxStore({ executor: executor(db) }, storage),
  };
}

/** Applies the store's migrations, as `npx nest-outbox migrate` does on deploy, and adds the application's tables. */
async function prepareFirstParty(executor: SqlExecutor): Promise<void> {
  await new PostgresOutboxStore({ executor }).migrate();
  for (const statement of BUSINESS_TABLES) {
    await executor.query(statement);
  }
}

async function firstPartyPgliteDatabase(): Promise<RecipeDatabase> {
  const client = new PGlite();
  const db = drizzlePglite(client);
  await prepareFirstParty(fromDrizzle(db));
  const query: RecipeDatabase['query'] = async (text, params) => (await client.query(text, params)).rows as never;

  return withQueries(
    query,
    FIRST_PARTY_TABLES,
    {
      storeClass: PostgresOutboxStore,
      // Every instance shares the one connection PGlite has; the test closes it, not the apps.
      module: () =>
        globalModule([DrizzleModule.forRoot({ db, autoCloseConnection: false })], [postgresOutboxStore(getDrizzleToken(), fromDrizzle)], DrizzleAppDatabase),
      close: () => client.close(),
    },
    'nest_outbox.inbox',
  );
}

async function firstPartyDrizzleDatabase(postgres: TestPostgres, name: string): Promise<RecipeDatabase> {
  const url = await postgres.createDatabase(name);
  const pool = new pg.Pool({ connectionString: url, max: 4 });
  await prepareFirstParty(fromPg(pool));
  const query: RecipeDatabase['query'] = async (text, params) => (await pool.query(text, params)).rows;

  return withQueries(
    query,
    FIRST_PARTY_TABLES,
    {
      storeClass: PostgresOutboxStore,
      // DrizzleModule opens the instance's pool and ends it in onApplicationShutdown.
      module: () =>
        globalModule(
          [DrizzleModule.forRoot({ drizzle, connection: { connectionString: url, max: 5 } })],
          [postgresOutboxStore(getDrizzleToken(), fromDrizzle)],
          DrizzleAppDatabase,
        ),
      close: () => endPool(pool),
    },
    'nest_outbox.inbox',
  );
}

async function firstPartyTypeOrmDatabase(postgres: TestPostgres, name: string): Promise<RecipeDatabase> {
  const url = await postgres.createDatabase(name);
  const pool = new pg.Pool({ connectionString: url, max: 4 });
  await prepareFirstParty(fromPg(pool));
  const query: RecipeDatabase['query'] = async (text, params) => (await pool.query(text, params)).rows;

  return withQueries(
    query,
    FIRST_PARTY_TABLES,
    {
      storeClass: PostgresOutboxStore,
      module: () =>
        globalModule(
          [TypeOrmModule.forRoot({ type: 'postgres', url, poolSize: 5, retryAttempts: 0 })],
          [postgresOutboxStore(DataSource, fromTypeOrm)],
          TypeOrmAppDatabase,
        ),
      close: () => endPool(pool),
    },
    'nest_outbox.inbox',
  );
}

async function firstPartyPrismaDatabase(postgres: TestPostgres, name: string): Promise<RecipeDatabase> {
  const url = await postgres.createDatabase(name);
  const pool = new pg.Pool({ connectionString: url, max: 4 });
  await prepareFirstParty(fromPg(pool));
  const { PrismaService } = await loadPrisma();
  const application = prismaApplication(PrismaService, url);
  const query: RecipeDatabase['query'] = async (text, params) => (await pool.query(text, params)).rows;

  return withQueries(
    query,
    FIRST_PARTY_TABLES,
    {
      storeClass: PostgresOutboxStore,
      module: () => globalModule([], [application.service, postgresOutboxStore(PrismaService, fromPrisma)], application.database),
      close: () => endPool(pool),
    },
    'nest_outbox.inbox',
  );
}

// ------------------------------------------------------------------ MySqlOutboxStore

/** The first-party store's tables on MySQL (`nest_outbox_*`, in the application's database), and the application's. */
const MYSQL_TABLES = ['nest_outbox_messages', 'nest_outbox_dead_letters', 'nest_outbox_inbox', 'it_orders', 'it_invoices'];

const MYSQL_BUSINESS_TABLES = [
  'CREATE TABLE it_orders (id integer NOT NULL PRIMARY KEY)',
  'CREATE TABLE it_invoices (id integer NOT NULL AUTO_INCREMENT PRIMARY KEY, order_id integer NOT NULL, consumer varchar(255) NOT NULL)',
];

/** The application's data access through TypeORM on MySQL: the same, with MySQL's placeholders. */
@Injectable()
class MySqlTypeOrmAppDatabase extends TypeOrmAppDatabase {
  override async insertOrder(tx: EntityManager, id: number) {
    await tx.query('INSERT INTO it_orders (id) VALUES (?)', [id]);
  }

  override async insertInvoice(tx: EntityManager, orderId: number, consumer: string) {
    await tx.query('INSERT INTO it_invoices (order_id, consumer) VALUES (?, ?)', [orderId, consumer]);
  }
}

const prismaMysqlClient = fixture('prisma-mysql/generated/client.ts');
const prismaMysqlSkip = existsSync(prismaMysqlClient)
  ? undefined
  : 'the MySQL Prisma client is not generated (run `npm run generate:prisma`)';

/** The MySQL `PrismaService`, by a computed path: its generated client is gitignored, and the PostgreSQL project doesn't generate it. */
async function loadMysqlPrisma(): Promise<Type<any>> {
  const { PrismaService } = await import(fixture('prisma-mysql/prisma.service.ts'));
  return PrismaService;
}

/**
 * `MySqlOutboxStore` as an application registers it: a factory provider that injects the database (`token`) and the
 * registry, with which the store registers itself for both contracts.
 */
function mysqlOutboxStore(token: InjectionToken, executor: (db: any) => mysqlStore.SqlExecutor): Provider {
  return {
    provide: mysqlStore.MySqlOutboxStore,
    inject: [token, OutboxStorage],
    useFactory: (db: unknown, storage: OutboxStorage) => new mysqlStore.MySqlOutboxStore({ executor: executor(db) }, storage),
  };
}

/**
 * A MySQL database of this process for a recipe: the store's migrations applied, as `npx nest-outbox migrate` does on
 * deploy, and the application's tables; a pool of one connection to look at it from outside.
 */
async function mysqlDatabase(server: TestMysql, name: string, module: (url: string) => Promise<() => DynamicModule>): Promise<RecipeDatabase> {
  const { url } = await server.createDatabase(name);
  const pool = mysql.createPool({ uri: url, connectionLimit: 1 });
  pool.on('connection', (connection) => {
    connection.query('SET SESSION lock_wait_timeout = 30');
  });
  await new mysqlStore.MySqlOutboxStore({ executor: mysqlStore.fromMysql2(pool) }).migrate();
  for (const statement of MYSQL_BUSINESS_TABLES) {
    await pool.query(statement);
  }

  const query: RecipeDatabase['query'] = async <T>(text: string, params?: unknown[]) => (await pool.query(text, params))[0] as T[];
  return {
    module: await module(url),
    storeClass: mysqlStore.MySqlOutboxStore,
    query,
    async count(table, consumer) {
      const from = table === 'outbox_inbox' ? 'nest_outbox_inbox' : table;
      const where = consumer === undefined ? '' : ' WHERE consumer = ?';
      const rows = await query<{ n: number }>(`SELECT COUNT(*) AS n FROM ${from}${where}`, consumer === undefined ? [] : [consumer]);
      return Number(rows[0]!.n);
    },
    async reset() {
      for (const table of MYSQL_TABLES) {
        await pool.query(`DELETE FROM ${table}`);
      }
    },
    close: () => pool.end(),
  };
}

const mysqlDrizzleModule = async (url: string) => () =>
  globalModule(
    // DrizzleModule opens the instance's pool and ends it in onApplicationShutdown.
    [DrizzleModule.forRoot({ drizzle: drizzleMysql, connection: { uri: url, connectionLimit: 2 } })],
    [mysqlOutboxStore(getDrizzleToken(), mysqlStore.fromDrizzle)],
    DrizzleAppDatabase,
  );

const mysqlTypeOrmModule = async (url: string) => () =>
  globalModule(
    [TypeOrmModule.forRoot({ type: 'mysql', url, poolSize: 2, retryAttempts: 0 })],
    [mysqlOutboxStore(DataSource, mysqlStore.fromTypeOrm)],
    MySqlTypeOrmAppDatabase,
  );

const mysqlPrismaModule = async (url: string) => {
  const PrismaService = await loadMysqlPrisma();
  const application = prismaApplication(PrismaService, url, () => '?');
  return () => globalModule([], [application.service, mysqlOutboxStore(PrismaService, mysqlStore.fromPrisma)], application.database);
};

// ------------------------------------------------------------------ the list

/**
 * Every recipe, given the result of `startPostgres()`. `prefix` keeps the databases of
 * different spec files apart on a shared server.
 */
export function recipes(postgres: TestPostgres | null, reason: string | undefined, prefix: string): Recipe[] {
  const noServer = postgres ? undefined : reason;
  const server = () => postgres!;

  return [
    {
      name: 'DrizzleOutboxStore on PGlite',
      skip: undefined,
      refusesRoot: true,
      open: () => drizzlePgliteDatabase(false),
      openConsumer: () => drizzlePgliteDatabase(true),
    },
    {
      name: 'DrizzleOutboxStore on PostgreSQL',
      skip: noServer,
      refusesRoot: true,
      open: (name) => drizzlePostgresDatabase(server(), `${prefix}_drizzle_${name}`, false),
      openConsumer: (name) => drizzlePostgresDatabase(server(), `${prefix}_drizzle_${name}`, true),
    },
    {
      name: 'TypeOrmOutboxStore on PostgreSQL',
      skip: noServer,
      refusesRoot: true,
      open: (name) => typeOrmDatabase(server(), `${prefix}_typeorm_${name}`),
      openConsumer: (name) => typeOrmDatabase(server(), `${prefix}_typeorm_${name}`),
    },
    {
      name: 'PrismaOutboxStore on PostgreSQL',
      skip: noServer ?? prismaSkip,
      refusesRoot: false,
      open: (name) => prismaDatabase(server(), `${prefix}_prisma_${name}`),
      openConsumer: (name) => prismaDatabase(server(), `${prefix}_prisma_${name}`),
    },
    {
      name: 'PostgresOutboxStore (fromDrizzle) on PGlite',
      skip: undefined,
      refusesRoot: true,
      open: () => firstPartyPgliteDatabase(),
      openConsumer: () => firstPartyPgliteDatabase(),
    },
    {
      name: 'PostgresOutboxStore (fromDrizzle) on PostgreSQL',
      skip: noServer,
      refusesRoot: true,
      open: (name) => firstPartyDrizzleDatabase(server(), `${prefix}_pgdrizzle_${name}`),
      openConsumer: (name) => firstPartyDrizzleDatabase(server(), `${prefix}_pgdrizzle_${name}`),
    },
    {
      name: 'PostgresOutboxStore (fromTypeOrm) on PostgreSQL',
      skip: noServer,
      refusesRoot: true,
      open: (name) => firstPartyTypeOrmDatabase(server(), `${prefix}_pgtypeorm_${name}`),
      openConsumer: (name) => firstPartyTypeOrmDatabase(server(), `${prefix}_pgtypeorm_${name}`),
    },
    {
      name: 'PostgresOutboxStore (fromPrisma) on PostgreSQL',
      skip: noServer ?? prismaSkip,
      refusesRoot: true,
      open: (name) => firstPartyPrismaDatabase(server(), `${prefix}_pgprisma_${name}`),
      openConsumer: (name) => firstPartyPrismaDatabase(server(), `${prefix}_pgprisma_${name}`),
    },
  ];
}

/**
 * The MySQL recipes, given the result of `startMysql()`: the first-party store through Drizzle, TypeORM and Prisma.
 * `prefix` keeps the databases of different spec files apart (keep it short: MySQL names hold 64 characters).
 */
export function mysqlRecipes(server: TestMysql | null, reason: string | undefined, prefix: string): Recipe[] {
  const skip = server ? undefined : reason;
  const recipe = (name: string, tag: string, module: (url: string) => Promise<() => DynamicModule>, extraSkip?: string): Recipe => {
    const open = (database: string) => mysqlDatabase(server!, `${prefix}_${tag}_${database}`, module);
    return { name, skip: skip ?? extraSkip, refusesRoot: true, open, openConsumer: open };
  };

  return [
    recipe('MySqlOutboxStore (fromDrizzle) on MySQL', 'mydrizzle', mysqlDrizzleModule),
    recipe('MySqlOutboxStore (fromTypeOrm) on MySQL', 'mytypeorm', mysqlTypeOrmModule),
    recipe('MySqlOutboxStore (fromPrisma) on MySQL', 'myprisma', mysqlPrismaModule, prismaMysqlSkip),
  ];
}

/**
 * The recipes of the project's database family (`sqlDialect`, vitest.config.ts): PostgreSQL's and PGlite's, or MySQL's,
 * with what ends the server's databases of this process (await it in `afterAll`).
 */
export async function integrationRecipes(prefix: string): Promise<{ recipes: Recipe[]; stop(): Promise<void> }> {
  if (inject('sqlDialect') === 'mysql') {
    const { mysql: server, reason } = await startMysql();
    return { recipes: mysqlRecipes(server, reason, prefix), stop: async () => server?.stop() };
  }

  const { postgres, reason } = await startPostgres();
  return { recipes: recipes(postgres, reason, prefix), stop: async () => postgres?.stop() };
}

/** The recipe's name, with the reason when it is skipped. */
export const titleOf = (recipe: Recipe) =>
  recipe.skip ? `${recipe.name} (skipped: ${recipe.skip.split('\n')[0]})` : recipe.name;

/**
 * Moves `Date.now()` forward without faking timers, so a lease or a backoff can be skipped
 * while sockets, pools and the relays' poll loops keep running on real time.
 */
export function controllableClock() {
  const realNow = Date.now.bind(Date);
  let offset = 0;
  const spy = vi.spyOn(Date, 'now').mockImplementation(() => realNow() + offset);

  return {
    advance(ms: number) {
      offset += ms;
    },
    restore: () => spy.mockRestore(),
  };
}

/** Subscribes to the outbox's diagnostics channels until `stop()`. */
export async function diagnostics() {
  const { subscribe, unsubscribe } = await import('node:diagnostics_channel');
  const received: Array<{ channel: string; event: any }> = [];
  const names = ['published', 'retry-scheduled', 'dead-lettered', 'lease-lost'].map((type) => `nestjs:outbox:${type}`);
  const listeners = names.map((name) => {
    const listener = (event: unknown) => received.push({ channel: name, event });
    subscribe(name, listener);
    return () => unsubscribe(name, listener);
  });

  return {
    received,
    /** The channels an event about `messageId` went to, in order. */
    of: (messageId: string) => received.filter((entry) => entry.event.message.id === messageId).map((entry) => entry.channel),
    stop: () => listeners.forEach((stop) => stop()),
  };
}
