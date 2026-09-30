/**
 * PostgresOutboxStore's migrations, on the kit's StoreSchema (@nestjs/store-kit's own suite covers its machinery with a
 * schema of its own): a new database, a rerun, processes migrating at once, `migrationSql()` against what `migrate()`
 * applies and under which lock, the tables and indexes, a colliding table, a schema behind the code (and ahead of it),
 * the production default, the default isolation, the options, and a first call inside the application's transaction.
 */
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PGlite } from '@electric-sql/pglite';
import { drizzle } from 'drizzle-orm/pglite';
import { migrate as drizzleMigrate } from 'drizzle-orm/pglite/migrator';
import { createPool as createMysqlPool } from 'mysql2/promise';
import pg from 'pg';
import * as root from '../../lib/index.js';
import {
  fromDrizzle,
  fromPg,
  OutboxSchemaError,
  PostgresOutboxStore,
  type SqlExecutor,
  type SqlTransaction,
} from '../../lib/postgres/index.js';
import { fromMysql2 } from '../../lib/mysql/index.js';
import { postgresOutboxSchema } from '../../lib/postgres/migrations/index.js';
import { endPool } from '../support/postgres.js';
import { message, testDatabase } from './support.js';

const { database, reason } = await testDatabase('store_migrations');
const pools: pg.Pool[] = [];

/** In a describe of tests that run on PostgreSQL: skips them, with the reason, where there's none. */
const onPostgres = () =>
  beforeEach((context) => {
    if (reason) {
      context.skip(reason);
    }
  });

afterAll(async () => {
  await Promise.all(pools.map((pool) => endPool(pool)));
});

/** A pool of its own, as each process has. */
const pool = () => {
  const opened = new pg.Pool({ connectionString: database!.url, max: 2 });
  pools.push(opened);
  return opened;
};

const store = (schema: string, options: { migrate?: boolean; executor?: SqlExecutor<'postgres'> } = {}) =>
  new PostgresOutboxStore({ executor: options.executor ?? fromPg(pool()), schema, migrate: options.migrate });

/** An executor that records every statement it runs, and its parameters. */
function recording(executor: SqlExecutor<'postgres'>): { executor: SqlExecutor<'postgres'>; statements: Array<{ text: string; params?: readonly unknown[] }> } {
  const statements: Array<{ text: string; params?: readonly unknown[] }> = [];
  const record = (tx: SqlTransaction): SqlTransaction => ({
    query: (text, params) => {
      statements.push({ text, params });
      return tx.query(text, params);
    },
  });
  return {
    statements,
    executor: {
      dialect: executor.dialect,
      query: (text, params) => {
        statements.push({ text, params });
        return executor.query(text, params);
      },
      transaction: (work, options) => executor.transaction((tx) => work(record(tx)), options),
      wrapTransaction: (transaction) => record(executor.wrapTransaction(transaction)),
    },
  };
}

/** The statements that change the schema: not the lock, and not the reads of what it has. */
const changes = (statements: Array<{ text: string }>) => statements.map((statement) => statement.text).filter((text) => !text.startsWith('SELECT'));

const rows = async (sql: string, params: unknown[] = []) => (await database!.admin.query(sql, params)).rows;

/** Everything about a schema's tables that migrations define, with the schema's name taken out. */
async function catalog(schema: string) {
  const anonymize = (value: unknown) => JSON.parse(JSON.stringify(value).replaceAll(schema, '<schema>'));
  return anonymize({
    columns: await rows(
      `SELECT table_name, column_name, ordinal_position, data_type, is_nullable, column_default, is_identity, identity_generation
       FROM information_schema.columns WHERE table_schema = $1 ORDER BY table_name, ordinal_position`,
      [schema],
    ),
    constraints: await rows(
      `SELECT conrelid::regclass::text AS table, conname, pg_get_constraintdef(oid) AS definition
       FROM pg_constraint WHERE connamespace = $1::regnamespace ORDER BY 1, 2`,
      [schema],
    ),
    indexes: await rows('SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = $1 ORDER BY tablename, indexname', [schema]),
    versions: await rows(`SELECT version, name FROM "${schema}".migrations ORDER BY version`),
  });
}

const tables = async (schema: string) =>
  (await rows('SELECT table_name FROM information_schema.tables WHERE table_schema = $1 ORDER BY table_name', [schema])).map((row) => row.table_name);

describe('migrate()', () => {
  onPostgres();

  it('creates the schema, its tables and the version record on a new database, and applies nothing the second time', async () => {
    const first = store('m_fresh');
    expect(await first.migrate()).toEqual([1]);
    expect(await tables('m_fresh')).toEqual(['dead_letters', 'inbox', 'messages', 'migrations']);
    expect(await rows('SELECT version, name FROM m_fresh.migrations')).toEqual([{ version: 1, name: 'initial' }]);
    expect(PostgresOutboxStore.schemaVersion).toBe(1);

    expect(await first.migrate()).toEqual([]);
    expect(await store('m_fresh').migrate()).toEqual([]);
    expect(await rows('SELECT count(*)::int AS n FROM m_fresh.migrations')).toEqual([{ n: 1 }]);
    await expect(store('m_fresh', { migrate: false }).onModuleInit()).resolves.toBeUndefined();
  });

  it('applies the migrations once when processes start together, each on its own connections', async () => {
    const stores = Array.from({ length: 8 }, () => store('m_together'));
    const applied = await Promise.all(stores.map((s) => s.migrate()));
    expect(applied.filter((versions) => versions.length > 0)).toEqual([[1]]);

    const starting = Array.from({ length: 8 }, () => store('m_starting'));
    await Promise.all(starting.map((s) => s.onModuleInit()));
    expect(await rows('SELECT version FROM m_starting.migrations')).toEqual([{ version: 1 }]);
    expect(await starting[3]!.recordInbox(undefined, 'billing', 'm-1', 1)).toBe(true);
  });

  it('runs the statements migrationSql() prints under its lock, and a database migrated with them is the same as one migrate() made', async () => {
    const recorder = recording(fromPg(pool()));
    await store('m_migrated', { executor: recorder.executor }).migrate();
    expect(recorder.statements[0]).toEqual({
      text: 'SELECT pg_advisory_xact_lock(hashtext($1::text))::text AS locked',
      params: ['@nestjs/outbox:migrate:m_migrated'],
    });
    const script = PostgresOutboxStore.migrationSql({ schema: 'm_migrated' });
    expect(script).toBe(
      `-- @nestjs/outbox: PostgresOutboxStore's schema "m_migrated", from version 0 to 1.\n-- Run it in one transaction.\n\n` +
        `${changes(recorder.statements).map((statement) => `${statement};`).join('\n\n')}\n`,
    );
    expect(changes(recorder.statements)).toEqual(postgresOutboxSchema.statements({ schema: 'm_migrated' }));

    // As a team applies it with its own tool: one script, in one transaction.
    const client = await pool().connect();
    try {
      await client.query(`BEGIN; ${PostgresOutboxStore.migrationSql({ schema: 'm_script' })} COMMIT;`);
    } finally {
      client.release();
    }
    expect(await catalog('m_script')).toEqual(await catalog('m_migrated'));
    await expect(store('m_script', { migrate: false }).onModuleInit()).resolves.toBeUndefined();
  });

  it("has the hand-written stores' keys and indexes (tests/fixtures/drizzle/0001_outbox.sql), one of them partial", async () => {
    await store('m_indexes').migrate();
    const indexes = await rows("SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'm_indexes' AND tablename <> 'migrations' ORDER BY 1");
    expect(indexes).toEqual([
      { indexname: 'dead_letters_pkey', indexdef: 'CREATE UNIQUE INDEX dead_letters_pkey ON m_indexes.dead_letters USING btree (id)' },
      {
        indexname: 'dead_letters_topic_failed_at',
        indexdef: 'CREATE INDEX dead_letters_topic_failed_at ON m_indexes.dead_letters USING btree (topic, failed_at)',
      },
      { indexname: 'inbox_pkey', indexdef: 'CREATE UNIQUE INDEX inbox_pkey ON m_indexes.inbox USING btree (consumer, message_id)' },
      { indexname: 'inbox_processed_at', indexdef: 'CREATE INDEX inbox_processed_at ON m_indexes.inbox USING btree (processed_at)' },
      { indexname: 'messages_id_key', indexdef: 'CREATE UNIQUE INDEX messages_id_key ON m_indexes.messages USING btree (id)' },
      {
        indexname: 'messages_key_seq',
        indexdef: 'CREATE INDEX messages_key_seq ON m_indexes.messages USING btree (key, seq) WHERE (key IS NOT NULL)',
      },
      { indexname: 'messages_pkey', indexdef: 'CREATE UNIQUE INDEX messages_pkey ON m_indexes.messages USING btree (seq)' },
    ]);
  });

  it("reads a claim's rows in seq order off the primary key, probing each keyed one's older messages through the partial index", async () => {
    await store('m_plan').migrate();
    await rows(`INSERT INTO m_plan.messages (id, topic, headers, key, created_at, available_at)
      SELECT 'm-' || i, 't', '{}', CASE WHEN i % 3 = 0 THEN NULL ELSE 'order-' || (i % 500) END, 0, 0 FROM generate_series(1, 20000) i`);
    await rows('ANALYZE m_plan.messages');
    const plan = await rows(`EXPLAIN (COSTS OFF)
      SELECT m.seq FROM m_plan.messages m
      WHERE m.available_at <= 10 AND (m.lease_until IS NULL OR m.lease_until <= 10)
        AND (m.key IS NULL OR NOT EXISTS (
          SELECT 1 FROM m_plan.messages older
          WHERE older.key = m.key AND older.seq < m.seq AND (older.available_at > 10 OR older.lease_until > 10)))
      ORDER BY m.seq LIMIT 50 FOR UPDATE OF m SKIP LOCKED`);
    const text = plan.map((row) => row['QUERY PLAN']).join('\n');
    expect(text).toContain('Index Scan using messages_pkey');
    expect(text).toContain('messages_key_seq');
  });

  it("fails on a schema that has other tables of the store's names, and creates nothing", async () => {
    await rows('CREATE SCHEMA m_taken');
    await rows('CREATE TABLE m_taken.inbox (id serial PRIMARY KEY)');
    const error = await store('m_taken').migrate().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(OutboxSchemaError);
    expect(error).toMatchObject({ schema: 'm_taken', version: 0, requiredVersion: 1, cause: { message: 'relation "inbox" already exists' } });
    expect((error as Error).message).toBe(
      'PostgresOutboxStore: migrating schema "m_taken" from version 0 to 1 failed, and nothing was applied: relation "inbox" already exists',
    );
    expect(await tables('m_taken')).toEqual(['inbox']);
  });
});

describe('a schema behind the code', () => {
  onPostgres();

  it('fails the startup (and every call) with an OutboxSchemaError that says how to migrate, changing nothing', async () => {
    const behind = store('m_behind', { migrate: false });
    const error = await behind.onModuleInit().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(OutboxSchemaError);
    expect(error).toBeInstanceOf(root.OutboxError);
    // The stores' subpaths export it (/postgres and /mysql), not the package's root.
    expect('OutboxSchemaError' in root).toBe(false);
    expect(error).toMatchObject({ name: 'OutboxSchemaError', schema: 'm_behind', version: 0, requiredVersion: 1 });
    expect((error as Error).message).toBe(
      'PostgresOutboxStore: schema "m_behind" is at version 0, and this version of @nestjs/outbox needs version 1. Apply its migrations: ' +
        'set `migrate: true` to apply them at startup, run `npx nest-outbox migrate --url <database url> --schema m_behind`, ' +
        "or apply `PostgresOutboxStore.migrationSql({ schema: 'm_behind', from: 0 })` with your migration tool.",
    );
    await expect(behind.stats(1)).rejects.toThrow(OutboxSchemaError);
    await expect(behind.claim({ owner: 'r1', now: 1, leaseMs: 1_000, limit: 10 })).rejects.toThrow(OutboxSchemaError);
    expect(await rows("SELECT nspname FROM pg_namespace WHERE nspname = 'm_behind'")).toEqual([]);

    // Checked again at the next call: migrated meanwhile, it serves.
    await store('m_behind').migrate();
    expect(await behind.stats(1)).toMatchObject({ pending: 0 });
  });

  it('serves a schema ahead of the code, which a newer version of the package migrated during a rolling deploy', async () => {
    await store('m_ahead').migrate();
    await rows("INSERT INTO m_ahead.migrations (version, name) VALUES (2, 'newer')");

    const older = store('m_ahead', { migrate: false });
    await expect(older.onModuleInit()).resolves.toBeUndefined();
    expect(await older.recordInbox(undefined, 'billing', 'm-1', 1)).toBe(true);
    expect(await store('m_ahead').migrate()).toEqual([]);
  });
});

describe('options', () => {
  onPostgres();

  it("don't migrate by default with NODE_ENV=production, and do otherwise", async () => {
    const previous = process.env.NODE_ENV;
    try {
      process.env.NODE_ENV = 'production';
      await expect(store('m_production').onModuleInit()).rejects.toThrow(OutboxSchemaError);
      process.env.NODE_ENV = 'development';
      await expect(store('m_production').onModuleInit()).resolves.toBeUndefined();
      delete process.env.NODE_ENV;
      await expect(store('m_unset').onModuleInit()).resolves.toBeUndefined();
    } finally {
      process.env.NODE_ENV = previous;
    }
    expect(await tables('m_production')).toContain('messages');
  });

  it("refuse connections whose default isolation isn't READ COMMITTED, before migrating (its races would fail)", async () => {
    const serializable = new pg.Pool({ connectionString: database!.url, max: 1, options: '-c default_transaction_isolation=serializable' });
    pools.push(serializable);
    const unready = new PostgresOutboxStore({ executor: fromPg(serializable), schema: 'm_isolation' });
    await expect(unready.onModuleInit()).rejects.toThrow(
      "PostgresOutboxStore needs the database's default transaction isolation to be READ COMMITTED (PostgreSQL's default), not serializable: its statements race each other",
    );
    await expect(unready.stats(1)).rejects.toThrow('not serializable');
    expect(await tables('m_isolation')).toEqual([]);
  });

  it('take a schema name of letters, digits and underscores, quoted in every statement', async () => {
    for (const schema of ['bad-name', '1st', '', 'x'.repeat(64), 'a"b', 'a$1']) {
      expect(() => new PostgresOutboxStore({ executor: fromPg(pool()), schema })).toThrow(TypeError);
      expect(() => PostgresOutboxStore.migrationSql({ schema })).toThrow(`PostgresOutboxStore: invalid schema ${JSON.stringify(schema)}.`);
    }
    expect(await store('Mixed_Case').migrate()).toEqual([1]);
    expect(await tables('Mixed_Case')).toContain('messages');
  });

  it('refuse an executor that is none or of another database, and a migrate that is no boolean', () => {
    expect(() => new PostgresOutboxStore({ executor: {} as SqlExecutor<'postgres'> })).toThrow('PostgresOutboxStore: `executor` must be a SqlExecutor');
    const executor = fromPg(pool());
    const mysql = {
      dialect: 'mysql',
      query: executor.query.bind(executor),
      transaction: executor.transaction.bind(executor),
      wrapTransaction: executor.wrapTransaction.bind(executor),
    };
    // A MySQL executor is a compile error first (the options take SqlExecutor<'postgres'>), then a TypeError.
    // @ts-expect-error
    expect(() => new PostgresOutboxStore({ executor: mysql as SqlExecutor<'mysql'> })).toThrow(
      "PostgresOutboxStore runs on PostgreSQL, and `executor` is a MySQL executor: import the executor from '@nestjs/outbox/postgres' (fromPg, fromDrizzle, fromTypeOrm, fromPrisma or fromKysely).",
    );
    // The executors @nestjs/outbox/mysql exports (nothing connects: a pool opens its connections at the first statement).
    const mysqlPool = createMysqlPool({ host: '127.0.0.1', port: 1, connectionLimit: 1 });
    // @ts-expect-error
    expect(() => new PostgresOutboxStore({ executor: fromMysql2(mysqlPool) })).toThrow(
      "PostgresOutboxStore runs on PostgreSQL, and `executor` is a MySQL executor: import the executor from '@nestjs/outbox/postgres'",
    );
    expect(() => fromPg(mysqlPool as never)).toThrow('fromPg() takes a node-postgres Pool (or a connected Client), not a mysql2 pool or connection');
    expect(() => new PostgresOutboxStore({ executor: fromPg(pool()), migrate: 'yes' as unknown as boolean })).toThrow(
      'PostgresOutboxStore: `migrate` must be true or false, not "yes".',
    );
  });
});

describe('migrationSql()', () => {
  it('prints the default schema from a new database, a range of versions, and never a downgrade', () => {
    const script = PostgresOutboxStore.migrationSql();
    expect(script).toMatch(/^-- @nestjs\/outbox: PostgresOutboxStore's schema "nest_outbox", from version 0 to 1\.\n/);
    expect(script).toContain('CREATE SCHEMA IF NOT EXISTS "nest_outbox";');
    expect(script).toContain(`INSERT INTO "nest_outbox".migrations (version, name) VALUES (1, 'initial');`);
    expect(PostgresOutboxStore.migrationSql({ from: 1 })).not.toContain('CREATE');

    expect(() => PostgresOutboxStore.migrationSql({ from: 1, to: 0 })).toThrow(RangeError);
    expect(() => PostgresOutboxStore.migrationSql({ from: 1, to: 0 })).toThrow("downgrades aren't supported");
    expect(() => PostgresOutboxStore.migrationSql({ to: 2 })).toThrow('no migrations lead from version 0 to 2');
    expect(() => PostgresOutboxStore.migrationSql({ from: -1 })).toThrow(RangeError);
  });
});

describe("drizzle-kit's statement breakpoints, on PGlite", () => {
  it("runs migrationSql({ statementBreakpoints: true }) through Drizzle's migrator, one statement at a time, and the store serves on it", async () => {
    const [migrated, byDrizzle] = [new PGlite(), new PGlite()];
    const folder = mkdtempSync(join(tmpdir(), 'obx-drizzle-'));
    mkdirSync(join(folder, 'meta'));
    writeFileSync(join(folder, '0000_outbox.sql'), PostgresOutboxStore.migrationSql({ statementBreakpoints: true }));
    writeFileSync(
      join(folder, 'meta', '_journal.json'),
      JSON.stringify({ version: '7', dialect: 'postgresql', entries: [{ idx: 0, version: '7', when: 1790000000000, tag: '0000_outbox', breakpoints: true }] }),
    );
    try {
      await new PostgresOutboxStore({ executor: fromDrizzle(drizzle(migrated)) }).migrate();
      const db = drizzle(byDrizzle);
      await drizzleMigrate(db, { migrationsFolder: folder });

      const indexes = (pglite: PGlite) =>
        pglite.query("SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = 'nest_outbox' ORDER BY tablename, indexname").then((result) => result.rows);
      expect(await indexes(byDrizzle)).toEqual(await indexes(migrated));

      const served = new PostgresOutboxStore({ executor: fromDrizzle(db), migrate: false });
      await expect(served.onModuleInit()).resolves.toBeUndefined();
      await db.transaction((tx) => served.add(tx, [message('order.placed', 'order-1')]));
      expect((await served.claim({ owner: 'r1', now: 1, leaseMs: 1_000, limit: 10 })).map((m) => m.topic)).toEqual(['order.placed']);
    } finally {
      rmSync(folder, { recursive: true, force: true });
      await migrated.close();
      await byDrizzle.close();
    }
  });
});

describe('a first call inside the application transaction, on PGlite', () => {
  it("checks the schema through that transaction instead of waiting for it, and can't migrate in it", async () => {
    const pglite = new PGlite();
    const db = drizzle(pglite);
    try {
      const unmigrated = new PostgresOutboxStore({ executor: fromDrizzle(db) });
      await expect(db.transaction((tx) => unmigrated.add(tx, [message('order.placed')]))).rejects.toThrow(
        "is at version 0, and this version of @nestjs/outbox needs version 1. The store applies its migrations when the application starts (onModuleInit), or at its first call outside a transaction: it can't apply them in yours.",
      );
      await expect(db.transaction((tx) => unmigrated.recordInbox(tx, 'billing', 'm-1', 1))).rejects.toThrow(OutboxSchemaError);

      await new PostgresOutboxStore({ executor: fromDrizzle(db) }).migrate();
      const fresh = new PostgresOutboxStore({ executor: fromDrizzle(db) });
      await db.transaction((tx) => fresh.add(tx, [message('order.placed')]));
      expect(await fresh.stats(1)).toMatchObject({ pending: 1, ready: 1 });

      const inboxFirst = new PostgresOutboxStore({ executor: fromDrizzle(db) });
      expect(await db.transaction((tx) => inboxFirst.recordInbox(tx, 'billing', 'm-1', 1))).toBe(true);
    } finally {
      await pglite.close();
    }
  });
});
