/**
 * MySqlOutboxStore's migrations, on the kit's MySQL StoreSchema (@nestjs/store-kit's own suite covers its machinery with
 * a schema of its own): a new database and a rerun, processes migrating at once, `migrationStatements()` and
 * `migrationSql()` against what `migrate()` runs, the tables, keys and collations, a run that failed halfway resuming
 * where it stopped, `sql_require_primary_key`, a schema behind the code (and ahead of it), the production default, the
 * options and the server checks in the store's words, drizzle-kit's MySQL migrator, and a first call inside the
 * application's transaction on a pool of one connection.
 */
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { drizzle } from 'drizzle-orm/mysql2';
import { migrate as drizzleMigrate } from 'drizzle-orm/mysql2/migrator';
import mysql from 'mysql2/promise';
import pg from 'pg';
import * as root from '../../lib/index.js';
import { fromMysql2, MySqlOutboxStore, OutboxSchemaError, type SqlExecutor, type SqlTransaction } from '../../lib/mysql/index.js';
import { mysqlOutboxSchema } from '../../lib/mysql/migrations/index.js';
import { fromPg, OutboxSchemaError as PostgresOutboxSchemaError } from '../../lib/postgres/index.js';
import { message, onMysql, recording, rows, testDatabase } from './support.js';

const { database, reason } = await testDatabase('migrations');
const pools: mysql.Pool[] = [];

// Each test's pools end with it: the server is shared, and a file's pools would otherwise add up.
afterEach(async () => {
  await Promise.all(pools.splice(0).map((opened) => opened.end()));
});

/** A pool of its own, as each process has: one connection is all migrate() uses. */
const pool = (options: mysql.PoolOptions = {}) => {
  const opened = mysql.createPool({ uri: database!.url, connectionLimit: 1, ...options });
  pools.push(opened);
  return opened;
};

const store = (schema: string, options: { migrate?: boolean; executor?: SqlExecutor } = {}) =>
  new MySqlOutboxStore({ executor: options.executor ?? fromMysql2(pool()), schema, migrate: options.migrate });

const admin = <T = Record<string, unknown>>(sql: string, params: unknown[] = []) => rows<T>(database!.admin, sql, params);

/** The tables whose names start with `<schema>_`. */
const tables = async (schema: string) =>
  (
    await admin<{ name: string }>(
      'SELECT TABLE_NAME AS name FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND LEFT(TABLE_NAME, ?) = ? ORDER BY TABLE_NAME',
      [schema.length + 1, `${schema}_`],
    )
  ).map((row) => row.name);

/** The DDL a run sent, in order: not the lock, the reads, nor the progress records. */
const ddl = (statements: Array<{ text: string }>) => statements.map((statement) => statement.text).filter((text) => /^(CREATE|ALTER|DROP)\b/.test(text));

/** Everything about a schema's tables that migrations define, with the schema's name taken out. */
async function catalog(schema: string) {
  const read = (sql: string) => admin(sql, [schema.length + 1, `${schema}_`]);
  const anonymize = (value: unknown) => JSON.parse(JSON.stringify(value).replaceAll(`${schema}_`, '<schema>_'));
  return anonymize({
    columns: await read(
      `SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name, ORDINAL_POSITION AS position, COLUMN_TYPE AS type, IS_NULLABLE AS nullable,
  COLUMN_DEFAULT AS column_default, CHARACTER_SET_NAME AS charset, COLLATION_NAME AS collation_name, COLUMN_KEY AS column_key, EXTRA AS extra
FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND LEFT(TABLE_NAME, ?) = ? ORDER BY TABLE_NAME, ORDINAL_POSITION`,
    ),
    indexes: await read(
      `SELECT TABLE_NAME AS table_name, INDEX_NAME AS index_name, SEQ_IN_INDEX AS seq, COLUMN_NAME AS column_name, NON_UNIQUE AS non_unique
FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND LEFT(TABLE_NAME, ?) = ? ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX`,
    ),
    versions: await admin(`SELECT version, name FROM \`${schema}_migrations\` WHERE applied_at IS NOT NULL ORDER BY version`),
  });
}

/** Runs `work` with `NODE_ENV` set to `value` (`undefined`: unset). */
async function withNodeEnv<T>(value: string | undefined, work: () => Promise<T>): Promise<T> {
  const previous = process.env.NODE_ENV;
  try {
    if (value === undefined) {
      delete process.env.NODE_ENV;
    } else {
      process.env.NODE_ENV = value;
    }
    return await work();
  } finally {
    process.env.NODE_ENV = previous;
  }
}

describe('migrate()', () => {
  onMysql(reason);

  it("creates the store's tables, the kit's, and the version record on a new database, and applies nothing the second time", async () => {
    const first = store('m_fresh');
    expect(await first.migrate()).toEqual([1]);
    expect(await tables('m_fresh')).toEqual(['m_fresh_dead_letters', 'm_fresh_inbox', 'm_fresh_locks', 'm_fresh_messages', 'm_fresh_migrations']);
    expect(await admin('SELECT version, name, started, applied, applied_at > 0 AS stamped FROM m_fresh_migrations')).toEqual([
      { version: 1, name: 'initial', started: 3, applied: 3, stamped: 1 },
    ]);
    expect(MySqlOutboxStore.schemaVersion).toBe(1);

    expect(await first.migrate()).toEqual([]);
    expect(await store('m_fresh').migrate()).toEqual([]);
    expect(await admin('SELECT COUNT(*) AS n FROM m_fresh_migrations')).toEqual([{ n: 1 }]);
    await expect(store('m_fresh', { migrate: false }).onModuleInit()).resolves.toBeUndefined();
  });

  it('applies the migrations once when processes start together, each on a connection of its own', async () => {
    const applied = await Promise.all(Array.from({ length: 4 }, () => store('m_together').migrate()));
    expect(applied.filter((versions) => versions.length > 0)).toEqual([[1]]);

    const starting = Array.from({ length: 4 }, () => store('m_starting'));
    await Promise.all(starting.map((s) => s.onModuleInit()));
    expect(await admin('SELECT version FROM m_starting_migrations')).toEqual([{ version: 1 }]);
    expect(await starting[3]!.recordInbox(undefined, 'billing', 'm-1', 1)).toBe(true);
  });

  it('runs the DDL that migrationStatements() lists; a database migrated statement by statement, or with migrationSql(), is the same, and serves', async () => {
    const recorder = recording(fromMysql2(pool()));
    await store('m_migrated', { executor: recorder.executor }).migrate();
    const statements = MySqlOutboxStore.migrationStatements({ schema: 'm_migrated' });
    expect(statements).toEqual(mysqlOutboxSchema.statements({ schema: 'm_migrated' }));
    expect(ddl(recorder.statements)).toEqual(statements.filter((statement) => !statement.startsWith('INSERT')));
    expect(statements.at(-1)).toBe("INSERT INTO `m_migrated_migrations` (version, name, applied_at) VALUES (1, 'initial', UNIX_TIMESTAMP() * 1000)");
    expect(MySqlOutboxStore.migrationSql({ schema: 'm_migrated' })).toBe(
      `-- @nestjs/outbox: MySqlOutboxStore's schema "m_migrated" (tables m_migrated_*), from version 0 to 1.\n` +
        "-- The statements don't run in one transaction: MySQL commits each DDL statement on its own. Apply them in order, each once.\n\n" +
        `${statements.map((statement) => `${statement};`).join('\n\n')}\n`,
    );

    // As a team applies them with its own tool: TypeORM's queryRunner.query() and mysql2 run one statement per call.
    const connection = await database!.admin.getConnection();
    try {
      for (const statement of MySqlOutboxStore.migrationStatements({ schema: 'm_script' })) {
        await connection.query(statement);
      }
    } finally {
      connection.release();
    }
    expect(await catalog('m_script')).toEqual(await catalog('m_migrated'));

    const served = store('m_script', { migrate: false });
    await expect(served.onModuleInit()).resolves.toBeUndefined();
    expect(await served.recordInbox(undefined, 'billing', 'm-1', 1)).toBe(true);
  });

  it('has the keys and indexes of the PostgreSQL store, binary collations on every key column, and no foreign key', async () => {
    await store('m_indexes').migrate();
    const indexes = await admin<{ table_name: string; index_name: string; columns: string; non_unique: number }>(
      `SELECT TABLE_NAME AS table_name, INDEX_NAME AS index_name, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS columns, MIN(NON_UNIQUE) AS non_unique
FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('m_indexes_messages', 'm_indexes_dead_letters', 'm_indexes_inbox')
GROUP BY TABLE_NAME, INDEX_NAME`,
    );
    const order = (index: (typeof indexes)[number]) => `${index.table_name} ${index.index_name}`;
    expect(indexes.sort((a, b) => (order(a) < order(b) ? -1 : 1))).toEqual([
      { table_name: 'm_indexes_dead_letters', index_name: 'PRIMARY', columns: 'id', non_unique: 0 },
      { table_name: 'm_indexes_dead_letters', index_name: 'dead_letters_topic_failed_at', columns: 'topic,failed_at', non_unique: 1 },
      { table_name: 'm_indexes_inbox', index_name: 'PRIMARY', columns: 'consumer,message_id', non_unique: 0 },
      { table_name: 'm_indexes_inbox', index_name: 'inbox_processed_at', columns: 'processed_at', non_unique: 1 },
      { table_name: 'm_indexes_messages', index_name: 'PRIMARY', columns: 'seq', non_unique: 0 },
      { table_name: 'm_indexes_messages', index_name: 'messages_id', columns: 'id', non_unique: 0 },
      { table_name: 'm_indexes_messages', index_name: 'messages_key_seq', columns: 'key,seq', non_unique: 1 },
    ]);
    const text = await admin<{ table_name: string; column_name: string; collation_name: string; length: number }>(
      `SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name, COLLATION_NAME AS collation_name, CHARACTER_MAXIMUM_LENGTH AS length
FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE 'm\\_indexes\\_%' AND DATA_TYPE = 'varchar' AND TABLE_NAME <> 'm_indexes_migrations'
ORDER BY TABLE_NAME, ORDINAL_POSITION`,
    );
    expect(text.every((column) => column.collation_name === 'utf8mb4_0900_bin')).toBe(true);
    expect(text.filter((column) => column.column_name !== 'reason').every((column) => Number(column.length) === 255)).toBe(true);
    expect(await admin("SELECT COLLATION_NAME AS c FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'm_indexes_messages' AND COLUMN_NAME = 'last_error'")).toEqual([
      { c: 'utf8mb4_0900_bin' },
    ]);
    expect(await admin('SELECT CONSTRAINT_NAME FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = DATABASE()')).toEqual([]);
  });

  it('stops at a table someone else took, says where, and resumes there once it is gone, without sending the statements before it again', async () => {
    await admin('CREATE TABLE m_taken_inbox (id int NOT NULL PRIMARY KEY)');
    const error = await store('m_taken').migrate().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(OutboxSchemaError);
    expect(error).toMatchObject({ schema: 'm_taken', version: 0, requiredVersion: 1, cause: { errno: 1050 } });
    expect((error as Error).message).toBe(
      'MySqlOutboxStore: migrating schema "m_taken" from version 0 to 1 stopped at migration 1 (initial), statement 3 of 3: ' +
        "Table 'm_taken_inbox' already exists. The statements before it are applied, and migrating again resumes at it.",
    );
    expect(await admin('SELECT started, applied, applied_at FROM m_taken_migrations WHERE version = 1')).toEqual([{ started: 2, applied: 2, applied_at: null }]);
    await expect(store('m_taken', { migrate: false }).onModuleInit()).rejects.toThrow('is at version 0');

    await admin('DROP TABLE m_taken_inbox');
    const recorder = recording(fromMysql2(pool()));
    expect(await store('m_taken', { executor: recorder.executor }).migrate()).toEqual([1]);
    expect(ddl(recorder.statements).filter((text) => !text.startsWith('CREATE TABLE IF NOT EXISTS'))).toEqual([
      mysqlOutboxSchema.statements({ schema: 'm_taken' })[4],
    ]);
    await expect(store('m_taken', { migrate: false }).onModuleInit()).resolves.toBeUndefined();
  });

  it('gives every table a primary key: it migrates with sql_require_primary_key on, as some managed MySQL sets it', async () => {
    const strict = pool();
    strict.on('connection', (connection) => {
      connection.query('SET SESSION sql_require_primary_key = ON');
    });
    expect(await store('m_keyed', { executor: fromMysql2(strict) }).migrate()).toEqual([1]);
  });
});

describe('a schema behind the code', () => {
  onMysql(reason);

  it('fails the startup (and every call) with an OutboxSchemaError that says how to migrate, changing nothing', async () => {
    const behind = store('m_behind', { migrate: false });
    const error = await behind.onModuleInit().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(OutboxSchemaError);
    expect(error).toBeInstanceOf(root.OutboxError);
    // One class for both dialects' stores, from their subpaths, not the package's root.
    expect(OutboxSchemaError).toBe(PostgresOutboxSchemaError);
    expect('OutboxSchemaError' in root).toBe(false);
    expect(error).toMatchObject({ name: 'OutboxSchemaError', schema: 'm_behind', version: 0, requiredVersion: 1 });
    expect((error as Error).message).toBe(
      'MySqlOutboxStore: schema "m_behind" is at version 0, and this version of @nestjs/outbox needs version 1. Apply its migrations: ' +
        'set `migrate: true` to apply them at startup, run `npx nest-outbox migrate --url <database url> --schema m_behind`, ' +
        "or apply `MySqlOutboxStore.migrationSql({ schema: 'm_behind', from: 0 })` with your migration tool.",
    );
    await expect(behind.stats(1)).rejects.toThrow(OutboxSchemaError);
    await expect(behind.claim({ owner: 'r1', now: 1, leaseMs: 1_000, limit: 10 })).rejects.toThrow(OutboxSchemaError);
    expect(await tables('m_behind')).toEqual([]);

    // Checked again at the next call: migrated meanwhile, it serves.
    await store('m_behind').migrate();
    expect(await behind.stats(1)).toMatchObject({ pending: 0 });
  });

  it('serves a schema ahead of the code, which a newer version of the package migrated during a rolling deploy', async () => {
    await store('m_ahead').migrate();
    await admin("INSERT INTO m_ahead_migrations (version, name, applied_at) VALUES (2, 'newer', 1)");

    const older = store('m_ahead', { migrate: false });
    await expect(older.onModuleInit()).resolves.toBeUndefined();
    expect(await older.recordInbox(undefined, 'billing', 'm-1', 1)).toBe(true);
    expect(await store('m_ahead').migrate()).toEqual([]);
  });
});

describe('options and the server', () => {
  onMysql(reason);

  it("don't migrate by default with NODE_ENV=production, and do otherwise", async () => {
    await withNodeEnv('production', () => expect(store('m_production').onModuleInit()).rejects.toThrow(OutboxSchemaError));
    await withNodeEnv('development', () => expect(store('m_production').onModuleInit()).resolves.toBeUndefined());
    await withNodeEnv(undefined, () => expect(store('m_unset').onModuleInit()).resolves.toBeUndefined());
    expect(await tables('m_production')).toContain('m_production_messages');
  });

  it("refuse a connection without MySQL's strict sql_mode, and one without a database, creating nothing", async () => {
    const lax = pool();
    lax.on('connection', (connection) => {
      connection.query("SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'");
    });
    const unready = store('m_lax', { executor: fromMysql2(lax) });
    await expect(unready.onModuleInit()).rejects.toThrow(
      'MySqlOutboxStore needs a strict sql_mode (STRICT_TRANS_TABLES, MySQL\'s default), and this connection\'s is "NO_ENGINE_SUBSTITUTION"',
    );
    await expect(unready.stats(1)).rejects.toThrow('needs a strict sql_mode');

    const serverOnly = mysql.createPool({ uri: process.env.SQL_TEST_MYSQL_URL!, connectionLimit: 1 });
    pools.push(serverOnly);
    await expect(store('m_nowhere', { executor: fromMysql2(serverOnly) }).onModuleInit()).rejects.toThrow(
      "MySqlOutboxStore keeps its tables in the connection's database, and this connection has none: name one in the pool's or the ORM's settings (its database, or the URL's path).",
    );
    expect([...(await tables('m_lax')), ...(await tables('m_nowhere'))]).toEqual([]);
  });
});

describe('options, without a database', () => {
  // Nothing connects: a pool opens its connections at the first statement.
  const executor = () => fromMysql2(mysql.createPool({ host: '127.0.0.1', port: 1, connectionLimit: 1 }));

  it('take a schema name of lowercase letters, digits and underscores, at most 40 characters, the start of every table name', () => {
    for (const schema of ['Mixed_Case', 'bad-name', '1st', '', 'x'.repeat(41), 'a`b', 'a$1']) {
      const refusal =
        `MySqlOutboxStore: invalid schema ${JSON.stringify(schema)}. Use lowercase letters, digits and underscores, not starting with a digit, at most 40 characters: ` +
        "the store's tables are named <schema>_<table> in the connection's database.";
      expect(() => new MySqlOutboxStore({ executor: executor(), schema })).toThrow(refusal);
      expect(() => MySqlOutboxStore.migrationSql({ schema })).toThrow(refusal);
    }
    expect(() => new MySqlOutboxStore({ executor: executor(), schema: 'x'.repeat(40) })).not.toThrow();
  });

  it('refuse an executor that is none or of another database, naming the subpath to import from, and a migrate that is no boolean', () => {
    expect(() => new MySqlOutboxStore({ executor: {} as SqlExecutor })).toThrow(
      'MySqlOutboxStore: `executor` must be a SqlExecutor, such as fromMysql2(pool), fromDrizzle(db), fromTypeOrm(dataSource), fromPrisma(prisma) or fromKysely(db).',
    );
    const postgres = fromPg(new pg.Pool({ connectionString: 'postgres://nobody@127.0.0.1:1/none' }));
    expect(() => new MySqlOutboxStore({ executor: postgres as never })).toThrow(
      "MySqlOutboxStore runs on MySQL, and `executor` is a PostgreSQL executor: import the executor from '@nestjs/outbox/mysql' (fromMysql2, fromDrizzle, fromTypeOrm, fromPrisma or fromKysely).",
    );
    expect(() => new MySqlOutboxStore({ executor: executor(), migrate: 'yes' as unknown as boolean })).toThrow(
      'MySqlOutboxStore: `migrate` must be true or false, not "yes".',
    );
  });

  it('refuse MariaDB, before any SQL of the store', async () => {
    const statements: string[] = [];
    const db: SqlTransaction = {
      query: async <R extends object>(text: string) => {
        statements.push(text);
        if (text === 'SELECT VERSION() AS version') {
          return [{ version: '11.4.2-MariaDB-ubu2404' }] as R[];
        }
        throw new Error(`no answer to: ${text}`);
      },
      execute: async () => ({ affectedRows: 0 }),
    };
    const mariadb = { dialect: 'mysql', ...db, transaction: (work: (tx: SqlTransaction) => Promise<unknown>) => work(db), wrapTransaction: () => db } as SqlExecutor;
    const refused = new MySqlOutboxStore({ executor: mariadb, schema: 'm_mariadb' });
    await expect(refused.onModuleInit()).rejects.toThrow(
      "MySqlOutboxStore runs on MySQL, and this server is MariaDB (11.4.2-MariaDB-ubu2404): MariaDB isn't supported yet.",
    );
    expect(statements).toEqual(['SELECT VERSION() AS version']);
  });
});

describe('migrationSql() and migrationStatements()', () => {
  it("print the default schema from a new database, a range of versions, and never a downgrade", () => {
    const script = MySqlOutboxStore.migrationSql();
    expect(script).toMatch(/^-- @nestjs\/outbox: MySqlOutboxStore's schema "nest_outbox" \(tables nest_outbox_\*\), from version 0 to 1\.\n/);
    for (const table of ['migrations', 'locks']) {
      expect(script).toContain(`CREATE TABLE IF NOT EXISTS \`nest_outbox_${table}\` (`);
    }
    for (const table of ['messages', 'dead_letters', 'inbox']) {
      expect(script).toContain(`CREATE TABLE \`nest_outbox_${table}\` (`);
    }
    expect(MySqlOutboxStore.migrationStatements({ from: 1 })).toEqual([]);
    expect(MySqlOutboxStore.migrationSql({ statementBreakpoints: true }).split('\n--> statement-breakpoint\n')).toHaveLength(MySqlOutboxStore.migrationStatements().length);

    expect(() => MySqlOutboxStore.migrationSql({ from: 1, to: 0 })).toThrow(RangeError);
    expect(() => MySqlOutboxStore.migrationSql({ from: 1, to: 0 })).toThrow("downgrades aren't supported");
    expect(() => MySqlOutboxStore.migrationStatements({ to: 2 })).toThrow('no migrations lead from version 0 to 2');
  });
});

describe("drizzle-kit's statement breakpoints, through Drizzle's MySQL migrator", () => {
  onMysql(reason);

  it("runs migrationSql({ statementBreakpoints: true }) one statement at a time: the schema is migrate()'s, and the store serves on it", async () => {
    await store('m_by_kit').migrate();
    const folder = mkdtempSync(join(tmpdir(), 'obx-drizzle-mysql-'));
    mkdirSync(join(folder, 'meta'));
    writeFileSync(join(folder, '0000_outbox.sql'), MySqlOutboxStore.migrationSql({ schema: 'm_by_drizzle', statementBreakpoints: true }));
    writeFileSync(
      join(folder, 'meta', '_journal.json'),
      JSON.stringify({ version: '5', dialect: 'mysql', entries: [{ idx: 0, version: '5', when: 1790000000000, tag: '0000_outbox', breakpoints: true }] }),
    );
    try {
      const connection = pool();
      const db = drizzle(connection);
      await drizzleMigrate(db, { migrationsFolder: folder, migrationsTable: 'drizzle_journal_outbox' });
      expect(await catalog('m_by_drizzle')).toEqual(await catalog('m_by_kit'));

      const served = new MySqlOutboxStore({ executor: fromMysql2(connection), schema: 'm_by_drizzle', migrate: false });
      await expect(served.onModuleInit()).resolves.toBeUndefined();
      const tx = await connection.getConnection();
      try {
        await tx.beginTransaction();
        await served.add(tx, [message('order.placed', 'order-1')]);
        await tx.commit();
      } finally {
        tx.release();
      }
      expect((await served.claim({ owner: 'r1', now: 1, leaseMs: 1_000, limit: 10 })).map((m) => m.topic)).toEqual(['order.placed']);
    } finally {
      rmSync(folder, { recursive: true, force: true });
    }
  });
});

describe('a first call inside the application transaction, on a pool of one connection', () => {
  onMysql(reason);

  it("checks the server and the schema through that transaction instead of waiting for another connection, and can't migrate in it", async () => {
    const single = pool();
    const executor = fromMysql2(single);
    const inTransaction = async <T>(work: (tx: mysql.PoolConnection) => Promise<T>): Promise<T> => {
      const connection = await single.getConnection();
      try {
        await connection.beginTransaction();
        const result = await work(connection);
        await connection.commit();
        return result;
      } catch (error) {
        await connection.rollback();
        throw error;
      } finally {
        connection.release();
      }
    };

    const unmigrated = new MySqlOutboxStore({ executor, schema: 'm_first' });
    await expect(inTransaction((tx) => unmigrated.add(tx, [message('order.placed')]))).rejects.toThrow(
      "is at version 0, and this version of @nestjs/outbox needs version 1. The store applies its migrations when the application starts (onModuleInit), or at its first call outside a transaction: it can't apply them in yours.",
    );
    await expect(inTransaction((tx) => unmigrated.recordInbox(tx, 'billing', 'm-1', 1))).rejects.toThrow(OutboxSchemaError);

    await new MySqlOutboxStore({ executor, schema: 'm_first' }).migrate();
    const fresh = new MySqlOutboxStore({ executor, schema: 'm_first' });
    await inTransaction((tx) => fresh.add(tx, [message('order.placed', 'order-1')]));
    expect(await fresh.stats(1)).toMatchObject({ pending: 1, ready: 1 });

    const inboxFirst = new MySqlOutboxStore({ executor, schema: 'm_first' });
    expect(await inTransaction((tx) => inboxFirst.recordInbox(tx, 'billing', 'm-1', 1))).toBe(true);
  });
});
