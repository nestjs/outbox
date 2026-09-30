/**
 * MySQL for tests: an existing server named by `SQL_TEST_MYSQL_URL` (`mysql://root:<password>@127.0.0.1:3306`), never a
 * server of the tests' own. Resolves to `null`, with the reason, when it's unset or unreachable, so the tests that need
 * it skip with a clear message instead of failing.
 *
 * The server is shared (a local one may belong to other projects' tests too): every database this helper creates is
 * named `<databasePrefix><name>_<pid>_<random>` (`obx_<host>_...`, the same scheme as on PostgreSQL), and nothing is
 * dropped except those: `stop()` (awaited in `afterAll`) drops the ones this process created, and
 * `sweepStaleMysqlDatabases()` (at the first `createDatabase()`, and in the MySQL project's global setup and teardown)
 * drops those under `databasePrefix` whose process is gone. No server setting is changed. MySQL names hold 64
 * characters: keep `name` under 40.
 */
import mysql from 'mysql2/promise';
import { databasePrefix } from './postgres.js';

export interface TestMysql {
  /** The URL of one of this helper's databases. */
  url(database: string): string;
  /** Creates a database of this process (`<databasePrefix><name>_<pid>_<random>`) and returns its name and URL. */
  createDatabase(name: string): Promise<{ name: string; url: string }>;
  /** Drops this process's databases. Await it. */
  stop(): Promise<void>;
}

export type TestMysqlResult = { mysql: TestMysql; reason?: undefined } | { mysql: null; reason: string };

export async function startMysql(): Promise<TestMysqlResult> {
  const connectionString = process.env.SQL_TEST_MYSQL_URL;
  if (!connectionString) {
    return { mysql: null, reason: 'SQL_TEST_MYSQL_URL is not set' };
  }

  try {
    await withConnection(connectionString, async (connection) => {
      await connection.query('SELECT 1');
    });
  } catch (error) {
    // The URL may hold a password: only the error's message goes into the reason.
    return { mysql: null, reason: `SQL_TEST_MYSQL_URL is unreachable: ${(error as Error).message}` };
  }

  const suffix = `_${process.pid}_${Math.random().toString(36).slice(2, 8)}`;
  const created: string[] = [];
  let swept: Promise<unknown> | undefined;
  const url = (database: string) => {
    const target = new URL(connectionString);
    target.pathname = `/${database}`;
    return target.toString();
  };
  return {
    mysql: {
      url,
      async createDatabase(name) {
        await (swept ??= sweepStaleMysqlDatabases(connectionString));
        const database = databasePrefix + name + suffix;
        if (database.length > 64) {
          throw new Error(`The test database's name "${database}" is longer than MySQL's 64 characters: shorten "${name}".`);
        }
        await withConnection(connectionString, async (connection) => {
          await connection.query(`DROP DATABASE IF EXISTS \`${database}\``);
          await connection.query(`CREATE DATABASE \`${database}\``);
        });
        created.push(database);
        return { name: database, url: url(database) };
      },
      async stop() {
        const names = created.splice(0);
        if (names.length === 0) {
          return;
        }

        await withConnection(connectionString, async (connection) => {
          for (const name of names) {
            await connection.query(`DROP DATABASE IF EXISTS \`${name}\``);
          }
        });
      },
    },
  };
}

/**
 * Drops this helper's databases (`databasePrefix`) whose process is no longer alive. Never touches any other
 * database, nor one whose process still runs.
 */
export async function sweepStaleMysqlDatabases(connectionString: string): Promise<string[]> {
  return withConnection(connectionString, async (connection) => {
    const [rows] = await connection.query<mysql.RowDataPacket[]>('SELECT SCHEMA_NAME AS name FROM information_schema.SCHEMATA WHERE LEFT(SCHEMA_NAME, ?) = ?', [
      databasePrefix.length,
      databasePrefix,
    ]);
    const dropped: string[] = [];
    for (const { name } of rows as Array<{ name: string }>) {
      const pid = Number(/_(\d+)_[a-z0-9]+$/.exec(name)?.[1]);
      if (!name.startsWith(databasePrefix) || !Number.isSafeInteger(pid) || pid === process.pid || isAlive(pid)) {
        continue;
      }

      // Another process sweeping at the same time may drop it first.
      await connection.query(`DROP DATABASE IF EXISTS \`${name}\``).then(
        () => dropped.push(name),
        () => undefined,
      );
    }
    return dropped;
  });
}

function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    // EPERM: it exists, it's just not ours to signal.
    return (error as NodeJS.ErrnoException).code === 'EPERM';
  }
}

async function withConnection<T>(connectionString: string, work: (connection: mysql.Connection) => Promise<T>): Promise<T> {
  const connection = await mysql.createConnection({ uri: connectionString, connectTimeout: 5_000 });
  try {
    // DROP DATABASE waits for the transactions that hold its tables, a year by default: a leaked one fails the
    // teardown after 30 seconds instead.
    await connection.query('SET SESSION lock_wait_timeout = 30');
    return await work(connection);
  } finally {
    await connection.end();
  }
}
