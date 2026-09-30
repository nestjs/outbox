/**
 * The executor types a user writes, from this package's entries: `SqlExecutor` from `@nestjs/outbox/postgres` is a
 * PostgreSQL executor, which PostgresOutboxStore takes, and from `@nestjs/outbox/mysql` a MySQL one, which
 * MySqlOutboxStore takes, so `const executor: SqlExecutor = fromDrizzle(db)` compiles for its dialect's store. The
 * other dialect's is a compile error (the `@ts-expect-error` lines, which the package's typecheck checks) and a
 * TypeError at run time. Nothing connects: the pools open their connections at their first statement.
 */
import { drizzle as drizzleMysql } from 'drizzle-orm/mysql2';
import { drizzle as drizzlePostgres } from 'drizzle-orm/node-postgres';
import { createPool } from 'mysql2/promise';
import pg from 'pg';
import { fromDrizzle as fromMysqlDrizzle, MySqlOutboxStore, type SqlExecutor as MySqlSqlExecutor } from '../../lib/mysql/index.js';
import { fromDrizzle, PostgresOutboxStore, type SqlExecutor } from '../../lib/postgres/index.js';

describe("the executor types of the package's entries", () => {
  it("takes an executor annotated with /postgres's SqlExecutor in PostgresOutboxStore, and with /mysql's in MySqlOutboxStore, and neither in the other", async () => {
    const pgPool = new pg.Pool({ connectionString: 'postgres://nobody@127.0.0.1:1/none' });
    const mysqlPool = createPool({ host: '127.0.0.1', port: 1, connectionLimit: 1 });
    try {
      const postgres: SqlExecutor = fromDrizzle(drizzlePostgres(pgPool));
      const mysql: MySqlSqlExecutor = fromMysqlDrizzle(drizzleMysql(mysqlPool));
      expect(new PostgresOutboxStore({ executor: postgres })).toBeInstanceOf(PostgresOutboxStore);
      expect(new MySqlOutboxStore({ executor: mysql })).toBeInstanceOf(MySqlOutboxStore);

      // @ts-expect-error A MySQL executor in PostgresOutboxStore's options
      expect(() => new PostgresOutboxStore({ executor: mysql })).toThrow('PostgresOutboxStore runs on PostgreSQL, and `executor` is a MySQL executor');
      // @ts-expect-error A PostgreSQL executor in MySqlOutboxStore's options
      expect(() => new MySqlOutboxStore({ executor: postgres })).toThrow('MySqlOutboxStore runs on MySQL, and `executor` is a PostgreSQL executor');
    } finally {
      await pgPool.end();
      await mysqlPool.end();
    }
  });
});
