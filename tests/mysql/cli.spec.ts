/**
 * The `nest-outbox` command on MySQL (lib/sql/cli.ts: the kit's runStoreCli() on both stores' schemas): `sql --dialect
 * mysql` prints what MySqlOutboxStore.migrationSql() does, `migrate` applies the migrations through a mysql:// URL,
 * `status` exits with 1 while the schema is behind, and a failed migration says where it stopped, in the outbox's words.
 */
import { runStoreCli, type StoreCliIo } from '@nestjs/store-kit';
import { fromMysql2, MySqlOutboxStore } from '../../lib/mysql/index.js';
import { outboxSchemas } from '../../lib/sql/outbox-schemas.js';
import { onMysql, rows, testDatabase } from './support.js';

const { database, reason } = await testDatabase('cli');

async function run(argv: string[], env: StoreCliIo['env'] = {}) {
  const output = { out: '', err: '' };
  const code = await runStoreCli(outboxSchemas, argv, { out: (text) => (output.out += text), err: (text) => (output.err += text), env });
  return { code, ...output };
}

describe('nest-outbox, for MySQL', () => {
  it('sql --dialect mysql prints the migrations as MySqlOutboxStore.migrationSql() does, without a database', async () => {
    expect(await run(['sql', '--dialect', 'mysql'])).toEqual({ code: 0, out: MySqlOutboxStore.migrationSql(), err: '' });
    expect(await run(['sql', '--dialect', 'mysql', '--schema', 'shop_outbox', '--from', '0', '--to', '1'])).toEqual({
      code: 0,
      out: MySqlOutboxStore.migrationSql({ schema: 'shop_outbox', from: 0, to: 1 }),
      err: '',
    });
    const breakpoints = await run(['sql', '--dialect', 'mysql', '--statement-breakpoints']);
    expect(breakpoints).toEqual({ code: 0, out: MySqlOutboxStore.migrationSql({ statementBreakpoints: true }), err: '' });
    expect((await run(['sql', '--dialect', 'mysql', '--schema', 'Shop'])).err).toContain('MySqlOutboxStore: invalid schema "Shop".');
  });

  it('names both stores and both URL schemes in its usage', async () => {
    const help = await run(['--help']);
    expect(help.out).toContain("\n\nPostgresOutboxStore's schema (@nestjs/outbox/postgres) or MySqlOutboxStore's (@nestjs/outbox/mysql):\n\n  migrate   ");
    expect(help.out).toContain('--url <url>        The database (postgres://... or mysql://...). Default: $DATABASE_URL');
    expect(help.out).toContain('--dialect <name>   sql: the database it\'s for (postgres, mysql). Default: postgres');
  });

  describe('on MySQL', () => {
    onMysql(reason);

    it('migrate applies the pending migrations once through a mysql:// URL, and status exits with 1 until they are', async () => {
      const url = database!.url;
      expect(await run(['status', '--url', url, '--schema', 'cli_outbox'])).toEqual({
        code: 1,
        out: 'Schema "cli_outbox" is at version 0; this version of @nestjs/outbox needs version 1.\n',
        err: '',
      });
      expect(await run(['migrate', '--schema', 'cli_outbox'], { DATABASE_URL: url })).toEqual({
        code: 0,
        out: 'Migrated schema "cli_outbox" to version 1 (applied 1).\n',
        err: '',
      });
      expect(await run(['migrate', '--url', url, '--schema', 'cli_outbox'])).toEqual({ code: 0, out: 'Schema "cli_outbox" is up to date (version 1).\n', err: '' });
      expect(await run(['status', '--url', url, '--schema', 'cli_outbox'])).toEqual({
        code: 0,
        out: 'Schema "cli_outbox" is at version 1; this version of @nestjs/outbox needs version 1.\n',
        err: '',
      });
      expect(await rows(database!.admin, 'SELECT version FROM cli_outbox_migrations')).toEqual([{ version: 1 }]);

      const store = new MySqlOutboxStore({ executor: fromMysql2(database!.admin), schema: 'cli_outbox', migrate: false });
      await expect(store.onModuleInit()).resolves.toBeUndefined();
    });

    it('reports where a migration it cannot finish stopped, and exits with 1', async () => {
      await rows(database!.admin, 'CREATE TABLE cli_taken_messages (id int NOT NULL PRIMARY KEY)');
      expect(await run(['migrate', '--url', database!.url, '--schema', 'cli_taken'])).toEqual({
        code: 1,
        out: '',
        err:
          'MySqlOutboxStore: migrating schema "cli_taken" from version 0 to 1 stopped at migration 1 (initial), statement 1 of 3: ' +
          "Table 'cli_taken_messages' already exists. The statements before it are applied, and migrating again resumes at it.\n",
      });
    });
  });
});
