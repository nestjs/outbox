/**
 * The `nest-outbox` command (lib/sql/cli.ts: the kit's runStoreCli() on the SQL stores' schemas): `sql` prints what
 * migrationSql() does, `migrate` applies the migrations, `status` exits with 1 while the schema is behind, and every
 * misuse says what to do instead, in the outbox's words.
 */
import { runStoreCli, type StoreCliIo } from '@nestjs/store-kit';
import { fromPg, PostgresOutboxStore } from '../../lib/postgres/index.js';
import { postgresOutboxSchema } from '../../lib/postgres/migrations/index.js';
import { testDatabase } from './support.js';

const { database, reason } = await testDatabase('store_cli');

async function run(argv: string[], env: StoreCliIo['env'] = {}) {
  const output = { out: '', err: '' };
  const code = await runStoreCli([postgresOutboxSchema], argv, { out: (text) => (output.out += text), err: (text) => (output.err += text), env });
  return { code, ...output };
}

describe('nest-outbox', () => {
  it('sql prints the migrations as migrationSql() does, without a database, with statement breakpoints on request', async () => {
    expect(await run(['sql'])).toEqual({ code: 0, out: PostgresOutboxStore.migrationSql(), err: '' });
    expect(await run(['sql', '--schema', 'shop_outbox', '--from', '0', '--to', '1'])).toEqual({
      code: 0,
      out: PostgresOutboxStore.migrationSql({ schema: 'shop_outbox', from: 0, to: 1 }),
      err: '',
    });

    const breakpoints = await run(['sql', '--dialect', 'postgres', '--statement-breakpoints']);
    expect(breakpoints).toEqual({ code: 0, out: PostgresOutboxStore.migrationSql({ statementBreakpoints: true }), err: '' });
    expect(breakpoints.out.split('\n--> statement-breakpoint\n')).toHaveLength(postgresOutboxSchema.statements().length);

    expect(await run(['sql', '--from', 'one'])).toEqual({ code: 1, out: '', err: '--from takes a version number, not "one".\n' });
    expect((await run(['sql', '--to', '9'])).err).toContain('no migrations lead from version 0 to 9');
    expect((await run(['sql', '--schema', 'bad-name'])).err).toContain('PostgresOutboxStore: invalid schema "bad-name".');
  });

  it('prints its usage for --help, and on stderr, exiting with 1, for no command, an unknown one or an unknown option', async () => {
    const help = await run(['--help']);
    expect(help).toMatchObject({ code: 0, err: '' });
    expect(help.out).toMatch(/^Usage: nest-outbox <command> \[options\]/);
    expect(help.out).toContain("PostgresOutboxStore's schema (@nestjs/outbox/postgres):");
    expect(help.out).toContain('Default: nest_outbox');
    expect(await run([])).toEqual({ code: 1, out: '', err: help.out });
    expect(await run(['upgrade'])).toEqual({ code: 1, out: '', err: `Unknown command "upgrade".\n\n${help.out}` });
    expect((await run(['sql', '--verbose'])).err).toMatch(/^Unknown option '--verbose'/);
  });

  it("needs the database for migrate and status: --url, else DATABASE_URL, of a dialect it serves (never echoing the URL)", async () => {
    expect(await run(['migrate'])).toEqual({ code: 1, out: '', err: 'nest-outbox migrate needs the database: pass --url, or set DATABASE_URL.\n' });
    expect((await run(['status'])).err).toBe('nest-outbox status needs the database: pass --url, or set DATABASE_URL.\n');
    expect(await run(['migrate', '--url', 'mysql://root:secret@127.0.0.1/shop'])).toEqual({
      code: 1,
      out: '',
      err: "nest-outbox: MySQL isn't supported yet: the stores of @nestjs/outbox run on PostgreSQL.\n",
    });
    expect(await run(['status', '--url', 'sqlite://secret.db'])).toEqual({
      code: 1,
      out: '',
      err: 'nest-outbox status takes a database URL that starts with postgres:// or postgresql://.\n',
    });
  });

  describe('on PostgreSQL', () => {
    beforeEach((context) => {
      if (reason) {
        context.skip(reason);
      }
    });

    it('migrate applies the pending migrations once, and status exits with 1 until they are', async () => {
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
      expect(await database!.admin.query('SELECT version FROM cli_outbox.migrations')).toMatchObject({ rows: [{ version: 1 }] });

      const store = new PostgresOutboxStore({ executor: fromPg(database!.admin), schema: 'cli_outbox', migrate: false });
      await expect(store.onModuleInit()).resolves.toBeUndefined();
    });

    it('reports a database it cannot migrate, and exits with 1', async () => {
      await database!.admin.query('CREATE SCHEMA cli_taken');
      await database!.admin.query('CREATE TABLE cli_taken.messages (id int)');
      expect(await run(['migrate', '--url', database!.url, '--schema', 'cli_taken'])).toEqual({
        code: 1,
        out: '',
        err: 'PostgresOutboxStore: migrating schema "cli_taken" from version 0 to 1 failed, and nothing was applied: relation "messages" already exists\n',
      });
    });
  });
});
