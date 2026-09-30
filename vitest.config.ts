import { configDefaults, defineConfig } from 'vitest/config';

/**
 * Generates the PostgreSQL Prisma client (the Prisma store recipe in tests/integration.ts, and fromPrisma() in
 * tests/postgres), offline, and sweeps the test databases that crashed runs left on the PostgreSQL server.
 */
const postgresSetup = ['tests/support/generate-prisma-client.ts', 'tests/support/global-setup.ts'];

export default defineConfig({
  // Legacy decorators with emitted metadata, as in the nestjs/nest monorepo, so
  // parameter decorators and DI type lookup work in the specs. Class fields declared
  // without an initializer are types only, as under `tsc` with `useDefineForClassFields: false`.
  oxc: {
    decorator: {
      legacy: true,
      emitDecoratorMetadata: true,
    },
    assumptions: { setPublicClassFields: true },
    typescript: { removeClassFieldsWithoutInitializer: true },
  },
  test: {
    globals: true,
    setupFiles: ['reflect-metadata'],
    // tests/postgres/ is PostgresOutboxStore's own project (@nestjs/outbox/postgres), and tests/mysql/ MySqlOutboxStore's
    // (@nestjs/outbox/mysql): their contracts through every executor, the application's transactions, races,
    // migrations and the command line, with timeouts for a busy shared server. `--project <name>` runs one alone.
    projects: [
      {
        extends: true,
        test: {
          name: 'outbox',
          include: ['tests/**/*.spec.ts'],
          exclude: [...configDefaults.exclude, 'tests/postgres/**', 'tests/mysql/**'],
          globalSetup: postgresSetup,
          // The integration suites run their recipes on PostgreSQL (and PGlite) here, on MySQL in outbox:mysql-store.
          provide: { sqlDialect: 'postgres' },
        },
      },
      {
        extends: true,
        test: {
          name: 'outbox:postgres-store',
          include: ['tests/postgres/**/*.spec.ts'],
          testTimeout: 30_000,
          hookTimeout: 30_000,
          globalSetup: postgresSetup,
        },
      },
      {
        extends: true,
        test: {
          name: 'outbox:mysql-store',
          // The integration suites too, on the MySQL recipes (tests/integration.ts: `sqlDialect`).
          include: ['tests/mysql/**/*.spec.ts', 'tests/*.integration.spec.ts'],
          testTimeout: 30_000,
          hookTimeout: 30_000,
          // MySQL comes from SQL_TEST_MYSQL_URL (its stale `obx_` databases swept before and after the run), else the
          // tests are skipped with the reason. Its own Prisma client (tests/fixtures/prisma-mysql), so no two projects'
          // setups write the same files. The server may be shared: four files at a time, after the other projects,
          // with pools of at most 3 connections (2 per application instance), keep this run's connections near 20, well
          // under max_connections (151 by default).
          globalSetup: ['tests/support/generate-prisma-mysql-client.ts', 'tests/support/mysql-global-setup.ts'],
          maxWorkers: 4,
          sequence: { groupOrder: 1 },
          provide: { sqlDialect: 'mysql' },
        },
      },
    ],
  },
});
