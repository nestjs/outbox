import { configDefaults, defineConfig } from 'vitest/config';

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
    // Generates the Prisma client (the Prisma store recipe in tests/integration.ts, and fromPrisma() in
    // tests/postgres), offline, and sweeps the test databases that crashed runs left on the server.
    globalSetup: ['tests/support/generate-prisma-client.ts', 'tests/support/global-setup.ts'],
    setupFiles: ['reflect-metadata'],
    // tests/postgres/ is PostgresOutboxStore's own project (@nestjs/outbox/postgres): its contracts through every
    // executor, the application's transactions, races, migrations and the command line, with timeouts for a busy
    // shared server. `--project outbox:postgres-store` runs it alone.
    projects: [
      {
        extends: true,
        test: {
          name: 'outbox',
          include: ['tests/**/*.spec.ts'],
          exclude: [...configDefaults.exclude, 'tests/postgres/**'],
        },
      },
      {
        extends: true,
        test: {
          name: 'outbox:postgres-store',
          include: ['tests/postgres/**/*.spec.ts'],
          testTimeout: 30_000,
          hookTimeout: 30_000,
        },
      },
    ],
  },
});
