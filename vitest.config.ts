import { defineConfig } from 'vitest/config';

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
    include: ['tests/**/*.spec.ts'],
    globals: true,
    // Generates the Prisma store recipe's client (tests/integration.ts), offline.
    globalSetup: ['tests/support/generate-prisma-client.ts'],
    setupFiles: ['reflect-metadata'],
  },
});
