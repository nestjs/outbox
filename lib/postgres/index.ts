// The `@nestjs/outbox/postgres` entry: the first-party PostgreSQL store. Nothing here imports a driver or an ORM: the
// executors (from @nestjs/store-kit, which every first-party store builds on) reach the client the application passes
// them.

// The store, and the error it fails startup with while its schema is behind
export { PostgresOutboxStore } from './postgres-outbox.store.js';
export { OutboxSchemaError } from '../sql/outbox-schema.error.js';
export type { PostgresOutboxStoreOptions } from './interfaces/index.js';

// Executors: the store's SQL through the application's pool or ORM, and its transactions
export { fromDrizzle, fromKysely, fromPg, fromPrisma, fromTypeOrm, type PrismaExecutorOptions } from '@nestjs/store-kit/postgres';
export type { SqlExecutor, SqlIsolationLevel, SqlTransaction, SqlTransactionOptions } from '@nestjs/store-kit/postgres';
