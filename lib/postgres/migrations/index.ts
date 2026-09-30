import { StoreSchema } from '@nestjs/store-kit/postgres';
import { OutboxSchemaError } from '../../errors/outbox-schema.error.js';
import { initialMigration } from './initial.migration.js';

/**
 * PostgresOutboxStore's schema: every version of it, in order (a new one goes last, and none ever changes once
 * released), and what the store, its statics and `nest-outbox` do with them.
 */
export const postgresOutboxSchema = new StoreSchema({
  packageName: '@nestjs/outbox',
  storeName: 'PostgresOutboxStore',
  command: 'nest-outbox',
  defaultSchema: 'nest_outbox',
  migrations: [initialMigration],
  createError: (message, details) => new OutboxSchemaError(message, details),
});
