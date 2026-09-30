import type { SqlExecutor } from '@nestjs/store-kit/mysql';

/**
 * What `new MySqlOutboxStore(options, storage)` takes.
 *
 * ```ts
 * const options: MySqlOutboxStoreOptions = { executor: fromDrizzle(db), schema: 'shop_outbox', migrate: false };
 * ```
 */
export interface MySqlOutboxStoreOptions {
  /**
   * How the store reaches the database: `fromMysql2(pool)`, `fromDrizzle(db)`, `fromTypeOrm(dataSource)`,
   * `fromPrisma(prisma)` or `fromKysely(db)`. The store's tables live in its connections' database. Its own statements
   * and transactions (claims, dead-letter moves) run on it, and `add()` and `recordInbox()` take that client's
   * transaction object: Drizzle's `tx`, a TypeORM `EntityManager`, a Prisma transaction client, a Kysely `Transaction`,
   * a mysql2 connection after `beginTransaction()`.
   */
  executor: SqlExecutor;
  /**
   * The name the store's tables start with, in the connection's database: `<schema>_messages`,
   * `<schema>_dead_letters` and `<schema>_inbox`, next to `<schema>_migrations` and `<schema>_locks`. Keep it for the
   * store alone. Lowercase letters, digits and underscores, not starting with a digit, at most 40 characters. Default:
   * `'nest_outbox'`.
   */
  schema?: string;
  /**
   * Apply the store's pending migrations at startup, one statement at a time under a lock, so processes that start
   * together migrate once. With `false`, startup fails with an `OutboxSchemaError` while the schema is behind this
   * version of the package: apply them with `npx nest-outbox migrate`, or with your own migration tool
   * (`MySqlOutboxStore.migrationStatements()`). Default: `true`, except when `NODE_ENV` is `production`.
   */
  migrate?: boolean;
}
