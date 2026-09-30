import type { SqlExecutor } from '@nestjs/store-kit/postgres';

/**
 * What `new PostgresOutboxStore(options, storage)` takes.
 *
 * ```ts
 * const options: PostgresOutboxStoreOptions = { executor: fromDrizzle(db), schema: 'shop_outbox', migrate: false };
 * ```
 */
export interface PostgresOutboxStoreOptions {
  /**
   * How the store reaches the database: `fromPg(pool)`, `fromDrizzle(db)`, `fromTypeOrm(dataSource)`,
   * `fromPrisma(prisma)` or `fromKysely(db)`. The store's own statements and transactions (claims, dead-letter moves)
   * run on it, and `add()` and `recordInbox()` take that client's transaction object: Drizzle's `tx`, a TypeORM
   * `EntityManager`, a Prisma transaction client, a Kysely `Transaction`, a node-postgres client after `BEGIN`.
   */
  executor: SqlExecutor<'postgres'>;
  /**
   * The schema that holds the store's tables (`messages`, `dead_letters`, `inbox`), created by its first migration:
   * keep it for the store alone. Letters, digits and underscores, not starting with a digit, at most 63 characters.
   * Default: `'nest_outbox'`.
   */
  schema?: string;
  /**
   * Apply the store's pending migrations at startup, in one transaction under an advisory lock, so processes that
   * start together migrate once. With `false`, startup fails with an `OutboxSchemaError` while the schema is behind
   * this version of the package: apply them with `npx nest-outbox migrate`, or with your own migration tool
   * (`PostgresOutboxStore.migrationSql()`). Default: `true`, except when `NODE_ENV` is `production`.
   */
  migrate?: boolean;
}
