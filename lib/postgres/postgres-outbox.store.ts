import { Logger, type OnModuleInit } from '@nestjs/common';
import {
  advisoryLock,
  columns,
  isNotATransactionError,
  quoteSchema,
  SqlParams,
  type MigrationSqlOptions,
  type SqlExecutor,
  type SqlTransaction,
  type SqlTransactionOptions,
  type StoreReadiness,
} from '@nestjs/store-kit/postgres';
import { OutboxTransactionRequiredError } from '../errors/outbox-transaction-required.error.js';
import type { OutboxDeadLetter, OutboxDeadLetterFilter, OutboxDeadLetterQuery } from '../interfaces/outbox-dead-letter.interface.js';
import type { OutboxInboxStore } from '../interfaces/outbox-inbox-store.interface.js';
import type { OutboxMessage } from '../interfaces/outbox-message.interface.js';
import type {
  OutboxClaimRequest,
  OutboxDeadLetterUpdate,
  OutboxRescheduleUpdate,
  OutboxStore,
  OutboxStoreStats,
} from '../interfaces/outbox-store.interface.js';
import { deadLetterConditions, deadLetterPage, epochMs, type DeadLetterConditions } from '../sql/outbox-inputs.util.js';
import { DEAD_LETTER_COLUMNS, MESSAGE_COLUMNS, toDeadLetter, toMessage, type Row } from '../sql/outbox-rows.util.js';
import type { OutboxStorage } from '../storage/outbox.storage.js';
import type { PostgresOutboxStoreOptions } from './interfaces/postgres-outbox-store-options.interface.js';
import { postgresOutboxSchema } from './migrations/index.js';

/** A statement that waited for a lock sees what the lock's holder committed. */
const READ_COMMITTED: SqlTransactionOptions = { isolationLevel: 'read committed' };

/**
 * The first-party `OutboxStore` and `OutboxInboxStore` on PostgreSQL, through the client the application already has
 * (`fromPg()`, `fromDrizzle()`, `fromTypeOrm()`, `fromPrisma()`, `fromKysely()`). It keeps the messages, the dead
 * letters and the consumers' inbox in a schema of its own (`nest_outbox` by default), which its migrations create and
 * bring up to date, and `add()` and `recordInbox()` write through the application's transaction, so messages and
 * inbox records commit or roll back with its rows.
 *
 * ```ts
 * @Module({
 *   imports: [
 *     DrizzleModule.forRoot({ drizzle, connection: process.env.DATABASE_URL!, schema }),
 *     OutboxModule.forRoot({ relay: { lease: '30s' } }),
 *   ],
 *   providers: [
 *     {
 *       provide: PostgresOutboxStore,
 *       inject: [getDrizzleToken(), OutboxStorage],
 *       useFactory: (db: Database, storage: OutboxStorage) => new PostgresOutboxStore({ executor: fromDrizzle(db) }, storage),
 *     },
 *   ],
 * })
 * export class AppModule {}
 * ```
 *
 * Given `storage`, it registers itself for both contracts (`storage.registerSource({ messages: this, inbox: this })`),
 * in a service that only consumes too: the tables it doesn't use stay empty. It checks its schema, or migrates it
 * (`migrate`), in `onModuleInit` (so before the relay starts), or at its first call outside Nest.
 */
export class PostgresOutboxStore implements OutboxStore, OutboxInboxStore, OnModuleInit {
  /**
   * The SQL of the store's migrations, for teams that apply migrations with their own tool (drizzle-kit, TypeORM,
   * Prisma Migrate, Flyway...) and run the store with `migrate: false`: from version `from` (default `0`, a new
   * database) to `to` (default: the version this version of the package needs), with the bookkeeping that tells the
   * store which versions a schema has. A schema's version is `SELECT max(version) FROM <schema>.migrations`.
   * Downgrades aren't supported. From version 0 it starts with `CREATE SCHEMA IF NOT EXISTS`, which needs the CREATE
   * privilege on the database even when the schema exists: drop that statement if someone created the schema for you.
   * With `statementBreakpoints`, drizzle-kit's `--> statement-breakpoint` separates the statements, for a custom
   * drizzle-kit migration: its migrator then runs them one at a time, which PGlite needs.
   *
   * ```ts
   * // drizzle/0003_outbox.sql, created empty by `npx drizzle-kit generate --custom --name=outbox`
   * writeFileSync('drizzle/0003_outbox.sql', PostgresOutboxStore.migrationSql({ statementBreakpoints: true }));
   * ```
   */
  static migrationSql(options: MigrationSqlOptions = {}): string {
    return postgresOutboxSchema.sql(options);
  }

  /**
   * The schema version this version of the package needs: its last migration.
   *
   * ```ts
   * const [{ version }] = await db.execute(sql`SELECT max(version) AS version FROM nest_outbox.migrations`);
   * const behind = version < PostgresOutboxStore.schemaVersion;
   * ```
   */
  static readonly schemaVersion = postgresOutboxSchema.latest;

  private readonly logger = new Logger('OutboxModule');
  private readonly executor: SqlExecutor;
  private readonly schema: string;
  private readonly t: Record<'messages' | 'deadLetters' | 'inbox', string>;
  /** The schema migrated (`migrate`) or checked before the first statement: see `onModuleInit()`. */
  private readonly readiness: StoreReadiness;

  constructor(options: PostgresOutboxStoreOptions, storage?: OutboxStorage) {
    const resolved = postgresOutboxSchema.resolveOptions(options);
    this.executor = resolved.executor;
    this.schema = resolved.schema;
    const s = quoteSchema(this.schema, 'PostgresOutboxStore');
    this.t = { messages: `${s}.messages`, deadLetters: `${s}.dead_letters`, inbox: `${s}.inbox` };
    this.readiness = postgresOutboxSchema.readiness({ ...resolved, logger: this.logger });
    storage?.registerSource({ messages: this, inbox: this });
  }

  /** Migrates the schema (`migrate`) or checks it, before the relay starts: startup fails if it can't serve. */
  async onModuleInit(): Promise<void> {
    await this.readiness.ready();
  }

  /**
   * Applies the migrations the schema hasn't had yet, whatever `migrate` says, in one transaction under an advisory
   * lock: of processes that migrate together, one applies them. Resolves to the versions it applied (`[]`: none were
   * pending).
   */
  migrate(): Promise<number[]> {
    return this.readiness.migrate();
  }

  // ---------------------------------------------------------------- producer

  /**
   * Inserts the messages through the application's transaction. First it takes a transaction-scoped lock per key on
   * that transaction, so a key's messages are numbered in the order their transactions commit. Anything but a
   * transaction object of the executor's client fails with `OutboxTransactionRequiredError`.
   */
  async add(transaction: unknown, messages: readonly OutboxMessage[]): Promise<void> {
    const tx = this.inTransaction(transaction, 'add');
    if (messages.length === 0) {
      return;
    }

    await this.readiness.readyIn(tx);
    await this.lockKeys(tx, messages);

    const rows = messages.map((m) => ({
      id: m.id,
      topic: m.topic,
      payload: m.payload,
      headers: m.headers,
      key: m.key,
      createdAt: epochMs(m.createdAt),
      availableAt: epochMs(m.availableAt),
    }));
    // One parameter for the whole batch (a statement takes at most 65,535), numbered in the batch's order.
    const p = new SqlParams();
    await tx.query(
      `INSERT INTO ${this.t.messages} (id, topic, payload, headers, key, created_at, available_at)
SELECT m.value->>'id', m.value->>'topic', nullif(m.value->'payload', 'null'), m.value->'headers', m.value->>'key',
  (m.value->>'createdAt')::bigint, (m.value->>'availableAt')::bigint
FROM jsonb_array_elements(${p.json(rows)}) WITH ORDINALITY AS m(value, n)
ORDER BY m.n`,
      p.values,
    );
  }

  // ---------------------------------------------------------------- relay

  /**
   * Leases the claimable messages in `seq` order, in one statement. The claimable rows come off the primary key in
   * order, up to `limit`, each checked against its key's older messages; `SKIP LOCKED` passes over a row another
   * connection is writing at that moment (a relay's fenced write, a release). A message whose predecessor in its key
   * isn't one of the rows the statement locked (passed over, or changed since the statement started) stays unleased,
   * and the rest of its key with it, so no key's run is split or reordered. Claims take turns under the claim lock, so
   * each one's statement starts after the one before it committed and sees its leases, instead of passing over its
   * rows (and the keys behind them) and returning a thinner batch.
   */
  async claim({ owner, now, leaseMs, limit }: OutboxClaimRequest): Promise<OutboxMessage[]> {
    await this.readiness.ready();
    return this.executor.transaction(async (tx) => {
      await advisoryLock(tx, `@nestjs/outbox:${this.schema}:claim`);

      const p = new SqlParams();
      const at = p.bigint(epochMs(now));
      const rows = await tx.query<Row>(
        `WITH due AS MATERIALIZED (
  SELECT m.seq, m.key FROM ${this.t.messages} m
  WHERE ${this.claimable('m', at)}
  ORDER BY m.seq
  LIMIT ${p.int(limit)}
  FOR UPDATE OF m SKIP LOCKED
), checked AS (
  SELECT d.seq, d.key, (
    SELECT o.seq FROM ${this.t.messages} o WHERE o.key = d.key AND o.seq < d.seq ORDER BY o.seq DESC LIMIT 1
  ) AS previous
  FROM due d
), cut AS (
  SELECT c.key, c.seq FROM checked c WHERE c.previous NOT IN (SELECT due.seq FROM due)
), taken AS (
  SELECT c.seq FROM checked c
  WHERE c.key IS NULL OR NOT EXISTS (SELECT 1 FROM cut WHERE cut.key = c.key AND cut.seq <= c.seq)
), leased AS (
  UPDATE ${this.t.messages} m SET lease_owner = ${p.text(owner)}, lease_until = ${p.bigint(epochMs(now + leaseMs))}
  FROM taken WHERE m.seq = taken.seq
  RETURNING m.*
)
SELECT ${columns(MESSAGE_COLUMNS, 'l')} FROM leased l ORDER BY l.seq`,
        p.values,
      );
      return rows.map(toMessage);
    }, READ_COMMITTED);
  }

  /** One `DELETE`, fenced by the lease owner. */
  async markPublished(id: string, owner: string): Promise<boolean> {
    await this.readiness.ready();
    const p = new SqlParams();
    const deleted = await this.executor.query<Row>(
      `DELETE FROM ${this.t.messages} WHERE id = ${p.text(id)} AND lease_owner = ${p.text(owner)} RETURNING id`,
      p.values,
    );
    return deleted.length === 1;
  }

  /** One `UPDATE`, fenced by the lease owner, that appends to the history in the statement that checks the lease. */
  async reschedule(id: string, owner: string, update: OutboxRescheduleUpdate): Promise<boolean> {
    await this.readiness.ready();
    const p = new SqlParams();
    const updated = await this.executor.query<Row>(
      `UPDATE ${this.t.messages}
SET attempts = ${p.int(update.attempts)}, available_at = ${p.bigint(epochMs(update.availableAt))}, last_error = ${p.text(update.error.error)},
  history = history || ${p.json([update.error])}, lease_owner = NULL, lease_until = NULL
WHERE id = ${p.text(id)} AND lease_owner = ${p.text(owner)}
RETURNING id`,
      p.values,
    );
    return updated.length === 1;
  }

  /**
   * One statement, fenced by the lease owner: the message's delete and the dead letter's insert commit together or
   * not at all. The dead letter keeps `seq`, and replaces an earlier one with the same id (a producer that reused a
   * custom id).
   */
  async deadLetter(id: string, owner: string, update: OutboxDeadLetterUpdate): Promise<boolean> {
    await this.readiness.ready();
    const p = new SqlParams();
    const moved = await this.executor.query<Row>(
      `WITH moved AS (
  DELETE FROM ${this.t.messages} WHERE id = ${p.text(id)} AND lease_owner = ${p.text(owner)} RETURNING *
)
INSERT INTO ${this.t.deadLetters} (id, seq, topic, payload, headers, key, created_at, attempts, last_error, history, reason, failed_at)
SELECT id, seq, topic, payload, headers, key, created_at, ${p.int(update.attempts)}, ${p.text(update.error.error)},
  history || ${p.json([update.error])}, ${p.text(update.reason)}, ${p.bigint(epochMs(update.failedAt))}
FROM moved
ON CONFLICT (id) DO UPDATE SET seq = excluded.seq, topic = excluded.topic, payload = excluded.payload,
  headers = excluded.headers, key = excluded.key, created_at = excluded.created_at, attempts = excluded.attempts,
  last_error = excluded.last_error, history = excluded.history, reason = excluded.reason, failed_at = excluded.failed_at
RETURNING id`,
      p.values,
    );
    return moved.length === 1;
  }

  /** One `UPDATE`, fenced by the lease owner. */
  async release(ids: readonly string[], owner: string): Promise<number> {
    if (ids.length === 0) {
      return 0;
    }

    await this.readiness.ready();
    const p = new SqlParams();
    const released = await this.executor.query<Row>(
      `UPDATE ${this.t.messages} SET lease_owner = NULL, lease_until = NULL
WHERE lease_owner = ${p.text(owner)} AND ${p.in('id', [...ids])}
RETURNING id`,
      p.values,
    );
    return released.length;
  }

  /**
   * One statement, so the counts come from one snapshot. `ready` is one pass over the messages in `seq` order per key:
   * a claim's check of each message's older ones, repeated over a whole backlog, grows with the square of a key's
   * length (minutes for a key of 40,000 messages while the relay is down).
   */
  async stats(now: number): Promise<OutboxStoreStats> {
    await this.readiness.ready();
    const p = new SqlParams();
    const at = p.bigint(epochMs(now));
    const [row] = await this.executor.query<Row>(
      `SELECT
  (SELECT count(*) FROM ${this.t.messages})::text AS pending,
  (SELECT count(*) FROM (
    SELECT m.key, m.available_at <= ${at} AND (m.lease_until IS NULL OR m.lease_until <= ${at}) AS free,
      bool_or(m.available_at > ${at} OR m.lease_until > ${at}) OVER (PARTITION BY m.key ORDER BY m.seq ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS behind
    FROM ${this.t.messages} m
  ) w WHERE w.free AND (w.key IS NULL OR NOT coalesce(w.behind, false)))::text AS ready,
  (SELECT count(*) FROM ${this.t.messages} WHERE lease_until > ${at})::text AS leased,
  (SELECT count(*) FROM ${this.t.deadLetters})::text AS dead_letters,
  (SELECT min(CASE WHEN attempts > 0 THEN created_at WHEN available_at <= ${at} THEN available_at END) FROM ${this.t.messages})::text AS oldest_due_at`,
      p.values,
    );
    return {
      pending: Number(row!.pending),
      ready: Number(row!.ready),
      leased: Number(row!.leased),
      deadLetters: Number(row!.dead_letters),
      oldestDueAt: row!.oldest_due_at === null ? null : Number(row!.oldest_due_at),
    };
  }

  // ---------------------------------------------------------------- dead letters

  /** Newest first; ids that failed at the same time in byte order, whatever the database's collation. */
  async listDeadLetters(query: OutboxDeadLetterQuery): Promise<OutboxDeadLetter[]> {
    const { limit, offset } = deadLetterPage(query);
    await this.readiness.ready();

    const p = new SqlParams();
    const where = [
      ...(query.topic !== undefined ? [`d.topic = ${p.text(query.topic)}`] : []),
      ...(query.key !== undefined ? [`d.key = ${p.text(query.key)}`] : []),
    ];
    const rows = await this.executor.query<Row>(
      `SELECT ${columns(DEAD_LETTER_COLUMNS, 'd')} FROM ${this.t.deadLetters} d${where.length > 0 ? ` WHERE ${where.join(' AND ')}` : ''}
ORDER BY d.failed_at DESC, d.id COLLATE "C" DESC LIMIT ${p.int(limit)} OFFSET ${p.int(offset)}`,
      p.values,
    );
    return rows.map(toDeadLetter);
  }

  async getDeadLetter(id: string): Promise<OutboxDeadLetter | undefined> {
    await this.readiness.ready();
    const p = new SqlParams();
    const [row] = await this.executor.query<Row>(
      `SELECT ${columns(DEAD_LETTER_COLUMNS, 'd')} FROM ${this.t.deadLetters} d WHERE d.id = ${p.text(id)}`,
      p.values,
    );
    return row && toDeadLetter(row);
  }

  /**
   * One transaction: a statement takes the matching dead letters (so a concurrent requeue or purge finds them gone)
   * and inserts them back with their `seq`. A pending message with the id of one of them (a producer that reused a
   * custom id) fails it, moving nothing.
   */
  async requeueDeadLetters(filter: OutboxDeadLetterFilter, now: number): Promise<number> {
    const conditions = deadLetterConditions(filter);
    if (!conditions) {
      return 0;
    }

    await this.readiness.ready();
    return this.executor.transaction(async (tx) => {
      const check = new SqlParams();
      const [clash] = await tx.query<Row>(
        `SELECT d.id FROM ${this.t.deadLetters} d JOIN ${this.t.messages} m ON m.id = d.id
WHERE ${this.deadLetterWhere(check, conditions)} LIMIT 1`,
        check.values,
      );
      if (clash) {
        throw new Error(`PostgresOutboxStore: can't requeue dead letter "${clash.id}": a message with that id is pending.`);
      }

      const p = new SqlParams();
      const [requeued] = await tx.query<Row>(
        `WITH moved AS (
  DELETE FROM ${this.t.deadLetters} d WHERE ${this.deadLetterWhere(p, conditions)} RETURNING d.*
), inserted AS (
  INSERT INTO ${this.t.messages} (seq, id, topic, payload, headers, key, created_at, available_at, attempts, last_error, history)
  SELECT seq, id, topic, payload, headers, key, created_at, ${p.bigint(epochMs(now))}, 0, last_error, history FROM moved
  RETURNING 1
)
SELECT count(*)::text AS n FROM inserted`,
        p.values,
      );
      return Number(requeued!.n);
    }, READ_COMMITTED);
  }

  async purgeDeadLetters(filter: OutboxDeadLetterFilter): Promise<number> {
    const conditions = deadLetterConditions(filter);
    if (!conditions) {
      return 0;
    }

    await this.readiness.ready();
    const p = new SqlParams();
    const [purged] = await this.executor.query<Row>(
      `WITH purged AS (DELETE FROM ${this.t.deadLetters} d WHERE ${this.deadLetterWhere(p, conditions)} RETURNING 1)
SELECT count(*)::text AS n FROM purged`,
      p.values,
    );
    return Number(purged!.n);
  }

  // ---------------------------------------------------------------- inbox

  /**
   * Insert-if-absent on the `(consumer, message_id)` key, in one statement: a concurrent delivery of the message waits
   * for this transaction, then finds the record. Through the application's transaction when given one (anything but a
   * transaction object of the executor's client fails with `OutboxTransactionRequiredError`), else at once.
   */
  async recordInbox(transaction: unknown, consumer: string, messageId: string, now: number): Promise<boolean> {
    let db: SqlTransaction = this.executor;
    if (transaction === undefined) {
      await this.readiness.ready();
    } else {
      db = this.inTransaction(transaction, 'recordInbox');
      await this.readiness.readyIn(db);
    }

    const p = new SqlParams();
    const inserted = await db.query<Row>(
      `INSERT INTO ${this.t.inbox} (consumer, message_id, processed_at) VALUES (${p.text(consumer)}, ${p.text(messageId)}, ${p.bigint(epochMs(now))})
ON CONFLICT (consumer, message_id) DO NOTHING
RETURNING consumer`,
      p.values,
    );
    return inserted.length === 1;
  }

  async hasInbox(consumer: string, messageId: string): Promise<boolean> {
    await this.readiness.ready();
    const p = new SqlParams();
    const [row] = await this.executor.query<Row>(
      `SELECT EXISTS (SELECT 1 FROM ${this.t.inbox} WHERE consumer = ${p.text(consumer)} AND message_id = ${p.text(messageId)})::text AS found`,
      p.values,
    );
    return row?.found === 'true';
  }

  async pruneInbox(before: number): Promise<number> {
    await this.readiness.ready();
    const p = new SqlParams();
    const [pruned] = await this.executor.query<Row>(
      `WITH pruned AS (DELETE FROM ${this.t.inbox} WHERE processed_at < ${p.bigint(Math.ceil(before))} RETURNING 1)
SELECT count(*)::text AS n FROM pruned`,
      p.values,
    );
    return Number(pruned!.n);
  }

  // ---------------------------------------------------------------- internals

  /**
   * The application's transaction; anything else (the database, the pool) is refused before any statement. The kit's
   * refusal (`isNotATransactionError()`: its code, never its class) becomes `OutboxTransactionRequiredError`, with it
   * as the cause; any other error goes through as it is.
   */
  private inTransaction(transaction: unknown, method: string): SqlTransaction {
    try {
      return this.executor.wrapTransaction(transaction);
    } catch (error) {
      if (isNotATransactionError(error)) {
        throw new OutboxTransactionRequiredError(`PostgresOutboxStore.${method}(): ${error.message}`, { cause: error });
      }
      throw error;
    }
  }

  /**
   * Commit order: a transaction adding a message with a key waits here until the transactions that added one with that
   * key before it commit or roll back, so `seq` numbers a key's messages in the order they become visible. One
   * transaction-scoped advisory lock per key, all in one statement:
   * - in the two-number form, `(hashtext('@nestjs/outbox:<schema>:key'), hashtext(key))`, a lock space of its own:
   *   the keys, however many, never collide with the store's claim lock or the migration lock (one number each);
   * - in the order of the keys' lock numbers, not of the keys: two transactions locking overlapping keys take them in
   *   the same order, keys whose numbers collide included, so they never deadlock on them.
   * Messages without a key take none.
   */
  private async lockKeys(tx: SqlTransaction, messages: readonly OutboxMessage[]): Promise<void> {
    const keys = [...new Set(messages.flatMap((m) => (m.key === null ? [] : [m.key])))];
    if (keys.length === 0) {
      return;
    }

    const p = new SqlParams();
    await tx.query(
      `SELECT pg_advisory_xact_lock(hashtext(${p.text(`@nestjs/outbox:${this.schema}:key`)}), k.lock_id)::text AS locked
FROM (SELECT DISTINCT hashtext(value) AS lock_id FROM jsonb_array_elements_text(${p.json(keys)}) ORDER BY 1) AS k`,
      p.values,
    );
  }

  /** Due, unleased (or its lease expired), and no older message of its key delayed or leased: as `alias`, at `at`. */
  private claimable(alias: string, at: string): string {
    return `${alias}.available_at <= ${at} AND (${alias}.lease_until IS NULL OR ${alias}.lease_until <= ${at})
  AND (${alias}.key IS NULL OR NOT EXISTS (
    SELECT 1 FROM ${this.t.messages} older
    WHERE older.key = ${alias}.key AND older.seq < ${alias}.seq AND (older.available_at > ${at} OR older.lease_until > ${at})
  ))`;
  }

  /** The dead letters (as `d`) a filter's conditions match. */
  private deadLetterWhere(p: SqlParams, conditions: DeadLetterConditions): string {
    const where = [
      ...(conditions.ids !== undefined ? [p.in('d.id', [...conditions.ids])] : []),
      ...(conditions.topic !== undefined ? [`d.topic = ${p.text(conditions.topic)}`] : []),
      ...(conditions.key !== undefined ? [`d.key = ${p.text(conditions.key)}`] : []),
      ...(conditions.failedBefore !== undefined ? [`d.failed_at < ${p.bigint(conditions.failedBefore)}`] : []),
    ];
    return where.length > 0 ? where.join(' AND ') : 'TRUE';
  }
}
