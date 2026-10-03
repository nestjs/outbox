import { Logger, type OnModuleInit } from '@nestjs/common';
import {
  columns,
  ensureLockRows,
  isNotATransactionError,
  lockKeys,
  mysqlErrorCode,
  quoteTable,
  retryOnDeadlock,
  SqlParams,
  type MigrationSqlOptions,
  type MigrationStatementsOptions,
  type SqlExecutor,
  type SqlTransaction,
  type StoreReadiness,
} from '@nestjs/store-kit/mysql';
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
import type { MySqlOutboxStoreOptions } from './interfaces/mysql-outbox-store-options.interface.js';
import { mysqlOutboxSchema } from './migrations/index.js';
import { bucketLocks, checkLength, keyLocks } from './mysql-outbox-keys.util.js';

/** ER_DUP_ENTRY: a duplicate key. MySQL rolls the statement back, not the transaction, which goes on. */
const DUPLICATE_KEY = 1062;

/** The lock claims take turns under, a row of the kit's `<schema>_locks` like the keys' buckets. */
const CLAIM_LOCK = 'claim';

/** Ids a statement names at most: a requeue or a release of more takes several statements, in one transaction. */
const IDS_PER_STATEMENT = 1_000;

/**
 * The JSON of a batch's messages one `INSERT` carries at most, in UTF-16 units (at most three bytes each), well under
 * MySQL's `max_allowed_packet` (64 MB by default): a larger batch takes several statements, numbered in its order.
 */
const BATCH_JSON_LENGTH = 1 << 20;

/**
 * The first-party `OutboxStore` and `OutboxInboxStore` on MySQL (8.4 LTS and 9.x), through the client the application
 * already has (`fromMysql2()`, `fromSequelize()`, `fromDrizzle()`, `fromTypeOrm()`, `fromPrisma()`,
 * `fromKysely()`). It keeps the messages, the dead letters and the consumers' inbox in tables of its own in the
 * connection's database (`nest_outbox_messages`, `nest_outbox_dead_letters`, `nest_outbox_inbox`), which its
 * migrations create and bring up to date, and `add()` and `recordInbox()` write through the application's
 * transaction, so messages and inbox records commit or roll back with its rows.
 *
 * ```ts
 * import { OutboxModule, OutboxStorage } from '@nestjs/outbox';
 * import { fromDrizzle, MySqlOutboxStore } from '@nestjs/outbox/mysql';
 * import { drizzle } from 'drizzle-orm/mysql2';
 *
 * @Module({
 *   imports: [
 *     // `mode: 'default'`: Drizzle's MySQL driver takes a schema only with a mode ('planetscale' is Vitess's)
 *     DrizzleModule.forRoot({ drizzle, connection: { uri: process.env.DATABASE_URL! }, schema, mode: 'default' }),
 *     OutboxModule.forRoot({ relay: { lease: '30s' } }),
 *   ],
 *   providers: [
 *     {
 *       provide: MySqlOutboxStore,
 *       inject: [getDrizzleToken(), OutboxStorage],
 *       useFactory: (db: Database, storage: OutboxStorage) => new MySqlOutboxStore({ executor: fromDrizzle(db) }, storage),
 *     },
 *   ],
 * })
 * export class AppModule {}
 * ```
 *
 * Given `storage`, it registers itself for both contracts (`storage.registerSource({ messages: this, inbox: this })`),
 * in a service that only consumes too: the tables it doesn't use stay empty. It checks the server and its schema, or
 * migrates it (`migrate`), then creates the rows of its locks (the claim's, and the 16,384 buckets the message keys
 * share), in `onModuleInit` (so before the relay starts), or at its first call outside Nest.
 *
 * Ids, topics, keys and consumer names are compared byte for byte, and hold at most 255 characters (a longer one fails
 * with a `RangeError` before any statement). Keys differing only in case or accents are different keys.
 */
export class MySqlOutboxStore implements OutboxStore, OutboxInboxStore, OnModuleInit {
  /**
   * The SQL of the store's migrations, for teams that apply migrations with their own tool (drizzle-kit, TypeORM,
   * Prisma Migrate, Flyway...) and run the store with `migrate: false`: from version `from` (default `0`, a new
   * database) to `to` (default: the version this version of the package needs), with the bookkeeping that tells the
   * store which versions a schema has (`<schema>_migrations`). MySQL commits each DDL statement on its own, so the
   * statements don't run in one transaction: apply them in order, each once. With `statementBreakpoints`, drizzle-kit's
   * `--> statement-breakpoint` separates them, for a custom drizzle-kit migration: its MySQL migrator then sends them one
   * at a time, which mysql2 needs. Downgrades aren't supported.
   *
   * ```ts
   * // drizzle/0003_outbox.sql, created empty by `npx drizzle-kit generate --custom --name=outbox`
   * writeFileSync('drizzle/0003_outbox.sql', MySqlOutboxStore.migrationSql({ statementBreakpoints: true }));
   * ```
   */
  static migrationSql(options: MigrationSqlOptions = {}): string {
    return mysqlOutboxSchema.sql(options);
  }

  /**
   * `migrationSql()`'s statements, one per string, for a migration tool that runs one statement per call (TypeORM's
   * `queryRunner.query()`, mysql2's `query()`).
   *
   * ```ts
   * export class Outbox1790000000000 implements MigrationInterface {
   *   async up(queryRunner: QueryRunner): Promise<void> {
   *     for (const statement of MySqlOutboxStore.migrationStatements()) {
   *       await queryRunner.query(statement);
   *     }
   *   }
   * }
   * ```
   */
  static migrationStatements(options: MigrationStatementsOptions = {}): string[] {
    return mysqlOutboxSchema.statements(options);
  }

  /**
   * The schema version this version of the package needs: its last migration.
   *
   * ```ts
   * const [rows] = await pool.query('SELECT MAX(version) AS version FROM nest_outbox_migrations WHERE applied_at IS NOT NULL');
   * const behind = rows[0].version < MySqlOutboxStore.schemaVersion;
   * ```
   */
  static readonly schemaVersion = mysqlOutboxSchema.latest;

  private readonly logger = new Logger('OutboxModule');
  private readonly executor: SqlExecutor;
  private readonly schema: string;
  private readonly t: Record<'messages' | 'deadLetters' | 'inbox', string>;
  /** The server and the schema checked, the schema migrated (`migrate`), before the first statement. */
  private readonly readiness: StoreReadiness;
  /** The rows of the store's locks, created once the schema is ready (see `ready()`). */
  private lockRows?: Promise<void>;

  constructor(options: MySqlOutboxStoreOptions, storage?: OutboxStorage) {
    const resolved = mysqlOutboxSchema.resolveOptions(options);
    this.executor = resolved.executor;
    this.schema = resolved.schema;
    const table = (name: string) => quoteTable(this.schema, name, mysqlOutboxSchema.storeName);
    this.t = { messages: table('messages'), deadLetters: table('dead_letters'), inbox: table('inbox') };
    this.readiness = mysqlOutboxSchema.readiness({ ...resolved, logger: this.logger });
    storage?.registerSource({ messages: this, inbox: this });
  }

  /**
   * Checks the server, migrates the schema (`migrate`) or checks it, and creates the rows of the store's locks, before
   * the relay starts.
   */
  async onModuleInit(): Promise<void> {
    await this.ready();
  }

  /**
   * Applies the migrations the schema hasn't had yet, whatever `migrate` says, one statement at a time under a lock
   * (`GET_LOCK()`): of processes that migrate together, one applies them, and one that stopped halfway resumes where it
   * stopped. Resolves to the versions it applied (`[]`: none were pending).
   */
  migrate(): Promise<number[]> {
    return this.readiness.migrate();
  }

  // ---------------------------------------------------------------- producer

  /**
   * Inserts the messages through the application's transaction. First it locks, on that transaction, the lock row of
   * each key's bucket (`<schema>_locks`), so a key's messages are numbered in the order their transactions commit.
   * Anything but a transaction object of the executor's client fails with `OutboxTransactionRequiredError`; an id,
   * topic or key longer than 255 characters with a `RangeError`, before any statement. A deadlock in the application's
   * transaction (MySQL's error 1213) rolled it back, so it rejects: the application runs its transaction again.
   */
  async add(transaction: unknown, messages: readonly OutboxMessage[]): Promise<void> {
    const tx = this.inTransaction(transaction, 'add');
    if (messages.length === 0) {
      return;
    }

    const rows = messages.map((m) => {
      checkLength(m.id, 'a message id', 'add');
      checkLength(m.topic, 'a topic', 'add');
      if (m.key !== null && m.key !== undefined) {
        checkLength(m.key, 'a key', 'add');
      }
      return {
        id: m.id,
        topic: m.topic,
        payload: m.payload,
        headers: m.headers,
        key: m.key ?? null,
        createdAt: epochMs(m.createdAt),
        availableAt: epochMs(m.availableAt),
      };
    });
    await this.readiness.readyIn(tx);

    const locks = keyLocks(rows.map((row) => row.key));
    if (locks.length > 0) {
      await lockKeys(tx, this.schema, locks);
    }

    // One JSON document per statement, whatever the batch's size, numbered in the batch's order: SqlParams.json(), which
    // sets the payloads' fractions with CAST(? AS DOUBLE), as MySQL 8's JSON text parser can round them.
    // ERROR ON ERROR: JSON_TABLE() otherwise turns a value its column can't hold into NULL.
    for (const batch of batches(rows)) {
      const p = new SqlParams();
      await tx.execute(
        `INSERT INTO ${this.t.messages} (id, topic, payload, headers, \`key\`, created_at, available_at, history)
SELECT j.id, j.topic, NULLIF(j.payload, CAST('null' AS JSON)), j.headers, j.message_key, j.created_at, j.available_at, JSON_ARRAY()
FROM JSON_TABLE(${p.json(batch)}, '$[*]' COLUMNS (
  n FOR ORDINALITY,
  id varchar(255) PATH '$.id' ERROR ON ERROR,
  topic varchar(255) PATH '$.topic' ERROR ON ERROR,
  payload json PATH '$.payload' ERROR ON ERROR,
  headers json PATH '$.headers' ERROR ON ERROR,
  message_key varchar(255) PATH '$.key' ERROR ON ERROR,
  created_at bigint PATH '$.createdAt' ERROR ON ERROR,
  available_at bigint PATH '$.availableAt' ERROR ON ERROR
)) AS j
ORDER BY j.n`,
        p.values,
      );
    }
  }

  // ---------------------------------------------------------------- relay

  /**
   * Leases the claimable messages in `seq` order, in one READ COMMITTED transaction under the claim lock (claims take
   * turns, so each sees the leases of the one before it):
   * 1. The claimable rows, locked, in `seq` order up to `limit`: due, unleased (or the lease expired), and the first of
   *    their key that is delayed, leased, or themselves is themselves. `SKIP LOCKED` passes over a row another
   *    connection is writing at that moment (a relay's fenced write, a release).
   * 2. For each of them with a key, whether a message of the key sits right before it that isn't one of the rows just
   *    locked (passed over, or committed since): then it stays unleased, and the rest of its key with it, so no key's
   *    run is split or reordered. A read of each gap's index range.
   * 3. The lease, on the rows kept.
   */
  async claim({ owner, now, leaseMs, limit }: OutboxClaimRequest): Promise<OutboxMessage[]> {
    checkLength(owner, 'a lease owner', 'claim');
    await this.ready();

    const at = epochMs(now);
    const until = epochMs(now + leaseMs);
    return retryOnDeadlock(this.executor, async (tx) => {
      await lockKeys(tx, this.schema, CLAIM_LOCK);

      // The rows come off the primary key in `seq` order, the scan stopping at `limit` (the index is pinned: a plan that
      // sorted would lock every claimable row, and a prepared statement's plan can differ). The subquery reads the key's
      // messages from its oldest, and stops at the first that is delayed or leased, or at the row itself: a probe as
      // long as the row's place in its key (MySQL doesn't bound an index range by the outer row's `seq`: a
      // `seq < m.seq` condition would read the whole key).
      const p = new SqlParams();
      const due = await tx.query<Row>(
        `SELECT CAST(m.seq AS CHAR) AS seq, ${columns(MESSAGE_COLUMNS, 'm')}
FROM ${this.t.messages} m FORCE INDEX (PRIMARY)
WHERE m.available_at <= ${p.bigint(at)} AND (m.lease_until IS NULL OR m.lease_until <= ${p.bigint(at)})
  AND (m.\`key\` IS NULL OR (
    SELECT o.seq FROM ${this.t.messages} o FORCE INDEX (messages_key_seq)
    WHERE o.\`key\` = m.\`key\` AND (o.seq >= m.seq OR o.available_at > ${p.bigint(at)} OR o.lease_until > ${p.bigint(at)})
    ORDER BY o.\`key\`, o.seq
    LIMIT 1
  ) = m.seq)
ORDER BY m.seq
LIMIT ${p.limit(limit)}
FOR UPDATE OF m SKIP LOCKED`,
        p.values,
      );
      if (due.length === 0) {
        return [];
      }

      const gaps = await this.gaps(tx, due);
      const cut = new Set<string>();
      const taken = due.filter((row) => {
        if (row.key === null) {
          return true;
        }
        if (cut.has(row.key) || gaps.has(row.seq!)) {
          cut.add(row.key);
          return false;
        }
        return true;
      });
      if (taken.length > 0) {
        const u = new SqlParams();
        await tx.execute(
          `UPDATE ${this.t.messages} SET lease_owner = ${u.text(owner)}, lease_until = ${u.bigint(until)}
WHERE seq IN (${taken.map((row) => `CAST(${u.text(row.seq)} AS SIGNED)`).join(', ')})`,
          u.values,
        );
      }
      return taken.map(toMessage);
    });
  }

  /** One `DELETE`, fenced by the lease owner. */
  async markPublished(id: string, owner: string): Promise<boolean> {
    await this.ready();
    const p = new SqlParams();
    const { affectedRows } = await this.executor.execute(
      `DELETE FROM ${this.t.messages} WHERE id = ${p.text(id)} AND lease_owner = ${p.text(owner)}`,
      p.values,
    );
    return affectedRows === 1;
  }

  /** One `UPDATE`, fenced by the lease owner, that appends to the history in the statement that checks the lease. */
  async reschedule(id: string, owner: string, update: OutboxRescheduleUpdate): Promise<boolean> {
    await this.ready();
    const p = new SqlParams();
    const { affectedRows } = await this.executor.execute(
      `UPDATE ${this.t.messages}
SET attempts = ${p.int(update.attempts)}, available_at = ${p.bigint(epochMs(update.availableAt))}, last_error = ${p.text(update.error.error)},
  history = JSON_MERGE_PRESERVE(history, ${p.json([update.error])}), lease_owner = NULL, lease_until = NULL
WHERE id = ${p.text(id)} AND lease_owner = ${p.text(owner)}`,
      p.values,
    );
    return affectedRows === 1;
  }

  /**
   * One READ COMMITTED transaction, fenced by the lease owner (the message's row, locked): the message's delete and the
   * dead letter's insert commit together or not at all. The dead letter keeps `seq`, and replaces an earlier one with
   * the same id (a producer that reused a custom id).
   */
  async deadLetter(id: string, owner: string, update: OutboxDeadLetterUpdate): Promise<boolean> {
    await this.ready();
    return retryOnDeadlock(this.executor, async (tx) => {
      const p = new SqlParams();
      const [held] = await tx.query<Row>(
        `SELECT CAST(m.seq AS CHAR) AS seq FROM ${this.t.messages} m WHERE m.id = ${p.text(id)} AND m.lease_owner = ${p.text(owner)} FOR UPDATE`,
        p.values,
      );
      if (!held) {
        return false;
      }

      const d = new SqlParams();
      await tx.execute(`DELETE FROM ${this.t.deadLetters} WHERE id = ${d.text(id)}`, d.values);
      const i = new SqlParams();
      await tx.execute(
        `INSERT INTO ${this.t.deadLetters} (id, seq, topic, payload, headers, \`key\`, created_at, attempts, last_error, history, reason, failed_at)
SELECT m.id, m.seq, m.topic, m.payload, m.headers, m.\`key\`, m.created_at, ${i.int(update.attempts)}, ${i.text(update.error.error)},
  JSON_MERGE_PRESERVE(m.history, ${i.json([update.error])}), ${i.text(update.reason)}, ${i.bigint(epochMs(update.failedAt))}
FROM ${this.t.messages} m WHERE m.seq = CAST(${i.text(held.seq!)} AS SIGNED)`,
        i.values,
      );
      const x = new SqlParams();
      await tx.execute(`DELETE FROM ${this.t.messages} WHERE seq = CAST(${x.text(held.seq!)} AS SIGNED)`, x.values);
      return true;
    });
  }

  /** `UPDATE`s fenced by the lease owner, in one READ COMMITTED transaction. */
  async release(ids: readonly string[], owner: string): Promise<number> {
    if (ids.length === 0) {
      return 0;
    }

    await this.ready();
    return retryOnDeadlock(this.executor, async (tx) => {
      let released = 0;
      for (const chunk of chunks(ids)) {
        const p = new SqlParams();
        const { affectedRows } = await tx.execute(
          `UPDATE ${this.t.messages} SET lease_owner = NULL, lease_until = NULL WHERE lease_owner = ${p.text(owner)} AND ${p.in('id', chunk)}`,
          p.values,
        );
        released += affectedRows;
      }
      return released;
    });
  }

  /**
   * One statement, so the counts come from one snapshot. `ready` is one pass over the messages in `seq` order per key:
   * a running count of the key's delayed or leased messages, which a ready one has none of before it. (A `MAX()` over
   * the rows before each one, `bool_or()`'s counterpart, MySQL computes again for every row: 26 seconds for a key of
   * 20,000 messages, where the running count takes milliseconds.)
   */
  async stats(now: number): Promise<OutboxStoreStats> {
    await this.ready();
    const p = new SqlParams();
    const at = epochMs(now);
    const [row] = await this.executor.query<Row>(
      `SELECT
  CAST((SELECT COUNT(*) FROM ${this.t.messages}) AS CHAR) AS pending,
  CAST((SELECT COUNT(*) FROM (
    SELECT w.message_key, w.free, SUM(w.blocking) OVER (PARTITION BY w.message_key ORDER BY w.seq ROWS UNBOUNDED PRECEDING) AS blocked
    FROM (
      SELECT m.\`key\` AS message_key, m.seq,
        m.available_at <= ${p.bigint(at)} AND (m.lease_until IS NULL OR m.lease_until <= ${p.bigint(at)}) AS free,
        CASE WHEN m.available_at > ${p.bigint(at)} OR m.lease_until > ${p.bigint(at)} THEN 1 ELSE 0 END AS blocking
      FROM ${this.t.messages} m
    ) w
  ) r WHERE r.free AND (r.message_key IS NULL OR r.blocked = 0)) AS CHAR) AS ready,
  CAST((SELECT COUNT(*) FROM ${this.t.messages} WHERE lease_until > ${p.bigint(at)}) AS CHAR) AS leased,
  CAST((SELECT COUNT(*) FROM ${this.t.deadLetters}) AS CHAR) AS dead_letters,
  CAST((SELECT MIN(CASE WHEN attempts > 0 THEN created_at WHEN available_at <= ${p.bigint(at)} THEN available_at END) FROM ${this.t.messages}) AS CHAR) AS oldest_due_at`,
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

  /** Newest first; ids that failed at the same time in byte order (the id column's binary collation). */
  async listDeadLetters(query: OutboxDeadLetterQuery): Promise<OutboxDeadLetter[]> {
    const { limit, offset } = deadLetterPage(query);
    await this.ready();

    const p = new SqlParams();
    const where = [
      ...(query.topic !== undefined ? [`d.topic = ${p.text(query.topic)}`] : []),
      ...(query.key !== undefined ? [`d.\`key\` = ${p.text(query.key)}`] : []),
    ];
    const rows = await this.executor.query<Row>(
      `SELECT ${columns(DEAD_LETTER_COLUMNS, 'd')} FROM ${this.t.deadLetters} d${where.length > 0 ? ` WHERE ${where.join(' AND ')}` : ''}
ORDER BY d.failed_at DESC, d.id DESC LIMIT ${p.limit(limit)} OFFSET ${p.limit(offset)}`,
      p.values,
    );
    return rows.map(toDeadLetter);
  }

  async getDeadLetter(id: string): Promise<OutboxDeadLetter | undefined> {
    await this.ready();
    const p = new SqlParams();
    const [row] = await this.executor.query<Row>(
      `SELECT ${columns(DEAD_LETTER_COLUMNS, 'd')} FROM ${this.t.deadLetters} d WHERE d.id = ${p.text(id)}`,
      p.values,
    );
    return row && toDeadLetter(row);
  }

  /**
   * One READ COMMITTED transaction: the matching dead letters, locked (so a concurrent requeue or purge waits, then
   * finds them gone), go back to the messages with their `seq`, and are deleted, by id: the filter is read once, so a
   * dead letter that arrives meanwhile stays. A pending message with the id of one of them (a producer that reused a
   * custom id) fails it, moving nothing.
   */
  async requeueDeadLetters(filter: OutboxDeadLetterFilter, now: number): Promise<number> {
    const conditions = deadLetterConditions(filter);
    if (!conditions) {
      return 0;
    }

    await this.ready();
    return retryOnDeadlock(this.executor, async (tx) => {
      const p = new SqlParams();
      const matched = await tx.query<Row>(
        `SELECT d.id FROM ${this.t.deadLetters} d WHERE ${this.deadLetterWhere(p, conditions)} ORDER BY d.id FOR UPDATE`,
        p.values,
      );
      const ids = matched.map((row) => row.id!);
      for (const chunk of chunks(ids)) {
        await this.refuseClash(tx, chunk);
      }

      let requeued = 0;
      for (const chunk of chunks(ids)) {
        const i = new SqlParams();
        try {
          const { affectedRows } = await tx.execute(
            `INSERT INTO ${this.t.messages} (seq, id, topic, payload, headers, \`key\`, created_at, available_at, attempts, last_error, history)
SELECT d.seq, d.id, d.topic, d.payload, d.headers, d.\`key\`, d.created_at, ${i.bigint(epochMs(now))}, 0, d.last_error, d.history
FROM ${this.t.deadLetters} d WHERE ${i.in('d.id', chunk)} ORDER BY d.seq`,
            i.values,
          );
          requeued += affectedRows;
        } catch (error) {
          // A producer added a message with one of the ids since the check.
          if (mysqlErrorCode(error) === DUPLICATE_KEY) {
            await this.refuseClash(tx, chunk);
          }
          throw error;
        }

        const x = new SqlParams();
        await tx.execute(`DELETE FROM ${this.t.deadLetters} WHERE ${x.in('id', chunk)}`, x.values);
      }
      return requeued;
    });
  }

  /** One `DELETE`, in a READ COMMITTED transaction. */
  async purgeDeadLetters(filter: OutboxDeadLetterFilter): Promise<number> {
    const conditions = deadLetterConditions(filter);
    if (!conditions) {
      return 0;
    }

    await this.ready();
    return retryOnDeadlock(this.executor, async (tx) => {
      const p = new SqlParams();
      const { affectedRows } = await tx.execute(`DELETE FROM ${this.t.deadLetters} AS d WHERE ${this.deadLetterWhere(p, conditions)}`, p.values);
      return affectedRows;
    });
  }

  // ---------------------------------------------------------------- inbox

  /**
   * Insert-if-absent on the `(consumer, message_id)` key, in one statement: a plain `INSERT`, and MySQL's duplicate-key
   * error (1062) for a record that exists, which rolls back the statement, not the transaction (never `INSERT IGNORE`,
   * which turns other errors into warnings). A concurrent delivery of the message waits for this transaction, then finds
   * the record. Through the application's transaction when given one (anything but a transaction object of the
   * executor's client fails with `OutboxTransactionRequiredError`), else at once.
   */
  async recordInbox(transaction: unknown, consumer: string, messageId: string, now: number): Promise<boolean> {
    let db: SqlTransaction = this.executor;
    if (transaction !== undefined) {
      db = this.inTransaction(transaction, 'recordInbox');
    }
    checkLength(consumer, 'a consumer name', 'recordInbox');
    checkLength(messageId, 'a message id', 'recordInbox');
    await (transaction === undefined ? this.ready() : this.readiness.readyIn(db));

    const p = new SqlParams();
    try {
      await db.execute(
        `INSERT INTO ${this.t.inbox} (consumer, message_id, processed_at) VALUES (${p.text(consumer)}, ${p.text(messageId)}, ${p.bigint(epochMs(now))})`,
        p.values,
      );
      return true;
    } catch (error) {
      if (mysqlErrorCode(error) === DUPLICATE_KEY) {
        return false;
      }
      throw error;
    }
  }

  /**
   * Whether `consumer` recorded `messageId`. A consumer name or message id longer than its column fails with
   * `recordInbox()`'s `RangeError`, before any statement: no record of it could exist, and `OutboxInbox.process()` would
   * otherwise run the handler, then fail to record it, at every redelivery.
   */
  async hasInbox(consumer: string, messageId: string): Promise<boolean> {
    checkLength(consumer, 'a consumer name', 'hasInbox');
    checkLength(messageId, 'a message id', 'hasInbox');
    await this.ready();
    const p = new SqlParams();
    const rows = await this.executor.query<Row>(
      `SELECT CAST(1 AS CHAR) AS found FROM ${this.t.inbox} WHERE consumer = ${p.text(consumer)} AND message_id = ${p.text(messageId)} LIMIT 1`,
      p.values,
    );
    return rows.length === 1;
  }

  /** One `DELETE`, in a READ COMMITTED transaction (no gap locks that would hold up the records being written). */
  async pruneInbox(before: number): Promise<number> {
    await this.ready();
    return retryOnDeadlock(this.executor, async (tx) => {
      const p = new SqlParams();
      const { affectedRows } = await tx.execute(`DELETE FROM ${this.t.inbox} WHERE processed_at < ${p.bigint(Math.ceil(before))}`, p.values);
      return affectedRows;
    });
  }

  // ---------------------------------------------------------------- internals

  /** The store's readiness (the server and the schema), then the rows of its locks: before its own first statement. */
  private async ready(): Promise<void> {
    await this.readiness.ready();
    await (this.lockRows ??= this.createLockRows());
  }

  /**
   * Creates the rows of every lock the store takes, the claim lock's and the 16,384 buckets', with the kit's
   * `ensureLockRows()`: in transactions of their own, which commit before any transaction takes those locks. When the
   * transaction that created a lock's row rolls back, MySQL makes the transactions waiting for that row deadlock
   * (1213): without the rows, a bucket's first producer, in the application's transaction, would create its row, and
   * its rollback would deadlock the producers waiting for the bucket. The rows there already are only noted, by reads
   * that take no lock (a starting process never waits for a producer's transaction). A failure is tried again at the
   * next call. (An `add()` that is the store's first call, inside the application's transaction before `onModuleInit()`
   * or any call outside one, creates the rows it locks there.)
   */
  private createLockRows(): Promise<void> {
    return ensureLockRows(this.executor, this.schema, [CLAIM_LOCK, ...bucketLocks()]).catch((error: unknown) => {
      this.lockRows = undefined;
      throw error;
    });
  }

  /**
   * The application's transaction; anything else (the database, the pool) is refused before any statement. A mysql2
   * connection outside a transaction is refused at its first statement (the kit reads the server's status then, before
   * anything is written): the same error. The kit's refusals (`isNotATransactionError()`: their code, never their class)
   * become `OutboxTransactionRequiredError`, with the refusal as the cause; any other error, a statement's own
   * `TypeError` included, goes through as it is.
   */
  private inTransaction(transaction: unknown, method: string): SqlTransaction {
    const refused = (error: unknown) =>
      isNotATransactionError(error) ? new OutboxTransactionRequiredError(`MySqlOutboxStore.${method}(): ${error.message}`, { cause: error }) : error;
    let tx: SqlTransaction;
    try {
      tx = this.executor.wrapTransaction(transaction);
    } catch (error) {
      throw refused(error);
    }

    let first = true;
    const run = async <T>(statement: () => Promise<T>): Promise<T> => {
      if (!first) {
        return statement();
      }
      first = false;
      try {
        return await statement();
      } catch (error) {
        throw refused(error);
      }
    };
    return {
      query: <R extends object>(text: string, params?: readonly unknown[]) => run(() => tx.query<R>(text, params)),
      execute: (text: string, params?: readonly unknown[]) => run(() => tx.execute(text, params)),
    };
  }

  /**
   * The rows of `due` (in `seq` order) that a message of their key sits right before, other than the previous row of
   * the key in `due`: for the first of a key, any older message of the key; for the next, any between the two. One
   * index range read per row, with constant bounds, in one statement.
   */
  private async gaps(tx: SqlTransaction, due: readonly Row[]): Promise<Set<string>> {
    const p = new SqlParams();
    const reads: string[] = [];
    const previous = new Map<string, string>();
    for (const row of due) {
      if (row.key === null) {
        continue;
      }

      const after = previous.get(row.key);
      reads.push(
        `(SELECT CAST(${p.text(row.seq)} AS CHAR) AS seq FROM ${this.t.messages} o FORCE INDEX (messages_key_seq)
WHERE o.\`key\` = ${p.text(row.key)} AND o.seq < CAST(${p.text(row.seq)} AS SIGNED)${after === undefined ? '' : ` AND o.seq > CAST(${p.text(after)} AS SIGNED)`}
LIMIT 1)`,
      );
      previous.set(row.key, row.seq!);
    }
    if (reads.length === 0) {
      return new Set();
    }

    const rows = await tx.query<Row>(reads.join('\nUNION ALL\n'), p.values);
    return new Set(rows.map((row) => row.seq!));
  }

  /** Throws the requeue clash's error when a pending message has one of `ids`. */
  private async refuseClash(tx: SqlTransaction, ids: readonly string[]): Promise<void> {
    const p = new SqlParams();
    const [clash] = await tx.query<Row>(`SELECT m.id FROM ${this.t.messages} m WHERE ${p.in('m.id', ids)} ORDER BY m.id LIMIT 1`, p.values);
    if (clash) {
      throw new Error(`MySqlOutboxStore: can't requeue dead letter "${clash.id}": a message with that id is pending.`);
    }
  }

  /** The dead letters (as `d`) a filter's conditions match. */
  private deadLetterWhere(p: SqlParams, conditions: DeadLetterConditions): string {
    const where = [
      ...(conditions.ids !== undefined ? [p.in('d.id', [...conditions.ids])] : []),
      ...(conditions.topic !== undefined ? [`d.topic = ${p.text(conditions.topic)}`] : []),
      ...(conditions.key !== undefined ? [`d.\`key\` = ${p.text(conditions.key)}`] : []),
      ...(conditions.failedBefore !== undefined ? [`d.failed_at < ${p.bigint(conditions.failedBefore)}`] : []),
    ];
    return where.length > 0 ? where.join(' AND ') : 'TRUE';
  }
}

/** `values` in statements of at most `IDS_PER_STATEMENT`. */
function chunks<T>(values: readonly T[]): T[][] {
  const result: T[][] = [];
  for (let start = 0; start < values.length; start += IDS_PER_STATEMENT) {
    result.push(values.slice(start, start + IDS_PER_STATEMENT));
  }
  return result;
}

/** `rows` in consecutive batches whose JSON is at most `BATCH_JSON_LENGTH` long (a larger row goes alone). */
function batches<T>(rows: readonly T[]): T[][] {
  const result: T[][] = [];
  let batch: T[] = [];
  let length = 0;
  for (const row of rows) {
    const size = JSON.stringify(row).length + 1;
    if (batch.length > 0 && length + size > BATCH_JSON_LENGTH) {
      result.push(batch);
      batch = [];
      length = 0;
    }
    batch.push(row);
    length += size;
  }
  if (batch.length > 0) {
    result.push(batch);
  }
  return result;
}
