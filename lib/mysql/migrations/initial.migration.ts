import { keyColumn, type StoreMigration } from '@nestjs/store-kit/mysql';
import { KEY_LENGTH } from '../mysql-outbox-keys.util.js';

/** An id, topic, key, consumer or lease owner: compared byte for byte, at most 255 characters. */
const key = keyColumn(KEY_LENGTH);

/**
 * The store's tables, in the connection's database next to the kit's `<schema>_migrations` and `<schema>_locks`. Times
 * are epoch milliseconds (`bigint`) from the producers' and the relays' clocks; payloads, headers and failure histories
 * are JSON, a `null` payload SQL `NULL`. Ids, topics, keys, consumer names and lease owners use a binary collation
 * (MySQL's default one takes `Order-1` for `order-1`, and `cafe` for `café`); the tables' other text is utf8mb4 too,
 * whatever the database's default. No foreign keys: a dead letter's move deletes the message in its transaction.
 */
export const initialMigration: StoreMigration = {
  version: 1,
  name: 'initial',
  up: (t) => [
    // A message until it's published or dead-lettered. `seq` is its place in its key: numbered at insert, in commit
    // order (add() locks the key's bucket first), never the producers' ids or clocks.
    `CREATE TABLE ${t('messages')} (
  seq bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
  id ${key} NOT NULL,
  topic ${key} NOT NULL,
  payload json,
  headers json NOT NULL,
  \`key\` ${key},
  created_at bigint NOT NULL,
  available_at bigint NOT NULL,
  attempts int NOT NULL DEFAULT 0,
  last_error mediumtext,
  history json NOT NULL DEFAULT (JSON_ARRAY()),
  lease_owner ${key},
  lease_until bigint,
  UNIQUE KEY messages_id (id),
  KEY messages_key_seq (\`key\`, seq)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin`,
    // A dead letter keeps its message's `seq`, so a requeue puts it back at its place in its key.
    `CREATE TABLE ${t('dead_letters')} (
  id ${key} NOT NULL PRIMARY KEY,
  seq bigint NOT NULL,
  topic ${key} NOT NULL,
  payload json,
  headers json NOT NULL,
  \`key\` ${key},
  created_at bigint NOT NULL,
  attempts int NOT NULL,
  last_error mediumtext,
  history json NOT NULL,
  reason varchar(32) NOT NULL,
  failed_at bigint NOT NULL,
  KEY dead_letters_topic_failed_at (topic, failed_at)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin`,
    // One record per consumer and message: the key two deliveries of a message meet at.
    `CREATE TABLE ${t('inbox')} (
  consumer ${key} NOT NULL,
  message_id ${key} NOT NULL,
  processed_at bigint NOT NULL,
  PRIMARY KEY (consumer, message_id),
  KEY inbox_processed_at (processed_at)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin`,
  ],
};
