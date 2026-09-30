// MySqlOutboxStore's keys (internal): how long its key columns are, which lock row a message's key takes, and the
// length checks that run before any statement.
import { createHash } from 'node:crypto';

/**
 * The length, in characters, of the columns that hold ids, topics, keys, consumer names and lease owners. InnoDB caps
 * an index key at 3,072 bytes (768 four-byte characters), and the inbox's key holds two of them.
 */
export const KEY_LENGTH = 255;

/**
 * How many lock rows the message keys share. A key's producers take turns on the row of its bucket (see
 * `keyLocks()`), so the kit's `<schema>_locks` table holds at most this many rows for them, however many keys pass
 * through. It is part of how instances agree: every process that adds to a schema must use the same number, so it
 * never changes for a schema. Two keys share a bucket with a chance of 1 in 16,384: then their producers take turns
 * too.
 */
export const KEY_BUCKETS = 16_384;

/** The bucket of a message's key: the first 32 bits of its SHA-256, modulo `KEY_BUCKETS`. */
export function keyBucket(key: string): number {
  return createHash('sha256').update(key, 'utf8').digest().readUInt32BE(0) % KEY_BUCKETS;
}

/** The lock keys of `keys`' buckets, each once (`lockKeys()` orders them): none for messages without a key. */
export function keyLocks(keys: Iterable<string | null>): string[] {
  const buckets = new Set<number>();
  for (const key of keys) {
    if (key !== null) {
      buckets.add(keyBucket(key));
    }
  }
  return [...buckets].map((bucket) => `key-bucket:${bucket}`);
}

/**
 * Throws a `RangeError` when `value` is longer than its column holds: MySQL would refuse it with an error that names
 * the column, not the value, and a `JSON_TABLE()` column would turn it into NULL. Counts characters as MySQL does
 * (code points, not UTF-16 units).
 */
export function checkLength(value: string, what: string, method: string): void {
  if (value.length <= KEY_LENGTH) {
    return;
  }

  const characters = [...value];
  if (characters.length > KEY_LENGTH) {
    throw new RangeError(
      `MySqlOutboxStore.${method}(): ${what} is at most ${KEY_LENGTH} characters on MySQL (its column's length), and "${characters.slice(0, 40).join('')}..." has ${characters.length}.`,
    );
  }
}
