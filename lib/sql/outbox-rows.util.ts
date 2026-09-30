// The SQL stores' rows (internal): the columns of their `messages` and `dead_letters` tables, which every dialect's
// migrations name the same, and what a row read with every column as text becomes.
import type { OutboxAttempt, OutboxDeadLetter, OutboxDeadLetterReason } from '../interfaces/outbox-dead-letter.interface.js';
import type { OutboxHeaders, OutboxMessage } from '../interfaces/outbox-message.interface.js';

/** A row as a SQL store reads it: every column as text (`NULL` as `null`), so no driver's type parsing applies. */
export type Row = Record<string, string | null>;

/** The columns an `OutboxMessage` is made of. */
export const MESSAGE_COLUMNS = ['id', 'topic', 'payload', 'headers', 'key', 'created_at', 'available_at', 'attempts', 'last_error'];

/** The columns an `OutboxDeadLetter` is made of. */
export const DEAD_LETTER_COLUMNS = [
  'id',
  'topic',
  'payload',
  'headers',
  'key',
  'created_at',
  'attempts',
  'last_error',
  'history',
  'reason',
  'failed_at',
];

export function toMessage(row: Row): OutboxMessage {
  return {
    id: row.id!,
    topic: row.topic!,
    payload: json(row.payload),
    headers: json(row.headers) as OutboxHeaders,
    key: row.key,
    createdAt: Number(row.created_at),
    availableAt: Number(row.available_at),
    attempts: Number(row.attempts),
    lastError: row.last_error,
  };
}

export function toDeadLetter(row: Row): OutboxDeadLetter {
  return {
    id: row.id!,
    topic: row.topic!,
    payload: json(row.payload),
    headers: json(row.headers) as OutboxHeaders,
    key: row.key,
    createdAt: Number(row.created_at),
    attempts: Number(row.attempts),
    lastError: row.last_error,
    history: json(row.history) as OutboxAttempt[],
    reason: row.reason as OutboxDeadLetterReason,
    failedAt: Number(row.failed_at),
  };
}

/** A JSON column read as text: SQL `NULL` (a `null` payload) is `null`. */
function json(value: string | null): unknown {
  return value === null ? null : JSON.parse(value);
}
