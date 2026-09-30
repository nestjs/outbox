// What the SQL stores check in their inputs before any statement (internal): values the in-memory store takes as they
// come, but a column type or the contract's rules refuse.
import type { OutboxDeadLetterFilter, OutboxDeadLetterQuery } from '../interfaces/outbox-dead-letter.interface.js';

/** Epoch milliseconds as a `bigint` column takes them: whole (a numeric `delay`, `availableAt` or lease can have a fraction). */
export function epochMs(value: number): number {
  return Math.floor(value);
}

/** A dead-letter filter's conditions, combined with AND. */
export interface DeadLetterConditions {
  ids?: readonly string[];
  topic?: string;
  key?: string;
  /** Whole epoch milliseconds: `failed_at < failedBefore`. */
  failedBefore?: number;
}

/**
 * The filter's conditions. Refuses an empty filter unless it says `all`, and is `null` for one that matches nothing
 * (`ids: []`), as the contract says.
 */
export function deadLetterConditions(filter: OutboxDeadLetterFilter): DeadLetterConditions | null {
  const { ids, topic, key, failedBefore, all } = filter;
  if (!all && ids === undefined && topic === undefined && key === undefined && failedBefore === undefined) {
    throw new Error('Refusing an empty dead-letter filter; pass { all: true }');
  }
  if (ids?.length === 0) {
    return null;
  }

  const conditions: DeadLetterConditions = { ids, topic, key };
  if (failedBefore !== undefined) {
    const before = +failedBefore;
    if (!Number.isFinite(before)) {
      throw new TypeError(`Dead-letter filter: \`failedBefore\` must be a valid Date or epoch milliseconds (got ${String(failedBefore)})`);
    }

    // `failed_at < 1.5` over whole milliseconds is `failed_at < 2`.
    conditions.failedBefore = Math.ceil(before);
  }

  return conditions;
}

/** `listDeadLetters()`'s page: whole numbers, `limit` 50 and `offset` 0 by default. */
export function deadLetterPage({ limit = 50, offset = 0 }: OutboxDeadLetterQuery): { limit: number; offset: number } {
  for (const [name, value] of [
    ['limit', limit],
    ['offset', offset],
  ] as const) {
    if (!Number.isInteger(value) || value < 0) {
      throw new TypeError(`listDeadLetters(): \`${name}\` must be a whole number of at least 0 (got ${String(value)})`);
    }
  }

  return { limit, offset };
}
