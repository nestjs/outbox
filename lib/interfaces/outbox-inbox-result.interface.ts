export type OutboxInboxResult<T> = { duplicate: true } | { duplicate: false; result: T };
