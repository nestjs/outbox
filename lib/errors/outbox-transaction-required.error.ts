import { OutboxError } from './outbox.error.js';

/**
 * `Outbox.add()` or `processInTransaction()` got no transaction handle, or a handle with
 * no open transaction. Stores throw it too, for a handle they can tell is not a
 * transaction (the database itself).
 */
export class OutboxTransactionRequiredError extends OutboxError {
  override name = 'OutboxTransactionRequiredError';
  constructor(detail = '', options?: { cause?: unknown }) {
    super(
      "This call must run inside the caller's transaction: pass the transaction handle " +
        `as the first argument, between BEGIN and COMMIT.${detail ? ` ${detail}` : ''}`,
      options,
    );
  }
}
