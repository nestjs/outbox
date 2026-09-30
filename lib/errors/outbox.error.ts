/**
 * Base class of the errors this package raises: `OutboxTransactionRequiredError` from
 * `add()` and `processInTransaction()`, the publish failures `retryIf` and the relay's
 * events can see (`OutboxPublishTimeoutError`, `OutboxNoHandlerError`), and
 * `OutboxSchemaError` from the SQL stores. `NonRetryableMessageError` is not one of them:
 * your code throws it.
 */
export abstract class OutboxError extends Error {}
