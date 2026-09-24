/**
 * Base class of the errors this package raises: `OutboxTransactionRequiredError` from
 * `add()` and `processInTransaction()`, and the publish failures `retryIf` and the relay's
 * events can see (`OutboxPublishTimeoutError`, `OutboxNoHandlerError`).
 * `NonRetryableMessageError` is not one of them: your code throws it.
 */
export abstract class OutboxError extends Error {}
