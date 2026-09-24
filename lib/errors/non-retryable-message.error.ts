/** Throw from a transport or handler to dead-letter the message without retrying. */
export class NonRetryableMessageError extends Error {
  override name = 'NonRetryableMessageError';
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
  }
}
