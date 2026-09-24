import { OutboxError } from './outbox.error.js';

/**
 * No `@OnOutboxMessage` handler for the topic in this process. Retried (a rolling
 * deploy can put an old instance's relay in front of a new producer's message).
 */
export class OutboxNoHandlerError extends OutboxError {
  override name = 'OutboxNoHandlerError';
  constructor(readonly topic: string) {
    super(`No @OnOutboxMessage handler for topic "${topic}"`);
  }
}
