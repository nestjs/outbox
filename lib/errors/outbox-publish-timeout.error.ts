import { OutboxError } from './outbox.error.js';

/**
 * A publish (a transport call, or the in-process handlers) outlived `relay.publishTimeout`.
 * The relay stops waiting and counts a failed attempt; the call itself may still be running.
 */
export class OutboxPublishTimeoutError extends OutboxError {
  override name = 'OutboxPublishTimeoutError';
  constructor(readonly timeoutMs: number) {
    super(`Publish did not settle within ${timeoutMs}ms`);
  }
}
