import type { OutboxMessage } from '../interfaces/outbox-message.interface.js';
import type { Awaitable } from '../interfaces/awaitable.interface.js';

/**
 * Hands a message to a broker. Resolving means the broker accepted it; throwing counts
 * as a failed attempt (throw `NonRetryableMessageError` to dead-letter it at once).
 *
 * Register it by name in `transports`: the class (Nest instantiates it, so it can
 * inject a client or a queue) or an instance.
 */
export abstract class OutboxTransport {
  /**
   * `signal` aborts when the relay stops waiting (`relay.publishTimeout`) and counts the
   * attempt as failed. Pass it to the client call, so the publish stops too.
   */
  abstract publish(message: OutboxMessage, options: { signal: AbortSignal }): Awaitable<void>;
}
