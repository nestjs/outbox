import type { Duration } from '../utils/duration.util.js';
import type { OutboxTransport } from '../transports/outbox.transport.js';
import type { OutboxMessage } from './outbox-message.interface.js';

export interface OutboxBackoffOptions {
  /** Wait before the first retry. Default 1s. */
  delay?: Duration;
  /** Growth per retry; 1 = constant. Default 2. */
  factor?: number;
  /** Cap for a single wait. Default 5m. */
  maxDelay?: Duration;
  /** `equal` (default): uniformly in [d/2, d]. `full`: in [0, d]. `none`: exactly d. */
  jitter?: 'full' | 'equal' | 'none';
}

export interface OutboxRetryOptions {
  /** Total publish attempts, including the first, before dead-lettering. Default 20. */
  attempts?: number;
  /** `attempt` is the number of the attempt that just failed (1-based). */
  backoff?:
    | OutboxBackoffOptions
    | ((attempt: number, error: unknown, message: OutboxMessage) => Duration);
  /** Return false to dead-letter at once. `NonRetryableMessageError` is never retried. */
  retryIf?: (error: unknown, attempt: number, message: OutboxMessage) => boolean;
}

export interface OutboxRelayOptions {
  /** Run the relay in this process. Default true. Turn off in API-only instances or consumer-only apps. */
  enabled?: boolean;
  /** Wait between polls when idle. Default 1s. */
  pollInterval?: Duration;
  /** Messages claimed per poll. Default 100. */
  batchSize?: number;
  /** How long a claim is exclusive. Default 30s. */
  lease?: Duration;
  /** Key groups published in parallel within a batch. Default 10. */
  concurrency?: number;
  /**
   * A publish that takes longer counts as a failed attempt. Default `lease / 3`.
   * The relay never starts a publish with less than this much lease left.
   */
  publishTimeout?: Duration;
}

/**
 * The options `forRootAsync()`'s factory returns, where `transports` are instances.
 * `forRoot()` takes the same options at the top level, where transports may also be classes
 * for Nest to instantiate. The store is not an option: a provider registers it with
 * `OutboxStorage.registerSource()`.
 */
export interface OutboxModuleOptions {
  /**
   * Transport instances by name. They join the top-level `transports`, where classes go;
   * each name is set in one of the two places.
   */
  transports?: Record<string, OutboxTransport>;
  /**
   * Picks the transport for a message: `'local'` (the `@OnOutboxMessage()` handlers) or a
   * name from `transports`. Optional with a single destination; required when this app
   * has both handlers and transports, or several transports.
   */
  route?: (message: OutboxMessage) => string;
  relay?: OutboxRelayOptions;
  /** `5` means `{ attempts: 5 }`; `false` means a single attempt. Default 20 attempts. */
  retry?: number | false | OutboxRetryOptions;
  /**
   * With `NODE_ENV=production`, startup fails while a store the application uses (the
   * messages, the inbox) isn't registered, because the in-memory default loses them on
   * restart. `true` accepts the in-memory store anyway. Default `false`.
   */
  allowInMemoryStorage?: boolean;
}
