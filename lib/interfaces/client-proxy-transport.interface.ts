import type { Observable } from 'rxjs';
import type { OutboxHeaders, OutboxMessage } from './outbox-message.interface.js';

/**
 * What a remote consumer receives by default: enough to deduplicate (`id`) and to
 * keep per-key order downstream (`key`).
 */
export interface OutboxEnvelope<P = unknown> {
  id: string;
  topic: string;
  key: string | null;
  headers: OutboxHeaders;
  createdAt: number;
  payload: P;
}

/**
 * The part of `ClientProxy` this transport uses. Structural, so the package has no
 * runtime or type dependency on `@nestjs/microservices`.
 */
export interface EventEmittingClient {
  emit(pattern: any, data: any): Observable<unknown>;
}

export interface ClientProxyTransportOptions {
  /**
   * Maps a message to `emit(pattern, data)`. Default: the topic as the pattern and the
   * `OutboxEnvelope` (the second argument) as data. Return a transport record
   * (`RmqRecord`, `NatsRecord`, a Kafka `{ key, value, headers }`) as `data` to use
   * broker headers and keys.
   */
  toPacket?: (message: OutboxMessage, envelope: OutboxEnvelope) => { pattern: unknown; data: unknown };
}
