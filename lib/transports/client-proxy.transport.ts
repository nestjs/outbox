import { Inject, Injectable, type InjectionToken, type Type } from '@nestjs/common';
import { lastValueFrom } from 'rxjs';
import type { OutboxHeaders, OutboxMessage } from '../interfaces/outbox-message.interface.js';
import { OutboxTransport } from './outbox.transport.js';
import type {
  OutboxEnvelope,
  EventEmittingClient,
  ClientProxyTransportOptions,
} from '../interfaces/client-proxy-transport.interface.js';

/**
 * A transport that publishes through the `ClientProxy` registered under `client` (the
 * `name` you gave `ClientsModule`), with `emit()`. Nest instantiates it inside
 * `OutboxModule`, so the client must be visible there: import its `ClientsModule` in
 * `imports`, or register it with `isGlobal`.
 *
 * "Published" means the client's emit observable completed. What that proves depends
 * on the transport: Kafka (acks), RabbitMQ with publisher confirms and NATS JetStream
 * confirm receipt by the broker; TCP, Redis pub/sub and core NATS only confirm the
 * write left this process. An emit can't be called back, so the relay's abort signal is
 * ignored: an emit that outlives `publishTimeout` may still arrive, next to its retry.
 */
export function ClientProxyTransport(
  client: InjectionToken,
  options: ClientProxyTransportOptions = {},
): Type<OutboxTransport> {
  @Injectable()
  class ClientProxyTransportHost extends OutboxTransport {
    private readonly toPacket: NonNullable<ClientProxyTransportOptions['toPacket']>;

    constructor(@Inject(client) private readonly client: EventEmittingClient) {
      super();
      this.toPacket = options.toPacket ?? (isKafkaClient(client) ? toKafkaRecord : envelopePacket);
    }

    async publish(message: OutboxMessage): Promise<void> {
      // A plain call, not a method call: a user's toPacket must not see the transport as `this`.
      const { toPacket } = this;
      const { pattern, data } = toPacket(message, toEnvelope(message));
      await lastValueFrom(this.client.emit(pattern, data), { defaultValue: undefined });
    }
  }

  // Names the client in DI errors: "Nest can't resolve dependencies of ClientProxyTransport(ANALYTICS)".
  Object.defineProperty(ClientProxyTransportHost, 'name', {
    value: `ClientProxyTransport(${describeToken(client)})`,
  });
  return ClientProxyTransportHost;
}

function envelopePacket(
  message: OutboxMessage,
  envelope: OutboxEnvelope,
): { pattern: string; data: OutboxEnvelope } {
  return { pattern: message.topic, data: envelope };
}

/**
 * The default `toPacket` through a `ClientKafka`: a Kafka record under the message's key
 * and headers, with the envelope as its value. The client's default serializer sends data
 * that has a `key` or a `value` as the record itself, and the envelope has a `key`: as
 * data, it would go out as a record with a null value. As the record key, the message's
 * key keeps a key's messages on one partition under the default partitioner, in the order
 * the relay publishes them.
 *
 * The transport recognizes a `ClientKafka` by the object injected under its token, so one
 * behind a facade or a test double with only `emit()` is not recognized: it opts in with
 * `toPacket: toKafkaRecord`.
 */
export function toKafkaRecord(
  message: OutboxMessage,
  envelope: OutboxEnvelope,
): {
  pattern: string;
  data: { key: string | null; value: OutboxEnvelope; headers: OutboxHeaders };
} {
  return {
    pattern: message.topic,
    data: { key: message.key, value: envelope, headers: message.headers },
  };
}

/**
 * `ClientKafka`, recognized by a method of `ClientKafkaProxy` that no other Nest client has.
 * Structural, so the package has no runtime dependency on `@nestjs/microservices`.
 */
function isKafkaClient(client: unknown): boolean {
  return (
    client !== null &&
    typeof client === 'object' &&
    'subscribeToResponseOf' in client &&
    typeof client.subscribeToResponseOf === 'function'
  );
}

function toEnvelope<P>(message: OutboxMessage<P>): OutboxEnvelope<P> {
  return {
    id: message.id,
    topic: message.topic,
    key: message.key,
    headers: message.headers,
    createdAt: message.createdAt,
    payload: message.payload,
  };
}

function describeToken(token: InjectionToken): string {
  if (typeof token === 'function') {
    return token.name;
  }
  if (typeof token === 'symbol') {
    return token.description ?? token.toString();
  }
  return String(token);
}
