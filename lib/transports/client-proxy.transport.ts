import { Inject, Injectable, type InjectionToken, type Type } from '@nestjs/common';
import { lastValueFrom } from 'rxjs';
import type { OutboxMessage } from '../interfaces/outbox-message.interface.js';
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
  const toPacket =
    options.toPacket ?? ((message, envelope) => ({ pattern: message.topic, data: envelope }));

  @Injectable()
  class ClientProxyTransportHost extends OutboxTransport {
    constructor(@Inject(client) private readonly client: EventEmittingClient) {
      super();
    }

    async publish(message: OutboxMessage): Promise<void> {
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
