import { ClientKafka, ClientMqtt, ClientNats, ClientRedis, ClientRMQ, ClientTCP } from '@nestjs/microservices';
import { KafkaRequestSerializer } from '@nestjs/microservices/serializers/kafka-request.serializer.js';
import { Subject, of, throwError, type Observable } from 'rxjs';
import {
  ClientProxyTransport,
  toKafkaRecord,
  type ClientProxyTransportOptions,
  type OutboxEnvelope,
  type OutboxMessage,
} from '../lib/index.js';
import { message } from './helpers.js';

describe('ClientProxyTransport', () => {
  const build = (
    emit: (pattern: unknown, data: unknown) => Observable<unknown>,
    client: object = {},
    options?: ClientProxyTransportOptions,
  ) => {
    const Transport = ClientProxyTransport('BROKER', options) as unknown as new (client: unknown) => {
      publish(message: OutboxMessage): Promise<void>;
    };
    return new Transport(Object.assign(client, { emit }));
  };

  const stored: OutboxMessage = {
    ...message('order.placed', 'order-1'),
    headers: { 'x-tenant': 'tenant-7' },
    payload: { orderId: 1 },
    createdAt: 5,
    availableAt: 6,
    attempts: 2,
    lastError: 'Error: down',
  };
  const envelope: OutboxEnvelope = {
    id: stored.id,
    topic: 'order.placed',
    key: 'order-1',
    headers: { 'x-tenant': 'tenant-7' },
    createdAt: 5,
    payload: { orderId: 1 },
  };

  it('emits the topic as the pattern and the envelope, without the relay bookkeeping, as data', async () => {
    const emitted: unknown[][] = [];
    await build((pattern, data) => (emitted.push([pattern, data]), of(undefined))).publish(stored);

    expect(emitted).toEqual([['order.placed', envelope]]);
  });

  it('emits a Kafka record through a ClientKafka: the envelope as its value, under the message key and headers', async () => {
    // A ClientKafka built without its constructor, which needs kafkajs on NestJS 11. emit()
    // serializes data the way ClientKafka does and keeps the topic (the pattern) and what
    // kafkajs writes of the record: its key, its value (parsed) and its headers.
    const serializer = new KafkaRequestSerializer();
    const sent: unknown[] = [];
    const kafka = build((pattern, data) => {
      const { key, value, headers } = serializer.serialize(data);
      sent.push({ topic: pattern, key, value: JSON.parse(value), headers });
      return of(undefined);
    }, Object.create(ClientKafka.prototype));

    await kafka.publish(stored);
    await kafka.publish({ ...stored, key: null });

    expect(sent).toEqual([
      { topic: 'order.placed', key: 'order-1', value: envelope, headers: { 'x-tenant': 'tenant-7' } },
      { topic: 'order.placed', key: null, value: { ...envelope, key: null }, headers: { 'x-tenant': 'tenant-7' } },
    ]);
  });

  it('emits the envelope itself through the other Nest clients', async () => {
    for (const Client of [ClientTCP, ClientRedis, ClientNats, ClientMqtt, ClientRMQ]) {
      const emitted: unknown[][] = [];
      const emit = (pattern: unknown, data: unknown) => (emitted.push([pattern, data]), of(undefined));
      await build(emit, Object.create(Client.prototype)).publish(stored);
      expect(emitted, Client.name).toEqual([['order.placed', envelope]]);
    }
  });

  it('uses a custom toPacket through a ClientKafka too', async () => {
    const emitted: unknown[][] = [];
    const emit = (pattern: unknown, data: unknown) => (emitted.push([pattern, data]), of(undefined));
    const kafka = build(emit, Object.create(ClientKafka.prototype), {
      toPacket: (message, envelope) => ({ pattern: `orders.${message.topic}`, data: envelope }),
    });

    await kafka.publish(stored);

    expect(emitted).toEqual([['orders.order.placed', envelope]]);
  });

  it('exports toKafkaRecord, so a ClientKafka behind a facade or a double can opt in', async () => {
    const emitted: unknown[][] = [];
    const emit = (pattern: unknown, data: unknown) => (emitted.push([pattern, data]), of(undefined));

    await build(emit, {}, { toPacket: toKafkaRecord }).publish(stored);

    expect(emitted).toEqual([
      ['order.placed', { key: 'order-1', value: envelope, headers: { 'x-tenant': 'tenant-7' } }],
    ]);
  });

  it('calls toPacket as the function it was given, not as a method of the transport', async () => {
    const receivers: unknown[] = [];
    const toPacket = function (this: unknown, message: OutboxMessage, envelope: OutboxEnvelope) {
      receivers.push(this);
      return { pattern: message.topic, data: envelope };
    };

    await build(() => of(undefined), {}, { toPacket }).publish(stored);

    expect(receivers).toEqual([undefined]);
  });

  it('resolves once the emit completes, values or not, and fails with its error', async () => {
    const pending = new Subject<unknown>();
    let resolved = false;
    const publishing = build(() => pending).publish(stored).then(() => (resolved = true));

    pending.next('ack');
    await Promise.resolve();
    expect(resolved).toBe(false);
    pending.complete();
    await publishing;
    expect(resolved).toBe(true);

    await expect(build(() => of()).publish(stored)).resolves.toBeUndefined();
    await expect(build(() => throwError(() => new Error('ECONNREFUSED'))).publish(stored)).rejects.toThrow('ECONNREFUSED');
  });

  it('is named after its client token, for DI errors', () => {
    class AnalyticsClient {}
    expect(ClientProxyTransport(AnalyticsClient).name).toBe('ClientProxyTransport(AnalyticsClient)');
    expect(ClientProxyTransport(Symbol('KAFKA')).name).toBe('ClientProxyTransport(KAFKA)');
    expect(ClientProxyTransport('NATS').name).toBe('ClientProxyTransport(NATS)');
  });
});
