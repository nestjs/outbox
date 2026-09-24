import { Controller, Inject, Injectable, Module, type INestMicroservice } from '@nestjs/common';
import { ClientsModule, EventPattern, Payload, Transport } from '@nestjs/microservices';
import { Test, type TestingModule } from '@nestjs/testing';
import type { AddressInfo, Server } from 'node:net';
import { of } from 'rxjs';
import {
  ClientProxyTransport,
  InMemoryOutboxStore,
  Outbox,
  OutboxEvents,
  OutboxInbox,
  OutboxModule,
  OutboxRelay,
  type OutboxEnvelope,
  type OutboxEvent,
} from '../lib/index.js';
import { inMemoryDatabase, pgliteDatabase, type TestDatabase, type TestStore } from './databases.js';
import { registeredStore, sleep, until } from './helpers.js';

const APP_DB = Symbol('APP_DB');

const consumer = {
  received: [] as OutboxEnvelope<{ orderId: number }>[],
  duplicates: 0,
};

/** A separate service: receives outbox messages over TCP and dedupes them with its own inbox. */
@Controller()
class ShippingConsumer {
  constructor(private readonly inbox: OutboxInbox) {}

  @EventPattern('order.placed')
  async onOrderPlaced(@Payload() envelope: OutboxEnvelope<{ orderId: number }>) {
    const { duplicate } = await this.inbox.process('shipping', envelope.id, () => {
      consumer.received.push(envelope);
    });
    if (duplicate) {
      consumer.duplicates++;
    }
  }
}

@Injectable()
class OrdersService {
  constructor(
    private readonly outbox: Outbox,
    @Inject(APP_DB) private readonly db: TestDatabase,
  ) {}

  async place(orderId: number) {
    await this.db.transaction(async (tx) => {
      await this.db.insertOrder(tx, orderId);
      await this.outbox.add(tx, {
        topic: 'order.placed',
        key: `order-${orderId}`,
        headers: { 'x-tenant': 'acme' },
        payload: { orderId },
      });
    });

    this.outbox.notify();
  }
}

/**
 * The producer on the in-memory store and on the tutorial's DrizzleOutboxStore on PGlite. The
 * consumer service keeps its inbox in an in-memory store of its own, which outlives its
 * restarts, as the consumer's database would.
 */
const targets: { name: string; open(): Promise<TestDatabase> }[] = [
  { name: 'InMemoryOutboxStore', open: async () => inMemoryDatabase() },
  { name: 'DrizzleOutboxStore on PGlite', open: pgliteDatabase },
];

for (const target of targets) {
  describe(`ClientProxyTransport (TCP microservice): producer on ${target.name}`, () => {
    let db: TestDatabase;
    let producerStore: TestStore;
    const consumerStore = new InMemoryOutboxStore();
    let microservice: INestMicroservice;
    let producer: TestingModule;
    let port: number;
    const events: OutboxEvent[] = [];

    async function startConsumer(listenOn = 0) {
      @Module({
        imports: [registeredStore(consumerStore), OutboxModule.forRoot({ relay: { enabled: false } })],
        controllers: [ShippingConsumer],
      })
      class ConsumerModule {}

      const moduleRef = await Test.createTestingModule({ imports: [ConsumerModule] }).compile();
      microservice = moduleRef.createNestMicroservice({
        transport: Transport.TCP,
        options: { host: '127.0.0.1', port: listenOn },
      });
      microservice.useLogger(false);
      await microservice.listen();

      return (microservice.unwrap<Server>().address() as AddressInfo).port;
    }

    beforeAll(async () => {
      db = await target.open();
      producerStore = db.store();
      port = await startConsumer();

      const broker = ClientsModule.register([
        { name: 'BROKER', transport: Transport.TCP, options: { host: '127.0.0.1', port } },
      ]);

      @Module({
        imports: [
          registeredStore(producerStore),
          // Plain forRoot: Nest instantiates the transport inside OutboxModule, where
          // `imports` makes the 'BROKER' client visible.
          OutboxModule.forRoot({
            imports: [broker],
            transports: { broker: ClientProxyTransport('BROKER') },
            relay: { pollInterval: '20ms', lease: '300ms', publishTimeout: '100ms' },
            retry: { attempts: 50, backoff: { delay: '20ms', maxDelay: '50ms', jitter: 'none' } },
          }),
        ],
        providers: [{ provide: APP_DB, useValue: db }, OrdersService],
      })
      class ProducerModule {}

      producer = await Test.createTestingModule({ imports: [ProducerModule] }).compile();
      producer.useLogger(false);
      await producer.init();
      producer.get(OutboxEvents).events$.subscribe((e) => events.push(e));
    }, 60_000); // PGlite loads its WASM on first open: slow under a parallel run
    afterAll(async () => {
      await producer?.close();
      await microservice?.close();
      await db?.close();
    });
    beforeEach(() => {
      consumer.received = [];
      consumer.duplicates = 0;
      events.length = 0;
    });

    it('delivers a committed message to a remote @EventPattern handler as an envelope', async () => {
      await producer.get(OrdersService).place(1);
      await until(() => consumer.received.length === 1);

      const [envelope] = consumer.received;
      expect(envelope).toMatchObject({
        topic: 'order.placed',
        key: 'order-1',
        headers: { 'x-tenant': 'acme' },
        payload: { orderId: 1 },
      });
      expect(events.find((e) => e.type === 'published')).toMatchObject({
        transport: 'broker',
        message: { id: envelope!.id },
      });
      await until(async () => (await producerStore.stats(Date.now())).pending === 0);
    });

    it('redelivers after a lost acknowledgement, and the consumer inbox drops the duplicate', async () => {
      const markPublished = vi.spyOn(producerStore, 'markPublished').mockImplementationOnce(() => {
        throw new Error('connection reset');
      });
      await producer.get(OrdersService).place(2);

      await until(() => consumer.duplicates === 1); // re-emitted once the 300ms lease expired
      expect(consumer.received.map((e) => e.payload.orderId)).toEqual([2]);

      await until(() => markPublished.mock.calls.length === 2);
      await until(async () => (await producerStore.stats(Date.now())).pending === 0);
      markPublished.mockRestore();
    });

    it('keeps retrying while the consumer is down and delivers once it is back', async () => {
      await microservice.close();
      await producer.get(OrdersService).place(3);

      await until(() => events.filter((e) => e.type === 'retry-scheduled').length >= 2);
      expect(consumer.received).toEqual([]);
      expect((await producerStore.stats(Date.now())).pending).toBe(1);

      await startConsumer(port);
      await until(() => consumer.received.length === 1, 5_000);
      expect(consumer.received[0]!.payload).toEqual({ orderId: 3 });
      await sleep(50);
      expect(await producerStore.stats(Date.now())).toMatchObject({ pending: 0, deadLetters: 0 });
    });
  });
}

describe('ClientProxyTransport (forRootAsync, toPacket)', () => {
  it('injects the named client and maps each message with toPacket', async () => {
    const db = inMemoryDatabase();
    const emitted: Array<[unknown, unknown]> = [];
    const KAFKA = Symbol('KAFKA');
    const fakeClient = { emit: (pattern: unknown, data: unknown) => (emitted.push([pattern, data]), of(undefined)) };

    @Module({ providers: [{ provide: KAFKA, useValue: fakeClient }], exports: [KAFKA] })
    class KafkaClientModule {}

    @Module({
      imports: [
        registeredStore(db.store()),
        OutboxModule.forRootAsync({
          imports: [KafkaClientModule],
          transports: {
            kafka: ClientProxyTransport(KAFKA, {
              // A Kafka record: the ordering key as the message key, the envelope as the value.
              toPacket: (message, envelope) => ({
                pattern: message.topic,
                data: { key: message.key, value: envelope, headers: message.headers },
              }),
            }),
          },
          useFactory: () => ({ relay: { enabled: false } }),
        }),
      ],
    })
    class AppModule {}

    const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    moduleRef.useLogger(false);
    await moduleRef.init();
    try {
      const outbox = moduleRef.get(Outbox);
      const [message] = await db.transaction((tx) =>
        outbox.add(tx, [
          { topic: 'order.placed', key: 'order-1', headers: { 'x-tenant': 'acme' }, payload: { orderId: 1 } },
        ]),
      );

      expect(await moduleRef.get(OutboxRelay).runOnce()).toMatchObject({ published: 1 });
      expect(emitted).toEqual([
        [
          'order.placed',
          {
            key: 'order-1',
            headers: { 'x-tenant': 'acme' },
            value: {
              id: message!.id,
              topic: 'order.placed',
              key: 'order-1',
              headers: { 'x-tenant': 'acme' },
              createdAt: message!.createdAt,
              payload: { orderId: 1 },
            },
          },
        ],
      ]);
    } finally {
      await moduleRef.close();
    }
  });

  it('names the client when it is not visible inside OutboxModule', async () => {
    @Module({
      imports: [
        OutboxModule.forRoot({
          transports: { broker: ClientProxyTransport('MISSING_CLIENT') },
          relay: { enabled: false },
        }),
      ],
    })
    class AppModule {}

    await expect(Test.createTestingModule({ imports: [AppModule] }).compile()).rejects.toThrow(
      /ClientProxyTransport\(MISSING_CLIENT\)/,
    );
  });
});
