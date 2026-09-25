/**
 * The order API publishes through `ClientProxyTransport` over a real `@nestjs/microservices`
 * TCP connection to a separate consumer service, a Nest microservice with its own database,
 * which dedupes with `OutboxInbox`: `processInTransaction()` in its own transaction (exactly
 * once for its writes) and `process()`. On every documented store recipe, on both sides.
 * TCP is the one transport that runs here without a broker; the README says so too.
 */
import { Controller, Injectable, Module, type INestMicroservice } from '@nestjs/common';
import { ClientsModule, EventPattern, Payload, Transport } from '@nestjs/microservices';
import { Test, type TestingModule } from '@nestjs/testing';
import type { AddressInfo, Server } from 'node:net';
import { of } from 'rxjs';
import { startPostgres } from './support/postgres.js';
import {
  ClientProxyTransport,
  OnOutboxMessage,
  Outbox,
  OutboxEvents,
  OutboxInbox,
  OutboxModule,
  OutboxRelay,
  type OutboxEnvelope,
  type OutboxEvent,
  type OutboxHandlerContext,
  type OutboxStore,
} from '../lib/index.js';
import { inMemoryDatabase } from './databases.js';
import { registeredStore, until } from './helpers.js';
import { AppDatabase, controllableClock, diagnostics, recipes, titleOf, type RecipeDatabase } from './integration.js';

const { postgres, reason } = await startPostgres();
afterAll(() => postgres?.stop());

interface OrderEvent {
  orderId: number;
  step: number;
}

const consumer = {
  /** Every envelope that reached the service, in arrival order, duplicates included. */
  arrived: [] as OutboxEnvelope<OrderEvent>[],
  /** Deliveries whose handler finished: its transaction committed or rolled back. */
  handled: 0,
  duplicates: 0,
  ledger: [] as string[],
};
const local: string[] = [];

/** The consumer service: an `@EventPattern()` handler per topic, deduped by the envelope id. */
@Controller()
class ShippingController {
  constructor(
    private readonly outboxInbox: OutboxInbox,
    private readonly appDatabase: AppDatabase,
  ) {}

  @EventPattern('order.placed')
  async onPlaced(@Payload() envelope: OutboxEnvelope<OrderEvent>) {
    consumer.arrived.push(envelope);
    try {
      const { duplicate } = await this.appDatabase.transaction((tx) =>
        Promise.resolve(
          this.outboxInbox.processInTransaction(tx, 'shipping', envelope.id, () =>
            this.appDatabase.insertInvoice(tx, envelope.payload.orderId, 'shipping'),
          ),
        ),
      );
      if (duplicate) {
        consumer.duplicates++;
      }
    } finally {
      consumer.handled++;
    }
  }

  @EventPattern('order.cancelled')
  async onCancelled(@Payload() envelope: OutboxEnvelope<OrderEvent>) {
    consumer.arrived.push(envelope);
    try {
      await this.outboxInbox.process('ledger', envelope.id, () => {
        consumer.ledger.push(envelope.id);
      });
    } finally {
      consumer.handled++;
    }
  }
}

@Injectable()
class InternalHandlers {
  @OnOutboxMessage('internal.order.audited', { consumer: 'audit' })
  audit(_event: OrderEvent, ctx: OutboxHandlerContext) {
    local.push(ctx.message.id);
  }
}

@Injectable()
class OrdersService {
  constructor(
    private readonly appDatabase: AppDatabase,
    private readonly outbox: Outbox,
  ) {}

  async place(orderId: number, topics: string[], options: { fail?: boolean } = {}) {
    const messages = await this.appDatabase.transaction(async (tx) => {
      await this.appDatabase.insertOrder(tx, orderId);
      const added = await this.outbox.add(
        tx,
        topics.map((topic, step) => ({
          topic,
          key: `order-${orderId}`,
          headers: { 'x-tenant': 'tenant-7' },
          payload: { orderId, step },
        })),
      );
      if (options.fail) {
        throw new Error('payment declined');
      }
      return added;
    });

    this.outbox.notify();
    return messages;
  }
}

for (const recipe of recipes(postgres, reason, 'outbox_tcp')) {
  describe.skipIf(recipe.skip)(`ClientProxyTransport over TCP to a consumer service: ${titleOf(recipe)}`, () => {
    let producerDatabase: RecipeDatabase;
    let consumerDatabase: RecipeDatabase;
    let microservice: INestMicroservice;
    let producer: TestingModule;
    let ShippingModule: new () => unknown;
    let events: OutboxEvent[];
    let channels: Awaited<ReturnType<typeof diagnostics>>;

    const settled = () => until(async () => (await producer.get(OutboxRelay).stats()).pending === 0, 10_000);
    const orders = () => producer.get(OrdersService);

    beforeAll(async () => {
      producerDatabase = await recipe.open('orders');
      consumerDatabase = await recipe.openConsumer('shipping');

      @Module({
        imports: [consumerDatabase.module(), OutboxModule.forRoot({ relay: { enabled: false } })],
        controllers: [ShippingController],
      })
      class ShippingServiceModule {}
      ShippingModule = ShippingServiceModule;

      const consumerRef = await Test.createTestingModule({ imports: [ShippingModule] }).compile();
      microservice = consumerRef.createNestMicroservice({ transport: Transport.TCP, options: { host: '127.0.0.1', port: 0 } });
      microservice.useLogger(false);
      await microservice.listen();
      const { port } = microservice.unwrap<Server>().address() as AddressInfo;

      @Module({
        imports: [
          producerDatabase.module(),
          OutboxModule.forRoot({
            imports: [ClientsModule.register([{ name: 'SHIPPING', transport: Transport.TCP, options: { host: '127.0.0.1', port } }])],
            transports: { shipping: ClientProxyTransport('SHIPPING') },
            route: (message) => (message.topic.startsWith('internal.') ? 'local' : 'shipping'),
            relay: { pollInterval: '50ms', lease: '3s' },
            retry: { attempts: 10, backoff: { delay: '20ms', jitter: 'none' } },
          }),
        ],
        providers: [OrdersService, InternalHandlers],
      })
      class OrdersModule {}

      producer = await Test.createTestingModule({ imports: [OrdersModule] }).compile();
      producer.useLogger(false);
      await producer.init();
      events = [];
      producer.get(OutboxEvents).events$.subscribe((event) => events.push(event));
      channels = await diagnostics();
    }, 60_000); // PGlite loads its WASM on first open: slow under a parallel run
    afterAll(async () => {
      channels?.stop();
      await producer?.close();
      await microservice?.close();
      await producerDatabase?.close();
      await consumerDatabase?.close();
    });

    beforeEach(async () => {
      await producerDatabase.reset();
      await consumerDatabase.reset();
      events.length = 0;
      channels.received.length = 0;
      consumer.arrived = [];
      consumer.handled = 0;
      consumer.duplicates = 0;
      consumer.ledger = [];
      local.length = 0;
    });
    afterEach(() => {
      vi.restoreAllMocks();
    });

    it('emits committed messages as envelopes, in key order, and the consumer applies each once', async () => {
      const messages = await orders().place(1, ['order.placed', 'order.cancelled', 'order.placed']);
      // Handled, not only arrived: the last delivery's transaction may still be committing.
      await until(() => consumer.handled === 3);
      await settled();

      expect(consumer.arrived.map((envelope) => envelope.id)).toEqual(messages.map((message) => message.id));
      expect(consumer.arrived[0]).toEqual({
        id: messages[0]!.id,
        topic: 'order.placed',
        key: 'order-1',
        headers: { 'x-tenant': 'tenant-7' },
        createdAt: messages[0]!.createdAt,
        payload: { orderId: 1, step: 0 },
      });

      expect(await consumerDatabase.count('it_invoices', 'shipping')).toBe(2);
      expect(await consumerDatabase.count('outbox_inbox', 'shipping')).toBe(2);
      expect(await consumerDatabase.count('outbox_inbox', 'ledger')).toBe(1);
      expect(consumer.ledger).toEqual([messages[1]!.id]);

      expect(events.map((event) => [event.type, event.message.id, 'transport' in event ? event.transport : undefined])).toEqual(
        messages.map((message) => ['published', message.id, 'shipping']),
      );
      expect(channels.received.map((entry) => entry.channel)).toEqual(Array(3).fill('nestjs:outbox:published'));
    });

    it('emits nothing for a transaction that rolls back', async () => {
      await expect(orders().place(2, ['order.placed'], { fail: true })).rejects.toThrow('payment declined');
      await new Promise((resolve) => setTimeout(resolve, 200)); // a few polls

      expect(consumer.arrived).toEqual([]);
      expect(await producerDatabase.count('it_orders')).toBe(0);
      expect(await producer.get(OutboxRelay).stats()).toMatchObject({ pending: 0 });
    });

    it('routes internal topics to the in-process handlers and the rest to the consumer service', async () => {
      const [audited, placed] = await orders().place(3, ['internal.order.audited', 'order.placed']);
      await until(() => consumer.arrived.length === 1 && local.length === 1);
      await settled();

      expect(local).toEqual([audited!.id]);
      expect(consumer.arrived.map((envelope) => envelope.id)).toEqual([placed!.id]);
      expect(events.map((event) => [event.message.id, 'transport' in event ? event.transport : undefined])).toEqual([
        [audited!.id, 'local'],
        [placed!.id, 'shipping'],
      ]);
    });

    it("drops a redelivery after the producer lost the acknowledgement, in the consumer's transaction", async () => {
      const clock = controllableClock();
      const store = producer.get<OutboxStore>(producerDatabase.storeClass as never);
      const markPublished = vi.spyOn(store, 'markPublished').mockRejectedValueOnce(new Error('connection reset'));

      const [message] = await orders().place(4, ['order.placed']);
      await until(() => consumer.arrived.length === 1 && markPublished.mock.calls.length === 1);
      expect(await producer.get(OutboxRelay).stats()).toMatchObject({ pending: 1, leased: 1 });

      clock.advance(3_000); // the lease expires: the relay emits it again
      await until(() => consumer.duplicates === 1);
      await settled();
      clock.restore();

      expect(consumer.arrived.map((envelope) => envelope.id)).toEqual([message!.id, message!.id]);
      expect(await consumerDatabase.count('it_invoices', 'shipping')).toBe(1);
      expect(events.filter((event) => event.type === 'published')).toHaveLength(1);
    });

    it('prunes consumer inbox entries older than a duration', async () => {
      await orders().place(5, ['order.placed', 'order.cancelled']);
      await until(() => consumer.arrived.length === 2);
      await until(async () => (await consumerDatabase.count('outbox_inbox')) === 2);

      const inbox = microservice.get(OutboxInbox);
      expect(await inbox.prune('1h')).toBe(0);

      const clock = controllableClock();
      clock.advance(2 * 3_600_000);
      expect(await inbox.prune('1h')).toBe(2);
      clock.restore();
      expect(await consumerDatabase.count('outbox_inbox')).toBe(0);
    });

    it('boots in production as a consumer-only service: its registered inbox is all the guard asks for', async () => {
      const previous = process.env.NODE_ENV;
      process.env.NODE_ENV = 'production';
      try {
        const consumerRef = await Test.createTestingModule({ imports: [ShippingModule] }).compile();
        consumerRef.useLogger(false);
        await consumerRef.init();
        await consumerRef.close();
      } finally {
        process.env.NODE_ENV = previous;
      }
    });
  });
}

describe("the README's testing recipe: stub the broker, not the outbox", () => {
  it('publishes to whatever the test registers under the client token', async () => {
    const database = inMemoryDatabase();
    const emitted: Array<[string, OutboxEnvelope]> = [];

    @Module({
      imports: [
        registeredStore(database.store()),
        OutboxModule.forRoot({
          // Nothing listens there: only the stub is ever called.
          imports: [ClientsModule.register([{ name: 'SHIPPING', transport: Transport.TCP, options: { host: '127.0.0.1', port: 1 } }])],
          transports: { shipping: ClientProxyTransport('SHIPPING') },
          relay: { enabled: false },
        }),
      ],
    })
    class AppModule {}

    const moduleRef = await Test.createTestingModule({ imports: [AppModule] })
      .overrideProvider('SHIPPING')
      .useValue({ emit: (pattern: string, envelope: OutboxEnvelope) => (emitted.push([pattern, envelope]), of(undefined)) })
      .compile();
    moduleRef.useLogger(false);
    await moduleRef.init();

    try {
      const [message] = await database.transaction((tx) => moduleRef.get(Outbox).add(tx, [{ topic: 'order.placed', payload: { orderId: 1 } }]));
      expect(await moduleRef.get(OutboxRelay).runOnce()).toMatchObject({ claimed: 1, published: 1 });
      expect(emitted).toEqual([['order.placed', expect.objectContaining({ id: message!.id, payload: { orderId: 1 } })]]);
    } finally {
      await moduleRef.close();
    }
  });
});
