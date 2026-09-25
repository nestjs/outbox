/**
 * Several instances of one application on one database, each a real Nest application with
 * its own connection (a pool of its own on PostgreSQL), relay and `@OnOutboxMessage()`
 * handlers, on every documented store recipe: an API-only instance that produces, relays
 * that race for the same messages, an instance that crashes, one that stalls past its lease,
 * and one that shuts down gracefully mid-batch.
 */
import { Inject, Injectable, Module } from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';
import { startPostgres } from './support/postgres.js';
import {
  OnOutboxMessage,
  Outbox,
  OutboxEvents,
  OutboxModule,
  OutboxRelay,
  type NewOutboxMessage,
  type OutboxEvent,
  type OutboxHandlerContext,
  type OutboxMessage,
  type OutboxRelayOptions,
  type OutboxStore,
} from '../lib/index.js';
import { deferred, sleep, until } from './helpers.js';
import { AppDatabase, controllableClock, diagnostics, recipes, titleOf, type RecipeDatabase } from './integration.js';

const { postgres, reason } = await startPostgres();
afterAll(() => postgres?.stop());

const INSTANCE = Symbol('INSTANCE');

interface Posting {
  orderId: number;
  step: number;
}

interface Delivery {
  instance: string;
  consumer: string;
  id: string;
  key: string | null;
  step: number;
}

const deliveries: Delivery[] = [];
const hooks = {
  slow: undefined as ((instance: string) => Promise<void>) | undefined,
};

const deliveriesOf = (consumer: string, instance?: string) =>
  deliveries.filter((delivery) => delivery.consumer === consumer && (instance === undefined || delivery.instance === instance));

@Injectable()
class LedgerHandlers {
  constructor(
    @Inject(INSTANCE) private readonly instance: string,
    private readonly appDatabase: AppDatabase,
  ) {}

  /** Exactly once across instances: the inbox record commits with the posting. */
  @OnOutboxMessage('order.placed', { consumer: 'ledger' })
  async post(posting: Posting, ctx: OutboxHandlerContext) {
    this.record('ledger', posting, ctx);
    await this.appDatabase.transaction((tx) =>
      Promise.resolve(ctx.processInTransaction(tx, () => this.appDatabase.insertInvoice(tx, posting.orderId, 'ledger'))),
    );
  }

  /** Sees every delivery: a message published twice shows up here twice. */
  @OnOutboxMessage('order.placed', { consumer: 'audit', inbox: false })
  audit(posting: Posting, ctx: OutboxHandlerContext) {
    this.record('audit', posting, ctx);
  }

  @OnOutboxMessage('order.slow', { consumer: 'slow' })
  async slow(posting: Posting, ctx: OutboxHandlerContext) {
    this.record('slow', posting, ctx);
    await hooks.slow?.(this.instance);
  }

  private record(consumer: string, posting: Posting, ctx: OutboxHandlerContext) {
    deliveries.push({ instance: this.instance, consumer, id: ctx.message.id, key: ctx.message.key, step: posting.step });
  }
}

@Injectable()
class OrdersService {
  constructor(
    private readonly appDatabase: AppDatabase,
    private readonly outbox: Outbox,
  ) {}

  async place(orderId: number, messages: NewOutboxMessage<Posting>[]): Promise<OutboxMessage<Posting>[]> {
    const added = await this.appDatabase.transaction(async (tx) => {
      await this.appDatabase.insertOrder(tx, orderId);
      return this.outbox.add(tx, messages);
    });

    this.outbox.notify();
    return added;
  }
}

interface Instance {
  name: string;
  moduleRef: TestingModule;
  relay: OutboxRelay;
  store: OutboxStore;
  events: OutboxEvent[];
  close(): Promise<void>;
}

for (const recipe of recipes(postgres, reason, 'outbox_instances')) {
  describe.skipIf(recipe.skip)(`Several application instances on one database: ${titleOf(recipe)}`, () => {
    let database: RecipeDatabase;
    let channels: Awaited<ReturnType<typeof diagnostics>>;
    const running = new Set<Instance>();

    async function start(name: string, relay: OutboxRelayOptions = {}): Promise<Instance> {
      @Module({
        imports: [
          database.module(),
          OutboxModule.forRoot({
            relay: { pollInterval: '50ms', lease: '3s', batchSize: 5, concurrency: 3, ...relay },
            retry: { attempts: 5, backoff: { delay: '20ms', jitter: 'none' } },
          }),
        ],
        providers: [{ provide: INSTANCE, useValue: name }, LedgerHandlers, OrdersService],
      })
      class InstanceModule {}

      const moduleRef = await Test.createTestingModule({ imports: [InstanceModule] }).compile();
      moduleRef.useLogger(false);
      const events: OutboxEvent[] = [];
      moduleRef.get(OutboxEvents).events$.subscribe((event) => events.push(event));
      await moduleRef.init();

      const instance: Instance = {
        name,
        moduleRef,
        relay: moduleRef.get(OutboxRelay),
        store: moduleRef.get<OutboxStore>(database.storeClass as never),
        events,
        async close() {
          running.delete(instance);
          await moduleRef.close();
        },
      };
      running.add(instance);
      return instance;
    }

    const placed = (orderId: number, key: string | null, step: number): NewOutboxMessage<Posting> => ({
      topic: 'order.placed',
      key,
      payload: { orderId, step },
    });
    const slow = (key: string, step: number): NewOutboxMessage<Posting> => ({ topic: 'order.slow', key, payload: { orderId: 0, step } });
    const publishedBy = (instance: Instance) =>
      instance.events.filter((event) => event.type === 'published').map((event) => event.message.id);

    beforeAll(async () => {
      database = await recipe.open('orders');
      channels = await diagnostics();
    }, 60_000); // PGlite loads its WASM on first open: slow under a parallel run
    afterAll(async () => {
      channels?.stop();
      await database?.close();
    });

    beforeEach(async () => {
      await database.reset();
      deliveries.length = 0;
      channels.received.length = 0;
      hooks.slow = undefined;
    });
    afterEach(async () => {
      vi.restoreAllMocks();
      await Promise.all([...running].map((instance) => instance.close()));
    });

    it('two relays never publish a message twice and keep per-key order; an API-only instance publishes nothing', async () => {
      const api = await start('api', { enabled: false });
      const a = await start('a');
      const b = await start('b');

      const keys = ['customer-0', 'customer-1', 'customer-2', 'customer-3', null];
      const added: OutboxMessage<Posting>[] = [];
      for (let step = 0; step < 12; step++) {
        const messages = keys.map((key, i) => placed(step * 10 + i, key, step));
        added.push(...(await api.moduleRef.get(OrdersService).place(step, messages)));
      }

      await until(async () => (await database.count('it_invoices', 'ledger')) === added.length, 20_000);
      await until(async () => (await api.relay.stats()).pending === 0, 10_000);

      const published = [...publishedBy(a), ...publishedBy(b)];
      expect(published.toSorted()).toEqual(added.map((message) => message.id).toSorted());
      expect(api.relay.running).toBe(false);
      expect(api.events).toEqual([]);

      // inbox: false sees every delivery, so a message published twice would show up twice.
      expect(deliveriesOf('audit').map((delivery) => delivery.id).toSorted()).toEqual(published.toSorted());
      expect(deliveriesOf('ledger')).toHaveLength(added.length);

      for (const key of keys.filter((key) => key !== null)) {
        const steps = deliveriesOf('ledger')
          .filter((delivery) => delivery.key === key)
          .map((delivery) => delivery.step);
        expect(steps).toEqual([...Array(12).keys()]);
      }
      expect(
        channels.received.filter((entry) => entry.channel === 'nestjs:outbox:published').map((entry) => entry.event.message.id).toSorted(),
      ).toEqual(published.toSorted());
   }, 30_000);

    it("publishes a key's messages in commit order: a second producer of the key waits for the first to commit", async () => {
      const first = await start('api-1', { enabled: false });
      const second = await start('api-2', { enabled: false });
      await start('relay');
      const outbox = first.moduleRef.get(Outbox);
      const appDatabase = first.moduleRef.get(AppDatabase);

      const added = deferred();
      const commit = deferred();
      const earlier = appDatabase.transaction(async (tx) => {
        await appDatabase.insertOrder(tx, 1);
        await outbox.add(tx, placed(1, 'order-1', 0));
        added.resolve();
        await commit.promise;
      });
      await added.promise;

      let laterCommitted = false;
      const later = second.moduleRef
        .get(OrdersService)
        .place(2, [placed(2, 'order-1', 1)])
        .then(() => (laterCommitted = true));
      await sleep(200);
      expect(laterCommitted).toBe(false); // waiting on the key's lock (on PGlite, on the one connection)
      expect(deliveries).toEqual([]);

      commit.resolve();
      await Promise.all([earlier, later]);
      await until(() => deliveriesOf('ledger').length === 2);
      expect(deliveriesOf('ledger').map((delivery) => [delivery.instance, delivery.step])).toEqual([
        ['relay', 0],
        ['relay', 1],
      ]);
    });

    it("publishes what a crashed instance had claimed once its lease expires, in the key's order", async () => {
      const api = await start('api', { enabled: false });
      const messages = await api.moduleRef
        .get(OrdersService)
        .place(1, [placed(1, 'order-1', 0), placed(2, 'order-1', 1), placed(3, 'order-1', 2)]);

      // The instance claims the batch, then its process dies before publishing any of it.
      const crashed = await start('crashed', { enabled: false });
      expect(await crashed.store.claim({ owner: 'crashed-relay', now: Date.now(), leaseMs: 3_000, limit: 10 })).toHaveLength(3);
      await crashed.close();

      const clock = controllableClock();
      const survivor = await start('survivor');
      await sleep(300);
      expect(deliveries).toEqual([]);
      expect(await survivor.relay.stats()).toMatchObject({ pending: 3, leased: 3, ready: 0 });

      clock.advance(3_000);
      await until(() => deliveriesOf('ledger').length === 3);
      await until(async () => (await survivor.relay.stats()).pending === 0);

      expect(deliveriesOf('ledger').map((delivery) => [delivery.instance, delivery.step])).toEqual([
        ['survivor', 0],
        ['survivor', 1],
        ['survivor', 2],
      ]);
      expect(publishedBy(survivor)).toEqual(messages.map((message) => message.id));
    });

    it("publishes again after an instance died between publishing and marking; the handlers' effects happen once", async () => {
      const api = await start('api', { enabled: false });
      const a = await start('a');
      const markPublished = vi.spyOn(a.store, 'markPublished').mockRejectedValueOnce(new Error('connection terminated'));

      const [message] = await api.moduleRef.get(OrdersService).place(1, [placed(1, 'order-1', 0)]);
      await until(() => markPublished.mock.calls.length === 1);
      await a.close(); // the instance dies with the message published but still leased

      const clock = controllableClock();
      const b = await start('b');
      clock.advance(3_000);
      await until(() => publishedBy(b).length === 1);

      expect(publishedBy(a)).toEqual([]);
      expect(publishedBy(b)).toEqual([message!.id]);
      expect(deliveriesOf('audit').map((delivery) => delivery.instance)).toEqual(['a', 'b']); // published twice
      expect(deliveriesOf('ledger').map((delivery) => delivery.instance)).toEqual(['a']); // skipped by the inbox
      expect(await database.count('it_invoices', 'ledger')).toBe(1);
    });

    it('emits lease-lost on an instance that stalled past its lease while another took the message over', async () => {
      const api = await start('api', { enabled: false });
      const gate = deferred();
      hooks.slow = (instance) => (instance === 'stalled' ? gate.promise : Promise.resolve());

      const stalled = await start('stalled', { lease: '10s', publishTimeout: '3s' });
      const [message] = await api.moduleRef.get(OrdersService).place(1, [slow('order-1', 0)]);
      await until(() => deliveriesOf('slow', 'stalled').length === 1);

      // Its event loop stalls past the lease (skewed clocks do the same): another instance claims.
      const clock = controllableClock();
      const other = await start('other', { lease: '10s', publishTimeout: '3s' });
      clock.advance(10_000);
      await until(() => publishedBy(other).length === 1);

      gate.resolve();
      await until(() => stalled.events.length === 1);

      expect(stalled.events).toEqual([{ type: 'lease-lost', message: expect.objectContaining({ id: message!.id }) }]);
      expect(publishedBy(other)).toEqual([message!.id]);
      expect(deliveriesOf('slow').map((delivery) => delivery.instance)).toEqual(['stalled', 'other']);
      expect(channels.of(message!.id)).toEqual(['nestjs:outbox:published', 'nestjs:outbox:lease-lost']);
      expect(await api.relay.stats()).toMatchObject({ pending: 0, deadLetters: 0 });
    });

    it('shuts down gracefully mid-batch: the publish in flight finishes, the rest is released for another instance at once', async () => {
      const api = await start('api', { enabled: false });
      hooks.slow = async (instance) => {
        if (instance === 'leaving') {
          await sleep(300);
        }
      };

      const leaving = await start('leaving');
      const messages = await api.moduleRef
        .get(OrdersService)
        .place(1, [slow('order-1', 0), slow('order-1', 1), slow('order-1', 2), slow('order-1', 3)]);
      await until(() => deliveriesOf('slow', 'leaving').length === 1);

      await leaving.close();
      expect(deliveriesOf('slow', 'leaving')).toHaveLength(1);
      expect(publishedBy(leaving)).toEqual([messages[0]!.id]);
      // Released, not left to expire: claimable right away.
      expect(await api.relay.stats()).toMatchObject({ pending: 3, leased: 0, ready: 3, inFlight: 0 });

      const next = await start('next');
      await until(() => publishedBy(next).length === 3);
      expect(deliveriesOf('slow').map((delivery) => [delivery.instance, delivery.step])).toEqual([
        ['leaving', 0],
        ['next', 1],
        ['next', 2],
        ['next', 3],
      ]);
    });
  });
}
