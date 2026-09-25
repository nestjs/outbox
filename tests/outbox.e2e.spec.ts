import { Inject, Injectable, Logger, Module, type DynamicModule, type OnApplicationShutdown } from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';
import { Observable } from 'rxjs';
import {
  NonRetryableMessageError,
  OnOutboxMessage,
  Outbox,
  OutboxDeadLetters,
  OutboxEvents,
  OutboxModule,
  OutboxRelay,
  OutboxTransactionRequiredError,
  type NewOutboxMessage,
  type OutboxEvent,
  type OutboxHandlerContext,
  type OutboxMessage,
  type OutboxModuleOptions,
} from '../lib/index.js';
import { inMemoryDatabase, pgliteDatabase, type TestDatabase, type TestStore } from './databases.js';
import { registeredStore } from './helpers.js';

const APP_DB = Symbol('APP_DB');

type Behavior = (message: OutboxMessage, ctx: OutboxHandlerContext) => unknown;

/** What the handlers do in the current test, and what they saw. */
const state = {
  calls: [] as Array<{
    consumer: string;
    id: string;
    key: string | null;
    seq?: number;
    attempt: number;
    at: number;
  }>,
  billing: undefined as Behavior | undefined,
  shipping: undefined as Behavior | undefined,
  audit: undefined as Behavior | undefined,
  signals: [] as AbortSignal[],
  teardowns: 0,
  /** `ctx.signal.aborted` when each call started. */
  startedAborted: [] as boolean[],
  /** What the application's database saw when it closed on shutdown. */
  atShutdown: undefined as { relayRunning: boolean; inFlight: number; pending: number } | undefined,
};

const seqOf = (m: OutboxMessage) => (m.payload as { seq?: number }).seq;
const record = (consumer: string, ctx: OutboxHandlerContext) => {
  const m = ctx.message;
  state.calls.push({ consumer, id: m.id, key: m.key, seq: seqOf(m), attempt: ctx.attempt, at: Date.now() });
};
const callsOf = (consumer: string) => state.calls.filter((c) => c.consumer === consumer);

@Injectable()
class OrdersService {
  constructor(
    private readonly outbox: Outbox,
    @Inject(APP_DB) private readonly db: TestDatabase,
  ) {}

  /** The business write and the message commit together, or not at all. */
  async place(orderId: number, messages: NewOutboxMessage[], options: { fail?: boolean; notify?: boolean } = {}) {
    await this.db.transaction(async (tx) => {
      await this.db.insertOrder(tx, orderId);
      await this.outbox.add(tx, messages);
      if (options.fail) {
        throw new Error('payment declined');
      }
    });

    if (options.notify ?? true) {
      this.outbox.notify();
    }
  }
}

/** Closes the application's database on shutdown, like the tutorial's DrizzleModule closes its pool. */
@Injectable()
class DatabaseLifecycle implements OnApplicationShutdown {
  constructor(private readonly relay: OutboxRelay) {}

  async onApplicationShutdown() {
    const { inFlight, pending } = await this.relay.stats();
    state.atShutdown = { relayRunning: this.relay.running, inFlight, pending };
  }
}

@Injectable()
class BillingHandlers {
  @OnOutboxMessage('order.placed', { consumer: 'billing' })
  onPlaced(_payload: unknown, ctx: OutboxHandlerContext) {
    record('billing', ctx);
    return state.billing?.(ctx.message, ctx);
  }

  @OnOutboxMessage('order.slow', { consumer: 'billing' })
  onSlow(_payload: unknown, ctx: OutboxHandlerContext) {
    record('billing', ctx);
    return new Promise((resolve) => setTimeout(resolve, 2_000));
  }

  /** Outlives a short publishTimeout, like a handler waiting on a slow downstream. */
  @OnOutboxMessage('order.sluggish', { consumer: 'billing' })
  onSluggish(_payload: unknown, ctx: OutboxHandlerContext) {
    record('billing', ctx);
    return new Promise((resolve) => setTimeout(resolve, 4_500));
  }

  /** Fails slowly the first time (after the relay gave up on it), then works. */
  @OnOutboxMessage('order.flaky', { consumer: 'billing' })
  async onFlaky(_payload: unknown, ctx: OutboxHandlerContext) {
    record('billing', ctx);
    state.startedAborted.push(ctx.signal.aborted);
    if (callsOf('billing').length === 1) {
      await new Promise((resolve) => setTimeout(resolve, 4_500));
      throw new Error('downstream timed out');
    }
  }

  /** An Observable that never completes on its own: the relay's timeout has to cut it off. */
  @OnOutboxMessage('order.stuck', { consumer: 'billing' })
  onStuck(_payload: unknown, ctx: OutboxHandlerContext) {
    record('billing', ctx);
    state.signals.push(ctx.signal);
    return new Observable<void>(() => () => void state.teardowns++);
  }

  /** Returns an Observable, like many Nest handlers: the delivery waits for it. */
  @OnOutboxMessage('order.observed', { consumer: 'billing' })
  onObserved(_payload: unknown, ctx: OutboxHandlerContext) {
    return new Observable<void>((subscriber) => {
      const timer = setTimeout(() => {
        record('billing', ctx);
        subscriber.next();
        subscriber.complete();
      }, 500);
      return () => clearTimeout(timer);
    });
  }
}

@Injectable()
class ShippingHandlers {
  constructor(@Inject(APP_DB) private readonly db: TestDatabase) {}

  /** processInTransaction: the inbox record and the invoice row commit in one transaction. */
  @OnOutboxMessage('order.placed', { consumer: 'shipping' })
  async onPlaced(payload: { orderId: number }, ctx: OutboxHandlerContext) {
    record('shipping', ctx);
    if (!state.shipping) {
      return;
    }

    await this.db.transaction((tx) =>
      ctx.processInTransaction(tx, async () => {
        await this.db.insertInvoice(tx, payload.orderId, 'shipping');
        state.shipping!(ctx.message, ctx);
      }),
    );
  }

  /** No inbox: sees every delivery, duplicates included. */
  @OnOutboxMessage('order.placed', { consumer: 'audit', inbox: false })
  onAudit(_payload: unknown, ctx: OutboxHandlerContext) {
    record('audit', ctx);
    return state.audit?.(ctx.message, ctx);
  }
}

/**
 * On the in-memory store, whose own transactions stand in for the application's database, and
 * on the tutorial's DrizzleOutboxStore on PGlite, where the outbox and the business rows
 * really commit together.
 */
const targets: { name: string; open(): Promise<TestDatabase> }[] = [
  { name: 'InMemoryOutboxStore', open: async () => inMemoryDatabase() },
  { name: 'DrizzleOutboxStore on PGlite', open: pgliteDatabase },
];

for (const target of targets) {
  describe(`Outbox (e2e, in-process handlers): ${target.name}`, () => {
    let db: TestDatabase;
    let store: TestStore;
    let moduleRef: TestingModule;
    let orders: OrdersService;
    let relay: OutboxRelay;
    let deadLetters: OutboxDeadLetters;
    let events: OutboxEvent[];

    async function boot(overrides: Partial<OutboxModuleOptions> = {}, outboxModule?: DynamicModule) {
      @Module({
        imports: [
          registeredStore(store),
          outboxModule ??
            OutboxModule.forRoot({
              relay: { pollInterval: '1s', lease: '10s' },
              retry: { attempts: 3, backoff: { delay: 1_000, jitter: 'none' } },
              ...overrides,
            }),
        ],
        providers: [
          { provide: APP_DB, useValue: db },
          OrdersService,
          BillingHandlers,
          ShippingHandlers,
          DatabaseLifecycle,
        ],
      })
      class AppModule {}

      moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
      moduleRef.useLogger(false);
      await moduleRef.init();

      orders = moduleRef.get(OrdersService);
      relay = moduleRef.get(OutboxRelay);
      deadLetters = moduleRef.get(OutboxDeadLetters);

      events = [];
      moduleRef.get(OutboxEvents).events$.subscribe((e) => events.push(e));
      await settle();
    }

    /** Runs due timers and the promise chains they start. */
    const settle = async (ms = 0) => {
      await vi.advanceTimersByTimeAsync(ms);
      for (let i = 0; i < 3; i++) {
        await vi.advanceTimersByTimeAsync(0);
      }
    };

    const placed = (orderId: number, extra: Partial<NewOutboxMessage> = {}): NewOutboxMessage => ({
      topic: 'order.placed',
      payload: { orderId },
      ...extra,
    });

    beforeAll(async () => {
      db = await target.open();
    }, 60_000); // PGlite loads its WASM on first open: slow under a parallel run
    afterAll(() => db?.close());

    beforeEach(async () => {
      await db.reset();
      store = db.store();
      vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });

      state.calls = [];
      state.billing = state.shipping = state.audit = undefined;
      state.signals = [];
      state.teardowns = 0;
      state.startedAborted = [];
      state.atShutdown = undefined;
    });
    afterEach(async () => {
      await moduleRef?.close();
      vi.useRealTimers();
    });

    it('publishes a message added inside a committed transaction', async () => {
      await boot();
      await orders.place(1, [placed(1, { key: 'customer-7', headers: { 'x-tenant': 'tenant-7' } })]);
      await settle();

      const [call] = callsOf('billing');
      expect(call).toMatchObject({ attempt: 1 });
      expect(await db.count('orders')).toBe(1);
      expect(events.find((e) => e.type === 'published')).toMatchObject({
        transport: 'local',
        message: {
          id: call!.id,
          topic: 'order.placed',
          key: 'customer-7',
          headers: { 'x-tenant': 'tenant-7' },
          payload: { orderId: 1 },
          attempts: 0,
        },
      });
      expect(await relay.stats()).toMatchObject({ pending: 0, deadLetters: 0, lagMs: 0 });
    });

    it('never publishes a message added inside a rolled-back transaction', async () => {
      await boot();
      await expect(orders.place(1, [placed(1)], { fail: true })).rejects.toThrow('payment declined');
      await settle(5_000);

      expect(state.calls).toEqual([]);
      expect(await db.count('orders')).toBe(0);
      expect(await relay.stats()).toMatchObject({ pending: 0 });
    });

    it('rejects add() outside a transaction, or without a transaction handle', async () => {
      await boot();
      const outbox = moduleRef.get(Outbox);
      expect(() => outbox.add(undefined, placed(1))).toThrow(/got no transaction handle/);

      // The database itself, not a transaction: a store that can tell refuses it.
      if (db.notATransaction !== undefined) {
        await expect(async () => outbox.add(db.notATransaction, placed(1))).rejects.toThrow(OutboxTransactionRequiredError);
      }

      expect(await relay.stats()).toMatchObject({ pending: 0 });
    });

    it('schedules a message with a delay, and refuses delay together with availableAt', async () => {
      await boot();
      const outbox = moduleRef.get(Outbox);
      const [message] = await db.transaction((tx) => outbox.add(tx, [placed(1, { delay: '5s' })]));
      expect(message!.availableAt - message!.createdAt).toBe(5_000);

      outbox.notify();
      await settle(4_000);
      expect(callsOf('billing')).toHaveLength(0);
      await settle(1_000);
      expect(callsOf('billing')).toHaveLength(1);

      await expect(db.transaction((tx) => outbox.add(tx, placed(2, { delay: 10, availableAt: 0 })))).rejects.toThrow(
        /either `delay` or `availableAt`/,
      );

      // An invalid Date became NaN and failed deep in the store ("NOT NULL constraint failed").
      await expect(
        db.transaction((tx) => outbox.add(tx, placed(3, { availableAt: new Date('next tuesday') }))),
      ).rejects.toThrow('Outbox message: `availableAt` must be a valid Date or epoch milliseconds (got NaN)');
    });

    it('waits for a handler that returns an Observable', async () => {
      await boot();
      await orders.place(1, [{ topic: 'order.observed', payload: {} }]);
      await settle();
      expect(callsOf('billing')).toHaveLength(0);
      await settle(500);
      expect(callsOf('billing')).toHaveLength(1);
      expect(events.filter((e) => e.type === 'published')).toHaveLength(1);
    });

    it('waits for the next poll unless notified after commit', async () => {
      await boot();
      await orders.place(1, [placed(1)], { notify: false });
      await settle(500);
      expect(callsOf('billing')).toHaveLength(0);
      await settle(500);
      expect(callsOf('billing')).toHaveLength(1);

      await orders.place(2, [placed(2)]); // notify() right after COMMIT
      await settle();
      expect(callsOf('billing')).toHaveLength(2);
    });

    it('retries with exponential backoff, then succeeds', async () => {
      await boot({ retry: { attempts: 5, backoff: { delay: 1_000, jitter: 'none' } } });
      state.billing = (_m, ctx) => {
        if (ctx.attempt < 3) {
          throw new Error(`billing unavailable (${ctx.attempt})`);
        }
      };

      const start = Date.now();
      await orders.place(1, [placed(1)]);
      await settle(10_000);

      expect(callsOf('billing').map((c) => [c.attempt, c.at - start])).toEqual([
        [1, 0],
        [2, 1_000], // retry 1 after 1s
        [3, 3_000], // retry 2 after 2s more
      ]);
      const retries = events.flatMap((e) => (e.type === 'retry-scheduled' ? [e] : []));
      expect(retries.map((e) => [e.attempt, e.delayMs])).toEqual([
        [1, 1_000],
        [2, 2_000],
      ]);
      expect(await relay.stats()).toMatchObject({ pending: 0, deadLetters: 0 });
    });

    it('dead-letters a message after its retry budget, with the full error history', async () => {
      await boot();
      state.billing = (_m, ctx) => {
        throw new Error(`card processor down (attempt ${ctx.attempt})`);
      };

      await orders.place(1, [placed(1)]);
      await settle(10_000);

      expect(callsOf('billing')).toHaveLength(3);
      const [dead] = await deadLetters.list();
      expect(dead).toMatchObject({
        topic: 'order.placed',
        payload: { orderId: 1 },
        reason: 'exhausted',
        attempts: 3,
        lastError: 'Error: card processor down (attempt 3)',
      });
      expect(dead!.history.map((h) => [h.attempt, h.error, h.transport])).toEqual([
        [1, 'Error: card processor down (attempt 1)', 'local'],
        [2, 'Error: card processor down (attempt 2)', 'local'],
        [3, 'Error: card processor down (attempt 3)', 'local'],
      ]);
      expect(await deadLetters.get(dead!.id)).toEqual(dead);
      expect(events.filter((e) => e.type === 'dead-lettered')).toHaveLength(1);
      expect(await relay.stats()).toMatchObject({ pending: 0, deadLetters: 1 });
    });

    it('dead-letters immediately on NonRetryableMessageError or when retryIf says no', async () => {
      await boot({ retry: { attempts: 5, retryIf: (error) => !(error instanceof TypeError) } });
      state.billing = (m) => {
        if (seqOf(m) === 1) {
          throw new NonRetryableMessageError('unknown currency');
        }
        throw new TypeError('payload.total is undefined');
      };

      await orders.place(1, [placed(1, { payload: { orderId: 1, seq: 1 } }), placed(1, { payload: { orderId: 1, seq: 2 } })]);
      await settle();

      const dead = await deadLetters.list();
      expect(dead.map((d) => [d.reason, d.attempts, d.history.length])).toEqual([
        ['rejected', 1, 1],
        ['rejected', 1, 1],
      ]);
      expect(dead.map((d) => d.lastError).sort()).toEqual([
        'NonRetryableMessageError: unknown currency',
        'TypeError: payload.total is undefined',
      ]);
    });

    it('dead-letters at once when every failing handler throws NonRetryableMessageError', async () => {
      await boot();
      state.billing = () => {
        throw new NonRetryableMessageError('billing: unknown currency');
      };
      state.shipping = () => {
        throw new NonRetryableMessageError('shipping: no such address');
      };

      await orders.place(1, [placed(1)]);
      await settle();

      const [dead] = await deadLetters.list();
      expect(dead).toMatchObject({ reason: 'rejected', attempts: 1 });
      expect(dead!.lastError).toContain('billing: unknown currency');
      expect(dead!.lastError).toContain('shipping: no such address');
      expect(events.filter((e) => e.type === 'retry-scheduled')).toHaveLength(0);
    });

    it('retries a mixed permanent and transient failure, then rejects once only the permanent one remains', async () => {
      await boot();
      state.billing = () => {
        throw new NonRetryableMessageError('unknown currency');
      };

      let shippingDown = true;
      state.shipping = () => {
        if (shippingDown) {
          throw new Error('shipping API timeout');
        }
      };

      await orders.place(1, [placed(1)]);
      await settle();
      expect(events.filter((e) => e.type === 'retry-scheduled')).toHaveLength(1);

      shippingDown = false;
      await settle(1_000);
      const [dead] = await deadLetters.list();
      expect(dead).toMatchObject({ reason: 'rejected', attempts: 2 });
      expect(await db.count('invoices', 'shipping')).toBe(1);
    });

    it("logs through Nest's logger: each scheduled retry, then the dead letter", async () => {
      await boot({ retry: { attempts: 2, backoff: { delay: 1_000, jitter: 'none' } } });
      const warnings: string[] = [];
      moduleRef.useLogger({
        log() {},
        error() {},
        warn: (message: string, context?: string) => warnings.push(`[${context}] ${message}`),
      });

      state.billing = () => {
        throw new Error('card processor down');
      };

      await orders.place(1, [placed(1)]);
      await settle(1_000);

      expect(warnings).toEqual([
        expect.stringMatching(
          /^\[OutboxRelay\] Retrying order\.placed \S+ in 1000ms \(attempt 1 of 2 failed\): Error: card processor down$/,
        ),
        expect.stringMatching(
          /^\[OutboxRelay\] Dead-lettered order\.placed \S+ after 2 attempt\(s\) \(exhausted\): Error: card processor down$/,
        ),
      ]);
      Logger.overrideLogger(false);
    });

    it("stops the relay before the application's database closes on shutdown", async () => {
      await boot();
      await orders.place(1, [{ topic: 'order.slow', payload: {} }]);
      await settle();
      expect(callsOf('billing')).toHaveLength(1); // in flight for 2s

      const closing = moduleRef.close();
      await settle(2_000);
      await closing;

      // onApplicationShutdown(), where an app closes its pool, ran after the relay stopped and
      // the in-flight message was published and marked: nothing wrote to a closed database.
      expect(state.atShutdown).toEqual({ relayRunning: false, inFlight: 0, pending: 0 });
      expect(events.filter((e) => e.type === 'published')).toHaveLength(1);
    });

    it('requeues a dead letter, which is then delivered under the same id; purges the rest', async () => {
      await boot();
      state.billing = () => {
        throw new Error('down');
      };

      await orders.place(1, [placed(1)]);
      await orders.place(2, [placed(2)]);
      await settle(10_000);

      const [second, first] = await deadLetters.list();
      expect([first, second].map((d) => (d!.payload as { orderId: number }).orderId)).toEqual([1, 2]);

      state.billing = undefined; // the downstream recovered
      expect(await deadLetters.requeue(first!.id)).toBe(1);
      await settle();
      expect(callsOf('billing').at(-1)).toMatchObject({ id: first!.id, attempt: 1 });
      expect(await deadLetters.list()).toHaveLength(1);

      await expect(deadLetters.purge({})).rejects.toThrow(/all: true/);
      expect(await deadLetters.purge({ topic: 'order.placed' })).toBe(1);
      expect(await relay.stats()).toMatchObject({ pending: 0, deadLetters: 0 });
    });

    it('keeps per-key order under failure while other keys keep flowing', async () => {
      await boot();
      let failures = 2;
      state.billing = (m) => {
        if (m.key === 'A' && seqOf(m) === 1 && failures-- > 0) {
          throw new Error('A1 rejected');
        }
      };

      await orders.place(1, [
        placed(1, { key: 'A', payload: { seq: 1 } }),
        placed(1, { key: 'B', payload: { seq: 1 } }),
        placed(1, { key: 'A', payload: { seq: 2 } }),
        placed(1, { payload: { seq: 1 } }),
        placed(1, { key: 'A', payload: { seq: 3 } }),
      ]);
      await settle();

      // Round 1: A1 fails; B1 and the keyless message go out; A2 and A3 wait behind A1.
      const keyed = (key: string | null) => callsOf('billing').filter((c) => c.key === key);
      expect(keyed('A').map((c) => c.seq)).toEqual([1]);
      expect(keyed('B').map((c) => c.seq)).toEqual([1]);
      expect(keyed(null).map((c) => c.seq)).toEqual([1]);

      await settle(10_000);
      expect(keyed('A').map((c) => [c.seq, c.attempt])).toEqual([
        [1, 1],
        [1, 2],
        [1, 3],
        [2, 1],
        [3, 1],
      ]);
      expect(await relay.stats()).toMatchObject({ pending: 0 });
    });

    it('dedupes a redelivered message through the inbox, but not with inbox: false', async () => {
      await boot();
      state.shipping = () => {}; // writes an invoice with processInTransaction, in its own transaction

      // The first publish reaches the handlers, then marking it published fails (say, the
      // connection dropped). After the lease expires the relay delivers it again.
      vi.spyOn(store, 'markPublished').mockImplementationOnce(() => {
        throw new Error('connection reset');
      });

      await orders.place(1, [placed(1)]);
      await settle();

      // Published, but not recorded as such: the message waits for its lease to expire.
      expect(await relay.stats()).toMatchObject({ pending: 1, leased: 1 });

      await settle(10_000);
      expect(events.filter((e) => e.type === 'published')).toHaveLength(1);
      expect(callsOf('audit')).toHaveLength(2); // inbox: false sees the duplicate
      expect(callsOf('billing')).toHaveLength(1); // the inbox skipped it before calling
      expect(callsOf('shipping')).toHaveLength(1); // its record committed with the invoice: skipped too
      expect(await db.count('invoices', 'shipping')).toBe(1);
    });

    it('processInTransaction rolls the record back with the handler: a failed attempt leaves none', async () => {
      await boot();
      let first = true;
      state.shipping = () => {
        if (first) {
          first = false;
          throw new Error('label printer jammed');
        }
      };

      await orders.place(1, [placed(1)]);
      await settle(10_000);

      expect(callsOf('shipping').map((c) => c.attempt)).toEqual([1, 2]);
      expect(await db.count('invoices', 'shipping')).toBe(1);
    });

    it('retries a partially failed fan-out without re-running the consumers that succeeded', async () => {
      await boot();
      state.shipping = () => {};

      let auditFailures = 1;
      state.audit = () => {
        if (auditFailures-- > 0) {
          throw new Error('audit log unavailable');
        }
      };

      await orders.place(1, [placed(1)]);
      await settle(10_000);

      expect(callsOf('audit').map((c) => c.attempt)).toEqual([1, 2]);
      expect(callsOf('billing').map((c) => c.attempt)).toEqual([1]);
      expect(callsOf('shipping').map((c) => c.attempt)).toEqual([1]);
      expect(await db.count('invoices')).toBe(1);
    });

    it('drains on shutdown: in-flight publishes finish, unstarted messages are released', async () => {
      await boot();
      await orders.place(1, [
        { topic: 'order.slow', key: 'K', payload: { seq: 1 } },
        { topic: 'order.slow', key: 'K', payload: { seq: 2 } },
      ]);
      await settle();
      expect(callsOf('billing').map((c) => c.seq)).toEqual([1]); // seq 1 in flight

      let closed = false;
      const closing = moduleRef.close().then(() => (closed = true));
      await settle(1_000);
      expect(closed).toBe(false); // waiting for the in-flight handler
      await settle(1_000);
      await closing;

      expect(callsOf('billing').map((c) => c.seq)).toEqual([1]);
      expect(events.filter((e) => e.type === 'published')).toHaveLength(1);
      // Released, not merely left to expire: another instance can take it right away.
      expect(await store.stats(Date.now())).toMatchObject({ pending: 1, leased: 0, ready: 1 });

      await settle(5_000);
      expect(callsOf('billing')).toHaveLength(1); // stopped relays don't claim
    });

    it('never runs a handler again while its timed-out invocation is still running', async () => {
      // publishTimeout gives up on the invocation, but the invocation keeps running. The retry
      // called the handler again while the first call was in flight, and the inbox (checked
      // before, recorded after) let every call through: three charges for one order.
      await boot({
        relay: { pollInterval: '1s', lease: '3s', publishTimeout: '1s' },
        retry: { attempts: 5, backoff: () => 0 },
      });
      await orders.place(1, [{ topic: 'order.sluggish', payload: {} }]);
      await settle(8_000);

      expect(callsOf('billing')).toHaveLength(1);
      expect(events.filter((e) => e.type === 'retry-scheduled').length).toBeGreaterThan(0);
      expect(events.filter((e) => e.type === 'published')).toHaveLength(1);
      expect(await relay.stats()).toMatchObject({ pending: 0, deadLetters: 0 });
    });

    it('never starts a handler for an attempt the relay already gave up on', async () => {
      // Attempt 2 waits behind attempt 1's call, which outlives both their timeouts and fails.
      // Attempt 2 then ran the handler anyway, with its signal already aborted.
      await boot({
        relay: { pollInterval: '1s', lease: '3s', publishTimeout: '1s' },
        retry: { attempts: 5, backoff: () => 0 },
      });
      await orders.place(1, [{ topic: 'order.flaky', payload: {} }]);
      await settle(8_000);

      expect(state.startedAborted).toEqual([false, false]);
      expect(callsOf('billing').map((c) => c.attempt)).toEqual([1, 3]);
      expect(events.filter((e) => e.type === 'published')).toHaveLength(1);
    });

    it("aborts the handler's signal and unsubscribes its Observable when the relay stops waiting", async () => {
      await boot({
        relay: { pollInterval: '1s', lease: '3s', publishTimeout: '1s' },
        retry: { attempts: 2, backoff: () => 0 },
      });
      await orders.place(1, [{ topic: 'order.stuck', payload: {} }]);
      await settle(1_000);
      expect(state.signals[0]!.aborted).toBe(true);
      expect(state.teardowns).toBe(1);

      // The interrupted delivery isn't recorded as processed: the retry runs the handler again.
      await settle(3_000);
      expect(callsOf('billing')).toHaveLength(2);
      expect(await deadLetters.list()).toEqual([
        expect.objectContaining({ reason: 'exhausted', attempts: 2, lastError: 'OutboxPublishTimeoutError: Publish did not settle within 1000ms' }),
      ]);
    });

    it('reports lag (how long the oldest due message has waited) and publish events', async () => {
      await boot({ relay: { enabled: false } });
      await orders.place(1, [placed(1)]);
      // A reminder for tomorrow isn't late, so it doesn't count as lag.
      await orders.place(2, [placed(2, { delay: '1d' })]);
      await settle(4_000);
      expect(await relay.stats()).toMatchObject({ pending: 2, ready: 1, lagMs: 4_000, inFlight: 0 });

      expect(await relay.runOnce()).toMatchObject({ claimed: 1, published: 1 });
      expect(events.map((e) => e.type)).toEqual(['published']);
      expect(await relay.stats()).toMatchObject({ pending: 1, lagMs: 0 });
    });

    it('is configurable with forRootAsync, values from the factory', async () => {
      await boot(
        {},
        OutboxModule.forRootAsync({
          useFactory: () => ({ relay: { pollInterval: '250ms' } }),
        }),
      );

      await orders.place(1, [placed(1)], { notify: false });
      await settle(250);
      expect(callsOf('billing')).toHaveLength(1);
    });
  });
}
