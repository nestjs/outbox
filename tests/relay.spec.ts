import { AsyncLocalStorage } from 'node:async_hooks';
import { startPostgres } from './support/postgres.js';
import {
  OutboxEvents,
  OutboxPublishTimeoutError,
  OutboxRelay,
  type OutboxEvent,
  type OutboxMessage,
  type OutboxRelayOptions,
  type OutboxStore,
  type OutboxTransport,
} from '../lib/index.js';
import { uuidv7 } from '../lib/utils/uuid.util.js';
import { crashable, inMemoryDatabase, pgliteDatabase, postgresDatabase, type TestDatabase } from './databases.js';
import { silentLogger, sleep, until } from './helpers.js';

const { postgres, reason } = await startPostgres();
afterAll(() => postgres?.stop());

/**
 * Plain relay instances, each with its own store on one database = separate app instances:
 * on the in-memory store (one store object), on DrizzleOutboxStore on PGlite (one connection,
 * so their transactions take turns) and on PostgreSQL (a pool per instance, so they overlap).
 */
const targets: { name: string; skip?: boolean; open(): Promise<TestDatabase> }[] = [
  { name: 'InMemoryOutboxStore', open: async () => inMemoryDatabase() },
  { name: 'DrizzleOutboxStore on PGlite', open: pgliteDatabase },
  {
    name: `DrizzleOutboxStore on PostgreSQL${postgres ? '' : ` (skipped: ${reason})`}`,
    skip: !postgres,
    open: () => postgresDatabase(postgres!, 'outbox_relay'),
  },
];

for (const target of targets) {
  describe.skipIf(target.skip)(`OutboxRelay (multiple instances, one database): ${target.name}`, () => {
    let database: TestDatabase;

    beforeAll(async () => {
      database = await target.open();
    }, 60_000); // PGlite loads its WASM on first open: slow under a parallel run
    afterAll(() => database?.close());
    beforeEach(() => database.reset());
    afterEach(() => {
      vi.useRealTimers();
    });

    const openStore = () => database.store();

    const relay = (
      store: OutboxStore,
      transport: OutboxTransport,
      options: OutboxRelayOptions = {},
      events?: OutboxEvent[],
    ) => {
      const outboxEvents = new OutboxEvents();
      if (events) {
        outboxEvents.events$.subscribe((e) => events.push(e));
      }

      return new OutboxRelay({
        store,
        transports: { sink: transport },
        relay: { enabled: false, lease: 1_000, ...options },
        retry: { attempts: 5, backoff: { delay: 10, jitter: 'none' } },
        logger: silentLogger,
        events: outboxEvents,
      });
    };

    const produce = async (count: number, key: (i: number) => string | null = () => null) => {
      const writer = openStore();
      const messages: OutboxMessage[] = [];

      for (let i = 0; i < count; i++) {
        messages.push({
          id: uuidv7(),
          topic: 'order.placed',
          payload: { seq: i },
          headers: {},
          key: key(i),
          createdAt: Date.now(),
          availableAt: Date.now(),
          attempts: 0,
          lastError: null,
        });
      }

      await database.transaction((tx) => writer.add(tx, messages));
      return messages;
    };

    it('recovers messages claimed by a relay that died: lease expiry, then a second relay delivers', async () => {
      vi.useFakeTimers({ toFake: ['Date'] });
      const messages = await produce(3, () => 'order-1');

      // Instance A claims the batch, then the process dies before publishing anything.
      const a = crashable(openStore());
      expect(await a.store.claim({ owner: 'instance-a', now: Date.now(), leaseMs: 1_000, limit: 10 })).toHaveLength(3);
      a.crash();

      const delivered: OutboxMessage[] = [];
      const b = relay(openStore(), { publish: (m) => void delivered.push(m) });

      expect(await b.runOnce()).toMatchObject({ claimed: 0 }); // still A's
      vi.advanceTimersByTime(1_000);
      expect(await b.runOnce()).toMatchObject({ claimed: 3, published: 3 });
      expect(delivered.map((m) => m.id)).toEqual(messages.map((m) => m.id));
    });

    it('publishes again if a relay dies after publishing but before marking (at-least-once)', async () => {
      vi.useFakeTimers({ toFake: ['Date'] });
      const [message] = await produce(1);
      const sink: string[] = [];

      const instanceA = crashable(openStore());
      const a = relay(instanceA.store, {
        publish: (m) => {
          sink.push(m.id);
          instanceA.crash(); // right after the broker accepted the message
        },
      });

      expect(await a.runOnce()).toMatchObject({ claimed: 1, published: 0, leaseLost: 1 });

      const b = relay(openStore(), { publish: (m) => void sink.push(m.id) });
      vi.advanceTimersByTime(1_000);
      expect(await b.runOnce()).toMatchObject({ published: 1 });
      // The duplicate is the price of never losing a message; consumers dedupe by id.
      expect(sink).toEqual([message!.id, message!.id]);
    });

    it('two relays running concurrently never publish a message twice and keep per-key order', async () => {
      const keyOf = (i: number) => (i % 4 === 0 ? null : `customer-${i % 5}`);
      const messages = await produce(120, keyOf);
      const byRelay: Record<string, number[]> = { a: [], b: [] };
      const deliveries: OutboxMessage[] = [];

      const transport = (name: string): OutboxTransport => ({
        publish: async (m) => {
          deliveries.push(m);
          byRelay[name]!.push((m.payload as { seq: number }).seq);
          await sleep(Math.random() * 2);
        },
      });
      const options = { batchSize: 7, concurrency: 3, lease: '10s' } as const;
      const relays = [relay(openStore(), transport('a'), options), relay(openStore(), transport('b'), options)];

      const drain = async (r: OutboxRelay) => {
        while (deliveries.length < messages.length) {
          const { claimed } = await r.runOnce();
          if (claimed === 0) {
            await sleep(1);
          }
        }
      };
      await Promise.all(relays.map(drain));

      const ids = deliveries.map((m) => m.id);
      expect(new Set(ids).size).toBe(messages.length);
      expect(ids).toHaveLength(messages.length);
      expect(byRelay.a!.length).toBeGreaterThan(0);
      expect(byRelay.b!.length).toBeGreaterThan(0);

      for (const key of new Set(messages.map((m) => m.key))) {
        if (key === null) {
          continue;
        }
        const order = deliveries.filter((m) => m.key === key).map((m) => (m.payload as { seq: number }).seq);
        expect(order).toEqual([...order].sort((x, y) => x - y));
      }
    });

    it('does not start a publish that could outlive its lease; releases the rest of the batch', async () => {
      vi.useFakeTimers({ toFake: ['Date'] });
      await produce(5, () => 'order-1');
      const published: number[] = [];
      const events: OutboxEvent[] = [];
      const r = relay(
        openStore(),
        {
          publish: (m) => {
            published.push((m.payload as { seq: number }).seq);
            vi.advanceTimersByTime(300); // a slow broker
          },
        },
        { lease: '1s', publishTimeout: '200ms' },
        events,
      );

      // Starts at 0, 300, 600 (600 + 200 <= 1000); at 900 there is not enough lease left.
      expect(await r.runOnce()).toMatchObject({ claimed: 5, published: 3, released: 2 });
      expect(published).toEqual([0, 1, 2]);
      expect(events.map((e) => e.type)).toEqual(['published', 'published', 'published']);

      expect(await r.runOnce()).toMatchObject({ claimed: 2, published: 2 });
      expect(published).toEqual([0, 1, 2, 3, 4]);
    });

    it('counts a publish that hangs past publishTimeout as a failed attempt', async () => {
      const [message] = await produce(1);
      const events: OutboxEvent[] = [];
      // A long lease: with a short one, a slow first claim on a server left less than
      // `publishTimeout` of it, and the relay released the message instead of publishing.
      const r = relay(openStore(), { publish: () => new Promise(() => {}) }, { lease: '10s', publishTimeout: 20 }, events);

      expect(await r.runOnce()).toMatchObject({ retried: 1 });
      const retry = events.find((e) => e.type === 'retry-scheduled');
      expect(retry).toMatchObject({ attempt: 1, delayMs: 10, message: { id: message!.id } });
      expect(String((retry as { error: Error }).error)).toMatch(/did not settle within 20ms/);
    });

    it("aborts the publish's signal when publishTimeout passes, so the transport can stop", async () => {
      await produce(1);
      const signals: AbortSignal[] = [];
      const r = relay(
        openStore(),
        {
          publish: (_m, { signal }) => {
            signals.push(signal);
            return new Promise(() => {}); // a broker call that would hang without the signal
          },
        },
        { lease: '10s', publishTimeout: 20 },
      );

      expect(await r.runOnce()).toMatchObject({ retried: 1 });
      const [signal] = signals;
      expect(signal!.aborted).toBe(true);
      expect(signal!.reason).toBeInstanceOf(OutboxPublishTimeoutError);
      expect(signal!.reason).toMatchObject({ timeoutMs: 20 });
    });

    it('polls on its own and publishes immediately when notified', async () => {
      const store = openStore();
      let polls = 0;
      const claim = store.claim.bind(store);

      // Counted once the claim has run: counted before it, a message produced meanwhile could
      // land in the first poll on a server, where a claim takes a few round trips.
      vi.spyOn(store, 'claim').mockImplementation(async (request) => {
        const batch = await claim(request);
        polls++;
        return batch;
      });

      const delivered: string[] = [];
      const r = relay(store, { publish: (m) => void delivered.push(m.id) }, { pollInterval: 60_000 });

      r.start();
      await until(() => polls === 1); // the initial poll found nothing; the next is a minute away
      const [message] = await produce(1);
      await sleep(20);
      expect(delivered).toEqual([]);

      r.notify();
      await until(() => delivered.length === 1);
      expect(delivered).toEqual([message!.id]);
      expect(polls).toBe(2);
      await r.stop();
    });

    it('keeps a relay-only worker process alive: the poll timer is referenced until stop() clears it', async () => {
      // A worker built with `NestFactory.createApplicationContext()` has nothing else on the
      // event loop. With the poll timer unref'd, the process exited right after bootstrap
      // (exit code 0, nothing logged) and never published a message.
      const timers = vi.spyOn(globalThis, 'setTimeout');
      const cleared = vi.spyOn(globalThis, 'clearTimeout');

      const store = openStore();
      let polls = 0;
      const claim = store.claim.bind(store);
      vi.spyOn(store, 'claim').mockImplementation((request) => (polls++, claim(request)));

      const r = relay(store, { publish: () => undefined }, { pollInterval: 60_000 });
      try {
        r.start();
        await until(() => polls === 1);

        // The first poll found nothing, and the loop scheduled the next one a minute out.
        const isPoll = ([, delay]: unknown[]) => delay === 60_000;
        await until(() => timers.mock.calls.some(isPoll));
        const pending = timers.mock.results[timers.mock.calls.findLastIndex(isPoll)]!.value as NodeJS.Timeout;
        expect(pending.hasRef()).toBe(true);

        await r.stop();
        expect(cleared).toHaveBeenCalledWith(pending);
      } finally {
        timers.mockRestore();
        cleared.mockRestore();
        await r.stop();
      }
    });

    it("delivers outside the async context of the caller that notified it, and stays outside", async () => {
      // What a request keeps in AsyncLocalStorage: its user, its locale.
      const request = new AsyncLocalStorage<string>();
      const store = openStore();
      let polls = 0;
      const claim = store.claim.bind(store);
      vi.spyOn(store, 'claim').mockImplementation((req) => (polls++, claim(req)));
      const seen: (string | undefined)[] = [];
      const r = relay(store, { publish: () => void seen.push(request.getStore()) }, { pollInterval: 20 });

      r.start();
      await until(() => polls === 1);
      await produce(1);
      request.run('alice', () => r.notify()); // right after the commit, inside the request
      await until(() => seen.length === 1);

      // The loop rescheduled itself after that poll: a later message, committed outside any
      // request, must not be delivered as that request either.
      await produce(1);
      await until(() => seen.length === 2);

      expect(seen).toEqual([undefined, undefined]);
      await r.stop();
    });

    it('hands retryIf the error, the attempt that failed and the message', async () => {
      const [message] = await produce(1);
      const seen: unknown[][] = [];
      const store = openStore();
      const r = new OutboxRelay({
        store,
        transports: { sink: { publish: () => Promise.reject(new Error('broker down')) } },
        relay: { enabled: false },
        retry: {
          attempts: 5,
          backoff: () => 0,
          retryIf: (error, attempt, m) => (seen.push([String(error), attempt, m.id]), attempt < 2),
        },
        logger: silentLogger,
      });

      expect(await r.runOnce()).toMatchObject({ retried: 1 });
      expect(await r.runOnce()).toMatchObject({ deadLettered: 1 });
      expect(seen).toEqual([
        ['Error: broker down', 1, message!.id],
        ['Error: broker down', 2, message!.id],
      ]);
      expect((await store.listDeadLetters({}))[0]).toMatchObject({ reason: 'rejected', attempts: 2 });
    });

    it('keeps retrying with the default backoff when retryIf or backoff throws', async () => {
      await produce(1);
      const events: OutboxEvent[] = [];
      const outboxEvents = new OutboxEvents();
      outboxEvents.events$.subscribe((e) => events.push(e));

      const r = new OutboxRelay({
        store: openStore(),
        transports: { sink: { publish: () => Promise.reject(new Error('broker down')) } },
        relay: { enabled: false },
        retry: {
          retryIf: () => {
            throw new Error('bug in retryIf');
          },
          backoff: () => 'soon' as '1s',
        },
        logger: silentLogger,
        events: outboxEvents,
      });

      expect(await r.runOnce()).toMatchObject({ retried: 1 });
      // The default backoff: 1s with equal jitter, so between 500 and 1000 ms.
      expect(events[0]).toMatchObject({ type: 'retry-scheduled', attempt: 1 });
      expect((events[0] as { delayMs: number }).delayMs).toBeGreaterThanOrEqual(500);
    });

    it('publishes each event on its diagnostics channel', async () => {
      const { subscribe, unsubscribe } = await import('node:diagnostics_channel');
      const received: Array<[string, unknown]> = [];
      const listeners = ['published', 'retry-scheduled', 'dead-lettered'].map((type) => {
        const name = `nestjs:outbox:${type}`;
        const listener = (event: unknown) => received.push([name, (event as OutboxEvent).type]);
        subscribe(name, listener);
        return () => unsubscribe(name, listener);
      });

      try {
        await produce(3);
        let calls = 0;
        const r = new OutboxRelay({
          store: openStore(),
          transports: {
            sink: {
              publish: () => {
                calls++;
                if (calls === 2) {
                  throw new Error('flaky');
                }
                if (calls === 3) {
                  throw new Error('down for good');
                }
              },
            },
          },
          relay: { enabled: false, concurrency: 1 },
          retry: { attempts: 2, backoff: () => 0, retryIf: (error) => String(error) !== 'Error: down for good' },
          logger: silentLogger,
        });

        await r.runOnce();
        expect(received).toEqual([
          ['nestjs:outbox:published', 'published'],
          ['nestjs:outbox:retry-scheduled', 'retry-scheduled'],
          ['nestjs:outbox:dead-lettered', 'dead-lettered'],
        ]);
      } finally {
        listeners.forEach((stop) => stop());
      }
    });

    it('fails at startup on a bad relay option, naming it', () => {
      const build = (relay: OutboxRelayOptions) =>
        new OutboxRelay({ store: openStore(), transports: {}, relay, logger: silentLogger });

      expect(() => build({ lease: '30 seconds' as '30s' })).toThrow(/relay\.lease: Invalid duration/);
      expect(() => build({ pollInterval: -1 })).toThrow(/relay\.pollInterval/);
      expect(() => build({ lease: '10s', publishTimeout: '10s' })).toThrow(
        /relay\.publishTimeout must be shorter than relay\.lease/,
      );

      // `Number(process.env.OUTBOX_CONCURRENCY)` with the variable unset is NaN: the relay
      // claimed batches and never published them.
      expect(() => build({ concurrency: NaN })).toThrow(
        'OutboxModule: relay.concurrency must be a whole number of at least 1 (got NaN)',
      );
      expect(() => build({ concurrency: 0 })).toThrow(/relay\.concurrency/);
      expect(() => build({ concurrency: 2.5 })).toThrow(/relay\.concurrency/);

      // 0 and 1.5 claimed nothing, and -1 claimed the whole table (a negative LIMIT means no
      // limit on some databases).
      for (const batchSize of [0, 1.5, -1]) {
        expect(() => build({ batchSize })).toThrow(/relay\.batchSize must be a whole number of at least 1/);
      }

      // A zero poll interval is a busy loop; a zero publish timeout fails every publish.
      expect(() => build({ pollInterval: 0 })).toThrow(/relay\.pollInterval must be longer than 0/);
      expect(() => build({ publishTimeout: 0 })).toThrow(/relay\.publishTimeout must be longer than 0/);

      // `enabled: process.env.OUTBOX_RELAY` is a string: "false" is truthy, so an API-only
      // instance quietly ran a relay.
      expect(() => build({ enabled: 'false' as unknown as boolean })).toThrow(
        'OutboxModule: relay.enabled must be a boolean (got the string "false")',
      );

      // A `route` that isn't a function failed at delivery, once per message, and every
      // message ended up dead-lettered after its retries.
      expect(() =>
        new OutboxRelay({ store: openStore(), transports: {}, route: 'local' as never, logger: silentLogger }),
      ).toThrow('OutboxModule: route must be a function (got the string "local")');
    });

    it('survives a transport that rejects with something other than an Error', async () => {
      // `Promise.reject()`, `throw undefined`, a Symbol: describing the error threw, runOnce()
      // rejected, and its bookkeeping raised an unhandled rejection, which exits the process.
      // The message stayed leased, so the next relay to claim it crashed the same way.
      const reasons: unknown[] = [undefined, Symbol('nope'), function reasons() {}];
      const messages = await produce(reasons.length);

      const unhandled: unknown[] = [];
      const onUnhandled = (reason: unknown) => void unhandled.push(reason);
      process.on('unhandledRejection', onUnhandled);
      try {
        const store = openStore();
        const r = relay(store, {
          publish: (m) => Promise.reject(reasons[(m.payload as { seq: number }).seq]),
        });

        expect(await r.runOnce()).toMatchObject({ claimed: 3, retried: 3 });
        await sleep(20);
        expect(unhandled).toEqual([]);

        // Rescheduled 10 ms out: claim them back to read what the store recorded.
        const retried = await store.claim({ owner: 'check', now: Date.now() + 1_000, leaseMs: 1_000, limit: 10 });
        const lastError = new Map(retried.map((m) => [m.id, m.lastError]));
        expect(messages.map((m) => lastError.get(m.id))).toEqual(['undefined', 'Symbol(nope)', '[function reasons]']);
      } finally {
        process.off('unhandledRejection', onUnhandled);
      }
    });
  });
}
