/**
 * OutboxRelay on the in-memory store, one instance at a time: batching, the poll loop,
 * concurrency, per-key failure handling, routing, backoff, store failures and the lifecycle.
 * Timers and the clock are fake, so every wait is exact.
 */
import {
  InMemoryOutboxStore,
  NonRetryableMessageError,
  OutboxEvents,
  OutboxPublishTimeoutError,
  OutboxRelay,
  type OutboxEvent,
  type OutboxMessage,
  type OutboxModuleOptions,
  type OutboxTransport,
} from '../lib/index.js';
import type { OutboxRelayConfig } from '../lib/interfaces/outbox-relay.interface.js';
import { deferred, message } from './helpers.js';

class Lines {
  readonly lines: string[] = [];
  log(line: string) {
    this.lines.push(`LOG ${line}`);
  }
  warn(line: string) {
    this.lines.push(`WARN ${line}`);
  }
  error(line: string) {
    this.lines.push(`ERROR ${line}`);
  }
}

const topics = (messages: OutboxMessage[]) => messages.map((m) => m.topic);

describe('OutboxRelay (one instance, in memory)', () => {
  let store: InMemoryOutboxStore;
  let logger: Lines;
  let events: OutboxEvent[];
  let relays: OutboxRelay[];

  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'], now: 1_000_000 });
    store = new InMemoryOutboxStore();
    logger = new Lines();
    events = [];
    relays = [];
  });

  afterEach(async () => {
    await Promise.all(relays.map((r) => r.stop()));
    vi.useRealTimers();
  });

  function build(
    transports: Record<string, OutboxTransport>,
    options: Partial<OutboxModuleOptions> & Pick<Partial<OutboxRelayConfig>, 'events' | 'store'> = {},
  ) {
    const outboxEvents = new OutboxEvents();
    outboxEvents.events$.subscribe((e) => events.push(e));

    const relay = new OutboxRelay({
      store,
      events: outboxEvents,
      logger,
      retry: { attempts: 3, backoff: { delay: 1_000, jitter: 'none' } },
      ...options,
      transports,
      relay: { enabled: false, lease: '30s', ...options.relay },
    });
    relays.push(relay);
    return relay;
  }

  const add = (...messages: OutboxMessage[]) =>
    store.add({}, messages.map((m) => ({ ...m, createdAt: Date.now(), availableAt: m.availableAt || Date.now() })));

  /** Runs due timers and the promise chains they start. */
  const settle = async (ms = 0) => {
    await vi.advanceTimersByTimeAsync(ms);
    for (let i = 0; i < 5; i++) {
      await vi.advanceTimersByTimeAsync(0);
    }
  };

  const countClaims = () => {
    const claims: number[] = [];
    const claim = store.claim.bind(store);
    vi.spyOn(store, 'claim').mockImplementation((request) => {
      const batch = claim(request);
      claims.push(batch.length);
      return batch;
    });
    return claims;
  };

  beforeAll(() => {
    // InMemoryOutboxStore warns once per store about foreign transaction handles.
    vi.spyOn(InMemoryOutboxStore['logger'], 'warn').mockImplementation(() => {});
  });
  afterAll(() => vi.restoreAllMocks());

  describe('batches and the poll loop', () => {
    it('claims at most batchSize per run, and drains full batches without waiting for the next poll', async () => {
      add(...['a', 'b', 'c', 'd', 'e'].map((t) => message(t)));
      const claims = countClaims();
      const published: string[] = [];
      const relay = build({ sink: { publish: (m) => void published.push(m.topic) } }, {
        relay: { batchSize: 2, pollInterval: '1m' },
      });

      relay.start();
      await settle();

      // Two full batches, then a short one that ends the drain.
      expect(claims).toEqual([2, 2, 1]);
      expect(published).toEqual(['a', 'b', 'c', 'd', 'e']);

      await settle(59_999);
      expect(claims).toHaveLength(3);
      await settle(1);
      expect(claims).toEqual([2, 2, 1, 0]);
    });

    it('runs another pass when notified while a pass is in flight', async () => {
      add(message('first'));
      const claims = countClaims();
      const gate = deferred();
      const published: string[] = [];
      const relay = build(
        {
          sink: {
            publish: async (m) => {
              published.push(m.topic);
              if (m.topic === 'first') {
                await gate.promise;
              }
            },
          },
        },
        { relay: { pollInterval: '1h' } },
      );

      relay.start();
      await settle();
      expect(published).toEqual(['first']);

      add(message('second'));
      relay.notify(); // the loop is busy: remembered, not dropped
      gate.resolve();
      await settle();

      expect(published).toEqual(['first', 'second']);
      expect(claims).toEqual([1, 1]);
    });

    it('starts once, ignores notify() while stopped, and starts again after stop()', async () => {
      add(message('a'));
      const claims = countClaims();
      const relay = build({ sink: { publish: () => undefined } }, { relay: { pollInterval: '1h' } });

      relay.notify();
      await settle();
      expect(claims).toEqual([]);
      expect(relay.running).toBe(false);

      relay.start();
      relay.start();
      await settle();
      expect(claims).toEqual([1]);
      expect(relay.running).toBe(true);

      await relay.stop();
      expect(relay.running).toBe(false);
      add(message('b'));
      relay.notify();
      await settle(3_600_000);
      expect(claims).toEqual([1]);

      relay.start();
      await settle();
      expect(claims).toEqual([1, 1]);
    });

    it('starts on bootstrap unless relay.enabled is false', async () => {
      const claims = countClaims();
      const off = build({ sink: { publish: () => undefined } }, { relay: { enabled: false } });
      off.onApplicationBootstrap();
      await settle();
      expect([off.running, claims.length]).toEqual([false, 0]);

      const on = build({ sink: { publish: () => undefined } }, { relay: { enabled: true, pollInterval: '1h' } });
      on.onApplicationBootstrap();
      await settle();
      expect([on.running, claims.length]).toEqual([true, 1]);

      await on.onModuleDestroy();
      expect(on.running).toBe(false);
    });
  });

  describe('concurrency and keys', () => {
    it('publishes at most `concurrency` key groups at once, and reports them in flight', async () => {
      add(...['a', 'b', 'c', 'd', 'e'].map((t) => message(t)));
      let running = 0;
      let peak = 0;
      const gates: Array<() => void> = [];
      const relay = build(
        {
          sink: {
            publish: () => {
              running++;
              peak = Math.max(peak, running);
              return new Promise<void>((resolve) => gates.push(() => (running--, resolve())));
            },
          },
        },
        { relay: { concurrency: 2 } },
      );

      const run = relay.runOnce();
      await settle();
      expect(running).toBe(2);
      expect((await relay.stats()).inFlight).toBe(2);

      while (gates.length > 0) {
        gates.shift()!();
        await settle();
      }

      expect(await run).toMatchObject({ claimed: 5, published: 5 });
      expect(peak).toBe(2);
      expect((await relay.stats()).inFlight).toBe(0);
    });

    it("reschedules a failed message and releases the rest of its key; other keys go on", async () => {
      add(message('a1', 'A'), message('a2', 'A'), message('b1', 'B'), message('a3', 'A'), message('free'));
      const attempted: string[] = [];
      const relay = build({
        sink: {
          publish: (m) => {
            attempted.push(m.topic);
            if (m.topic === 'a1') {
              throw new Error('rejected by the broker');
            }
          },
        },
      });

      expect(await relay.runOnce()).toEqual({
        claimed: 5,
        published: 2,
        retried: 1,
        deadLettered: 0,
        released: 2,
        leaseLost: 0,
      });
      expect(attempted.sort()).toEqual(['a1', 'b1', 'free']);

      // Released, not leased: but a1 waits to retry, and holds A back until then.
      expect(store.stats(Date.now())).toMatchObject({ pending: 3, leased: 0, ready: 0 });
      await settle(1_000);
      expect(topics(store.claim({ owner: 'next', now: Date.now(), leaseMs: 1_000, limit: 10 }))).toEqual([
        'a1',
        'a2',
        'a3',
      ]);
    });

    it('lets the rest of a key through in the same batch once its head is dead-lettered', async () => {
      add(message('a1', 'A'), message('a2', 'A'));
      const attempted: string[] = [];
      const relay = build({
        sink: {
          publish: (m) => {
            attempted.push(m.topic);
            if (m.topic === 'a1') {
              throw new NonRetryableMessageError('malformed');
            }
          },
        },
      });

      expect(await relay.runOnce()).toMatchObject({ claimed: 2, deadLettered: 1, published: 1, released: 0 });
      expect(attempted).toEqual(['a1', 'a2']);
      expect(events.map((e) => e.type)).toEqual(['dead-lettered', 'published']);
    });
  });

  describe('routing', () => {
    it('routes to the first transport other than local when there is no route', async () => {
      add(message('a'));
      const seen: string[] = [];
      const relay = build({
        local: { publish: () => void seen.push('local') },
        broker: { publish: () => void seen.push('broker') },
      });

      await relay.runOnce();
      expect(seen).toEqual(['broker']);
      expect(events[0]).toMatchObject({ type: 'published', transport: 'broker' });
    });

    it('dead-letters a message routed to a transport that does not exist, without retrying', async () => {
      const m = message('order.placed');
      add(m);
      const relay = build({ sink: { publish: () => undefined } }, { route: () => 'kafka', retry: 5 });

      expect(await relay.runOnce()).toMatchObject({ deadLettered: 1, retried: 0 });
      expect(store.getDeadLetter(m.id)).toMatchObject({
        reason: 'rejected',
        attempts: 1,
        lastError: 'NonRetryableMessageError: No outbox transport named "kafka"',
        history: [{ attempt: 1, transport: 'kafka' }],
      });
      expect(events[0]).toMatchObject({ type: 'dead-lettered', transport: 'kafka', reason: 'rejected' });
    });

    it('counts a route that throws as a failed attempt with no transport', async () => {
      add(message('order.placed'));
      const relay = build(
        { sink: { publish: () => undefined } },
        {
          route: () => {
            throw new Error('unknown tenant');
          },
        },
      );

      expect(await relay.runOnce()).toMatchObject({ retried: 1 });
      expect(events[0]).toMatchObject({ type: 'retry-scheduled', transport: undefined, attempt: 1, delayMs: 1_000 });
      expect(String((events[0] as { error: unknown }).error)).toBe('Error: unknown tenant');
    });
  });

  describe('retries', () => {
    it('backs off exponentially up to maxDelay, hands the transport the failures so far, then dead-letters', async () => {
      const m = message('order.placed');
      add(m);
      const seen: Array<[number, string | null]> = [];
      const relay = build(
        {
          sink: {
            publish: (msg) => {
              seen.push([msg.attempts, msg.lastError]);
              throw new Error(`down ${msg.attempts + 1}`);
            },
          },
        },
        { retry: { attempts: 4, backoff: { delay: 100, factor: 3, maxDelay: 500, jitter: 'none' } } },
      );

      for (const delay of [100, 300, 500]) {
        expect(await relay.runOnce()).toMatchObject({ retried: 1 });
        await settle(delay - 1);
        expect(await relay.runOnce()).toMatchObject({ claimed: 0 }); // not due yet
        await settle(1);
      }
      expect(await relay.runOnce()).toMatchObject({ deadLettered: 1 });

      expect(seen).toEqual([
        [0, null],
        [1, 'Error: down 1'],
        [2, 'Error: down 2'],
        [3, 'Error: down 3'],
      ]);
      expect(events.flatMap((e) => (e.type === 'retry-scheduled' ? [e.delayMs] : []))).toEqual([100, 300, 500]);

      const dead = store.getDeadLetter(m.id)!;
      expect(dead).toMatchObject({ reason: 'exhausted', attempts: 4, lastError: 'Error: down 4' });
      expect(dead.history.map((h) => [h.attempt, h.at - 1_000_000, h.transport])).toEqual([
        [1, 0, 'sink'],
        [2, 100, 'sink'],
        [3, 400, 'sink'],
        [4, 900, 'sink'],
      ]);
    });

    it('gives a publish lease / 3 by default', async () => {
      add(message('slow'));
      const relay = build({ sink: { publish: () => new Promise(() => {}) } }, { relay: { lease: '3s' } });

      const run = relay.runOnce();
      await settle(999);
      expect(events).toEqual([]);
      await settle(1);

      expect(await run).toMatchObject({ retried: 1 });
      const error = (events[0] as { error: unknown }).error;
      expect(error).toBeInstanceOf(OutboxPublishTimeoutError);
      expect(error).toMatchObject({ timeoutMs: 1_000 });
    });
  });

  describe('store failures', () => {
    it('logs a failed claim and reports an empty run', async () => {
      vi.spyOn(store, 'claim').mockImplementation(() => {
        throw new Error('connection refused');
      });
      const relay = build({ sink: { publish: () => undefined } });

      expect(await relay.runOnce()).toEqual({
        claimed: 0,
        published: 0,
        retried: 0,
        deadLettered: 0,
        released: 0,
        leaseLost: 0,
      });
      expect(logger.lines).toEqual(['ERROR Outbox store claim failed: Error: connection refused']);
    });

    it('counts a published message it could not mark as lost, and releases the rest of its key', async () => {
      add(message('a1', 'A'), message('a2', 'A'));
      vi.spyOn(store, 'markPublished').mockImplementationOnce(() => {
        throw new Error('connection reset');
      });
      const relay = build({ sink: { publish: () => undefined } });

      expect(await relay.runOnce()).toMatchObject({ published: 0, leaseLost: 1, released: 1 });
      expect(events).toEqual([]); // not published as far as anyone can tell: it goes out again
      expect(logger.lines).toContain('ERROR Outbox store markPublished failed: Error: connection reset');
      expect(store.stats(Date.now())).toMatchObject({ pending: 2, leased: 1 });
    });

    it.each([
      ['reschedule', 'retry', () => new Error('flaky')],
      ['deadLetter', 'dead letter', () => new NonRetryableMessageError('bad')],
    ] as const)('reports a lost lease when %s finds the message taken over (%s)', async (method, _, error) => {
      const m = message('order.placed');
      add(m);
      vi.spyOn(store, method).mockReturnValueOnce(false);
      const relay = build({
        sink: {
          publish: () => {
            throw error();
          },
        },
      });

      expect(await relay.runOnce()).toMatchObject({ leaseLost: 1, retried: 0, deadLettered: 0 });
      expect(events).toEqual([{ type: 'lease-lost', message: expect.objectContaining({ id: m.id }) }]);
      expect(logger.lines).toEqual([
        `WARN Lease on order.placed ${m.id} was taken over; it may be published more than once`,
      ]);
    });

    it('logs a store that throws while recording a failure, and leaves the lease to expire', async () => {
      add(message('a1', 'A'), message('a2', 'A'));
      vi.spyOn(store, 'reschedule').mockImplementation(() => {
        throw new Error('deadlock detected');
      });
      const relay = build({
        sink: {
          publish: () => {
            throw new Error('down');
          },
        },
      });

      expect(await relay.runOnce()).toMatchObject({ retried: 0, leaseLost: 1, released: 1 });
      expect(logger.lines).toContain('ERROR Outbox store fail failed: Error: deadlock detected');
    });

    it('logs a failed release; the leases simply expire', async () => {
      add(message('a1', 'A'), message('a2', 'A'));
      vi.spyOn(store, 'release').mockImplementation(() => {
        throw new Error('connection reset');
      });
      const relay = build({
        sink: {
          publish: () => {
            throw new Error('down');
          },
        },
      });

      expect(await relay.runOnce()).toMatchObject({ retried: 1, released: 0 });
      expect(logger.lines).toContain('ERROR Outbox store release failed: Error: connection reset');
      expect(store.stats(Date.now())).toMatchObject({ leased: 1 });
      expect(store.stats(Date.now() + 30_000)).toMatchObject({ leased: 0 });
    });

    it('keeps delivering when an event subscriber throws', async () => {
      add(message('a'), message('b'));
      const throwing = {
        emit() {
          throw new Error('metrics exporter down');
        },
      } as unknown as OutboxEvents;
      const relay = build({ sink: { publish: () => undefined } }, { events: throwing });

      expect(await relay.runOnce()).toMatchObject({ published: 2 });
      expect(store.stats(Date.now()).pending).toBe(0);
      expect(logger.lines.filter((l) => l === 'ERROR Outbox event subscriber threw: Error: metrics exporter down')).toHaveLength(2);
    });

    it('reads the store on every use, so a store settled after construction is the one used', async () => {
      let current: InMemoryOutboxStore | undefined;
      const relay = build({ sink: { publish: () => undefined } }, { store: () => current! });

      current = store;
      add(message('a'));
      expect(await relay.runOnce()).toMatchObject({ published: 1 });
    });
  });

  describe('stop', () => {
    it('waits for the publish in flight, releases the rest of its key, and claims nothing afterwards', async () => {
      add(message('k1', 'K'), message('k2', 'K'), message('k3', 'K'));
      const gate = deferred();
      const published: string[] = [];
      const relay = build({
        sink: {
          publish: async (m) => {
            await gate.promise;
            published.push(m.topic);
          },
        },
      });
      relay.start();
      await settle();

      let stopped = false;
      const stopping = relay.stop().then(() => (stopped = true));
      await settle();
      expect(stopped).toBe(false);

      gate.resolve();
      await stopping;
      expect(published).toEqual(['k1']);
      expect(store.stats(Date.now())).toMatchObject({ pending: 2, leased: 0, ready: 2 });

      const claim = vi.spyOn(store, 'claim');
      expect(await relay.runOnce()).toMatchObject({ claimed: 0 });
      expect(claim).not.toHaveBeenCalled();
    });

    it('waits for a runOnce() started outside the loop', async () => {
      add(message('a'));
      const gate = deferred();
      const relay = build({ sink: { publish: () => gate.promise } });

      const run = relay.runOnce();
      await settle();
      let stopped = false;
      const stopping = relay.stop().then(() => (stopped = true));
      await settle();
      expect(stopped).toBe(false);

      gate.resolve();
      await stopping;
      expect(await run).toMatchObject({ published: 1 });
    });
  });

  describe('stats', () => {
    it('reports the lag of the longest-waiting due message and nothing for a scheduled one', async () => {
      const relay = build({ sink: { publish: () => undefined } });
      expect(await relay.stats()).toEqual({
        pending: 0,
        ready: 0,
        leased: 0,
        deadLetters: 0,
        oldestDueAt: null,
        lagMs: 0,
        inFlight: 0,
      });

      add(message('now'));
      add(message('tomorrow', null, Date.now() + 86_400_000));
      await settle(2_500);
      expect(await relay.stats()).toMatchObject({ pending: 2, ready: 1, lagMs: 2_500 });
    });
  });
});
