/**
 * OutboxModule as an application sees it: what it makes injectable where, what it checks
 * when defined and at startup, and the order things stop in on shutdown.
 */
import { subscribe, unsubscribe } from 'node:diagnostics_channel';
import { Injectable, Module } from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';
import {
  Outbox,
  OutboxDeadLetters,
  OutboxEvents,
  OutboxInbox,
  OutboxModule,
  OutboxRelay,
  OutboxStorage,
  type OutboxEvent,
} from '../lib/index.js';
import { message } from './helpers.js';

@Injectable()
class Checkout {
  constructor(
    readonly outbox: Outbox,
    readonly inbox: OutboxInbox,
    readonly deadLetters: OutboxDeadLetters,
    readonly events: OutboxEvents,
    readonly relay: OutboxRelay,
    readonly storage: OutboxStorage,
  ) {}
}

@Module({ providers: [Checkout] })
class CheckoutModule {}

describe('OutboxModule lifecycle', () => {
  let moduleRef: TestingModule | undefined;

  afterEach(async () => {
    await moduleRef?.close();
    moduleRef = undefined;
    vi.useRealTimers();
  });

  async function boot(...imports: any[]) {
    const ref = await Test.createTestingModule({ imports }).compile();
    ref.useLogger(false);
    await ref.init();
    return (moduleRef = ref);
  }

  it('is global by default: a feature module injects every service without importing it', async () => {
    const ref = await boot(OutboxModule.forRoot({ relay: { enabled: false } }), CheckoutModule);
    const checkout = ref.get(Checkout);

    expect(checkout.outbox).toBeInstanceOf(Outbox);
    expect(checkout.relay).toBe(ref.get(OutboxRelay));
    expect(checkout.storage.messages).toBe(checkout.storage.inbox);
  });

  it('with isGlobal: false, a feature module has to import it', async () => {
    await expect(
      Test.createTestingModule({
        imports: [OutboxModule.forRoot({ relay: { enabled: false }, isGlobal: false }), CheckoutModule],
      }).compile(),
    ).rejects.toThrow(/Nest can't resolve dependencies of the Checkout/);

    await expect(
      Test.createTestingModule({
        imports: [OutboxModule.forRootAsync({ isGlobal: false, useFactory: () => ({}) }), CheckoutModule],
      }).compile(),
    ).rejects.toThrow(/Nest can't resolve dependencies of the Checkout/);
  });

  it('refuses a transport that is neither a class nor an object with publish(), when defined', () => {
    for (const transport of [{ send() {} }, null, 'kafka']) {
      expect(() => OutboxModule.forRoot({ transports: { broker: transport as never } })).toThrow(
        'OutboxModule: transports.broker must be an OutboxTransport class or an object with publish()',
      );
    }
  });

  it('fails at startup on invalid options returned by the async factory, naming them', async () => {
    await expect(boot(OutboxModule.forRootAsync({ useFactory: () => ({ retry: { attempts: 0 } }) }))).rejects.toThrow(
      'OutboxModule: retry.attempts must be a whole number of at least 1 (got 0)',
    );
    await expect(
      boot(OutboxModule.forRootAsync({ useFactory: async () => ({ relay: { batchSize: 0 } }) })),
    ).rejects.toThrow('OutboxModule: relay.batchSize must be a whole number of at least 1 (got 0)');
  });

  it('runs the relay by default, and stops it before completing events$ on shutdown', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
    const ref = await boot(OutboxModule.forRoot({ transports: { sink: { publish: () => undefined } } }));
    const relay = ref.get(OutboxRelay);
    expect(relay.running).toBe(true);

    const seen: string[] = [];
    ref.get(OutboxEvents).events$.subscribe({
      next: (e) => seen.push(e.type),
      complete: () => seen.push(`complete (relay running: ${relay.running})`),
    });

    await ref.get(OutboxStorage).messages.add({}, [{ ...message('a'), availableAt: Date.now() }]);
    ref.get(Outbox).notify();
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(0);

    await ref.close();
    moduleRef = undefined;
    expect(seen).toEqual(['published', 'complete (relay running: false)']);
  });
});

describe('OutboxEvents', () => {
  it('publishes each event on its channel and to events$, and completes on shutdown', () => {
    const events = new OutboxEvents();
    const received: string[] = [];
    const listener = (event: unknown) => received.push(`channel ${(event as OutboxEvent).type}`);
    subscribe('nestjs:outbox:lease-lost', listener);

    try {
      let completed = false;
      events.events$.subscribe({ next: (e) => received.push(`events$ ${e.type}`), complete: () => (completed = true) });

      events.emit({ type: 'lease-lost', message: message('a') });
      events.emit({ type: 'published', message: message('b'), transport: 'local', durationMs: 1 });
      expect(received).toEqual(['channel lease-lost', 'events$ lease-lost', 'events$ published']);

      events.onApplicationShutdown();
      expect(completed).toBe(true);

      events.emit({ type: 'lease-lost', message: message('c') });
      expect(received).toHaveLength(4); // the channel still hears it; events$ is done
    } finally {
      unsubscribe('nestjs:outbox:lease-lost', listener);
    }
  });
});
