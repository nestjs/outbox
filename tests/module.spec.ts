import { Inject, Injectable, Module, Scope } from '@nestjs/common';
import { REQUEST } from '@nestjs/core';
import { getDrizzleToken } from '@nestjs/drizzle';
import { Test, type TestingModule } from '@nestjs/testing';
import { DrizzleOutboxStore } from './fixtures/database/drizzle-outbox.store.js';
import {
  InMemoryOutboxStore,
  OUTBOX_MODULE_OPTIONS,
  OnOutboxMessage,
  Outbox,
  OutboxDeadLetters,
  OutboxModule,
  OutboxRelay,
  OutboxStorage,
  OutboxTransport,
  type OutboxMessage,
  type OutboxModuleAsyncOptions,
  type OutboxModuleOptions,
  type OutboxOptionsFactory,
} from '../lib/index.js';
import { inMemoryDatabase, pgliteDatabase, type DrizzleTestDatabase } from './databases.js';
import { registeredStore } from './helpers.js';

const CONFIG = Symbol('CONFIG');
const BROKER = Symbol('BROKER');

/** A transport class Nest instantiates, injecting its client. */
@Injectable()
class BrokerTransport extends OutboxTransport {
  constructor(@Inject(BROKER) private readonly sent: OutboxMessage[]) {
    super();
  }
  publish(message: OutboxMessage) {
    this.sent.push(message);
  }
}

@Injectable()
class EmailHandlers {
  readonly handled: string[] = [];

  @OnOutboxMessage('order.placed', { consumer: 'email' })
  send(payload: { orderId: number }) {
    this.handled.push(`email ${payload.orderId}`);
  }
}

@Module({ providers: [EmailHandlers] })
class EmailModule {}

describe('OutboxModule configuration', () => {
  /** The application's database: the tutorial's Drizzle schema on PGlite. */
  let db: DrizzleTestDatabase;
  let moduleRef: TestingModule | undefined;

  beforeAll(async () => {
    db = await pgliteDatabase();
  }, 60_000); // PGlite loads its WASM on first open: slow under a parallel run
  afterAll(() => db?.close());
  beforeEach(() => db.reset());
  afterEach(async () => {
    await moduleRef?.close();
    moduleRef = undefined;
  });

  @Module({
    providers: [
      // The Drizzle database, under the token the tutorial's DrizzleModule registers it with.
      { provide: getDrizzleToken(), useFactory: () => db.db },
      { provide: CONFIG, useValue: { relayEnabled: false } },
      { provide: BROKER, useFactory: () => [] }, // a fresh list per test
    ],
    exports: [getDrizzleToken(), CONFIG, BROKER],
  })
  class InfraModule {}

  /** The shape of an app's store: a provider that injects its database and registers itself. */
  @Module({ imports: [InfraModule], providers: [DrizzleOutboxStore] })
  class StoreModule {}

  const compile = async (...imports: any[]) => {
    const ref = await Test.createTestingModule({ imports }).compile();
    ref.useLogger(false);
    await ref.init();
    return (moduleRef = ref);
  };

  it('uses the registered store, and instantiates transport classes with DI next to transport instances', async () => {
    const audit: string[] = [];
    const ref = await compile(
      StoreModule,
      OutboxModule.forRoot({
        imports: [InfraModule],
        transports: {
          broker: BrokerTransport,
          audit: { publish: (message) => void audit.push(message.topic) },
        },
        route: (message) => (message.topic.startsWith('audit.') ? 'audit' : 'broker'),
        relay: { enabled: false },
      }),
    );

    expect(ref.get(OutboxStorage).messages).toBe(ref.get(DrizzleOutboxStore));

    const outbox = ref.get(Outbox);
    await db.transaction((tx) =>
      outbox.add(tx, [
        { topic: 'order.placed', payload: {} },
        { topic: 'audit.order.placed', payload: {} },
      ]),
    );

    expect(await ref.get(OutboxRelay).runOnce()).toMatchObject({ published: 2 });
    expect(ref.get<OutboxMessage[]>(BROKER).map((m) => m.topic)).toEqual(['order.placed']);
    expect(audit).toEqual(['audit.order.placed']);

    // The value options are what OUTBOX_MODULE_OPTIONS holds; the structure is not.
    expect(Object.keys(ref.get(OUTBOX_MODULE_OPTIONS)).sort()).toEqual(['relay', 'route']);
  });

  it('takes the structure at the top level of forRootAsync, and values from the factory', async () => {
    const ref = await compile(
      StoreModule,
      OutboxModule.forRootAsync({
        imports: [InfraModule],
        transports: { broker: BrokerTransport },
        inject: [CONFIG],
        useFactory: (config: { relayEnabled: boolean }) => ({ relay: { enabled: config.relayEnabled } }),
      }),
    );

    expect(ref.get(OutboxStorage).messages).toBeInstanceOf(DrizzleOutboxStore);
    expect(ref.get(OutboxRelay).running).toBe(false);
  });

  it('builds the options with a useClass factory, next to top-level classes', async () => {
    @Injectable()
    class OutboxConfig implements OutboxOptionsFactory {
      constructor(@Inject(CONFIG) private readonly config: { relayEnabled: boolean }) {}

      createOutboxOptions(): OutboxModuleOptions {
        return { relay: { enabled: this.config.relayEnabled } };
      }
    }

    // The type a library that wraps OutboxModule accepts and passes through.
    const options: OutboxModuleAsyncOptions = {
      imports: [InfraModule],
      transports: { broker: BrokerTransport },
      useClass: OutboxConfig,
    };
    const ref = await compile(StoreModule, OutboxModule.forRootAsync(options));

    expect(ref.get(OutboxRelay).running).toBe(false);

    const outbox = ref.get(Outbox);
    await db.transaction((tx) => outbox.add(tx, { topic: 'order.placed', payload: {} }));

    expect(await ref.get(OutboxRelay).runOnce()).toMatchObject({ published: 1 });
    expect(ref.get<OutboxMessage[]>(BROKER).map((m) => m.topic)).toEqual(['order.placed']);
  });

  it('takes transport instances from the async factory, next to top-level classes', async () => {
    const audit: string[] = [];
    const ref = await compile(
      StoreModule,
      OutboxModule.forRootAsync({
        imports: [InfraModule],
        transports: { broker: BrokerTransport },
        useFactory: () => ({
          transports: { audit: { publish: (message: OutboxMessage) => void audit.push(message.topic) } },
          route: (message) => (message.topic.startsWith('audit.') ? 'audit' : 'broker'),
          relay: { enabled: false },
        }),
      }),
    );

    const outbox = ref.get(Outbox);
    await db.transaction((tx) =>
      outbox.add(tx, [
        { topic: 'order.placed', payload: {} },
        { topic: 'audit.order.placed', payload: {} },
      ]),
    );

    expect(await ref.get(OutboxRelay).runOnce()).toMatchObject({ published: 2 });
    expect(ref.get<OutboxMessage[]>(BROKER).map((m) => m.topic)).toEqual(['order.placed']);
    expect(audit).toEqual(['audit.order.placed']);
  });

  it('refuses a `store` option, which registration replaced, and a reserved transport name', async () => {
    const store = new InMemoryOutboxStore();
    const hint = /`store` is not an option\. Implement OutboxStore and OutboxInboxStore in a provider that injects OutboxStorage/;

    expect(() => OutboxModule.forRoot({ store } as never)).toThrow(hint);
    expect(() => OutboxModule.forRootAsync({ store, useFactory: () => ({}) } as never)).toThrow(hint);
    await expect(compile(OutboxModule.forRootAsync({ useFactory: () => ({ store }) as never }))).rejects.toThrow(hint);

    expect(() => OutboxModule.forRoot({ transports: { local: BrokerTransport } })).toThrow(
      /"local" is the built-in in-process transport/,
    );
  });

  it('fails at startup when the async factory returns a transport class', async () => {
    await expect(
      compile(
        OutboxModule.forRootAsync({
          // @ts-expect-error: the factory returns instances; a transport class goes at the top level
          useFactory: () => ({ transports: { broker: BrokerTransport } }),
        }),
      ),
    ).rejects.toThrow(
      'OutboxModule: the forRootAsync() factory returned a class as `transports.broker` (BrokerTransport).',
    );

    await expect(
      compile(OutboxModule.forRootAsync({ useFactory: () => ({ transports: { broker: { path: 'x' } } }) as never })),
    ).rejects.toThrow('OutboxModule: `transports.broker` returned by the forRootAsync() factory must be an OutboxTransport instance');
  });

  it('fails at startup when a transport is set at the top level and in the async factory', async () => {
    await expect(
      compile(
        OutboxModule.forRootAsync({
          imports: [InfraModule],
          transports: { broker: BrokerTransport },
          useFactory: () => ({ transports: { broker: { publish() {} } } }),
        }),
      ),
    ).rejects.toThrow('OutboxModule: `transports.broker` is set both at the top level of forRootAsync()');

    await expect(
      compile(OutboxModule.forRootAsync({ useFactory: () => ({ transports: { local: { publish() {} } } }) })),
    ).rejects.toThrow(/"local" is the built-in in-process transport/);
  });

  it('requires `route` when handlers and a transport compete for messages', async () => {
    await expect(
      compile(
        OutboxModule.forRoot({ imports: [InfraModule], transports: { broker: BrokerTransport } }),
        EmailModule,
      ),
    ).rejects.toThrow(/set `route` to choose a transport for each message\. This app can publish to "local", "broker"/);

    await expect(
      compile(
        OutboxModule.forRoot({
          imports: [InfraModule],
          transports: { broker: BrokerTransport, audit: { publish() {} } },
        }),
      ),
    ).rejects.toThrow(/This app can publish to "broker", "audit"\./);
  });

  it('routes to the only destination when there is one', async () => {
    const memory = inMemoryDatabase();
    const ref = await compile(
      registeredStore(memory.store()),
      OutboxModule.forRoot({ relay: { enabled: false } }),
      EmailModule,
    );

    const outbox = ref.get(Outbox);
    await memory.transaction((tx) => outbox.add(tx, { topic: 'order.placed', payload: { orderId: 7 } }));
    await ref.get(OutboxRelay).runOnce();

    expect(ref.get(EmailHandlers).handled).toEqual(['email 7']);
  });

  it('requires a consumer name on every handler', () => {
    expect(() => OnOutboxMessage('order.placed', {} as { consumer: string })).toThrow(
      /@OnOutboxMessage\("order\.placed"\) needs \{ consumer \}/,
    );
  });

  it('refuses handlers on request-scoped providers instead of ignoring them', async () => {
    @Injectable({ scope: Scope.REQUEST })
    class ScopedHandlers {
      constructor(@Inject(REQUEST) readonly request: unknown) {}
      @OnOutboxMessage('order.placed', { consumer: 'scoped' })
      handle() {}
    }

    @Module({ providers: [ScopedHandlers] })
    class ScopedModule {}

    await expect(
      compile(OutboxModule.forRoot(), ScopedModule),
    ).rejects.toThrow(/ScopedHandlers\.handle: @OnOutboxMessage\(\) handlers must be on singleton providers/);
  });

  it('reads `retry: false` as a single attempt', async () => {
    const memory = inMemoryDatabase();
    const ref = await compile(
      registeredStore(memory.store()),
      OutboxModule.forRoot({
        transports: { broker: { publish: () => Promise.reject(new Error('broker down')) } },
        relay: { enabled: false },
        retry: false,
      }),
    );

    const outbox = ref.get(Outbox);
    await memory.transaction((tx) => outbox.add(tx, { topic: 'order.placed', payload: {} }));

    expect(await ref.get(OutboxRelay).runOnce()).toMatchObject({ deadLettered: 1 });
    expect(await ref.get(OutboxDeadLetters).list()).toEqual([
      expect.objectContaining({ reason: 'exhausted', attempts: 1, lastError: 'Error: broker down' }),
    ]);
  });
});
