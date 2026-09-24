/**
 * OutboxStorage: how an application's stores get registered, by contract (`messages`,
 * `inbox`), and every rule that keeps the registration from going wrong silently.
 */
import { Inject, Injectable, Module, Scope, type LoggerService, type OnModuleInit } from '@nestjs/common';
import { REQUEST } from '@nestjs/core';
import { Test, type TestingModule } from '@nestjs/testing';
import {
  InMemoryOutboxStore,
  OnOutboxMessage,
  Outbox,
  OutboxInbox,
  OutboxModule,
  OutboxRelay,
  OutboxStorage,
  type OutboxInboxStore,
  type OutboxStorageSources,
} from '../lib/index.js';
import { LOCK_STORAGE } from '../lib/storage/outbox.storage.js';

type RootOptions = NonNullable<Parameters<typeof OutboxModule.forRoot>[0]>;

/** An application's store: an in-memory store under another name, registering itself for both contracts. */
@Injectable()
class AppOutboxStore extends InMemoryOutboxStore {
  constructor(storage: OutboxStorage) {
    super();
    storage.registerSource({ messages: this, inbox: this });
  }
}

@Injectable()
class OtherOutboxStore extends InMemoryOutboxStore {
  constructor(storage: OutboxStorage) {
    super();
    storage.registerSource({ messages: this, inbox: this });
  }
}

/** A consumer-only service's store: the three inbox methods, nothing else. */
@Injectable()
class InboxStore implements OutboxInboxStore {
  readonly records = new Map<string, number>();
  constructor(storage: OutboxStorage) {
    storage.registerSource({ inbox: this });
  }
  recordInbox(_tx: unknown, consumer: string, messageId: string, now: number) {
    const key = `${consumer}/${messageId}`;
    if (this.records.has(key)) {
      return false;
    }
    this.records.set(key, now);
    return true;
  }
  hasInbox(consumer: string, messageId: string) {
    return this.records.has(`${consumer}/${messageId}`);
  }
  pruneInbox() {
    return 0;
  }
}

/** A producer's store for the messages alone. */
@Injectable()
class MessagesStore extends InMemoryOutboxStore {
  constructor(storage: OutboxStorage) {
    super();
    storage.registerSource({ messages: this });
  }
}

@Injectable()
class EmailHandler {
  @OnOutboxMessage('order.placed', { consumer: 'email' })
  send() {}
}

@Injectable()
class NoInboxHandler {
  @OnOutboxMessage('order.placed', { consumer: 'metrics', inbox: false })
  count() {}
}

class Lines implements LoggerService {
  readonly lines: string[] = [];
  log(message: string, context?: string) {
    this.lines.push(`[${context}] ${message}`);
  }
  error(message: string) {
    this.lines.push(`ERROR ${message}`);
  }
  warn(message: string) {
    this.lines.push(`WARN ${message}`);
  }
}

describe('OutboxStorage', () => {
  let moduleRef: TestingModule | undefined;
  const env = process.env.NODE_ENV;

  afterEach(async () => {
    process.env.NODE_ENV = env;
    await moduleRef?.close();
    moduleRef = undefined;
  });

  async function start(providers: any[], options: RootOptions = {}, logger: LoggerService | false = false) {
    const ref = await Test.createTestingModule({
      imports: [OutboxModule.forRoot({ relay: { enabled: false }, ...options })],
      providers,
    }).compile();

    ref.useLogger(logger);
    await ref.init(); // a failed init leaves nothing to close (close() would rethrow its error)
    return (moduleRef = ref);
  }

  async function stop() {
    await moduleRef?.close();
    moduleRef = undefined;
  }

  describe('registration by contract', () => {
    it('uses the store a provider registered for both contracts in its constructor', async () => {
      const ref = await start([AppOutboxStore]);
      const store = ref.get(AppOutboxStore);
      expect(ref.get(OutboxStorage).messages).toBe(store);
      expect(ref.get(OutboxStorage).inbox).toBe(store);

      const outbox = ref.get(Outbox);
      await store.transaction((tx) => outbox.add(tx, { topic: 'order.placed', payload: {} }));
      expect(await ref.get(OutboxRelay).stats()).toMatchObject({ pending: 1 });

      await ref.get(OutboxInbox).process('billing', 'm1', () => undefined);
      expect(store.hasInbox('billing', 'm1')).toBe(true);
    });

    it('runs in memory when nothing is registered: one in-memory store for both contracts', async () => {
      const ref = await start([]);
      const storage = ref.get(OutboxStorage);
      expect(storage.messages).toBeInstanceOf(InMemoryOutboxStore);
      expect(storage.inbox).toBe(storage.messages);
      expect(await ref.get(OutboxRelay).stats()).toMatchObject({ pending: 0 });
    });

    it('takes the inbox alone from a consumer-only service, which implements three methods', async () => {
      const ref = await start([InboxStore]);
      const inbox = ref.get(OutboxInbox);
      expect(await inbox.process('analytics', 'm1', () => 'first')).toEqual({ duplicate: false, result: 'first' });
      expect(await inbox.process('analytics', 'm1', () => 'again')).toEqual({ duplicate: true });

      expect(ref.get(InboxStore).records.has('analytics/m1')).toBe(true);
      expect(ref.get(OutboxStorage).inbox).toBe(ref.get(InboxStore));
      expect(ref.get(OutboxStorage).messages).toBeInstanceOf(InMemoryOutboxStore);
    });

    it('takes each contract from a different provider', async () => {
      const ref = await start([MessagesStore, InboxStore]);
      expect(ref.get(OutboxStorage).messages).toBe(ref.get(MessagesStore));
      expect(ref.get(OutboxStorage).inbox).toBe(ref.get(InboxStore));
    });

    it('checks the shape at once, per contract, naming the missing methods', () => {
      const storage = new OutboxStorage();
      class HalfStore {
        add() {}
        claim() {}
      }

      expect(() => storage.registerSource({ messages: new HalfStore() } as unknown as OutboxStorageSources)).toThrow(
        "OutboxStorage.registerSource(): HalfStore doesn't implement OutboxStore for `messages`: markPublished(), " +
          'reschedule(), deadLetter(), release(), stats(), listDeadLetters(), getDeadLetter(), requeueDeadLetters(), ' +
          'purgeDeadLetters() are missing.',
      );
      expect(() =>
        storage.registerSource({ inbox: { recordInbox() {}, hasInbox() {} } } as unknown as OutboxStorageSources),
      ).toThrow("OutboxStorage.registerSource(): an object doesn't implement OutboxInboxStore for `inbox`: pruneInbox() is missing.");
      expect(() => storage.registerSource({ inbox: undefined } as unknown as OutboxStorageSources)).toThrow(
        'OutboxStorage.registerSource(): expected an object implementing OutboxInboxStore as `inbox`, got undefined.',
      );
      expect(() => storage.registerSource({ messages: InMemoryOutboxStore } as unknown as OutboxStorageSources)).toThrow(
        'got the class InMemoryOutboxStore (pass an instance).',
      );

      // A call with one refused store registers nothing, the valid stores of that call included.
      expect(() =>
        storage.registerSource({ messages: new InMemoryOutboxStore(), inbox: {} } as unknown as OutboxStorageSources),
      ).toThrow('OutboxInboxStore');
      storage.registerSource({ messages: new InMemoryOutboxStore() });
    });

    it('takes the contracts by name: a store passed as itself says which call it meant', () => {
      const storage = new OutboxStorage();
      expect(() => storage.registerSource(new InMemoryOutboxStore() as unknown as OutboxStorageSources)).toThrow(
        'OutboxStorage.registerSource(): the contracts go by name, and InMemoryOutboxStore was passed as itself. ' +
          'Name what it implements: `registerSource({ messages: this, inbox: this })`.',
      );

      const inboxOnly = { recordInbox() {}, hasInbox() {}, pruneInbox() {} };
      expect(() => storage.registerSource(inboxOnly as unknown as OutboxStorageSources)).toThrow(
        'Name what it implements: `registerSource({ inbox: this })`.',
      );

      expect(() => storage.registerSource({})).toThrow(
        'OutboxStorage.registerSource(): the contracts go by name, and got none. Pass the stores a provider ' +
          'implements: `registerSource({ messages: this, inbox: this })`. The contracts are messages (OutboxStore), ' +
          'inbox (OutboxInboxStore).',
      );
      expect(() => storage.registerSource({ message: new InMemoryOutboxStore() } as unknown as OutboxStorageSources)).toThrow(
        'OutboxStorage.registerSource(): unknown contract `message`. The contracts are messages (OutboxStore), inbox (OutboxInboxStore).',
      );
      expect(() => storage.registerSource(undefined as unknown as OutboxStorageSources)).toThrow(
        'OutboxStorage.registerSource(): the contracts go by name, and got undefined.',
      );
    });

    it('refuses a second source for a contract, naming both, unless it replaces the first on purpose', async () => {
      await expect(start([AppOutboxStore, OtherOutboxStore])).rejects.toThrow(
        "OutboxStorage.registerSource(): OtherOutboxStore can't register `messages`, AppOutboxStore already did. " +
          'Register each contract once, or pass { replace: true } to replace it on purpose (tests, wrappers).',
      );
      await expect(start([InboxStore, AppOutboxStore])).rejects.toThrow(
        "OutboxStorage.registerSource(): AppOutboxStore can't register `inbox`, InboxStore already did.",
      );

      const storage = new OutboxStorage();
      const first = new InMemoryOutboxStore();
      const second = new InMemoryOutboxStore();
      storage.registerSource({ messages: first, inbox: first });

      expect(() => storage.registerSource({ inbox: first })).toThrow(
        "OutboxStorage.registerSource(): InMemoryOutboxStore can't register `inbox`, it already did (the same instance, twice).",
      );

      storage.registerSource({ inbox: second }, { replace: true });
      Object.assign(storage, { logger: { log() {} } });
      expect(storage.messages).toBe(first);
      expect(storage.inbox).toBe(second);
    });
  });

  describe('the lock', () => {
    it("locks in OutboxModule's onModuleInit: a later registration throws", async () => {
      const ref = await start([AppOutboxStore]);
      expect(() => ref.get(OutboxStorage).registerSource({ messages: new InMemoryOutboxStore() }, { replace: true })).toThrow(
        'OutboxStorage.registerSource(): InMemoryOutboxStore (messages) registered after OutboxModule initialized ' +
          '(or after its storage was first read), which already uses AppOutboxStore. Register from the constructor ' +
          'of a singleton provider: providers of lazy-loaded modules, request-scoped and transient providers, and ' +
          'lifecycle hooks run too late.',
      );
    });

    it('is internal: no public lock method, and locking again changes nothing', async () => {
      const ref = await start([AppOutboxStore]);
      const storage = ref.get(OutboxStorage);
      expect(Object.getOwnPropertyNames(OutboxStorage.prototype)).not.toContain('lock');
      expect('lock' in storage).toBe(false);

      storage[LOCK_STORAGE]();
      expect(storage.messages).toBe(ref.get(AppOutboxStore));
    });

    it('refuses a request-scoped source: it is created per request, after the lock', async () => {
      @Injectable({ scope: Scope.REQUEST })
      class RequestScopedStore extends InMemoryOutboxStore {
        constructor(@Inject(REQUEST) readonly request: unknown, storage: OutboxStorage) {
          super();
          storage.registerSource({ messages: this, inbox: this });
        }
      }

      const ref = await start([RequestScopedStore]);
      expect(ref.get(OutboxStorage).messages).toBeInstanceOf(InMemoryOutboxStore);
      await expect(ref.resolve(RequestScopedStore)).rejects.toThrow(
        /RequestScopedStore \(messages, inbox\) registered after OutboxModule initialized .* which already uses InMemoryOutboxStore \(the default/,
      );
    });

    it.each([
      ['the messages store (the relay)', (ref: TestingModule) => ref.get(OutboxRelay).stats().then((s) => `pending ${s.pending}`)],
      ['the inbox store', (ref: TestingModule) => ref.get(OutboxInbox).process('early', 'm1', () => 'ran').then((r) => `duplicate ${r.duplicate}`)],
    ])('locks at the first read of %s when that comes before its onModuleInit', async (_, use) => {
      // A provider of a module initialized before OutboxModule's hook uses the outbox.
      const events: string[] = [];
      let ref!: TestingModule;

      @Injectable()
      class EarlyUser implements OnModuleInit {
        async onModuleInit() {
          events.push(await use(ref));
          expect(() => ref.get(OutboxStorage).registerSource({ messages: new InMemoryOutboxStore() }, { replace: true })).toThrow(
            'registered after OutboxModule initialized (or after its storage was first read)',
          );
        }
      }
      @Module({ providers: [EarlyUser] })
      class EarlyModule {}
      @Module({ imports: [EarlyModule] })
      class FeatureModule {}

      ref = moduleRef = await Test.createTestingModule({
        imports: [OutboxModule.forRoot({ relay: { enabled: false } }), FeatureModule],
        providers: [AppOutboxStore],
      }).compile();
      ref.useLogger(false);
      await ref.init();

      expect(events).toHaveLength(1);
      expect(ref.get(OutboxStorage).messages).toBe(ref.get(AppOutboxStore));
      expect(ref.get(OutboxStorage).inbox).toBe(ref.get(AppOutboxStore));
    });

    it('logs the active sources when it locks', async () => {
      const cases: [any[], RootOptions, string][] = [
        [[AppOutboxStore], {}, 'OutboxStorage: AppOutboxStore'],
        [[], {}, 'OutboxStorage: InMemoryOutboxStore (the default: state is lost on restart and not shared between instances)'],
        // Consumer-only: the messages store isn't used, so it isn't listed.
        [[InboxStore], {}, 'OutboxStorage: InboxStore (inbox)'],
        [[InboxStore], { relay: { enabled: true, pollInterval: '1h' } }, 'OutboxStorage: InboxStore (inbox); in-memory (messages)'],
        [[MessagesStore, InboxStore], {}, 'OutboxStorage: MessagesStore (messages); InboxStore (inbox)'],
      ];

      for (const [providers, options, line] of cases) {
        const logs = new Lines();
        await start(providers, options, logs);
        expect(logs.lines).toContain(`[OutboxModule] ${line}`);
        expect(logs.lines.filter((l) => l.includes('OutboxStorage:'))).toHaveLength(1);
        await stop();
      }
    });
  });

  describe('production guard', () => {
    beforeEach(() => (process.env.NODE_ENV = 'production'));

    it('fails at startup with no source, naming the interfaces and how to register them', async () => {
      await expect(start([], { relay: { enabled: true } })).rejects.toThrow(
        'OutboxStorage: no store is registered for `messages` (OutboxStore) and `inbox` (OutboxInboxStore), and ' +
          'NODE_ENV is "production": in memory, messages and inbox records would be lost on restart ' +
          'and not shared between instances. Implement OutboxStore and OutboxInboxStore in a provider that injects ' +
          'OutboxStorage and calls `storage.registerSource({ messages: this, inbox: this })` in its constructor, or ' +
          'set `allowInMemoryStorage: true` in the OutboxModule options to run in memory anyway.',
      );

      const registered = await start([AppOutboxStore], { relay: { enabled: true, pollInterval: '1h' } });
      expect(registered.get(OutboxStorage).messages).toBeInstanceOf(AppOutboxStore);
    });

    it('requires the messages store where the app publishes or relays, and no inbox without inbox handlers', async () => {
      const transports = { broker: { publish: () => undefined } };

      // An API-only instance: it publishes (a transport), and the relay runs elsewhere.
      await expect(start([], { transports })).rejects.toThrow('no store is registered for `messages` (OutboxStore), and');
      await expect(start([InboxStore], { transports })).rejects.toThrow('for `messages` (OutboxStore), and');
      await start([MessagesStore], { transports });
      await stop();

      // The relay runs: it reads the messages store, whatever the app publishes to.
      await expect(start([InboxStore], { relay: { enabled: true } })).rejects.toThrow('for `messages` (OutboxStore), and');

      // A handler that keeps no inbox needs none.
      await start([MessagesStore, NoInboxHandler]);
    });

    it('requires the inbox where a handler keeps it, or where the app only consumes', async () => {
      await expect(start([MessagesStore, EmailHandler])).rejects.toThrow(
        'OutboxStorage: no store is registered for `inbox` (OutboxInboxStore), and NODE_ENV is "production": in ' +
          'memory, inbox records would be lost on restart and not shared between instances. Implement ' +
          'OutboxInboxStore in a provider that injects OutboxStorage and calls `storage.registerSource({ inbox: this })`',
      );

      // Consumer-only (the relay off, nowhere to publish): the inbox, and only the inbox.
      await expect(start([])).rejects.toThrow('no store is registered for `inbox` (OutboxInboxStore), and');
      const consumer = await start([InboxStore]);
      expect(consumer.get(OutboxStorage).inbox).toBe(consumer.get(InboxStore));
    });

    it("fails at the first read of a store the configuration didn't show, instead of running in memory", async () => {
      const consumer = await start([InboxStore]);
      expect(() => consumer.get(OutboxStorage).messages).toThrow(
        'OutboxStorage: no store is registered for `messages` (OutboxStore), and NODE_ENV is "production"',
      );
      await expect(consumer.get(OutboxRelay).stats()).rejects.toThrow('for `messages` (OutboxStore)');
      await stop();

      // A producer that also calls OutboxInbox directly, for messages from another service.
      const producer = await start([MessagesStore], { transports: { broker: { publish: () => undefined } } });
      await expect(producer.get(OutboxInbox).process('analytics', 'm1', () => undefined)).rejects.toThrow(
        'no store is registered for `inbox` (OutboxInboxStore)',
      );
    });

    it('runs in memory with allowInMemoryStorage, from forRoot or the async factory', async () => {
      const ref = await start([EmailHandler], { allowInMemoryStorage: true, relay: { enabled: true, pollInterval: '1h' } });
      expect(ref.get(OutboxStorage).messages).toBeInstanceOf(InMemoryOutboxStore);
      expect(ref.get(OutboxStorage).inbox).toBeInstanceOf(InMemoryOutboxStore);
      await stop();

      moduleRef = await Test.createTestingModule({
        imports: [OutboxModule.forRootAsync({ useFactory: () => ({ allowInMemoryStorage: true, relay: { enabled: false } }) })],
      }).compile();
      moduleRef.useLogger(false);
      await moduleRef.init();
      expect(moduleRef.get(OutboxStorage).messages).toBeInstanceOf(InMemoryOutboxStore);
    });

    it('refuses a non-boolean allowInMemoryStorage instead of reading a string as consent', async () => {
      // `allowInMemoryStorage: process.env.ALLOW_IN_MEMORY_OUTBOX` with the variable set to
      // "false" is truthy: the guard was bypassed, and production ran in memory.
      await expect(start([], { allowInMemoryStorage: 'false' as unknown as boolean })).rejects.toThrow(
        'OutboxModule: allowInMemoryStorage must be a boolean (got the string "false")',
      );
    });
  });

  describe('in tests', () => {
    it('lets a test swap the store: an overridden provider does not register, so the default applies', async () => {
      const ref = await Test.createTestingModule({
        imports: [OutboxModule.forRoot({ relay: { enabled: false } })],
        providers: [AppOutboxStore],
      })
        .overrideProvider(AppOutboxStore)
        .useValue(new InMemoryOutboxStore())
        .compile();

      moduleRef = ref;
      ref.useLogger(false);
      await ref.init();

      expect(ref.get(OutboxStorage).messages).toBeInstanceOf(InMemoryOutboxStore);
      expect(ref.get(OutboxStorage).messages).not.toBe(ref.get(AppOutboxStore));
    });

    it("lets a test register a fake before init(), replacing the app's source", async () => {
      const fake = new InMemoryOutboxStore();
      const ref = await Test.createTestingModule({
        imports: [OutboxModule.forRoot({ relay: { enabled: false } })],
        providers: [AppOutboxStore],
      }).compile();

      moduleRef = ref;
      ref.useLogger(false);
      ref.get(OutboxStorage).registerSource({ messages: fake, inbox: fake }, { replace: true });
      await ref.init();

      expect(ref.get(OutboxStorage).messages).toBe(fake);
      expect(ref.get(OutboxStorage).inbox).toBe(fake);
    });
  });
});

describe('OutboxStorage built without Nest', () => {
  const env = process.env.NODE_ENV;
  afterEach(() => {
    process.env.NODE_ENV = env;
  });

  const quiet = (storage: OutboxStorage): OutboxStorage => {
    Object.assign(storage, { logger: new Lines() });
    return storage;
  };

  it('counts both contracts as used, so the production guard asks for both', () => {
    process.env.NODE_ENV = 'production';
    const storage = quiet(new OutboxStorage());
    storage.registerSource({ messages: new InMemoryOutboxStore() });

    expect(() => storage.messages).toThrow('no store is registered for `inbox` (OutboxInboxStore)');
  });

  it('stays open after the production guard refused to lock: a late registration still counts', () => {
    process.env.NODE_ENV = 'production';
    const storage = quiet(new OutboxStorage());
    expect(() => storage.inbox).toThrow(/for `messages` \(OutboxStore\) and `inbox` \(OutboxInboxStore\)/);

    const store = new InMemoryOutboxStore();
    storage.registerSource({ messages: store, inbox: store });
    expect(storage.inbox).toBe(store);
    expect(() => storage.registerSource({ messages: store }, { replace: true })).toThrow(/registered after/);
  });

  it('shares one in-memory default between the contracts no source registered', () => {
    const storage = quiet(new OutboxStorage());
    expect(storage.messages).toBeInstanceOf(InMemoryOutboxStore);
    expect(storage.inbox).toBe(storage.messages);
  });

  it('runs the unregistered contract in memory next to a registered one outside production', () => {
    const storage = quiet(new OutboxStorage());
    const inbox = { recordInbox: () => true, hasInbox: () => false, pruneInbox: () => 0 };
    storage.registerSource({ inbox });

    expect(storage.inbox).toBe(inbox);
    expect(storage.messages).toBeInstanceOf(InMemoryOutboxStore);
  });
});
