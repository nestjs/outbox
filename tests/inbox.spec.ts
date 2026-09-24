import { Inject, Injectable, Module } from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';
import {
  OnOutboxMessage,
  OutboxInbox,
  OutboxModule,
  OutboxRelay,
  OutboxTransactionRequiredError,
  type OutboxHandlerContext,
  type OutboxMessage,
  type OutboxStorage,
  type OutboxInboxStore,
} from '../lib/index.js';
import { uuidv7 } from '../lib/utils/uuid.util.js';
import { inMemoryDatabase, pgliteDatabase, type TestDatabase, type TestStore } from './databases.js';
import { registeredStore, sleep, storageWith } from './helpers.js';

/**
 * On the in-memory store (synchronous; its own transactions stand in for the application's
 * database) and on the tutorial's DrizzleOutboxStore on PGlite (asynchronous, like any store
 * on a database server; the inbox record commits with the business rows).
 */
const targets: { name: string; sync: boolean; open(): Promise<TestDatabase> }[] = [
  { name: 'InMemoryOutboxStore', sync: true, open: async () => inMemoryDatabase() },
  { name: 'DrizzleOutboxStore on PGlite', sync: false, open: pgliteDatabase },
];

for (const target of targets) {
  describe(`OutboxInbox: ${target.name}`, () => {
    let db: TestDatabase;
    let store: TestStore;

    beforeAll(async () => {
      db = await target.open();
    }, 60_000); // PGlite loads its WASM on first open: slow under a parallel run
    afterAll(() => db?.close());
    beforeEach(async () => {
      await db.reset();
      store = db.store();
    });

    describe('process', () => {
      it('runs the work once when the same message arrives twice at once', async () => {
        // A broker redelivery (or the relay's retry of a publish that timed out) while the first
        // delivery is still running: both checked the inbox before either recorded the id.
        const inbox = new OutboxInbox(storageWith(store));
        let runs = 0;
        const ship = async () => {
          runs++;
          await sleep(20);
          return 'shipped';
        };

        const results = await Promise.all([
          inbox.process('shipping', 'm1', ship),
          inbox.process('shipping', 'm1', ship),
          inbox.process('billing', 'm1', ship), // another consumer: not held back
        ]);

        expect(runs).toBe(2);
        expect(results).toEqual([{ duplicate: false, result: 'shipped' }, { duplicate: true }, { duplicate: false, result: 'shipped' }]);
      });

      it('runs the work for a waiting delivery when the first one failed', async () => {
        const inbox = new OutboxInbox(storageWith(store));
        let runs = 0;
        const ship = async () => {
          if (++runs === 1) {
            await sleep(20);
            throw new Error('carrier API down');
          }
          return runs;
        };

        const [first, second] = await Promise.allSettled([
          inbox.process('shipping', 'm1', ship),
          inbox.process('shipping', 'm1', ship),
        ]);

        expect(first).toMatchObject({ status: 'rejected', reason: new Error('carrier API down') });
        expect(second).toEqual({ status: 'fulfilled', value: { duplicate: false, result: 2 } });
        expect(await store.hasInbox('shipping', 'm1')).toBe(true);
      });
    });

    describe('processInTransaction', () => {
      const invoices = () => db.count('invoices');

      if (target.sync) {
        it('is synchronous with a synchronous store: runs the work once, then reports duplicates', async () => {
          const inbox = new OutboxInbox(storageWith(store));
          const results: unknown[] = [];

          for (let i = 0; i < 2; i++) {
            await db.transaction((tx) => {
              const result = inbox.processInTransaction(tx, 'shipping', 'm1', () => (db.insertInvoice(tx, 1, 'shipping'), 'done'));
              expect(result).not.toBeInstanceOf(Promise); // nothing to await before the commit
              results.push(result);
            });
          }

          expect(results).toEqual([{ duplicate: false, result: 'done' }, { duplicate: true }]);
          expect(await invoices()).toBe(1);
        });
      }

      it('rolls the record back with the work: a failed attempt leaves nothing behind', async () => {
        const inbox = new OutboxInbox(storageWith(store));

        await expect(
          db.transaction((tx) =>
            inbox.processInTransaction(tx, 'shipping', 'm1', async () => {
              await db.insertInvoice(tx, 1, 'shipping');
              throw new Error('carrier API down');
            }),
          ),
        ).rejects.toThrow('carrier API down');
        expect(await store.hasInbox('shipping', 'm1')).toBe(false);
        expect(await invoices()).toBe(0);

        await db.transaction((tx) => inbox.processInTransaction(tx, 'shipping', 'm1', () => db.insertInvoice(tx, 1, 'shipping')));
        expect(await invoices()).toBe(1);
      });

      if (!target.sync) {
        it('dedupes with an asynchronous store', async () => {
          const inbox = new OutboxInbox(storageWith(store));
          const run = () => db.transaction((tx) => inbox.processInTransaction(tx, 'shipping', 'm1', () => db.insertInvoice(tx, 1, 'shipping')));

          expect(await run()).toMatchObject({ duplicate: false });
          expect(await run()).toEqual({ duplicate: true });
          expect(await invoices()).toBe(1);
        });
      }

      if (!target.sync) {
        it('dedupes with an asynchronous store even when the caller forgets to await', async () => {
          // The old `if (!ctx.markProcessed(tx))` returned a promise here, which is always
          // truthy, so the check never fired. The work now runs behind the check.
          const inbox = new OutboxInbox(storageWith(store));
          let runs = 0;

          const results = await db.transaction(async (tx) => {
            const work = () => (runs++, db.insertInvoice(tx, 1, 'shipping'));
            const pending = [
              inbox.processInTransaction(tx, 'shipping', 'm1', work),
              inbox.processInTransaction(tx, 'shipping', 'm1', work),
              inbox.processInTransaction(tx, 'shipping', 'm1', work),
            ];

            expect(pending.every((r) => r instanceof Promise)).toBe(true);
            expect(runs).toBe(0); // nothing ran before the store answered

            return Promise.all(pending);
          });

          expect(results.map((r) => r.duplicate)).toEqual([false, true, true]);
          expect(runs).toBe(1);
          expect(await invoices()).toBe(1);
        });
      }

      it('awaits asynchronous work before reporting the result', async () => {
        const inbox = new OutboxInbox(storageWith(store));
        const result = await db.transaction((tx) => inbox.processInTransaction(tx, 'shipping', 'm1', async () => 42));
        expect(result).toEqual({ duplicate: false, result: 42 });
      });

      it('refuses to run without a transaction: the record would not commit with the work', async () => {
        const inbox = new OutboxInbox(storageWith(store));
        let runs = 0;
        expect(() => inbox.processInTransaction(undefined, 'shipping', 'm1', () => runs++)).toThrow(
          OutboxTransactionRequiredError,
        );

        // The database itself, with no transaction open: a store that can tell refuses it too.
        if (db.notATransaction !== undefined) {
          await expect(async () => inbox.processInTransaction(db.notATransaction, 'shipping', 'm1', () => runs++)).rejects.toThrow(
            OutboxTransactionRequiredError,
          );
        }

        expect(runs).toBe(0);
        expect(await store.hasInbox('shipping', 'm1')).toBe(false);
      });

      it('prunes entries older than a duration', async () => {
        vi.useFakeTimers({ toFake: ['Date'], now: 10_000 });
        try {
          const inbox = new OutboxInbox(storageWith(store));
          await store.recordInbox(undefined, 'billing', 'old', 1_000);
          await store.recordInbox(undefined, 'billing', 'new', 9_500);

          expect(await inbox.prune('1s')).toBe(1); // everything processed before 9_000
          expect(await store.hasInbox('billing', 'old')).toBe(false);
          expect(await store.hasInbox('billing', 'new')).toBe(true);
        } finally {
          vi.useRealTimers();
        }
      });
    });
  });
}

describe('OutboxInbox.processInTransaction with a broken store', () => {
  it('rejects a store whose recordInbox does not answer with a boolean', () => {
    const broken = { recordInbox: () => 1 } as unknown as OutboxInboxStore;
    expect(() => new OutboxInbox({ inbox: broken } as OutboxStorage).processInTransaction({}, 'c', 'm', () => 0)).toThrow(
      /must return \(or resolve to\) a boolean/,
    );
  });
});

const APP_DB = Symbol('APP_DB');
const shippingCalls: number[] = [];
const billing = { failures: 0 };

@Injectable()
class Handlers {
  constructor(@Inject(APP_DB) private readonly db: TestDatabase) {}

  /** processInTransaction on an async store: the pattern from the README's database section. */
  @OnOutboxMessage('order.placed', { consumer: 'shipping' })
  async ship(_payload: unknown, ctx: OutboxHandlerContext) {
    await this.db.transaction(async (tx) => {
      await ctx.processInTransaction(tx, async () => {
        shippingCalls.push(ctx.attempt);
        await this.db.insertInvoice(tx, 1, 'shipping');
      });
    });
  }

  /** Fails the first delivery, so the message (and `ship`) is delivered twice. */
  @OnOutboxMessage('order.placed', { consumer: 'billing' })
  bill() {
    if (billing.failures-- > 0) {
      throw new Error('billing down');
    }
  }
}

describe('@OnOutboxMessage processInTransaction on an asynchronous store (e2e, DrizzleOutboxStore on PGlite)', () => {
  let db: TestDatabase;
  let store: TestStore;
  let moduleRef: TestingModule;

  beforeAll(async () => {
    db = await pgliteDatabase();
  }, 60_000); // PGlite loads its WASM on first open: slow under a parallel run
  afterAll(() => db?.close());

  beforeEach(async () => {
    await db.reset();
    store = db.store();
    shippingCalls.length = 0;
    billing.failures = 1;

    @Module({
      imports: [
        registeredStore(store),
        OutboxModule.forRoot({
          relay: { enabled: false },
          retry: { backoff: () => 0 },
        }),
      ],
      providers: [{ provide: APP_DB, useValue: db }, Handlers],
    })
    class AppModule {}

    moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    moduleRef.useLogger(false);
    await moduleRef.init();
  });
  afterEach(async () => {
    await moduleRef.close();
  });

  it('skips the work on redelivery', async () => {
    const message: OutboxMessage = {
      id: uuidv7(),
      topic: 'order.placed',
      payload: {},
      headers: {},
      key: null,
      createdAt: 0,
      availableAt: 0,
      attempts: 0,
      lastError: null,
    };

    await db.transaction((tx) => store.add(tx, [message]));
    const relay = moduleRef.get(OutboxRelay);

    expect(await relay.runOnce()).toMatchObject({ claimed: 1, retried: 1 }); // billing failed
    expect(await relay.runOnce()).toMatchObject({ claimed: 1, published: 1 });

    expect(shippingCalls).toEqual([1]); // skipped on attempt 2: its record committed with the invoice
    expect(await db.count('invoices')).toBe(1);
  });
});
