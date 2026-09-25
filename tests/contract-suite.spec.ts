/**
 * `@nestjs/outbox/testing`: the store contract suites themselves. They run against the
 * in-memory store with every option, manage each case's harness, and catch stores that
 * break the rule a case names.
 */
import { AssertionError } from 'node:assert';
import {
  InMemoryOutboxStore,
  type OutboxClaimRequest,
  type OutboxDeadLetterFilter,
  type OutboxDeadLetterQuery,
  type OutboxMessage,
} from '../lib/index.js';
import {
  outboxInboxStoreContract,
  outboxStoreContract,
  type OutboxStoreContractCase,
  type OutboxStoreHarness,
} from '../lib/testing/index.js';

const harnessOf = (store: InMemoryOutboxStore, extra: Pick<OutboxStoreHarness, 'notATransaction' | 'close'> = {}) => ({
  store,
  transaction: <T>(work: (tx: unknown) => Promise<T>) => store.transaction(work),
  ...extra,
});

const names = (cases: OutboxStoreContractCase[]) => cases.map((c) => c.name);

describe('the store contract suites', () => {
  describe('InMemoryOutboxStore, serial cases only, refusing null as a transaction', () => {
    const harness = () => harnessOf(new InMemoryOutboxStore(), { notATransaction: null });

    describe('OutboxStore', () => {
      for (const c of outboxStoreContract(harness)) {
        it(c.name, c.run);
      }
    });

    describe('OutboxInboxStore', () => {
      for (const c of outboxInboxStoreContract(harness)) {
        it(c.name, c.run);
      }
    });
  });

  describe('cases', () => {
    it('adds the concurrency cases only with `concurrent`, after the serial ones, under unique names', () => {
      const create = () => harnessOf(new InMemoryOutboxStore());

      for (const contract of [outboxStoreContract, outboxInboxStoreContract]) {
        const serial = names(contract(create));
        const all = names(contract(create, { concurrent: true }));

        expect(all.slice(0, serial.length)).toEqual(serial);
        expect(all.length).toBeGreaterThan(serial.length);
        expect(new Set(all).size).toBe(all.length);
        expect(names(contract(create, { concurrent: false }))).toEqual(serial);
      }

      expect(names(outboxStoreContract(create, { concurrent: true }))).toContain(
        'never hands a message to two relays, nor a key to two relays at once',
      );
      expect(names(outboxInboxStoreContract(create, { concurrent: true }))).toContain(
        'makes two deliveries of a message meet at the inbox key',
      );
    });

    it('creates a harness per case, lazily, and closes it whether the case passes or fails', async () => {
      const lifecycle: string[] = [];
      let created = 0;
      const create = async () => {
        const n = ++created;
        lifecycle.push(`open ${n}`);
        return harnessOf(new InMemoryOutboxStore(), { close: async () => void lifecycle.push(`close ${n}`) });
      };

      const cases = outboxStoreContract(create);
      expect(created).toBe(0);

      await cases.find((c) => c.name === 'add() of an empty batch writes nothing')!.run();

      const failing = outboxStoreContract(async () => {
        const harness = await create();
        harness.store.stats = () => ({ pending: 1, ready: 0, leased: 0, deadLetters: 0, oldestDueAt: null });
        return harness;
      });
      await expect(failing.find((c) => c.name === 'add() of an empty batch writes nothing')!.run()).rejects.toThrow(
        AssertionError,
      );

      expect(lifecycle).toEqual(['open 1', 'close 1', 'open 2', 'close 2']);
    });

    it('skips the refusal case when the harness has no handle to refuse', async () => {
      const store = new InMemoryOutboxStore();
      const add = vi.spyOn(store, 'add');
      const refusal = outboxStoreContract(() => harnessOf(store)).find(
        (c) => c.name === 'refuses a handle that is not a transaction',
      )!;

      await refusal.run();
      expect(add).not.toHaveBeenCalled();
    });
  });

  describe('catches a store that breaks the contract', () => {
    beforeAll(() => {
      // The non-transactional store below writes through handles it doesn't own.
      vi.spyOn(InMemoryOutboxStore['logger'], 'warn').mockImplementation(() => {});
    });
    afterAll(() => vi.restoreAllMocks());

    /** Writes whoever asks: the owner of the latest claim stands in for the caller's. */
    class UnfencedStore extends InMemoryOutboxStore {
      private readonly owners = new Map<string, string>();
      override claim(request: OutboxClaimRequest) {
        const batch = super.claim(request);
        for (const m of batch) {
          this.owners.set(m.id, request.owner);
        }
        return batch;
      }
      override markPublished(id: string) {
        return super.markPublished(id, this.owners.get(id) ?? '');
      }
    }

    /** Claims as if no message had a key. */
    class KeyBlindStore extends InMemoryOutboxStore {
      private readonly keys = new Map<string, string | null>();
      override add(tx: unknown, messages: readonly OutboxMessage[]) {
        for (const m of messages) {
          this.keys.set(m.id, m.key);
        }
        super.add(tx, messages.map((m) => ({ ...m, key: null })));
      }
      override claim(request: OutboxClaimRequest) {
        return super.claim(request).map((m) => ({ ...m, key: this.keys.get(m.id) ?? null }));
      }
    }

    /** Ignores the caller's transaction and writes at once. */
    class AutocommitStore extends InMemoryOutboxStore {
      override add(_tx: unknown, messages: readonly OutboxMessage[]) {
        super.add({ foreign: true }, messages);
      }
    }

    /** Lists dead letters oldest first. */
    class OldestFirstStore extends InMemoryOutboxStore {
      override listDeadLetters(query: OutboxDeadLetterQuery) {
        return super.listDeadLetters({ ...query, limit: Number.MAX_SAFE_INTEGER, offset: 0 })
          .reverse()
          .slice(query.offset ?? 0, (query.offset ?? 0) + (query.limit ?? 50));
      }
    }

    /** Purges everything when given an empty filter. */
    class PurgeAllStore extends InMemoryOutboxStore {
      override purgeDeadLetters(filter: OutboxDeadLetterFilter) {
        return super.purgeDeadLetters(Object.keys(filter).length === 0 ? { all: true } : filter);
      }
    }

    /** Keeps the lease on a released message. */
    class StickyLeaseStore extends InMemoryOutboxStore {
      override release(ids: readonly string[]) {
        return ids.length;
      }
    }

    /** Keeps only the last failed attempt of a dead letter. */
    class LastAttemptStore extends InMemoryOutboxStore {
      override listDeadLetters(query: OutboxDeadLetterQuery) {
        return super.listDeadLetters(query).map((d) => ({ ...d, history: d.history.slice(-1) }));
      }
      override getDeadLetter(id: string) {
        const dead = super.getDeadLetter(id);
        return dead && { ...dead, history: dead.history.slice(-1) };
      }
    }

    /** Treats every delivery as the first. */
    class ForgetfulInbox extends InMemoryOutboxStore {
      override recordInbox(tx: unknown, consumer: string, messageId: string, now: number) {
        void super.recordInbox(tx, consumer, messageId, now);
        return true;
      }
    }

    const breaks: [string, () => InMemoryOutboxStore, 'store' | 'inbox', string][] = [
      ['ignores the lease owner', () => new UnfencedStore(), 'store', 'fences every write by the claim owner'],
      [
        'ignores keys when claiming',
        () => new KeyBlindStore(),
        'store',
        'claims a contiguous run per key and never skips ahead of a blocked message',
      ],
      [
        "writes outside the caller's transaction",
        () => new AutocommitStore(),
        'store',
        "add() writes through the caller's transaction: a rollback leaves nothing behind",
      ],
      [
        'lists dead letters oldest first',
        () => new OldestFirstStore(),
        'store',
        'requeues a dead letter with its failure history, and lists by key, newest first, in pages',
      ],
      [
        'accepts an empty purge filter',
        () => new PurgeAllStore(),
        'store',
        'requeues and purges dead letters by id or filter, and refuses an empty filter',
      ],
      [
        'keeps released leases',
        () => new StickyLeaseStore(),
        'store',
        'releases leases so the messages are claimable again at once',
      ],
      [
        'keeps only the last failed attempt',
        () => new LastAttemptStore(),
        'store',
        'keeps a key blocked while its head waits to retry, and unblocks it on dead-letter',
      ],
      [
        'records a consumer twice',
        () => new ForgetfulInbox(),
        'inbox',
        'records inbox entries once per consumer, inside a transaction when given one',
      ],
    ];

    it.each(breaks)('a store that %s fails "%s"', async (_, create, contract, name) => {
      const harness = () => harnessOf(create());
      const cases = contract === 'store' ? outboxStoreContract(harness) : outboxInboxStoreContract(harness);
      const target = cases.find((c) => c.name === name);

      expect(target, name).toBeDefined();
      await expect(target!.run()).rejects.toThrow(AssertionError);
    });
  });
});
