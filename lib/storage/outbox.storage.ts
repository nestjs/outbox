import { Inject, Injectable, Logger, Optional } from '@nestjs/common';
import { describeValue } from '../utils/backoff.util.js';
import type { OutboxModuleOptions } from '../interfaces/outbox-module-options.interface.js';
import { OUTBOX_MODULE_OPTIONS } from '../outbox.module-definition.js';
import type { OutboxStore } from '../interfaces/outbox-store.interface.js';
import { OUTBOX_INBOX_STORE_METHODS, OUTBOX_STORE_METHODS } from './outbox-storage.constants.js';
import type { OutboxInboxStore } from '../interfaces/outbox-inbox-store.interface.js';
import { InMemoryOutboxStore } from '../stores/in-memory-outbox.store.js';
import type {
  OutboxStorageSources,
  OutboxStorageContract,
  OutboxStorageRegisterOptions,
  OutboxStorageUsage,
} from '../interfaces/outbox-storage.interface.js';

/** Internal: locks the registry. `OutboxModule.onModuleInit()` and the first read call it. */
export const LOCK_STORAGE = Symbol('OutboxStorage.lock');

/**
 * Internal: what the application uses, for the production guard. `OutboxModule` sets it to
 * a function of its configuration (see {@link OutboxStorage}).
 */
export const STORAGE_USAGE = Symbol('OutboxStorage.usage');

interface ContractSpec {
  interfaceName: string;
  /** What in-memory storage loses, for the production guard. */
  holds: string;
  methods: readonly string[];
}

const CONTRACTS: Record<OutboxStorageContract, ContractSpec> = {
  messages: { interfaceName: 'OutboxStore', holds: 'messages', methods: OUTBOX_STORE_METHODS },
  inbox: { interfaceName: 'OutboxInboxStore', holds: 'inbox records', methods: OUTBOX_INBOX_STORE_METHODS },
};
const CONTRACT_NAMES = Object.keys(CONTRACTS) as OutboxStorageContract[];
const KNOWN = CONTRACT_NAMES.map((name) => `${name} (${CONTRACTS[name].interfaceName})`).join(', ');
const DEFAULT = 'InMemoryOutboxStore (the default: state is lost on restart and not shared between instances)';

/**
 * The registry the outbox reads its stores from. Your store provider registers itself in
 * its constructor, naming the contracts it implements:
 *
 * ```ts
 * @Injectable()
 * export class DrizzleOutboxStore implements OutboxStore<Transaction>, OutboxInboxStore<Transaction> {
 *   constructor(@InjectDrizzle() private readonly db: Database, storage: OutboxStorage) {
 *     storage.registerSource({ messages: this, inbox: this });
 *   }
 * }
 * ```
 *
 * A contract no source registered uses `InMemoryOutboxStore`: the app boots and runs, but
 * its state is lost on restart and not shared between instances. The registry locks in
 * `OutboxModule`'s `onModuleInit`, after every provider constructor has run and before the
 * relay starts, or at the first read of a store if that is earlier (another module's
 * `onModuleInit`).
 *
 * Production guard: at the lock, with `NODE_ENV=production`, a contract the application
 * uses must have a source, unless `allowInMemoryStorage` is set. The configuration shows
 * which ones it uses:
 * - `messages`, when the application publishes (a transport or an `@OnOutboxMessage()`
 *   handler is configured) or runs the relay (`relay.enabled`, the default);
 * - `inbox`, when an `@OnOutboxMessage()` handler keeps its inbox (the default), or when the
 *   application doesn't publish (a consumer-only service, which uses the module for
 *   `OutboxInbox` alone).
 * A store the configuration doesn't show, read in production with no source (`OutboxInbox`
 * called directly in an application that also publishes), fails at that first read with
 * the same message, instead of running in memory.
 */
@Injectable()
export class OutboxStorage {
  private readonly logger = new Logger('OutboxModule');
  private readonly registered = new Map<OutboxStorageContract, object>();
  private readonly active = new Map<OutboxStorageContract, object>();
  private fallback?: InMemoryOutboxStore;
  private locked = false;
  /** Internal: see {@link STORAGE_USAGE}. Without it (a registry built with `new`), both contracts count as used. */
  [STORAGE_USAGE]?: () => OutboxStorageUsage;

  constructor(@Optional() @Inject(OUTBOX_MODULE_OPTIONS) private readonly options?: OutboxModuleOptions) {
    // `allowInMemoryStorage: process.env.X` is a string, and "false" is truthy: never read
    // anything but `true` as consent to lose messages.
    const allow = options?.allowInMemoryStorage;
    if (allow !== undefined && typeof allow !== 'boolean') {
      throw new TypeError(`OutboxModule: allowInMemoryStorage must be a boolean (got ${describeValue(allow)})`);
    }
  }

  /**
   * Registers stores by contract: `{ messages: this, inbox: this }`, or `{ inbox: this }`
   * in a service that only consumes. Call it from the constructor of a singleton provider.
   * Throws, registering nothing, when a store lacks a method of its contract, when a
   * contract already has a source (unless `replace`), and once the registry has locked.
   */
  registerSource(sources: OutboxStorageSources, options: OutboxStorageRegisterOptions = {}): void {
    const entries = validate(sources);
    if (this.locked) {
      throw new Error(
        `OutboxStorage.registerSource(): ${describe(entries)} registered after OutboxModule initialized (or after ` +
          `its storage was first read), which already uses ${this.summary(CONTRACT_NAMES)}. Register from the ` +
          'constructor of a singleton provider: providers of lazy-loaded modules, request-scoped and transient ' +
          'providers, and lifecycle hooks run too late.',
      );
    }

    if (!options.replace) {
      for (const [contract, source] of entries) {
        const previous = this.registered.get(contract);
        if (previous) {
          throw new Error(
            `OutboxStorage.registerSource(): ${nameOf(source)} can't register \`${contract}\`, ` +
              `${previous === source ? 'it already did (the same instance, twice)' : `${nameOf(previous)} already did`}. ` +
              'Register each contract once, or pass { replace: true } to replace it on purpose (tests, wrappers).',
          );
        }
      }
    }

    for (const [contract, source] of entries) {
      this.registered.set(contract, source);
    }
  }

  /** The messages store in use: the registered one, or the in-memory default. Reading it locks the registry. */
  get messages(): OutboxStore<any> {
    return this.source('messages') as OutboxStore<any>;
  }

  /** The inbox store in use: the registered one, or the in-memory default. Reading it locks the registry. */
  get inbox(): OutboxInboxStore<any> {
    return this.source('inbox') as OutboxInboxStore<any>;
  }

  /** Freezes the sources, logs them, and enforces the production guard (which leaves the registry open). */
  [LOCK_STORAGE](): void {
    if (this.locked) {
      return;
    }

    const usage = this[STORAGE_USAGE]?.() ?? { messages: true, inbox: true };
    const inUse = CONTRACT_NAMES.filter((contract) => usage[contract]);
    const missing = inUse.filter((contract) => !this.registered.has(contract));
    if (missing.length > 0 && this.refusesInMemory()) {
      throw new Error(productionError(missing));
    }

    this.locked = true;
    for (const [contract, source] of this.registered) {
      this.active.set(contract, source);
    }

    this.logger.log(`OutboxStorage: ${this.summary(inUse)}`);
  }

  private source(contract: OutboxStorageContract): object {
    this[LOCK_STORAGE]();

    let source = this.active.get(contract);
    if (!source) {
      // A contract the configuration didn't show as used: never in memory in production either.
      if (this.refusesInMemory()) {
        throw new Error(productionError([contract]));
      }
      source = this.fallback ??= new InMemoryOutboxStore();
      this.active.set(contract, source);
    }

    return source;
  }

  private refusesInMemory(): boolean {
    return process.env.NODE_ENV === 'production' && !this.options?.allowInMemoryStorage;
  }

  /** `DrizzleOutboxStore`, `InboxStore (inbox)`, `DrizzleOutboxStore (messages); in-memory (inbox)`. */
  private summary(inUse: OutboxStorageContract[]): string {
    const sources = new Set(this.registered.values());
    if (sources.size === 0) {
      return DEFAULT;
    }
    if (sources.size === 1 && this.registered.size === CONTRACT_NAMES.length) {
      return nameOf([...sources][0]);
    }

    const listed = CONTRACT_NAMES.filter((contract) => inUse.includes(contract) || this.registered.has(contract));
    const groups = new Map<string, string[]>();
    // The registered sources first, then what runs in memory.
    for (const contract of [...listed.filter((c) => this.registered.has(c)), ...listed.filter((c) => !this.registered.has(c))]) {
      const source = this.registered.get(contract);
      const name = source ? nameOf(source) : 'in-memory';
      groups.set(name, [...(groups.get(name) ?? []), contract]);
    }

    return [...groups].map(([name, contracts]) => `${name} (${contracts.join(', ')})`).join('; ');
  }
}

/** The `[contract, store]` pairs of a `registerSource()` argument, after checking every one. */
function validate(sources: OutboxStorageSources): [OutboxStorageContract, object][] {
  if (sources === null || typeof sources !== 'object') {
    throw new TypeError(`OutboxStorage.registerSource(): the contracts go by name, and got ${nameOf(sources)}. ${usage()}`);
  }

  // A store passed as itself (`registerSource(this)`): say which call it meant.
  const implemented = CONTRACT_NAMES.filter((contract) =>
    CONTRACTS[contract].methods.some((method) => typeof (sources as Record<string, unknown>)[method] === 'function'),
  );
  if (implemented.length > 0) {
    throw new TypeError(
      `OutboxStorage.registerSource(): the contracts go by name, and ${nameOf(sources)} was passed as itself. ` +
        `Name what it implements: \`registerSource({ ${implemented.map((contract) => `${contract}: this`).join(', ')} })\`.`,
    );
  }

  const keys = Object.keys(sources);
  if (keys.length === 0) {
    throw new TypeError(`OutboxStorage.registerSource(): the contracts go by name, and got none. ${usage()}`);
  }

  const unknown = keys.filter((key) => !Object.hasOwn(CONTRACTS, key));
  if (unknown.length > 0) {
    throw new TypeError(
      `OutboxStorage.registerSource(): unknown contract ${unknown.map((key) => `\`${key}\``).join(', ')}. ` +
        `The contracts are ${KNOWN}.`,
    );
  }

  return (keys as OutboxStorageContract[]).map((contract) => {
    const source = sources[contract] as unknown;
    const { interfaceName, methods } = CONTRACTS[contract];
    if (source === null || typeof source !== 'object') {
      throw new TypeError(
        `OutboxStorage.registerSource(): expected an object implementing ${interfaceName} as \`${contract}\`, ` +
          `got ${nameOf(source)}.`,
      );
    }

    const missing = methods.filter((method) => typeof (source as Record<string, unknown>)[method] !== 'function');
    if (missing.length > 0) {
      throw new TypeError(
        `OutboxStorage.registerSource(): ${nameOf(source)} doesn't implement ${interfaceName} for \`${contract}\`: ` +
          `${missing.map((method) => `${method}()`).join(', ')} ${missing.length === 1 ? 'is' : 'are'} missing.`,
      );
    }

    return [contract, source];
  });
}

function usage(): string {
  return `Pass the stores a provider implements: \`registerSource({ messages: this, inbox: this })\`. The contracts are ${KNOWN}.`;
}

function productionError(missing: OutboxStorageContract[]): string {
  const specs = missing.map((contract) => CONTRACTS[contract]);
  const example = missing.map((contract) => `${contract}: this`).join(', ');
  return (
    `OutboxStorage: no store is registered for ${and(missing.map((contract, i) => `\`${contract}\` (${specs[i]!.interfaceName})`))}, ` +
    `and NODE_ENV is "production": in memory, ${and(specs.map((spec) => spec.holds))} would be lost on restart and not ` +
    `shared between instances. Implement ${and(specs.map((spec) => spec.interfaceName))} in a provider that injects ` +
    `OutboxStorage and calls \`storage.registerSource({ ${example} })\` in its constructor, or set ` +
    '`allowInMemoryStorage: true` in the OutboxModule options to run in memory anyway.'
  );
}

/** `a`, `a and b`, `a, b, and c`. */
function and(items: string[]): string {
  if (items.length <= 2) {
    return items.join(' and ');
  }
  return `${items.slice(0, -1).join(', ')}, and ${items.at(-1)}`;
}

/** How a message names a value: its class, or what it is instead of an instance. */
function nameOf(value: unknown): string {
  if (typeof value === 'function') {
    return `the class ${value.name || '(anonymous)'} (pass an instance)`;
  }
  if (value === null || typeof value !== 'object') {
    return String(value);
  }

  const name = (value as object).constructor?.name;
  return name && name !== 'Object' ? name : 'an object';
}

/** `AppOutboxStore (messages, inbox)`: the sources of a `registerSource()` call. */
function describe(entries: [OutboxStorageContract, object][]): string {
  const names = [...new Set(entries.map(([, source]) => nameOf(source)))].join(', ');
  return `${names} (${entries.map(([contract]) => contract).join(', ')})`;
}
