import type { OutboxStore } from './outbox-store.interface.js';
import type { OutboxInboxStore } from './outbox-inbox-store.interface.js';

/**
 * The storage contracts, by name: what `registerSource()` takes. One provider usually
 * implements both (`{ messages: this, inbox: this }`); a service that only consumes
 * registers the inbox alone (`{ inbox: this }`).
 */
export interface OutboxStorageSources<Tx = any> {
  /** Messages and dead letters: what `Outbox.add()`, the relay and `OutboxDeadLetters` use. */
  messages?: OutboxStore<Tx>;
  /** Consumer deduplication: what `OutboxInbox` and the `@OnOutboxMessage()` handlers use. */
  inbox?: OutboxInboxStore<Tx>;
}

export type OutboxStorageContract = keyof OutboxStorageSources;

/** `registerSource()` options. */
export interface OutboxStorageRegisterOptions {
  /** Replace a source that is already registered (tests, a wrapper around it). */
  replace?: boolean;
}

/** Internal: the contracts the application uses, as far as its configuration shows. */
export type OutboxStorageUsage = Record<OutboxStorageContract, boolean>;
