import type { LoggerService } from '@nestjs/common';
import type { OutboxStoreStats, OutboxStore } from './outbox-store.interface.js';
import type { OutboxEvents } from '../events/outbox-events.service.js';
import type { OutboxModuleOptions } from './outbox-module-options.interface.js';
import type { OutboxTransport } from '../transports/outbox.transport.js';

/** `OutboxRelay.stats()`: the store's counts, plus this relay's view. */
export interface OutboxStats extends OutboxStoreStats {
  /**
   * How long the longest-waiting message has waited, in ms (0 when none is): the number
   * to alert on. It grows while a downstream is down, and scheduled messages don't add to it.
   */
  lagMs: number;
  /** Messages this relay is publishing right now. */
  inFlight: number;
}

/** What `runOnce()` did with the batch it claimed. */
export interface OutboxRelayRunResult {
  claimed: number;
  published: number;
  retried: number;
  deadLettered: number;
  released: number;
  leaseLost: number;
}

/** What the module (or a test) builds a relay from. Not part of the public API. */
export interface OutboxRelayConfig extends OutboxModuleOptions {
  /** The store, or a function that returns the registered one when the relay needs it. */
  store: OutboxStore<any> | (() => OutboxStore<any>);
  /** Every transport by name, `local` included. */
  transports: Record<string, OutboxTransport>;
  events?: OutboxEvents;
  logger?: LoggerService;
}
