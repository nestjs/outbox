import { Module, type DynamicModule, type OnModuleInit } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import type { OutboxModuleOptions } from './interfaces/outbox-module-options.interface.js';
import {
  ConfigurableModuleClass,
  OUTBOX_MODULE_OPTIONS,
  OUTBOX_TRANSPORTS,
  storeOptionError,
  type OPTIONS_TYPE,
  type OutboxModuleAsyncOptions,
  type OutboxModuleRootOptions,
} from './outbox.module-definition.js';
import { OutboxDeadLetters } from './services/outbox-dead-letters.service.js';
import { OutboxDispatcher } from './services/outbox-dispatcher.service.js';
import { OutboxEvents } from './events/outbox-events.service.js';
import { OutboxInbox } from './services/outbox-inbox.service.js';
import { OutboxRelay } from './services/outbox-relay.service.js';
import { Outbox } from './outbox.service.js';
import { LOCK_STORAGE, OutboxStorage, STORAGE_USAGE } from './storage/outbox.storage.js';
import type { OutboxTransport } from './transports/outbox.transport.js';
import { LOCAL_TRANSPORT } from './outbox.constants.js';

/**
 * `OutboxModule.forRoot({ transports, route, relay, retry })`, or
 * `forRootAsync({ transports, imports, inject, useFactory })`, where the factory (or a
 * `useClass` class's `createOutboxOptions()`) returns the other options, and may return
 * `transports` as instances built from injected configuration (classes stay at the top
 * level). Global by default. The store is a provider of the app's that registers itself
 * with `OutboxStorage`; without one, the outbox runs in memory.
 */
@Module({
  imports: [DiscoveryModule],
  providers: [
    OutboxStorage,
    OutboxInbox,
    OutboxEvents,
    OutboxDispatcher,
    {
      provide: OutboxRelay,
      inject: [OUTBOX_MODULE_OPTIONS, OutboxStorage, OUTBOX_TRANSPORTS, OutboxDispatcher, OutboxEvents],
      useFactory: (
        options: OutboxModuleOptions,
        storage: OutboxStorage,
        transports: Record<string, OutboxTransport>,
        dispatcher: OutboxDispatcher,
        events: OutboxEvents,
      ) =>
        new OutboxRelay({
          ...options,
          store: () => storage.messages,
          transports: { ...transports, [LOCAL_TRANSPORT]: dispatcher },
          events,
        }),
    },
    Outbox,
    OutboxDeadLetters,
  ],
  exports: [OutboxStorage, Outbox, OutboxRelay, OutboxDeadLetters, OutboxInbox, OutboxEvents],
})
export class OutboxModule extends ConfigurableModuleClass implements OnModuleInit {
  constructor(
    private readonly storage: OutboxStorage,
    dispatcher: OutboxDispatcher,
  ) {
    super();
    // The production guard checks the contracts this configuration uses (see OutboxStorage).
    storage[STORAGE_USAGE] = () => dispatcher.storageUsage();
  }

  static forRoot(options: OutboxModuleRootOptions = {}): DynamicModule {
    if (options && 'store' in options) {
      throw storeOptionError();
    }
    return super.forRoot(options as typeof OPTIONS_TYPE);
  }

  static forRootAsync(options: OutboxModuleAsyncOptions): DynamicModule {
    if (options && 'store' in options) {
      throw storeOptionError();
    }
    return super.forRootAsync(options);
  }

  /**
   * Every provider constructor has run, so the store providers have registered, and the
   * relay starts later, in `onApplicationBootstrap`: the stores are fixed from here on (if
   * a read in another module's `onModuleInit` hasn't fixed them already).
   */
  onModuleInit() {
    this.storage[LOCK_STORAGE]();
  }
}
