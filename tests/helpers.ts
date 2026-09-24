import { Module, type DynamicModule, type LoggerService } from '@nestjs/common';
import { OutboxStorage, type OutboxInboxStore, type OutboxMessage, type OutboxStore } from '../lib/index.js';
import { uuidv7 } from '../lib/utils/uuid.util.js';

export function deferred<T = void>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });

  return { promise, resolve, reject };
}

export const silentLogger: LoggerService = { log() {}, error() {}, warn() {} };

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export async function until(check: () => boolean | Promise<boolean>, timeout = 3_000) {
  const start = Date.now();
  while (!(await check())) {
    if (Date.now() - start > timeout) {
      throw new Error('Timed out waiting');
    }
    await sleep(5);
  }
}

/** A registry with `store` registered for both contracts, for services built without Nest. */
export function storageWith(store: OutboxStore<any> & OutboxInboxStore<any>): OutboxStorage {
  const storage = new OutboxStorage();
  storage.registerSource({ messages: store, inbox: store });
  Object.assign(storage, { logger: silentLogger }); // no application to route the lock's log line to
  return storage;
}

export function message(topic: string, key: string | null = null, availableAt = 0): OutboxMessage {
  return {
    id: uuidv7(),
    topic,
    payload: { topic },
    headers: {},
    key,
    createdAt: 0,
    availableAt,
    attempts: 0,
    lastError: null,
  };
}

const TEST_STORE = Symbol('TEST_STORE');

/**
 * A module with a provider that registers `store`, the way an application's store provider
 * does from its constructor. Import it next to `OutboxModule`.
 */
export function registeredStore(store: OutboxStore<any> & OutboxInboxStore<any>): DynamicModule {
  @Module({})
  class TestStoreModule {}

  return {
    module: TestStoreModule,
    providers: [
      {
        provide: TEST_STORE,
        inject: [OutboxStorage],
        useFactory: (storage: OutboxStorage) => {
          storage.registerSource({ messages: store, inbox: store });
          return store;
        },
      },
    ],
  };
}
