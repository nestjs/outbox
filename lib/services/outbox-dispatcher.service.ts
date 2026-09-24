import { Inject, Injectable, type OnModuleInit } from '@nestjs/common';
import { DiscoveryService, MetadataScanner } from '@nestjs/core';
import { isObservable, type Observable } from 'rxjs';
import { OUTBOX_HANDLER_METADATA, LOCAL_TRANSPORT } from '../outbox.constants.js';
import type { OutboxHandlerContext, OutboxHandlerMetadata } from '../interfaces/outbox-handler.interface.js';
import { describeError } from '../utils/describe-error.util.js';
import { OutboxNoHandlerError } from '../errors/outbox-no-handler.error.js';
import { NonRetryableMessageError } from '../errors/non-retryable-message.error.js';
import type { OutboxMessage } from '../interfaces/outbox-message.interface.js';
import type { OutboxModuleOptions } from '../interfaces/outbox-module-options.interface.js';
import { OutboxInbox } from './outbox-inbox.service.js';
import type { OutboxStorageUsage } from '../interfaces/outbox-storage.interface.js';
import { OUTBOX_MODULE_OPTIONS, OUTBOX_TRANSPORTS } from '../outbox.module-definition.js';
import { OutboxTransport } from '../transports/outbox.transport.js';

interface HandlerEntry {
  consumer: string;
  inbox: boolean;
  invoke: (payload: unknown, context: OutboxHandlerContext) => unknown;
}

/**
 * The in-process (`local`) transport: publishing a message runs every
 * `@OnOutboxMessage()` handler for its topic, each behind its own inbox entry.
 *
 * Handlers run concurrently. If any fails, the publish fails and the message is
 * retried; handlers that already succeeded are skipped on the retry by their inbox
 * entry, so a partial fan-out failure doesn't re-run the successful consumers. When
 * every failing handler threw `NonRetryableMessageError`, the publish fails permanently.
 * When the relay stops waiting (`publishTimeout`), the handlers' `signal` aborts and an
 * Observable a handler returned is unsubscribed.
 */
@Injectable()
export class OutboxDispatcher extends OutboxTransport implements OnModuleInit {
  private readonly handlers = new Map<string, HandlerEntry[]>();
  private scanned = false;

  constructor(
    private readonly discovery: DiscoveryService,
    private readonly scanner: MetadataScanner,
    private readonly inbox: OutboxInbox,
    @Inject(OUTBOX_MODULE_OPTIONS) private readonly options: OutboxModuleOptions,
    @Inject(OUTBOX_TRANSPORTS) private readonly transports: Record<string, OutboxTransport>,
  ) {
    super();
  }

  onModuleInit() {
    this.scan();
    this.assertRouteIsUnambiguous();
  }

  /**
   * The storage contracts this application uses, as far as its configuration shows, for
   * `OutboxStorage`'s production guard: `messages` when it publishes (a transport or a
   * handler) or runs the relay; `inbox` when a handler keeps its inbox, or when it doesn't
   * publish (a consumer-only service, which uses the module for `OutboxInbox` alone).
   * Scans the handlers if `onModuleInit` hasn't yet (a read in an earlier module's hook).
   */
  storageUsage(): OutboxStorageUsage {
    this.scan();

    const entries = [...this.handlers.values()].flat();
    const publishes = entries.length > 0 || Object.keys(this.transports).length > 0;
    return {
      messages: publishes || this.options.relay?.enabled !== false,
      inbox: !publishes || entries.some((entry) => entry.inbox),
    };
  }

  /** Finds the `@OnOutboxMessage()` handlers, once. */
  private scan() {
    if (this.scanned) {
      return;
    }

    this.handlers.clear(); // a scan that threw may have left some behind
    const seen = new Set<object>();
    const wrappers = [...this.discovery.getProviders(), ...this.discovery.getControllers()];
    for (const wrapper of wrappers) {
      const { instance } = wrapper;
      if (!instance || typeof instance !== 'object' || seen.has(instance)) {
        continue;
      }
      seen.add(instance);

      const prototype = Object.getPrototypeOf(instance);
      for (const method of this.scanner.getAllMethodNames(prototype)) {
        const metadata: OutboxHandlerMetadata[] | undefined = Reflect.getMetadata(
          OUTBOX_HANDLER_METADATA,
          prototype[method],
        );
        if (!metadata) {
          continue;
        }
        if (!wrapper.isDependencyTreeStatic()) {
          throw new Error(
            `${wrapper.name}.${method}: @OnOutboxMessage() handlers must be on singleton providers, ` +
              'and this one is request-scoped (or depends on a request-scoped provider)',
          );
        }

        for (const meta of metadata) {
          const entry: HandlerEntry = {
            consumer: meta.consumer,
            inbox: meta.inbox ?? true,
            invoke: (payload, context) => instance[method](payload, context),
          };

          for (const topic of meta.topics) {
            const list = this.handlers.get(topic) ?? [];
            if (list.some((e) => e.consumer === entry.consumer)) {
              throw new Error(`Duplicate outbox consumer "${entry.consumer}" for topic "${topic}"`);
            }
            this.handlers.set(topic, [...list, entry]);
          }
        }
      }
    }

    this.scanned = true;
  }

  /** Topics with at least one handler in this process. */
  get topics(): string[] {
    return [...this.handlers.keys()];
  }

  async publish(message: OutboxMessage, { signal }: { signal: AbortSignal } = { signal: NEVER_ABORTED }): Promise<void> {
    const entries = this.handlers.get(message.topic);
    if (!entries?.length) {
      throw new OutboxNoHandlerError(message.topic);
    }

    const results = await Promise.allSettled(entries.map((entry) => this.run(entry, message, signal)));
    const errors = results.flatMap((r) => (r.status === 'rejected' ? [r.reason] : []));
    if (errors.length === 1) {
      throw errors[0];
    }
    if (errors.length > 1) {
      const aggregate = new AggregateError(
        errors,
        `${errors.length} handlers failed for "${message.topic}"`,
      );

      // Every failure is permanent: retrying can't help, so dead-letter it now. If any
      // failure is transient, retry: handlers that succeeded are skipped by their inbox,
      // and once only permanent failures remain, that attempt dead-letters the message.
      if (errors.every((error) => error instanceof NonRetryableMessageError)) {
        throw new NonRetryableMessageError(describeError(aggregate), { cause: aggregate });
      }
      throw aggregate;
    }
  }

  private async run(entry: HandlerEntry, message: OutboxMessage, signal: AbortSignal): Promise<void> {
    const context: OutboxHandlerContext = {
      message,
      consumer: entry.consumer,
      attempt: message.attempts + 1,
      signal,
      processInTransaction: (tx, work) =>
        this.inbox.processInTransaction(tx, entry.consumer, message.id, work),
    };

    const invoke = async () => {
      // The relay may have given up on this attempt while it waited behind an earlier one.
      signal.throwIfAborted();
      const result = entry.invoke(message.payload, context);
      return isObservable(result) ? lastValue(result, signal) : result;
    };

    if (entry.inbox) {
      await this.inbox.process(entry.consumer, message.id, invoke);
      return;
    }
    await invoke();
  }

  /** With handlers and a transport, or several transports, a missing `route` would guess. */
  private assertRouteIsUnambiguous() {
    if (this.options.route) {
      return;
    }

    const names = Object.keys(this.transports);
    if (names.length > 1 || (names.length === 1 && this.handlers.size > 0)) {
      const destinations = [...(this.handlers.size > 0 ? [LOCAL_TRANSPORT] : []), ...names];
      throw new Error(
        `OutboxModule: set \`route\` to choose a transport for each message. This app can publish to ` +
          `${destinations.map((name) => `"${name}"`).join(', ')}` +
          (this.handlers.size > 0 ? ` ("${LOCAL_TRANSPORT}" runs the @OnOutboxMessage() handlers).` : '.'),
      );
    }
  }
}

const NEVER_ABORTED = new AbortController().signal;

/**
 * The Observable's last value, like `lastValueFrom()`. On abort it unsubscribes and
 * rejects with the signal's reason: an interrupted handler must not count as processed.
 */
function lastValue(source: Observable<unknown>, signal: AbortSignal): Promise<unknown> {
  if (signal.aborted) {
    return Promise.reject(signal.reason);
  }

  return new Promise((resolve, reject) => {
    let last: unknown;
    const abort = () => {
      subscription.unsubscribe();
      reject(signal.reason);
    };

    const subscription = source.subscribe({
      next: (value) => (last = value),
      error: (error) => {
        signal.removeEventListener('abort', abort);
        reject(error);
      },
      complete: () => {
        signal.removeEventListener('abort', abort);
        resolve(last);
      },
    });

    if (!subscription.closed) {
      signal.addEventListener('abort', abort, { once: true });
    }
  });
}
