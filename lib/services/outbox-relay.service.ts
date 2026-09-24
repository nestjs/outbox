import {
  Logger,
  type LoggerService,
  type OnApplicationBootstrap,
  type OnModuleDestroy,
} from '@nestjs/common';
import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';
import {
  computeBackoff,
  describeValue,
  optionMs,
  resolveRetry,
  type ResolvedRetry,
} from '../utils/backoff.util.js';
import type { Duration } from '../utils/duration.util.js';
import { describeError } from '../utils/describe-error.util.js';
import { OutboxPublishTimeoutError } from '../errors/outbox-publish-timeout.error.js';
import { NonRetryableMessageError } from '../errors/non-retryable-message.error.js';
import { OutboxEvents } from '../events/outbox-events.service.js';
import type { OutboxEvent } from '../events/outbox-events.interface.js';
import type { OutboxMessage } from '../interfaces/outbox-message.interface.js';
import type {
  OutboxStats,
  OutboxRelayRunResult,
  OutboxRelayConfig,
} from '../interfaces/outbox-relay.interface.js';
import type { OutboxAttempt } from '../interfaces/outbox-dead-letter.interface.js';
import type { OutboxStore } from '../interfaces/outbox-store.interface.js';
import type { OutboxTransport } from '../transports/outbox.transport.js';
import { LOCAL_TRANSPORT } from '../outbox.constants.js';

type Outcome = 'published' | 'retried' | 'dead-lettered' | 'lost';

/**
 * Claims due messages with a lease, publishes them, and records the outcome.
 *
 * - One poll loop per process; `notify()` runs it right away (after a commit). The loop
 *   runs in the async context the relay started in, never in the caller's.
 * - Each claim gets its own fencing token, so a relay that stalled past its lease
 *   can't overwrite the state written by whoever re-claimed the message.
 * - Messages sharing a key are published sequentially within a batch. The first
 *   failure reschedules that message and releases the rest of its key.
 * - A publish gets `publishTimeout`; then its `signal` aborts and the attempt fails.
 * - `stop()` stops claiming, lets in-flight publishes settle, releases the leases it
 *   still holds on unstarted messages, and clears the poll timer, which otherwise keeps
 *   the process alive (a relay-only worker has nothing else on the event loop).
 */
export class OutboxRelay implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly getStore: () => OutboxStore<any>;
  private readonly transports: Record<string, OutboxTransport>;
  private readonly route: (message: OutboxMessage) => string;
  private readonly logger: LoggerService;
  private readonly events: OutboxEvents;
  private readonly enabled: boolean;
  private readonly pollInterval: number;
  private readonly batchSize: number;
  private readonly lease: number;
  private readonly concurrency: number;
  private readonly publishTimeout: number;
  private readonly retry: ResolvedRetry;

  private started = false;
  private stopping = false;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private loop: Promise<void> | undefined;
  private again = false;
  private readonly runs = new Set<Promise<OutboxRelayRunResult>>();
  private inFlight = 0;
  /**
   * Runs a function in the async context `start()` was called in (bootstrap). The relay's
   * timers go through it: `notify()` is called inside a request, and a timer created there
   * would run the poll, every handler it delivers to, and every poll it reschedules in that
   * request's AsyncLocalStorage stores (its user, its locale).
   */
  private inRelayContext: <R>(fn: () => R) => R = (fn) => fn();

  constructor(config: OutboxRelayConfig) {
    const { store } = config;
    this.getStore = typeof store === 'function' ? store : () => store;
    this.transports = config.transports;

    const fallback =
      Object.keys(config.transports).find((name) => name !== LOCAL_TRANSPORT) ?? LOCAL_TRANSPORT;
    if (config.route !== undefined && typeof config.route !== 'function') {
      throw new TypeError(`OutboxModule: route must be a function (got ${describeValue(config.route)})`);
    }
    this.route = config.route ?? (() => fallback);
    this.logger = config.logger ?? new Logger(OutboxRelay.name);
    this.events = config.events ?? new OutboxEvents();

    const relay = config.relay ?? {};
    // `enabled: process.env.OUTBOX_RELAY` is a string, and "false" is truthy.
    if (relay.enabled !== undefined && typeof relay.enabled !== 'boolean') {
      throw new TypeError(`OutboxModule: relay.enabled must be a boolean (got ${describeValue(relay.enabled)})`);
    }

    this.enabled = relay.enabled ?? true;
    this.pollInterval = positiveMs(relay.pollInterval ?? 1_000, 'relay.pollInterval');
    this.batchSize = wholeNumber(relay.batchSize ?? 100, 'relay.batchSize');
    this.lease = positiveMs(relay.lease ?? 30_000, 'relay.lease');
    this.concurrency = wholeNumber(relay.concurrency ?? 10, 'relay.concurrency');

    this.publishTimeout =
      relay.publishTimeout === undefined
        ? Math.max(1, Math.floor(this.lease / 3))
        : positiveMs(relay.publishTimeout, 'relay.publishTimeout');
    if (this.publishTimeout >= this.lease) {
      throw new Error('OutboxModule: relay.publishTimeout must be shorter than relay.lease');
    }

    this.retry = resolveRetry(config.retry);
  }

  onApplicationBootstrap() {
    if (this.enabled) {
      this.start();
    }
  }

  /**
   * Runs in `onModuleDestroy`, the first shutdown phase, so in-flight publishes finish
   * before `ClientsModule` closes its clients (it does so in `onApplicationShutdown`).
   */
  async onModuleDestroy() {
    await this.stop();
  }

  get running(): boolean {
    return this.started && !this.stopping;
  }

  start(): void {
    if (this.started) {
      return;
    }

    this.started = true;
    this.stopping = false;
    this.inRelayContext = AsyncLocalStorage.snapshot();
    this.schedule(0);
  }

  /** Poll now instead of waiting for the next tick. Call it after your transaction commits. */
  notify(): void {
    if (!this.running) {
      return;
    }
    if (this.loop) {
      this.again = true;
      return;
    }
    this.schedule(0);
  }

  /**
   * Stops claiming, waits for in-flight publishes (each for at most `publishTimeout`), and
   * releases the leases on claimed messages it hasn't started.
   */
  async stop(): Promise<void> {
    this.stopping = true;
    clearTimeout(this.timer);
    this.timer = undefined;
    await this.loop;
    await Promise.allSettled(this.runs);
    this.started = false;
  }

  async stats(): Promise<OutboxStats> {
    const now = Date.now();
    const stats = await this.store.stats(now);

    return {
      ...stats,
      lagMs: stats.oldestDueAt === null ? 0 : Math.max(0, now - stats.oldestDueAt),
      inFlight: this.inFlight,
    };
  }

  /**
   * Claims one batch and processes it. The poll loop calls this; tests and scripts can
   * too (for example to flush the outbox in an e2e test without a running loop).
   */
  runOnce(): Promise<OutboxRelayRunResult> {
    const run = this.claimAndProcess();
    this.runs.add(run);

    // Not `run.finally()`: its derived promise would reject unhandled when `run` does.
    const forget = () => this.runs.delete(run);
    run.then(forget, forget);
    return run;
  }

  /** Read on every use: the registry settles the store after the relay is constructed. */
  private get store(): OutboxStore<any> {
    return this.getStore();
  }

  /**
   * The poll timer is referenced on purpose: a relay-only worker (an application context
   * with no server) has nothing else on the event loop, and an unreferenced timer let the
   * process exit right after bootstrap. `stop()` clears it, so shutdown never waits on it.
   */
  private schedule(delay: number) {
    if (!this.running) {
      return;
    }

    clearTimeout(this.timer);
    this.timer = setTimeout(() => this.inRelayContext(() => this.poll()), delay);
  }

  private poll() {
    this.timer = undefined;
    if (this.loop) {
      this.again = true;
      return;
    }

    this.loop = (async () => {
      do {
        this.again = false;
        const result = await this.runOnce();
        // A full batch means there is probably more; drain before sleeping.
        if (result.claimed >= this.batchSize) {
          this.again = true;
        }
      } while (this.again && this.running);
    })()
      .catch((error) => this.logger.error(`Relay loop failed: ${describeError(error)}`))
      .finally(() => {
        this.loop = undefined;
        this.schedule(this.pollInterval);
      });
  }

  private async claimAndProcess(): Promise<OutboxRelayRunResult> {
    const result: OutboxRelayRunResult = {
      claimed: 0,
      published: 0,
      retried: 0,
      deadLettered: 0,
      released: 0,
      leaseLost: 0,
    };
    if (this.stopping) {
      return result;
    }

    const owner = randomUUID();
    const claimedAt = Date.now();
    let messages: OutboxMessage[];
    try {
      messages = await this.store.claim({
        owner,
        now: claimedAt,
        leaseMs: this.lease,
        limit: this.batchSize,
      });
    } catch (error) {
      this.storeError('claim', error);
      return result;
    }

    if (messages.length === 0) {
      return result;
    }
    result.claimed = messages.length;

    const leaseUntil = claimedAt + this.lease;
    const groups = groupByKey(messages);
    await runPool(groups, this.concurrency, async (group) => {
      try {
        await this.processGroup(group, owner, leaseUntil, result);
      } catch (error) {
        // Not a failed publish (those are recorded): a bug, such as a logger that throws.
        // The rest of this group keeps its lease until it expires; other groups go on.
        this.logger.error(`Relay failed on ${group[0]!.topic} ${group[0]!.id}: ${describeError(error)}`);
      }
    });

    return result;
  }

  private async processGroup(
    group: OutboxMessage[],
    owner: string,
    leaseUntil: number,
    result: OutboxRelayRunResult,
  ): Promise<void> {
    for (let i = 0; i < group.length; i++) {
      if (this.stopping) {
        return this.release(group.slice(i), owner, result);
      }

      // Never start a publish that might outlive the lease: past that point another
      // relay may claim the same message and both would publish it.
      if (Date.now() + this.publishTimeout > leaseUntil) {
        return this.release(group.slice(i), owner, result);
      }

      const outcome = await this.deliver(group[i]!, owner);
      switch (outcome) {
        case 'published':
          result.published++;
          break;
        case 'dead-lettered':
          // The key moves on: later messages are no longer blocked by this one.
          result.deadLettered++;
          break;
        case 'retried':
          result.retried++;
          return this.release(group.slice(i + 1), owner, result);
        case 'lost':
          result.leaseLost++;
          return this.release(group.slice(i + 1), owner, result);
      }
    }
  }

  private async deliver(message: OutboxMessage, owner: string): Promise<Outcome> {
    const startedAt = Date.now();
    let transportName: string | undefined;
    this.inFlight++;
    try {
      transportName = this.route(message);
      const transport = this.transports[transportName];
      if (!transport) {
        throw new NonRetryableMessageError(`No outbox transport named "${transportName}"`);
      }
      await publishWithin(transport, message, this.publishTimeout);
    } catch (error) {
      this.inFlight--;
      return this.fail(message, owner, error, transportName);
    }
    this.inFlight--;

    let marked: boolean;
    try {
      marked = await this.store.markPublished(message.id, owner);
    } catch (error) {
      // Published but not marked: the lease expires and the message goes out again.
      this.storeError('markPublished', error);
      return 'lost';
    }

    if (!marked) {
      return this.leaseLost(message);
    }

    this.emit({
      type: 'published',
      message,
      transport: transportName!,
      durationMs: Date.now() - startedAt,
    });
    return 'published';
  }

  private async fail(
    message: OutboxMessage,
    owner: string,
    error: unknown,
    transport: string | undefined,
  ): Promise<Outcome> {
    const now = Date.now();
    const attempt = message.attempts + 1;
    const record: OutboxAttempt = { attempt, at: now, error: describeError(error), transport };
    const retryable = !(error instanceof NonRetryableMessageError) && this.retryIf(error, attempt, message);

    try {
      if (!retryable || attempt >= this.retry.attempts) {
        const reason = retryable ? 'exhausted' : 'rejected';
        const moved = await this.store.deadLetter(message.id, owner, {
          attempts: attempt,
          reason,
          failedAt: now,
          error: record,
        });
        if (!moved) {
          return this.leaseLost(message);
        }

        this.logger.warn(
          `Dead-lettered ${message.topic} ${message.id} after ${attempt} attempt(s) (${reason}): ${record.error}`,
        );
        this.emit({ type: 'dead-lettered', message, transport, error, attempt, reason });
        return 'dead-lettered';
      }

      const delayMs = this.backoff(attempt, error, message);
      const rescheduled = await this.store.reschedule(message.id, owner, {
        attempts: attempt,
        availableAt: now + delayMs,
        error: record,
      });
      if (!rescheduled) {
        return this.leaseLost(message);
      }

      this.logger.warn(
        `Retrying ${message.topic} ${message.id} in ${delayMs}ms ` +
          `(attempt ${attempt} of ${this.retry.attempts} failed): ${record.error}`,
      );
      this.emit({ type: 'retry-scheduled', message, transport, error, attempt, delayMs });
      return 'retried';
    } catch (storeError) {
      this.storeError('fail', storeError);
      return 'lost';
    }
  }

  /** `retry.retryIf`, guarded: a throwing predicate must not wedge the message. */
  private retryIf(error: unknown, attempt: number, message: OutboxMessage): boolean {
    if (!this.retry.retryIf) {
      return true;
    }

    try {
      return this.retry.retryIf(error, attempt, message);
    } catch (predicateError) {
      this.logger.error(`retry.retryIf threw, retrying: ${describeError(predicateError)}`);
      return true;
    }
  }

  /** `retry.backoff`, guarded: a throwing function or a bad duration falls back to the default. */
  private backoff(attempt: number, error: unknown, message: OutboxMessage): number {
    try {
      return computeBackoff(this.retry.backoff, attempt, error, message);
    } catch (backoffError) {
      this.logger.error(`retry.backoff failed, using the default: ${describeError(backoffError)}`);
      return computeBackoff(resolveRetry(undefined).backoff, attempt, error, message);
    }
  }

  private async release(
    messages: OutboxMessage[],
    owner: string,
    result: OutboxRelayRunResult,
  ): Promise<void> {
    if (messages.length === 0) {
      return;
    }

    try {
      result.released += await this.store.release(
        messages.map((m) => m.id),
        owner,
      );
    } catch (error) {
      // The leases simply expire.
      this.storeError('release', error);
    }
  }

  private leaseLost(message: OutboxMessage): Outcome {
    this.logger.warn(
      `Lease on ${message.topic} ${message.id} was taken over; it may be published more than once`,
    );
    this.emit({ type: 'lease-lost', message });
    return 'lost';
  }

  private storeError(operation: string, error: unknown) {
    this.logger.error(`Outbox store ${operation} failed: ${describeError(error)}`);
  }

  private emit(event: OutboxEvent) {
    try {
      this.events.emit(event);
    } catch (error) {
      this.logger.error(`Outbox event subscriber threw: ${describeError(error)}`);
    }
  }
}

/** Keyed messages grouped per key (in claim order); keyless messages are groups of one. */
export function groupByKey(messages: OutboxMessage[]): OutboxMessage[][] {
  const groups: OutboxMessage[][] = [];
  const byKey = new Map<string, OutboxMessage[]>();

  for (const message of messages) {
    if (message.key === null) {
      groups.push([message]);
      continue;
    }

    let group = byKey.get(message.key);
    if (!group) {
      group = [];
      byKey.set(message.key, group);
      groups.push(group);
    }
    group.push(message);
  }

  return groups;
}

async function runPool<T>(items: T[], limit: number, fn: (item: T) => Promise<void>) {
  let next = 0;
  const worker = async () => {
    while (next < items.length) {
      await fn(items[next++]!);
    }
  };

  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
}

/**
 * Calls `transport.publish()` and waits at most `ms`. On timeout the signal aborts (with
 * the `OutboxPublishTimeoutError` as its reason) and the attempt fails; a transport or
 * handler that ignores the signal keeps running in the background.
 */
async function publishWithin(transport: OutboxTransport, message: OutboxMessage, ms: number): Promise<void> {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;

  try {
    await Promise.race([
      Promise.resolve().then(() => transport.publish(message, { signal: controller.signal })),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          const error = new OutboxPublishTimeoutError(ms);
          controller.abort(error);
          reject(error);
        }, ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function positiveMs(value: Duration, option: string): number {
  const ms = optionMs(value, option);
  if (ms <= 0) {
    throw new TypeError(`OutboxModule: ${option} must be longer than 0 (got ${String(value)})`);
  }
  return ms;
}

function wholeNumber(value: number, option: string): number {
  if (!Number.isInteger(value) || value < 1) {
    throw new TypeError(`OutboxModule: ${option} must be a whole number of at least 1 (got ${value})`);
  }
  return value;
}
