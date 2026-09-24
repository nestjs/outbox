import { toMs, type Duration } from './duration.util.js';
import type { OutboxMessage } from '../interfaces/outbox-message.interface.js';
import type {
  OutboxModuleOptions,
  OutboxRetryOptions,
} from '../interfaces/outbox-module-options.interface.js';

const DEFAULT_ATTEMPTS = 20;

const DEFAULT_BACKOFF = { delay: 1_000, factor: 2, maxDelay: 5 * 60_000, jitter: 'equal' } as const;

const JITTERS: readonly unknown[] = ['full', 'equal', 'none'];

type BackoffFn = (attempt: number, error: unknown, message: OutboxMessage) => Duration;

/** Backoff with every value resolved to milliseconds, or the user's function. */
type ResolvedBackoff =
  | { delay: number; factor: number; maxDelay: number; jitter: 'full' | 'equal' | 'none' }
  | BackoffFn;

export interface ResolvedRetry {
  attempts: number;
  backoff: ResolvedBackoff;
  retryIf?: OutboxRetryOptions['retryIf'];
}

/** Resolves the `retry` option once, at startup: bad values fail naming the option. */
export function resolveRetry(retry: OutboxModuleOptions['retry']): ResolvedRetry {
  const options: OutboxRetryOptions =
    retry === false ? { attempts: 1 } : typeof retry === 'number' ? { attempts: retry } : (retry ?? {});
  const attempts = options.attempts ?? DEFAULT_ATTEMPTS;
  if (!Number.isInteger(attempts) || attempts < 1) {
    throw new TypeError(
      `OutboxModule: retry.attempts must be a whole number of at least 1 (got ${attempts}). ` +
        'Use `retry: false` for a single attempt.',
    );
  }

  const { backoff, retryIf } = options;
  if (retryIf !== undefined && typeof retryIf !== 'function') {
    throw new TypeError(`OutboxModule: retry.retryIf must be a function (got ${typeof retryIf})`);
  }

  if (typeof backoff === 'function') {
    return { attempts, backoff, retryIf };
  }

  if (backoff !== undefined && (backoff === null || typeof backoff !== 'object')) {
    throw new TypeError(
      'OutboxModule: retry.backoff must be { delay, factor, maxDelay, jitter } or a function ' +
        `(got ${backoff === null ? 'null' : typeof backoff}). For a fixed wait, use { delay: ${String(backoff)}, factor: 1 }.`,
    );
  }

  const factor = backoff?.factor ?? DEFAULT_BACKOFF.factor;
  if (!(factor >= 1)) {
    throw new TypeError(`OutboxModule: retry.backoff.factor must be at least 1 (got ${factor})`);
  }

  const jitter = backoff?.jitter ?? DEFAULT_BACKOFF.jitter;
  if (!JITTERS.includes(jitter)) {
    throw new TypeError(
      `OutboxModule: retry.backoff.jitter must be "full", "equal" or "none" (got ${JSON.stringify(jitter)})`,
    );
  }

  return {
    attempts,
    retryIf,
    backoff: {
      delay: optionMs(backoff?.delay ?? DEFAULT_BACKOFF.delay, 'retry.backoff.delay'),
      factor,
      maxDelay: optionMs(backoff?.maxDelay ?? DEFAULT_BACKOFF.maxDelay, 'retry.backoff.maxDelay'),
      jitter,
    },
  };
}

/** How a startup error names a value of the wrong type: `the string "false"`, `null`, `an object`. */
export function describeValue(value: unknown): string {
  if (value === null) {
    return 'null';
  }
  if (typeof value === 'string') {
    return `the string ${JSON.stringify(value)}`;
  }
  if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') {
    return `the ${typeof value} ${String(value)}`;
  }
  if (typeof value === 'function') {
    return `the function ${value.name || '(anonymous)'}`;
  }
  if (typeof value === 'object') {
    return 'an object';
  }
  return typeof value;
}

/** `toMs()` for a module option: the error names the option. */
export function optionMs(value: Duration, option: string): number {
  try {
    return toMs(value);
  } catch (error) {
    throw new TypeError(`OutboxModule: ${option}: ${(error as Error).message}`);
  }
}

/**
 * Delay before the next attempt, after attempt number `attempt` (1-based) failed.
 * `equal` jitter keeps a floor of half the computed delay, so a broker outage never
 * turns into immediate re-publishes. `@nestjs/http-client` uses full jitter because a
 * fleet of callers retries one request; here one relay retries each message.
 */
export function computeBackoff(
  backoff: ResolvedBackoff,
  attempt: number,
  error: unknown,
  message: OutboxMessage,
  random: () => number = Math.random,
): number {
  if (typeof backoff === 'function') {
    return Math.floor(toMs(backoff(attempt, error, message)));
  }

  const { delay, factor, maxDelay, jitter } = backoff;
  const ceiling = Math.min(maxDelay, delay * factor ** (attempt - 1));
  switch (jitter) {
    case 'none':
      return Math.floor(ceiling);
    case 'full':
      return Math.floor(random() * ceiling);
    default:
      return Math.floor(ceiling / 2 + (random() * ceiling) / 2);
  }
}
