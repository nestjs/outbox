import { computeBackoff, resolveRetry } from '../lib/utils/backoff.util.js';
import { toMs } from '../lib/utils/duration.util.js';
import { OutboxError } from '../lib/errors/outbox.error.js';
import { describeError } from '../lib/utils/describe-error.util.js';
import { OutboxPublishTimeoutError } from '../lib/errors/outbox-publish-timeout.error.js';
import { OutboxTransactionRequiredError } from '../lib/errors/outbox-transaction-required.error.js';
import { OutboxNoHandlerError } from '../lib/errors/outbox-no-handler.error.js';
import { NonRetryableMessageError } from '../lib/errors/non-retryable-message.error.js';
import { groupByKey } from '../lib/services/outbox-relay.service.js';
import { uuidv7 } from '../lib/utils/uuid.util.js';
import type { OutboxMessage } from '../lib/index.js';

const msg = (id: string, key: string | null) => ({ id, key }) as OutboxMessage;

describe('uuidv7', () => {
  afterEach(() => vi.useRealTimers());

  it('is an RFC 9562 v7 UUID carrying the millisecond timestamp', () => {
    vi.useFakeTimers({ toFake: ['Date'], now: new Date('2026-09-22T10:00:00.000Z') });
    const id = uuidv7();
    expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(parseInt(id.replace(/-/g, '').slice(0, 12), 16)).toBe(Date.now());
  });

  it('stays strictly increasing within one millisecond and when the clock steps back', () => {
    vi.useFakeTimers({ toFake: ['Date'], now: 1_000_000 });
    const ids = Array.from({ length: 10_000 }, () => uuidv7()); // overflows the 12-bit counter
    vi.setSystemTime(500_000);
    ids.push(uuidv7(), uuidv7());
    expect([...ids].sort()).toEqual(ids);
    expect(new Set(ids).size).toBe(ids.length);
  });
});

describe('retry options', () => {
  const m = msg('1', null);
  const backoff = (options: Parameters<typeof resolveRetry>[0]) => resolveRetry(options).backoff;

  it('grows exponentially up to maxDelay', () => {
    const delays = [1, 2, 3, 4, 5].map((n) =>
      computeBackoff(backoff({ backoff: { delay: 100, maxDelay: 1_000, jitter: 'none' } }), n, null, m),
    );
    expect(delays).toEqual([100, 200, 400, 800, 1_000]);
  });

  it('accepts durations as strings', () => {
    const resolved = backoff({ backoff: { delay: '1s', maxDelay: '1m', jitter: 'none' } });
    expect(resolved).toEqual({ delay: 1_000, factor: 2, maxDelay: 60_000, jitter: 'none' });
    expect(computeBackoff(resolved, 10, null, m)).toBe(60_000);
  });

  it('applies equal jitter by default and full jitter on request', () => {
    expect(computeBackoff(backoff({ backoff: { delay: 1_000 } }), 1, null, m, () => 0)).toBe(500);
    expect(computeBackoff(backoff({ backoff: { delay: 1_000 } }), 1, null, m, () => 0.999)).toBe(999);
    expect(computeBackoff(backoff({ backoff: { delay: 1_000, jitter: 'full' } }), 1, null, m, () => 0)).toBe(0);
  });

  it('accepts a function returning milliseconds or a duration', () => {
    expect(computeBackoff(backoff({ backoff: (attempt, error) => attempt * 10 + (error as number) }), 3, 4, m)).toBe(34);
    expect(computeBackoff(backoff({ backoff: (attempt) => `${attempt}s` }), 3, null, m)).toBe(3_000);
  });

  it('reads `retry: 5` as five attempts and `retry: false` as one; defaults to 20', () => {
    expect(resolveRetry(5).attempts).toBe(5);
    expect(resolveRetry(false).attempts).toBe(1);
    expect(resolveRetry(undefined).attempts).toBe(20);
    expect(resolveRetry({ backoff: { delay: 10 } }).attempts).toBe(20);
  });

  it('fails at startup on invalid values, naming the option', () => {
    expect(() => resolveRetry(0)).toThrow(/retry\.attempts must be a whole number of at least 1 \(got 0\)/);
    expect(() => resolveRetry({ attempts: 2.5 })).toThrow(/retry\.attempts/);
    expect(() => resolveRetry({ backoff: { delay: '1 minute' as '1m' } })).toThrow(
      /retry\.backoff\.delay: Invalid duration "1 minute"/,
    );
    expect(() => resolveRetry({ backoff: { factor: 0.5 } })).toThrow(/retry\.backoff\.factor/);

    // These failed on every retry at runtime instead (a guarded call, logged each time), or
    // silently fell back to equal jitter.
    expect(() => resolveRetry({ retryIf: true as never })).toThrow(
      'OutboxModule: retry.retryIf must be a function (got boolean)',
    );
    expect(() => resolveRetry({ backoff: 1_000 as never })).toThrow(
      "OutboxModule: retry.backoff must be { delay, factor, maxDelay, jitter } or a function (got number). For a fixed wait, use { delay: 1000, factor: 1 }.",
    );
    expect(() => resolveRetry({ backoff: { jitter: 'Full' as 'full' } })).toThrow(
      'OutboxModule: retry.backoff.jitter must be "full", "equal" or "none" (got "Full")',
    );
  });

  it('parses durations', () => {
    expect([toMs(250), toMs('250ms'), toMs('30s'), toMs('15m'), toMs('1.5h'), toMs('3d'), toMs('1w')]).toEqual([
      250, 250, 30_000, 900_000, 5_400_000, 259_200_000, 604_800_000,
    ]);
    expect(() => toMs(-1)).toThrow(/non-negative/);
  });
});

describe('helpers', () => {
  it('groups keyed messages per key and keeps keyless ones apart, in order', () => {
    const groups = groupByKey([msg('1', 'a'), msg('2', null), msg('3', 'b'), msg('4', 'a'), msg('5', null)]);
    expect(groups.map((g) => g.map((x) => x.id))).toEqual([['1', '4'], ['2'], ['3'], ['5']]);
  });

  it('describes errors compactly, including aggregates, and truncates long ones', () => {
    expect(describeError(new TypeError('bad'))).toBe('TypeError: bad');
    expect(describeError(new AggregateError([new Error('a'), 'b'], '2 failed'))).toBe(
      'AggregateError: 2 failed [Error: a; b]',
    );
    expect(describeError({ code: 42 })).toBe('{"code":42}');
    expect(describeError(new Error('x'.repeat(5_000))).length).toBeLessThanOrEqual(2_001);
  });

  it('gives the errors an app may catch a common base, and keeps its own signal apart', () => {
    for (const error of [
      new OutboxTransactionRequiredError(),
      new OutboxPublishTimeoutError(250),
      new OutboxNoHandlerError('order.placed'),
    ]) {
      expect(error).toBeInstanceOf(OutboxError);
      expect(error.name).toBe(error.constructor.name);
    }

    expect(new OutboxPublishTimeoutError(250)).toMatchObject({ timeoutMs: 250 });

    // Thrown by the app to stop retries, not raised by the package.
    expect(new NonRetryableMessageError('no')).not.toBeInstanceOf(OutboxError);
  });

  it('describes whatever was thrown without throwing itself', () => {
    const hostile = Object.assign(Object.create(null), {
      toJSON() {
        throw new Error('no');
      },
    });

    expect(describeError(undefined)).toBe('undefined');
    expect(describeError(null)).toBe('null');
    expect(describeError(Symbol('lost'))).toBe('Symbol(lost)');
    expect(describeError(function flaky() {})).toBe('[function flaky]');
    expect(describeError(hostile)).toBe('[object Object]');
    expect(describeError(new AggregateError([undefined, Symbol('s')], 'both'))).toBe(
      'AggregateError: both [undefined; Symbol(s)]',
    );
  });
});
