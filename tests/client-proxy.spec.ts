import { Subject, of, throwError, type Observable } from 'rxjs';
import { ClientProxyTransport, type OutboxMessage } from '../lib/index.js';
import { message } from './helpers.js';

describe('ClientProxyTransport', () => {
  const build = (emit: (pattern: unknown, data: unknown) => Observable<unknown>) => {
    const Transport = ClientProxyTransport('BROKER') as unknown as new (client: unknown) => {
      publish(message: OutboxMessage): Promise<void>;
    };
    return new Transport({ emit });
  };

  const stored: OutboxMessage = {
    ...message('order.placed', 'order-1'),
    headers: { 'x-tenant': 'tenant-7' },
    payload: { orderId: 1 },
    createdAt: 5,
    availableAt: 6,
    attempts: 2,
    lastError: 'Error: down',
  };

  it('emits the topic as the pattern and the envelope, without the relay bookkeeping, as data', async () => {
    const emitted: unknown[][] = [];
    await build((pattern, data) => (emitted.push([pattern, data]), of(undefined))).publish(stored);

    expect(emitted).toEqual([
      [
        'order.placed',
        {
          id: stored.id,
          topic: 'order.placed',
          key: 'order-1',
          headers: { 'x-tenant': 'tenant-7' },
          createdAt: 5,
          payload: { orderId: 1 },
        },
      ],
    ]);
  });

  it('resolves once the emit completes, values or not, and fails with its error', async () => {
    const pending = new Subject<unknown>();
    let resolved = false;
    const publishing = build(() => pending).publish(stored).then(() => (resolved = true));

    pending.next('ack');
    await Promise.resolve();
    expect(resolved).toBe(false);
    pending.complete();
    await publishing;
    expect(resolved).toBe(true);

    await expect(build(() => of()).publish(stored)).resolves.toBeUndefined();
    await expect(build(() => throwError(() => new Error('ECONNREFUSED'))).publish(stored)).rejects.toThrow('ECONNREFUSED');
  });

  it('is named after its client token, for DI errors', () => {
    class AnalyticsClient {}
    expect(ClientProxyTransport(AnalyticsClient).name).toBe('ClientProxyTransport(AnalyticsClient)');
    expect(ClientProxyTransport(Symbol('KAFKA')).name).toBe('ClientProxyTransport(KAFKA)');
    expect(ClientProxyTransport('NATS').name).toBe('ClientProxyTransport(NATS)');
  });
});
