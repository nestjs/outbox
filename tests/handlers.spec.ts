/**
 * The in-process transport: how `@OnOutboxMessage()` handlers are discovered, what they
 * receive, and how their outcomes become the publish's outcome.
 */
import { Controller, Injectable, Module, type Type } from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';
import { EMPTY, Observable, of, throwError } from 'rxjs';
import {
  InMemoryOutboxStore,
  NonRetryableMessageError,
  OnOutboxMessage,
  Outbox,
  OutboxDeadLetters,
  OutboxEvents,
  OutboxModule,
  OutboxNoHandlerError,
  OutboxRelay,
  OutboxStorage,
  type OutboxEvent,
  type OutboxHandlerContext,
  type OutboxModuleOptions,
} from '../lib/index.js';
import { registeredStore } from './helpers.js';

const calls: string[] = [];
const contexts: OutboxHandlerContext[] = [];

@Injectable()
class OrderHandlers {
  @OnOutboxMessage(['order.placed', 'order.cancelled'], { consumer: 'crm' })
  sync(payload: { orderId: number }, ctx: OutboxHandlerContext) {
    calls.push(`crm ${ctx.message.topic} ${payload.orderId}`);
    contexts.push(ctx);
  }

  @OnOutboxMessage('order.placed', { consumer: 'loyalty' })
  @OnOutboxMessage('order.refunded', { consumer: 'loyalty-refunds', inbox: false })
  points(payload: { orderId: number }, ctx: OutboxHandlerContext) {
    calls.push(`${ctx.consumer} ${ctx.message.topic} ${payload.orderId}`);
  }
}

@Controller()
class WebhooksController {
  @OnOutboxMessage('order.shipped', { consumer: 'webhooks' })
  notify(payload: { orderId: number }) {
    calls.push(`webhooks ${payload.orderId}`);
  }
}

describe('@OnOutboxMessage handlers', () => {
  let moduleRef: TestingModule | undefined;
  let store: InMemoryOutboxStore;
  let events: OutboxEvent[];

  beforeEach(() => {
    calls.length = 0;
    contexts.length = 0;
    store = new InMemoryOutboxStore();
  });
  afterEach(async () => {
    await moduleRef?.close();
    moduleRef = undefined;
  });

  async function boot(providers: any[], controllers: Type[] = [], options: OutboxModuleOptions = {}) {
    const ref = await Test.createTestingModule({
      imports: [registeredStore(store), OutboxModule.forRoot({ relay: { enabled: false }, ...options })],
      providers,
      controllers,
    }).compile();
    ref.useLogger(false);
    await ref.init();

    events = [];
    ref.get(OutboxEvents).events$.subscribe((e) => events.push(e));
    return (moduleRef = ref);
  }

  async function publish(ref: TestingModule, ...messages: { topic: string; payload: unknown }[]) {
    await store.transaction(async (tx) => ref.get(Outbox).add(tx, messages));
    return ref.get(OutboxRelay).runOnce();
  }

  it('runs a handler for each of its topics, stacked decorators, and handlers on controllers', async () => {
    const ref = await boot([OrderHandlers], [WebhooksController]);

    const result = await publish(
      ref,
      { topic: 'order.placed', payload: { orderId: 1 } },
      { topic: 'order.cancelled', payload: { orderId: 2 } },
      { topic: 'order.refunded', payload: { orderId: 3 } },
      { topic: 'order.shipped', payload: { orderId: 4 } },
    );

    expect(result).toMatchObject({ published: 4 });
    expect(calls.sort()).toEqual([
      'crm order.cancelled 2',
      'crm order.placed 1',
      'loyalty order.placed 1',
      'loyalty-refunds order.refunded 3',
      'webhooks 4',
    ]);
  });

  it('hands each handler its context: the message, its consumer name and the attempt number', async () => {
    const ref = await boot([OrderHandlers]);
    await publish(ref, { topic: 'order.cancelled', payload: { orderId: 9 } });

    const [ctx] = contexts;
    expect(ctx).toMatchObject({
      consumer: 'crm',
      attempt: 1,
      message: { topic: 'order.cancelled', payload: { orderId: 9 }, attempts: 0 },
    });
    expect(ctx!.signal).toBeInstanceOf(AbortSignal);
    expect(ctx!.signal.aborted).toBe(false);
  });

  it('records a delivery in the inbox of each consumer that keeps one, and not otherwise', async () => {
    const ref = await boot([OrderHandlers]);
    await publish(ref, { topic: 'order.placed', payload: { orderId: 1 } }, { topic: 'order.refunded', payload: { orderId: 1 } });

    const [placed] = contexts;
    const inbox = ref.get(OutboxStorage).inbox;
    expect(inbox.hasInbox('crm', placed!.message.id)).toBe(true);
    expect(inbox.hasInbox('loyalty', placed!.message.id)).toBe(true);

    const refunded = (events.find((e) => e.type === 'published' && e.message.topic === 'order.refunded') as {
      message: { id: string };
    }).message;
    expect(inbox.hasInbox('loyalty-refunds', refunded.id)).toBe(false);
  });

  it('runs a handler once when its provider is also registered under another token', async () => {
    const ref = await boot([OrderHandlers, { provide: 'ORDER_HANDLERS', useExisting: OrderHandlers }]);
    await publish(ref, { topic: 'order.cancelled', payload: { orderId: 5 } });
    expect(calls).toEqual(['crm order.cancelled 5']);
  });

  it('refuses two handlers with the same consumer name on one topic', async () => {
    @Injectable()
    class Duplicate {
      @OnOutboxMessage('order.placed', { consumer: 'crm' })
      again() {}
    }

    await expect(boot([OrderHandlers, Duplicate])).rejects.toThrow('Duplicate outbox consumer "crm" for topic "order.placed"');
  });

  it('refuses invalid topic or consumer options at decorator time', () => {
    expect(() => OnOutboxMessage('', { consumer: 'crm' })).toThrow(
      '@OnOutboxMessage("") needs a topic: a non-empty string or array of non-empty strings.',
    );
    expect(() => OnOutboxMessage([], { consumer: 'crm' })).toThrow(
      '@OnOutboxMessage([]) needs a topic: a non-empty string or array of non-empty strings.',
    );
    expect(() => OnOutboxMessage(['order.placed', ''], { consumer: 'crm' })).toThrow(
      '@OnOutboxMessage(["order.placed",""]) needs a topic: a non-empty string or array of non-empty strings.',
    );
    expect(() => OnOutboxMessage(null as any, { consumer: 'crm' })).toThrow(
      '@OnOutboxMessage(null) needs a topic: a non-empty string or array of non-empty strings.',
    );
    expect(() => OnOutboxMessage('order.placed', {} as any)).toThrow(
      '@OnOutboxMessage("order.placed") needs { consumer }',
    );
    expect(() => OnOutboxMessage('order.placed', { consumer: '' })).toThrow(
      '@OnOutboxMessage("order.placed") needs { consumer }',
    );
  });

  it('retries a message no handler in this process subscribes to (another version may)', async () => {
    const ref = await boot([OrderHandlers]);
    const result = await publish(ref, { topic: 'order.archived', payload: {} });

    expect(result).toMatchObject({ retried: 1, deadLettered: 0 });
    const [retry] = events;
    expect(retry).toMatchObject({ type: 'retry-scheduled', transport: 'local' });
    expect((retry as { error: unknown }).error).toBeInstanceOf(OutboxNoHandlerError);
    expect((retry as { error: OutboxNoHandlerError }).error.topic).toBe('order.archived');
  });

  describe('outcomes', () => {
    const behaviour: Record<string, () => unknown> = {};

    @Injectable()
    class Fanout {
      @OnOutboxMessage('fanout', { consumer: 'first', inbox: false })
      first() {
        return behaviour.first?.();
      }

      @OnOutboxMessage('fanout', { consumer: 'second', inbox: false })
      second() {
        return behaviour.second?.();
      }
    }

    beforeEach(() => {
      delete behaviour.first;
      delete behaviour.second;
    });

    it('fails the publish with the error of the only failing handler', async () => {
      const ref = await boot([Fanout], [], { retry: 5 });
      behaviour.second = () => {
        throw new TypeError('bad payload');
      };

      expect(await publish(ref, { topic: 'fanout', payload: {} })).toMatchObject({ retried: 1 });
      expect((events[0] as { error: unknown }).error).toBeInstanceOf(TypeError);
    });

    it('aggregates the errors of several failing handlers, retried while any is transient', async () => {
      const ref = await boot([Fanout], [], { retry: 5 });
      behaviour.first = () => {
        throw new NonRetryableMessageError('permanent');
      };
      behaviour.second = () => Promise.reject(new Error('transient'));

      expect(await publish(ref, { topic: 'fanout', payload: {} })).toMatchObject({ retried: 1 });
      const error = (events[0] as { error: AggregateError }).error;
      expect(error).toBeInstanceOf(AggregateError);
      expect(error.message).toBe('2 handlers failed for "fanout"');
      expect(error.errors.map(String)).toEqual(['NonRetryableMessageError: permanent', 'Error: transient']);
    });

    it('waits for the last value of an Observable and fails with its error', async () => {
      const ref = await boot([Fanout], [], { retry: 5 });
      let emitted = 0;
      behaviour.first = () =>
        new Observable((subscriber) => {
          subscriber.next(emitted++);
          subscriber.next(emitted++);
          subscriber.complete();
        });
      behaviour.second = () => EMPTY;

      expect(await publish(ref, { topic: 'fanout', payload: {} })).toMatchObject({ published: 1 });
      expect(emitted).toBe(2);

      behaviour.first = () => of(1);
      behaviour.second = () => throwError(() => new Error('stream failed'));
      expect(await publish(ref, { topic: 'fanout', payload: {} })).toMatchObject({ retried: 1 });
      expect(String((events.at(-1) as { error: unknown }).error)).toBe('Error: stream failed');
    });

    it('dead-letters with the aggregate when every failing handler is permanent', async () => {
      const ref = await boot([Fanout], [], { retry: 5 });
      behaviour.first = () => {
        throw new NonRetryableMessageError('unknown currency');
      };
      behaviour.second = () => {
        throw new NonRetryableMessageError('no such address');
      };

      expect(await publish(ref, { topic: 'fanout', payload: {} })).toMatchObject({ deadLettered: 1 });
      const [dead] = await ref.get(OutboxDeadLetters).list();
      expect(dead!.lastError).toBe(
        'NonRetryableMessageError: AggregateError: 2 handlers failed for "fanout" ' +
          '[NonRetryableMessageError: unknown currency; NonRetryableMessageError: no such address]',
      );
    });
  });
});
