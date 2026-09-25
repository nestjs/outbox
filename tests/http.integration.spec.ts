/**
 * The order API of the README on every documented store recipe, served over HTTP by Express
 * and by Fastify: a route writes the order and its message in one transaction of the app's
 * ORM, the relay delivers it to `@OnOutboxMessage()` handlers (one of them exactly once with
 * `ctx.processInTransaction()`), and the README's guarded admin controller lists, requeues
 * and purges dead letters.
 */
import {
  BadRequestException,
  Body,
  Controller,
  Delete,
  Get,
  Injectable,
  Module,
  NotFoundException,
  Param,
  Post,
  Query,
  UseGuards,
  type CallHandler,
  type CanActivate,
  type ExecutionContext,
  type INestApplication,
  type NestInterceptor,
} from '@nestjs/common';
import { AsyncLocalStorage } from 'node:async_hooks';
import { Observable } from 'rxjs';
import request from 'supertest';
import { adapters, createApp } from './support/adapters.js';
import { startPostgres } from './support/postgres.js';
import {
  NonRetryableMessageError,
  OnOutboxMessage,
  Outbox,
  OutboxDeadLetters,
  OutboxEvents,
  OutboxModule,
  OutboxRelay,
  type OutboxDeadLetter,
  type OutboxEvent,
  type OutboxHandlerContext,
} from '../lib/index.js';
import { until } from './helpers.js';
import { AppDatabase, diagnostics, recipes, titleOf, type RecipeDatabase } from './integration.js';

const { postgres, reason } = await startPostgres();
afterAll(() => postgres?.stop());

interface OrderPlaced {
  orderId: number;
}

/** Per-request state, as `AuthenticationContext` or `I18nContext` keep it. */
const requestContext = new AsyncLocalStorage<{ requestId: string }>();

const state = {
  calls: [] as Array<{ consumer: string; id: string; attempt: number; requestId?: string }>,
  invoicingFailures: 0,
  declined: false,
  gatewayDown: false,
};

const callsOf = (consumer: string) => state.calls.filter((call) => call.consumer === consumer);
const record = (consumer: string, ctx: OutboxHandlerContext) =>
  state.calls.push({ consumer, id: ctx.message.id, attempt: ctx.attempt, requestId: requestContext.getStore()?.requestId });

/** Runs each request in its own async context. */
class RequestContextInterceptor implements NestInterceptor {
  private requests = 0;

  intercept(_context: ExecutionContext, next: CallHandler) {
    const store = { requestId: `request-${++this.requests}` };
    return new Observable((subscriber) => requestContext.run(store, () => next.handle().subscribe(subscriber)));
  }
}

@Injectable()
class OrderHandlers {
  constructor(private readonly appDatabase: AppDatabase) {}

  @OnOutboxMessage('order.placed', { consumer: 'billing' })
  bill(_order: OrderPlaced, ctx: OutboxHandlerContext) {
    record('billing', ctx);
  }

  /** Exactly once: the inbox record commits with the invoice. */
  @OnOutboxMessage('order.placed', { consumer: 'invoicing' })
  async invoice(order: OrderPlaced, ctx: OutboxHandlerContext) {
    record('invoicing', ctx);
    await this.appDatabase.transaction(async (tx) =>
      ctx.processInTransaction(tx, async () => {
        await this.appDatabase.insertInvoice(tx, order.orderId, 'invoicing');
        if (state.invoicingFailures > 0) {
          state.invoicingFailures--;
          throw new Error('invoice numbering service down');
        }
      }),
    );
  }

  @OnOutboxMessage('order.placed', { consumer: 'audit', inbox: false })
  audit(_order: OrderPlaced, ctx: OutboxHandlerContext) {
    record('audit', ctx);
  }

  @OnOutboxMessage('payment.capture', { consumer: 'payments' })
  capture(_payment: OrderPlaced, ctx: OutboxHandlerContext) {
    record('payments', ctx);
    if (state.declined) {
      throw new NonRetryableMessageError('card declined');
    }
    if (state.gatewayDown) {
      throw new Error('payment gateway down');
    }
  }
}

@Controller()
class OrdersController {
  constructor(
    private readonly appDatabase: AppDatabase,
    private readonly outbox: Outbox,
  ) {}

  @Post('orders')
  async place(@Body() body: { id: number; decline?: boolean }) {
    await this.appDatabase.transaction(async (tx) => {
      await this.appDatabase.insertOrder(tx, body.id);
      await this.outbox.add(tx, { topic: 'order.placed', key: `customer-${body.id}`, payload: { orderId: body.id } });
      if (body.decline) {
        throw new BadRequestException('payment declined');
      }
    });

    this.outbox.notify();
    return { id: body.id, requestId: requestContext.getStore()?.requestId };
  }

  @Post('orders/outside-transaction')
  async placeOutsideTransaction(@Body() body: { id: number }) {
    await this.outbox.add(this.appDatabase.root, { topic: 'order.placed', payload: { orderId: body.id } });
    return { id: body.id };
  }

  @Post('payments')
  async pay(@Body() body: { id: number }) {
    const [message] = await this.appDatabase.transaction((tx) =>
      Promise.resolve(this.outbox.add(tx, [{ topic: 'payment.capture', payload: { orderId: body.id } }])),
    );

    this.outbox.notify();
    return { messageId: message!.id };
  }
}

@Injectable()
class AdminGuard implements CanActivate {
  canActivate(context: ExecutionContext) {
    return context.switchToHttp().getRequest().headers['x-admin-token'] === 'let-me-in';
  }
}

/** The README's admin recipe: the app owns the routes and the guard. */
@Controller('admin/outbox')
@UseGuards(AdminGuard)
class OutboxAdminController {
  constructor(
    private readonly outboxDeadLetters: OutboxDeadLetters,
    private readonly outboxRelay: OutboxRelay,
  ) {}

  @Get('dead-letters')
  list(@Query('topic') topic?: string) {
    return this.outboxDeadLetters.list({ topic });
  }

  @Get('dead-letters/:id')
  async get(@Param('id') id: string) {
    const deadLetter = await this.outboxDeadLetters.get(id);
    if (!deadLetter) {
      throw new NotFoundException();
    }
    return deadLetter;
  }

  @Post('dead-letters/:id/requeue')
  async requeue(@Param('id') id: string) {
    return { requeued: await this.outboxDeadLetters.requeue(id) };
  }

  @Delete('dead-letters/:id')
  async purge(@Param('id') id: string) {
    return { purged: await this.outboxDeadLetters.purge(id) };
  }

  @Get('stats')
  stats() {
    return this.outboxRelay.stats();
  }
}

for (const recipe of recipes(postgres, reason, 'outbox_http')) {
  describe.skipIf(recipe.skip)(`Outbox over HTTP: ${titleOf(recipe)}`, () => {
    let database: RecipeDatabase;

    beforeAll(async () => {
      database = await recipe.open('orders');
    }, 60_000); // PGlite loads its WASM on first open: slow under a parallel run
    afterAll(() => database?.close());

    describe.each(adapters)('on $name', ({ name: adapter }) => {
      let app: INestApplication;
      let events: OutboxEvent[];
      let channels: Awaited<ReturnType<typeof diagnostics>>;

      const http = () => request(app.getHttpServer());
      const admin = (test: request.Test) => test.set('x-admin-token', 'let-me-in');
      const settled = () => until(async () => (await app.get(OutboxRelay).stats()).pending === 0, 10_000);
      const eventsOf = (id: string) => events.filter((event) => event.message.id === id);

      beforeAll(async () => {
        @Module({
          imports: [
            database.module(),
            OutboxModule.forRoot({
              relay: { pollInterval: '50ms', lease: '5s' },
              retry: { attempts: 3, backoff: { delay: '20ms', factor: 2, jitter: 'none' } },
            }),
          ],
          controllers: [OrdersController, OutboxAdminController],
          providers: [OrderHandlers, AdminGuard],
        })
        class AppModule {}

        app = await createApp(adapter, AppModule, {
          setup: (app) => {
            app.useLogger(false);
            app.useGlobalInterceptors(new RequestContextInterceptor());
          },
        });
        events = [];
        app.get(OutboxEvents).events$.subscribe((event) => events.push(event));
        channels = await diagnostics();
      }, 30_000);
      afterAll(async () => {
        channels?.stop();
        await app?.close();
      });

      beforeEach(async () => {
        await database.reset();
        events.length = 0;
        channels.received.length = 0;
        state.calls = [];
        state.invoicingFailures = 0;
        state.declined = false;
        state.gatewayDown = false;
      });

      it('commits the order and its message in one transaction; every handler runs once', async () => {
        const { body: order } = await http().post('/orders').send({ id: 1 }).expect(201);
        expect(order).toEqual({ id: 1, requestId: expect.stringMatching(/^request-/) });
        await until(() => callsOf('billing').length + callsOf('invoicing').length + callsOf('audit').length === 3);
        await settled();

        const [billing] = callsOf('billing');
        expect(billing).toMatchObject({ attempt: 1 });
        expect(await database.count('it_orders')).toBe(1);
        expect(await database.count('it_invoices', 'invoicing')).toBe(1);
        expect(await database.count('outbox_inbox', 'billing')).toBe(1);
        expect(await database.count('outbox_inbox', 'invoicing')).toBe(1);
        expect(await database.count('outbox_inbox', 'audit')).toBe(0);

        expect(eventsOf(billing!.id)).toEqual([
          expect.objectContaining({
            type: 'published',
            transport: 'local',
            message: expect.objectContaining({ topic: 'order.placed', key: 'customer-1', payload: { orderId: 1 } }),
          }),
        ]);
        expect(channels.of(billing!.id)).toEqual(['nestjs:outbox:published']);
      });

      it("runs handlers in the relay's async context, never in the request that committed and notified", async () => {
        const { body: order } = await http().post('/orders').send({ id: 8 }).expect(201);
        await until(() => callsOf('billing').length === 1);
        await settled();

        expect(order.requestId).toBeDefined();
        expect(callsOf('billing')[0]!.requestId).toBeUndefined();
      });

      it('leaves neither the order nor its message when the request fails inside the transaction', async () => {
        await http().post('/orders').send({ id: 2, decline: true }).expect(400);
        await new Promise((resolve) => setTimeout(resolve, 200)); // a few polls

        expect(await database.count('it_orders')).toBe(0);
        expect(state.calls).toEqual([]);
        expect(events).toEqual([]);
        const { body } = await admin(http().get('/admin/outbox/stats')).expect(200);
        expect(body).toMatchObject({ pending: 0, deadLetters: 0 });
      });

      it.runIf(recipe.refusesRoot)('refuses the database itself as a transaction handle', async () => {
        await http().post('/orders/outside-transaction').send({ id: 3 }).expect(500);

        const { body } = await admin(http().get('/admin/outbox/stats')).expect(200);
        expect(body).toMatchObject({ pending: 0 });
      });

      it('retries only the consumer that failed: the inbox skips the ones that succeeded', async () => {
        state.invoicingFailures = 1;
        await http().post('/orders').send({ id: 4 }).expect(201);
        await until(() => callsOf('invoicing').length === 2);
        await settled();

        expect(callsOf('invoicing').map((call) => call.attempt)).toEqual([1, 2]);
        expect(callsOf('billing').map((call) => call.attempt)).toEqual([1]);
        expect(callsOf('audit').map((call) => call.attempt)).toEqual([1, 2]); // inbox: false
        // The failed attempt's invoice rolled back with its inbox record.
        expect(await database.count('it_invoices', 'invoicing')).toBe(1);

        const id = callsOf('invoicing')[0]!.id;
        expect(eventsOf(id).map((event) => event.type)).toEqual(['retry-scheduled', 'published']);
        expect(eventsOf(id)[0]).toMatchObject({ attempt: 1, delayMs: 20, transport: 'local' });
        expect(channels.of(id)).toEqual(['nestjs:outbox:retry-scheduled', 'nestjs:outbox:published']);
      });

      it('dead-letters a rejected message; the guarded admin API lists, requeues under the same id and purges it', async () => {
        state.declined = true;
        const { body: payment } = await http().post('/payments').send({ id: 5 }).expect(201);
        await until(() => eventsOf(payment.messageId).some((event) => event.type === 'dead-lettered'));

        await http().get('/admin/outbox/dead-letters').expect(403);
        const { body: listed } = await admin(http().get('/admin/outbox/dead-letters').query({ topic: 'payment.capture' })).expect(200);
        expect(listed).toEqual([
          expect.objectContaining({ id: payment.messageId, topic: 'payment.capture', reason: 'rejected', attempts: 1 }),
        ]);
        const { body: unfiltered } = await admin(http().get('/admin/outbox/dead-letters')).expect(200);
        expect(unfiltered.map((deadLetter: OutboxDeadLetter) => deadLetter.id)).toEqual([payment.messageId]);
        await admin(http().get('/admin/outbox/dead-letters').query({ topic: 'order.placed' })).expect(200, []);

        const { body: deadLetter } = await admin(http().get(`/admin/outbox/dead-letters/${payment.messageId}`)).expect(200);
        expect(deadLetter).toMatchObject({
          payload: { orderId: 5 },
          lastError: 'NonRetryableMessageError: card declined',
          history: [expect.objectContaining({ attempt: 1, transport: 'local', error: 'NonRetryableMessageError: card declined' })],
        });
        expect(channels.of(payment.messageId)).toEqual(['nestjs:outbox:dead-lettered']);
        expect((await admin(http().get('/admin/outbox/stats')).expect(200)).body).toMatchObject({ pending: 0, deadLetters: 1 });

        // The card was updated: requeue it. It goes out again under the same id, with a fresh budget.
        state.declined = false;
        await admin(http().post(`/admin/outbox/dead-letters/${payment.messageId}/requeue`)).expect(201, { requeued: 1 });
        await until(() => callsOf('payments').length === 2);
        await settled();
        expect(callsOf('payments').map((call) => [call.id, call.attempt])).toEqual([
          [payment.messageId, 1],
          [payment.messageId, 1],
        ]);
        await admin(http().get('/admin/outbox/dead-letters')).expect(200, []);
        await admin(http().get(`/admin/outbox/dead-letters/${payment.messageId}`)).expect(404);

        // Another one fails for good and is purged.
        state.declined = true;
        const { body: second } = await http().post('/payments').send({ id: 6 }).expect(201);
        await until(() => eventsOf(second.messageId).some((event) => event.type === 'dead-lettered'));
        await admin(http().delete(`/admin/outbox/dead-letters/${second.messageId}`)).expect(200, { purged: 1 });
        await admin(http().post(`/admin/outbox/dead-letters/${second.messageId}/requeue`)).expect(201, { requeued: 0 });
        expect((await admin(http().get('/admin/outbox/stats')).expect(200)).body).toMatchObject({ pending: 0, deadLetters: 0 });
      });

      it('dead-letters a message after its retry budget, backing off between attempts', async () => {
        state.gatewayDown = true;
        const { body: payment } = await http().post('/payments').send({ id: 7 }).expect(201);
        await until(() => eventsOf(payment.messageId).some((event) => event.type === 'dead-lettered'));

        expect(callsOf('payments').map((call) => call.attempt)).toEqual([1, 2, 3]);
        expect(
          eventsOf(payment.messageId).map((event) => [event.type, 'delayMs' in event ? event.delayMs : undefined]),
        ).toEqual([
          ['retry-scheduled', 20],
          ['retry-scheduled', 40],
          ['dead-lettered', undefined],
        ]);
        expect(eventsOf(payment.messageId)[2]).toMatchObject({ reason: 'exhausted', attempt: 3, transport: 'local' });
        expect(channels.of(payment.messageId)).toEqual([
          'nestjs:outbox:retry-scheduled',
          'nestjs:outbox:retry-scheduled',
          'nestjs:outbox:dead-lettered',
        ]);

        const { body: deadLetter } = await admin(http().get(`/admin/outbox/dead-letters/${payment.messageId}`)).expect(200);
        expect(deadLetter).toMatchObject({ reason: 'exhausted', attempts: 3 });
        expect(deadLetter.history.map((attempt: { attempt: number }) => attempt.attempt)).toEqual([1, 2, 3]);
      });
    });
  });
}
