import type {
  OnOutboxMessageOptions,
  OutboxHandlerMetadata,
} from '../interfaces/outbox-handler.interface.js';
import { OUTBOX_HANDLER_METADATA } from '../outbox.constants.js';

/**
 * Runs the method for every outbox message published on `topic` to the in-process
 * (`local`) transport. The method receives `(payload, context)`. Stackable.
 *
 * ```ts
 * @OnOutboxMessage('order.placed', { consumer: 'order-confirmation-email' })
 * sendConfirmation(order: Order, ctx: OutboxHandlerContext) { ... }
 * ```
 */
export function OnOutboxMessage(
  topic: string | string[],
  options: OnOutboxMessageOptions,
): MethodDecorator {
  const topics = Array.isArray(topic) ? topic : [topic];
  if (
    topics.length === 0 ||
    topics.some((t) => typeof t !== 'string' || t === '')
  ) {
    throw new TypeError(
      `@OnOutboxMessage(${JSON.stringify(topic)}) needs a topic: a non-empty string or array of non-empty strings.`,
    );
  }

  if (typeof options?.consumer !== 'string' || options.consumer === '') {
    throw new TypeError(
      `@OnOutboxMessage(${JSON.stringify(topic)}) needs { consumer }: the name its inbox ` +
        'entries are recorded under. Keep it stable across renames and deploys.',
    );
  }

  return (_target, _key, descriptor) => {
    const handler = descriptor.value as object;
    const existing: OutboxHandlerMetadata[] =
      Reflect.getMetadata(OUTBOX_HANDLER_METADATA, handler) ?? [];
    Reflect.defineMetadata(
      OUTBOX_HANDLER_METADATA,
      [...existing, { ...options, topics }],
      handler,
    );

    return descriptor;
  };
}
