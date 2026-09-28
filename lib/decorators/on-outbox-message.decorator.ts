import type {
  OnOutboxMessageOptions,
  OutboxHandlerMetadata,
} from '../interfaces/outbox-handler.interface.js';
import { OUTBOX_HANDLER_METADATA } from '../outbox.constants.js';
import { describeValue } from '../utils/backoff.util.js';
import { isName } from '../utils/name.util.js';

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
  // A copy: later changes to the caller's array can't skip the checks below.
  const topics = Array.isArray(topic) ? [...topic] : [topic];
  if (topics.length === 0 || !topics.every(isName)) {
    throw new TypeError(
      `@OnOutboxMessage(${label(topic)}) needs a topic: a non-empty string or array of non-empty strings.`,
    );
  }
  const repeated = topics.find((t, i) => topics.indexOf(t) !== i);
  if (repeated !== undefined) {
    throw new TypeError(`@OnOutboxMessage(${label(topic)}) lists "${repeated}" more than once.`);
  }

  if (!isName(options?.consumer)) {
    throw new TypeError(
      `@OnOutboxMessage(${label(topic)}) needs { consumer }: the name its inbox ` +
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

/** The `topic` argument as the error shows it; `JSON.stringify()` throws on a BigInt or a cycle. */
function label(topic: unknown): string {
  try {
    return JSON.stringify(topic) ?? describeValue(topic);
  } catch {
    return describeValue(topic);
  }
}
