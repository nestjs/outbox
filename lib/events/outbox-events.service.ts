import { Injectable, type OnApplicationShutdown } from '@nestjs/common';
import { Subject, type Observable } from 'rxjs';
import type { OutboxEvent } from './outbox-events.interface.js';
import { channels } from './outbox.channels.js';

/**
 * The relay's events in this application, for metrics and alerting. Every event is also
 * published on its `node:diagnostics_channel` channel (`nestjs:outbox:<type>`), where
 * instrumentation can subscribe without depending on Nest.
 */
@Injectable()
export class OutboxEvents implements OnApplicationShutdown {
  private readonly subject = new Subject<OutboxEvent>();
  readonly events$: Observable<OutboxEvent> = this.subject.asObservable();

  emit(event: OutboxEvent): void {
    const target = channels[event.type];
    if (target.hasSubscribers) {
      target.publish(event);
    }
    this.subject.next(event);
  }

  // After onModuleDestroy, where the relay drains and may still emit.
  onApplicationShutdown() {
    this.subject.complete();
  }
}
