import { channel, type Channel } from 'node:diagnostics_channel';
import type { OutboxEvent } from './outbox-events.interface.js';

export const channels: Record<OutboxEvent['type'], Channel> = {
  published: channel('nestjs:outbox:published'),
  'retry-scheduled': channel('nestjs:outbox:retry-scheduled'),
  'dead-lettered': channel('nestjs:outbox:dead-lettered'),
  'lease-lost': channel('nestjs:outbox:lease-lost'),
};
