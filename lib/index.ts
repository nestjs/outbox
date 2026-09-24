// Module and options
export { OutboxModule } from './outbox.module.js';
export {
  OUTBOX_MODULE_OPTIONS,
  type OutboxModuleAsyncOptions,
  type OutboxOptionsFactory,
} from './outbox.module-definition.js';
export type {
  OutboxBackoffOptions,
  OutboxModuleOptions,
  OutboxRelayOptions,
  OutboxRetryOptions,
} from './interfaces/index.js';
export type { Duration } from './utils/index.js';

// Producing: add messages inside your transaction
export { Outbox } from './outbox.service.js';
export type { NewOutboxMessage, OutboxMessage } from './interfaces/index.js';

// Consuming in-process
export * from './decorators/index.js';
export type { OutboxHandlerContext } from './interfaces/index.js';

// Consuming in another service: publish through a ClientProxy, dedupe with the inbox
export type { ClientProxyTransportOptions, OutboxEnvelope, OutboxInboxResult } from './interfaces/index.js';
export { OutboxInbox } from './services/index.js';

// Operating: the relay, dead letters, events
export { OutboxDeadLetters, OutboxRelay } from './services/index.js';
export type {
  OutboxDeadLetter,
  OutboxDeadLetterTarget,
  OutboxRelayRunResult,
  OutboxStats,
} from './interfaces/index.js';
export {
  OutboxEvents,
  type OutboxDeadLetteredEvent,
  type OutboxEvent,
  type OutboxLeaseLostEvent,
  type OutboxPublishedEvent,
  type OutboxRetryScheduledEvent,
} from './events/index.js';

// Errors
export * from './errors/index.js';

// Storage: implement OutboxStore (messages) and OutboxInboxStore (inbox) in a provider and
// register it with OutboxStorage
export { OutboxStorage } from './storage/index.js';
export type {
  OutboxAttempt,
  OutboxClaimRequest,
  OutboxDeadLetterFilter,
  OutboxDeadLetterQuery,
  OutboxDeadLetterReason,
  OutboxDeadLetterUpdate,
  OutboxInboxStore,
  OutboxRescheduleUpdate,
  OutboxStorageContract,
  OutboxStorageRegisterOptions,
  OutboxStorageSources,
  OutboxStore,
  OutboxStoreStats,
} from './interfaces/index.js';
// The default and test double. Production stores live on the app's database: the docs'
// Drizzle, TypeORM and Prisma recipes.
export * from './stores/index.js';

// Extension points: a transport for your broker, and the built-in ClientProxy one
export * from './transports/index.js';
