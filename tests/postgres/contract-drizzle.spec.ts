/**
 * The OutboxStore and OutboxInboxStore contracts (`@nestjs/outbox/testing`) on PostgresOutboxStore through fromDrizzle():
 * on PostgreSQL (a pool of real connections, races included), and on PGlite (one connection, which serializes every
 * race).
 */
import { describeContract, drizzleClient, openPglite, testDatabase } from './support.js';

const { database, reason } = await testDatabase('store_contract_drizzle');

describeContract(`PostgresOutboxStore through ${drizzleClient.name} on PostgreSQL`, async () => (database ? drizzleClient.open(database.url) : null), 'nest_outbox', reason);

describeContract('PostgresOutboxStore through fromDrizzle (PGlite)', openPglite, 'nest_outbox');
