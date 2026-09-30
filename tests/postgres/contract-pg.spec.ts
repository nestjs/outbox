/**
 * The OutboxStore and OutboxInboxStore contracts (`@nestjs/outbox/testing`) on PostgresOutboxStore through fromPg() on
 * PostgreSQL, races included: a pool of real connections, and a client after BEGIN as the application's transaction.
 */
import { describeContract, pgClient, testDatabase } from './support.js';

const { database, reason } = await testDatabase('store_contract_pg');

describeContract(`PostgresOutboxStore through ${pgClient.name} on PostgreSQL`, async () => (database ? pgClient.open(database.url) : null), 'nest_outbox', reason);
