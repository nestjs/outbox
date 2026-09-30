/**
 * The OutboxStore and OutboxInboxStore contracts (`@nestjs/outbox/testing`) on PostgresOutboxStore through fromKysely()
 * on PostgreSQL, races included, with the trx of db.transaction().execute() as the application's transaction.
 */
import { describeContract, kyselyClient, testDatabase } from './support.js';

const { database, reason } = await testDatabase('store_contract_kysely');

describeContract(`PostgresOutboxStore through ${kyselyClient.name} on PostgreSQL`, async () => (database ? kyselyClient.open(database.url) : null), 'nest_outbox', reason);
