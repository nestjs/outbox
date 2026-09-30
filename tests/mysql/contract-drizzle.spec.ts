/**
 * The OutboxStore and OutboxInboxStore contracts (`@nestjs/outbox/testing`) on MySqlOutboxStore through fromDrizzle() on
 * MySQL, races included, with the tx of db.transaction() as the application's transaction.
 */
import { describeContract, drizzleClient, testDatabase } from './support.js';

const { database, reason } = await testDatabase('contract_drizzle');

describeContract(`MySqlOutboxStore through ${drizzleClient.name} on MySQL`, async () => (database ? drizzleClient.open(database.url) : null), 'nest_outbox', reason);
