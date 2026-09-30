/**
 * The OutboxStore and OutboxInboxStore contracts (`@nestjs/outbox/testing`) on MySqlOutboxStore through fromKysely() on
 * MySQL, races included, with the trx of db.transaction().execute() as the application's transaction.
 */
import { describeContract, kyselyClient, testDatabase } from './support.js';

const { database, reason } = await testDatabase('contract_kysely');

describeContract(`MySqlOutboxStore through ${kyselyClient.name} on MySQL`, async () => (database ? kyselyClient.open(database.url) : null), 'nest_outbox', reason);
