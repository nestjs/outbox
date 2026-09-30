/**
 * The OutboxStore and OutboxInboxStore contracts (`@nestjs/outbox/testing`) on MySqlOutboxStore through fromTypeOrm() on
 * MySQL, races included, with the EntityManager of dataSource.transaction() as the application's transaction.
 */
import { describeContract, typeOrmClient, testDatabase } from './support.js';

const { database, reason } = await testDatabase('contract_typeorm');

describeContract(`MySqlOutboxStore through ${typeOrmClient.name} on MySQL`, async () => (database ? typeOrmClient.open(database.url) : null), 'nest_outbox', reason);
