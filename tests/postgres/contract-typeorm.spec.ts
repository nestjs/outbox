/**
 * The OutboxStore and OutboxInboxStore contracts (`@nestjs/outbox/testing`) on PostgresOutboxStore through fromTypeOrm()
 * on PostgreSQL, races included, with the EntityManager of dataSource.transaction() as the application's transaction.
 */
import { describeContract, testDatabase, typeOrmClient } from './support.js';

const { database, reason } = await testDatabase('store_contract_typeorm');

describeContract(`PostgresOutboxStore through ${typeOrmClient.name} on PostgreSQL`, async () => (database ? typeOrmClient.open(database.url) : null), 'nest_outbox', reason);
