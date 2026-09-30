/**
 * The OutboxStore and OutboxInboxStore contracts (`@nestjs/outbox/testing`) on MySqlOutboxStore through fromMysql2() on
 * MySQL, races included: a pool of real connections, and a connection after beginTransaction() as the application's
 * transaction.
 */
import { describeContract, mysql2Client, testDatabase } from './support.js';

const { database, reason } = await testDatabase('contract_mysql2');

describeContract(`MySqlOutboxStore through ${mysql2Client.name} on MySQL`, async () => (database ? mysql2Client.open(database.url) : null), 'nest_outbox', reason);
