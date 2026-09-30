/**
 * The OutboxStore and OutboxInboxStore contracts (`@nestjs/outbox/testing`) on MySqlOutboxStore through fromPrisma() on
 * MySQL, races included, with the transaction client of prisma.$transaction() as the application's transaction.
 */
import { describeContract, prismaClient, testDatabase } from './support.js';

const { database, reason } = await testDatabase('contract_prisma');

describeContract(`MySqlOutboxStore through ${prismaClient.name} on MySQL`, async () => (database ? prismaClient.open(database.url) : null), 'nest_outbox', reason);
