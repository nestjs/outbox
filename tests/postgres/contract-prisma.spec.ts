/**
 * The OutboxStore and OutboxInboxStore contracts (`@nestjs/outbox/testing`) on PostgresOutboxStore through fromPrisma()
 * on PostgreSQL, races included, with the transaction client of prisma.$transaction() as the application's transaction.
 */
import { describeContract, prismaClient, testDatabase } from './support.js';

const { database, reason } = await testDatabase('store_contract_prisma');

describeContract(`PostgresOutboxStore through ${prismaClient.name} on PostgreSQL`, async () => (database ? prismaClient.open(database.url) : null), 'nest_outbox', reason);
