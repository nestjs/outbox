/**
 * Vitest global setup of the MySQL project: generates the Prisma client that fromPrisma()'s MySQL tests and the MySQL
 * integration recipe run on into tests/fixtures/prisma-mysql/generated (gitignored).
 */
import { generatePrismaClient } from './generate-prisma-client.js';

export default function generateMysqlPrismaClient() {
  generatePrismaClient('tests/fixtures/prisma-mysql/prisma.config.ts');
}
