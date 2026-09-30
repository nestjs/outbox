/**
 * Vitest global setup: generates the Prisma client of the Prisma store recipe and fromPrisma()'s PostgreSQL tests into
 * tests/fixtures/prisma/generated (gitignored), as `npx prisma generate` does in an app. Offline: Prisma 7's client
 * needs no engine download. The MySQL project generates its own (generate-prisma-mysql-client.ts) into its own folder,
 * so the projects' setups never write the same files.
 */
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

export function generatePrismaClient(config: string): void {
  // One command string: npm is npm.cmd on Windows, which needs a shell, and Node 24
  // deprecates passing an args array with `shell: true` (DEP0190).
  const result = spawnSync(`npm exec --silent -- prisma generate --config ${config}`, {
    cwd: fileURLToPath(new URL('../..', import.meta.url)),
    encoding: 'utf8',
    shell: true,
  });
  if (result.status !== 0) {
    throw new Error(`prisma generate failed:\n${result.stderr || result.stdout}`);
  }
}

export default function generatePostgresPrismaClient() {
  generatePrismaClient('tests/fixtures/prisma/prisma.config.ts');
}
