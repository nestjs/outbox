/**
 * Vitest global setup: generates the Prisma store recipe's client into
 * tests/fixtures/prisma/generated (gitignored), as `npx prisma generate` does in an app.
 * Offline: Prisma 7's client needs no engine download.
 */
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

export default function generatePrismaClient() {
  const result = spawnSync('npm', ['run', '--silent', 'generate:prisma'], {
    cwd: fileURLToPath(new URL('../..', import.meta.url)),
    encoding: 'utf8',
    shell: true,
  });
  if (result.status !== 0) {
    throw new Error(`prisma generate failed:\n${result.stderr || result.stdout}`);
  }
}
