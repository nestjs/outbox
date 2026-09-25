/**
 * Vitest's global setup: with `SQL_TEST_PG_URL`, sweeps the test databases that runs which
 * crashed or were interrupted left on the server, before the run and again after it (the
 * workers have exited by then, so a file that died before its `afterAll` is caught too). Only
 * this helper's databases, and only those whose process is gone (support/postgres.ts).
 */
import { sweepStaleDatabases } from './postgres.js';

export default async function setup() {
  const url = process.env.SQL_TEST_PG_URL;
  if (!url) {
    return;
  }

  await sweepStaleDatabases(url);
  return async () => {
    await sweepStaleDatabases(url);
  };
}
