/**
 * The MySQL project's global setup: with `SQL_TEST_MYSQL_URL`, sweeps the test databases that runs which crashed or
 * were interrupted left on the server, before the run and again after it (the workers have exited by then, so a file
 * that died before its `afterAll` is caught too). Only this helper's databases (`obx_<host>_`), and only those whose
 * process is gone (support/mysql.ts). An unreachable server is left to the tests, which skip with the reason.
 */
import { sweepStaleMysqlDatabases } from './mysql.js';

export default async function setup() {
  const url = process.env.SQL_TEST_MYSQL_URL;
  if (!url) {
    return;
  }

  const sweep = () => sweepStaleMysqlDatabases(url).catch(() => []);
  await sweep();
  return async () => {
    await sweep();
  };
}
