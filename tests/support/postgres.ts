/**
 * A throwaway PostgreSQL cluster for tests: `initdb` into a temporary directory, started with
 * `pg_ctl` on a random loopback port, stopped and deleted afterwards. Uses the locally
 * installed binaries (`SQL_TEST_PG_BIN`, else `/usr/local/bin`, else `PATH`); nothing is
 * downloaded. Resolves to `null`, with the reason, when they are missing or don't run, so a
 * suite can skip with a clear message instead of failing.
 */
import { execFileSync, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { chmodSync, copyFileSync, existsSync, mkdirSync, mkdtempSync, readdirSync, realpathSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { hostname, tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

export interface TestPostgres {
  port: number;
  /** `postgres://postgres@127.0.0.1:<port>/<database>` */
  url(database?: string): string;
  /** Creates a database (dropping a previous one with the same name) and returns its URL. */
  createDatabase(name: string): Promise<string>;
  /** Stops the throwaway cluster, or drops this process's databases on an external server. Await it. */
  stop(): Promise<void>;
}

export type TestPostgresResult = { postgres: TestPostgres; reason?: undefined } | { postgres: null; reason: string };

export async function startPostgres(): Promise<TestPostgresResult> {
  if (process.env.SQL_TEST_PG_URL) {
    return externalPostgres(process.env.SQL_TEST_PG_URL);
  }
  const bin = findBinDir();
  if (!bin) {
    return { postgres: null, reason: 'initdb/pg_ctl/postgres not found (set SQL_TEST_PG_BIN)' };
  }
  const runnable = runnableBinDir(bin);
  if ('reason' in runnable) {
    return { postgres: null, reason: runnable.reason };
  }

  const dir = mkdtempSync(join(tmpdir(), 'nest-sql-pg-'));
  const data = join(dir, 'data');
  const port = await freePort();
  try {
    execFileSync(join(runnable.dir, 'initdb'), ['-D', data, '-U', 'postgres', '-A', 'trust', '--no-sync', '--encoding=UTF8', '--locale=C'], {
      stdio: 'pipe',
      env: runnable.env,
    });
    const options = [
      `-p ${port}`,
      '-c listen_addresses=127.0.0.1',
      "-c unix_socket_directories=''",
      '-c fsync=off',
      '-c synchronous_commit=off',
      '-c full_page_writes=off',
      '-c max_connections=200',
    ].join(' ');
    execFileSync(join(runnable.dir, 'pg_ctl'), ['-D', data, '-o', options, '-l', join(dir, 'server.log'), '-w', '-t', '30', 'start'], {
      stdio: 'pipe',
      env: runnable.env,
    });
  } catch (error) {
    rmSync(dir, { recursive: true, force: true });
    runnable.dispose();
    const stderr = (error as { stderr?: Buffer }).stderr?.toString().trim();
    return { postgres: null, reason: `could not start a throwaway cluster: ${stderr || (error as Error).message}` };
  }

  const url = (database = 'postgres') => `postgres://postgres@127.0.0.1:${port}/${database}`;
  let stopped = false;
  // Synchronous, so it also runs from the 'exit' hook below.
  const stop = () => {
    if (stopped) {
      return;
    }
    stopped = true;
    spawnSync(join(runnable.dir, 'pg_ctl'), ['-D', data, '-m', 'immediate', '-w', 'stop'], { stdio: 'pipe', env: runnable.env });
    rmSync(dir, { recursive: true, force: true });
    runnable.dispose();
  };
  // A crashed or interrupted test run must not leave a server behind.
  process.once('exit', stop);

  return {
    postgres: {
      port,
      url,
      stop: async () => stop(),
      async createDatabase(name) {
        const { default: pg } = await import('pg');
        const client = new pg.Client({ connectionString: url() });
        await client.connect();
        try {
          await client.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
          await client.query(`CREATE DATABASE "${name}"`);
        } finally {
          await client.end();
        }
        return url(name);
      },
    },
  };
}

function findBinDir(): string | undefined {
  const candidates = [process.env.SQL_TEST_PG_BIN, '/usr/local/bin', '/opt/homebrew/bin', '/usr/lib/postgresql/16/bin', '/usr/lib/postgresql/15/bin', '/usr/lib/postgresql/14/bin'];
  for (const dir of candidates) {
    if (dir && ['initdb', 'pg_ctl', 'postgres'].every((name) => existsSync(join(dir, name)))) {
      return dir;
    }
  }
  const which = spawnSync('which', ['initdb'], { encoding: 'utf8' });
  return which.status === 0 ? dirname(which.stdout.trim()) : undefined;
}

/**
 * The directory to run `initdb` and `pg_ctl` from. Usually `bin` itself. A Homebrew
 * `postgresql@14` linked against an ICU that Homebrew has since upgraded fails with
 * "Library not loaded: .../libicui18n.72.dylib" while the old ICU is still in the Cellar;
 * then this builds a directory whose `postgres` is a wrapper that points the dynamic loader
 * at it. `initdb` and `pg_ctl` start `postgres` through `/bin/sh`, which drops `DYLD_*`
 * variables, so exporting the variable from here would not reach it.
 */
function runnableBinDir(bin: string): { dir: string; env: NodeJS.ProcessEnv; dispose(): void } | { reason: string } {
  const env = { ...process.env, LC_ALL: 'C' };
  const probe = spawnSync(join(bin, 'postgres'), ['-V'], { encoding: 'utf8', env });
  if (probe.status === 0) {
    return { dir: bin, env, dispose: () => {} };
  }

  const missing = /Library not loaded: (\S+)/.exec(probe.stderr ?? '')?.[1];
  const libraryDir = missing && findLibrary(missing.split('/').pop()!);
  if (!libraryDir) {
    return { reason: `${join(bin, 'postgres')} -V failed: ${(probe.stderr || probe.error?.message || '').trim().split('\n')[0]}` };
  }
  const real = realpathSync(join(bin, 'postgres'));
  const prefix = dirname(dirname(real));
  const wrapper = mkdtempSync(join(tmpdir(), 'nest-sql-pgbin-'));
  mkdirSync(join(wrapper, 'bin'));
  // initdb and pg_ctl find `postgres` next to their own resolved path, and share/ and lib/
  // one level up: copies (not symlinks) in bin/, symlinks for the rest.
  for (const name of ['initdb', 'pg_ctl']) {
    copyFileSync(realpathSync(join(bin, name)), join(wrapper, 'bin', name));
  }
  for (const name of ['share', 'lib']) {
    symlinkSync(join(prefix, name), join(wrapper, name));
  }
  const variable = process.platform === 'darwin' ? 'DYLD_LIBRARY_PATH' : 'LD_LIBRARY_PATH';
  writeFileSync(join(wrapper, 'bin', 'postgres'), `#!/bin/sh\n${variable}='${libraryDir}' exec '${real}' "$@"\n`);
  chmodSync(join(wrapper, 'bin', 'postgres'), 0o755);
  const dispose = () => rmSync(wrapper, { recursive: true, force: true });
  process.once('exit', dispose);
  const retry = spawnSync(join(wrapper, 'bin', 'postgres'), ['-V'], { encoding: 'utf8', env });
  if (retry.status !== 0) {
    dispose();
    return { reason: `postgres doesn't run: ${(retry.stderr ?? '').trim().split('\n')[0]}` };
  }
  return { dir: join(wrapper, 'bin'), env, dispose };
}

function findLibrary(file: string): string | undefined {
  for (const cellar of ['/usr/local/Cellar', '/opt/homebrew/Cellar']) {
    if (!existsSync(cellar)) {
      continue;
    }
    for (const formula of readdirSync(cellar)) {
      const formulaDir = join(cellar, formula);
      for (const version of safeReaddir(formulaDir)) {
        const lib = join(formulaDir, version, 'lib');
        if (existsSync(join(lib, file))) {
          return lib;
        }
      }
    }
  }
  return undefined;
}

function safeReaddir(dir: string): string[] {
  try {
    return readdirSync(dir);
  } catch {
    return [];
  }
}

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address() as { port: number };
      server.close(() => resolve(port));
    });
  });
}

/**
 * `SQL_TEST_PG_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres` runs the Postgres
 * targets on an existing server instead of a throwaway cluster, e.g. a local container.
 * Several test files share it, so every database this helper creates is named
 * `<prefix><name>_<pid>_<random>`; `url(name)` and `createDatabase(name)` agree on it.
 *
 * Nothing is dropped except this helper's own databases: `stop()` (awaited in `afterAll`) drops
 * the ones this process created, and `sweepStaleDatabases()` (at a process's first
 * `createDatabase()`) drops those under `databasePrefix` whose process is gone: a run that
 * crashed or was interrupted before its `afterAll`.
 */
function externalPostgres(connectionString: string): TestPostgresResult {
  const base = new URL(connectionString);
  const suffix = `_${process.pid}_${Math.random().toString(36).slice(2, 8)}`;
  const created: string[] = [];
  let swept: Promise<unknown> | undefined;
  const url = (database = 'postgres') => {
    const target = new URL(base);
    target.pathname = `/${database === 'postgres' ? base.pathname.slice(1) || 'postgres' : databasePrefix + database + suffix}`;
    return target.toString();
  };
  return {
    postgres: {
      port: Number(base.port || 5432),
      url,
      async stop() {
        const names = created.splice(0);
        if (names.length === 0) {
          return;
        }

        await withClient(connectionString, async (client) => {
          for (const name of names) {
            await client.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
          }
        });
      },
      async createDatabase(name) {
        await (swept ??= sweepStaleDatabases(connectionString));
        const database = databasePrefix + name + suffix;
        await withClient(connectionString, async (client) => {
          await client.query(`DROP DATABASE IF EXISTS "${database}" WITH (FORCE)`);
          await client.query(`CREATE DATABASE "${database}"`);
        });
        created.push(database);
        return url(name);
      },
    },
  };
}

/**
 * What every database this helper creates on an external server starts with: the package, and
 * this host (a server shared by several machines never sees one sweep another's databases).
 */
export const databasePrefix = `obx_${createHash('sha256').update(hostname()).digest('hex').slice(0, 6)}_`;

/**
 * Drops this helper's databases (`databasePrefix`) whose process is no longer alive. Never
 * touches any other database, nor one whose process still runs.
 */
export async function sweepStaleDatabases(connectionString: string): Promise<string[]> {
  return withClient(connectionString, async (client) => {
    const { rows } = await client.query<{ datname: string }>('SELECT datname FROM pg_database WHERE left(datname, $1) = $2', [
      databasePrefix.length,
      databasePrefix,
    ]);
    const dropped: string[] = [];
    for (const { datname } of rows) {
      const pid = Number(/_(\d+)_[a-z0-9]+$/.exec(datname)?.[1]);
      if (!Number.isSafeInteger(pid) || pid === process.pid || isAlive(pid)) {
        continue;
      }

      // Another process sweeping at the same time may drop it first.
      await client.query(`DROP DATABASE IF EXISTS "${datname}" WITH (FORCE)`).then(
        () => dropped.push(datname),
        () => undefined,
      );
    }
    return dropped;
  });
}

function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    // EPERM: it exists, it's just not ours to signal.
    return (error as NodeJS.ErrnoException).code === 'EPERM';
  }
}

async function withClient<T>(connectionString: string, work: (client: import('pg').Client) => Promise<T>): Promise<T> {
  const { default: pg } = await import('pg');
  const client = new pg.Client({ connectionString });
  await client.connect();
  try {
    return await work(client);
  } finally {
    await client.end();
  }
}
