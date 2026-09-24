/**
 * A throwaway PostgreSQL cluster for tests: `initdb` into a temporary directory, started with
 * `pg_ctl` on a random loopback port, stopped and deleted afterwards. Uses the locally
 * installed binaries (`SQL_TEST_PG_BIN`, else `/usr/local/bin`, else `PATH`); nothing is
 * downloaded. Resolves to `null`, with the reason, when they are missing or don't run, so a
 * suite can skip with a clear message instead of failing.
 */
import { execFileSync, spawnSync } from 'node:child_process';
import { chmodSync, copyFileSync, existsSync, mkdirSync, mkdtempSync, readdirSync, realpathSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

export interface TestPostgres {
  port: number;
  /** `postgres://postgres@127.0.0.1:<port>/<database>` */
  url(database?: string): string;
  /** Creates a database (dropping a previous one with the same name) and returns its URL. */
  createDatabase(name: string): Promise<string>;
  stop(): void;
}

export type TestPostgresResult = { postgres: TestPostgres; reason?: undefined } | { postgres: null; reason: string };

export async function startPostgres(): Promise<TestPostgresResult> {
  if (process.env.SQL_TEST_PG_URL) return externalPostgres(process.env.SQL_TEST_PG_URL);
  const bin = findBinDir();
  if (!bin) return { postgres: null, reason: 'initdb/pg_ctl/postgres not found (set SQL_TEST_PG_BIN)' };
  const runnable = runnableBinDir(bin);
  if ('reason' in runnable) return { postgres: null, reason: runnable.reason };

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
  const stop = () => {
    if (stopped) return;
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
      stop,
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
    if (dir && ['initdb', 'pg_ctl', 'postgres'].every((name) => existsSync(join(dir, name)))) return dir;
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
  if (probe.status === 0) return { dir: bin, env, dispose: () => {} };

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
  for (const name of ['initdb', 'pg_ctl']) copyFileSync(realpathSync(join(bin, name)), join(wrapper, 'bin', name));
  for (const name of ['share', 'lib']) symlinkSync(join(prefix, name), join(wrapper, name));
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
    if (!existsSync(cellar)) continue;
    for (const formula of readdirSync(cellar)) {
      const formulaDir = join(cellar, formula);
      for (const version of safeReaddir(formulaDir)) {
        const lib = join(formulaDir, version, 'lib');
        if (existsSync(join(lib, file))) return lib;
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
 * Several test files share it, so every database gets a per-process suffix; `url(name)` and
 * `createDatabase(name)` agree on it, and nothing is dropped except this process's databases.
 */
function externalPostgres(connectionString: string): TestPostgresResult {
  const base = new URL(connectionString);
  const suffix = `_${process.pid}_${Math.random().toString(36).slice(2, 8)}`;
  const created: string[] = [];
  const url = (database = 'postgres') => {
    const target = new URL(base);
    target.pathname = `/${database === 'postgres' ? base.pathname.slice(1) || 'postgres' : database + suffix}`;
    return target.toString();
  };
  return {
    postgres: {
      port: Number(base.port || 5432),
      url,
      stop() {
        if (created.length === 0) return;
        const names = created.splice(0);
        void (async () => {
          const { default: pg } = await import('pg');
          const client = new pg.Client({ connectionString: url() });
          await client.connect().catch(() => undefined);
          for (const name of names) await client.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`).catch(() => undefined);
          await client.end().catch(() => undefined);
        })();
      },
      async createDatabase(name) {
        const { default: pg } = await import('pg');
        const client = new pg.Client({ connectionString: url() });
        await client.connect();
        try {
          await client.query(`DROP DATABASE IF EXISTS "${name + suffix}" WITH (FORCE)`);
          await client.query(`CREATE DATABASE "${name + suffix}"`);
          created.push(name + suffix);
        } finally {
          await client.end();
        }
        return url(name);
      },
    },
  };
}
