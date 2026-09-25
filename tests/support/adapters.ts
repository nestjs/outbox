import type { INestApplication, Type } from '@nestjs/common';
import { ExpressAdapter } from '@nestjs/platform-express';
import { FastifyAdapter } from '@nestjs/platform-fastify';
import { Test, type TestingModuleBuilder } from '@nestjs/testing';

export const adapters = [
  { name: 'express', create: () => new ExpressAdapter() },
  { name: 'fastify', create: () => new FastifyAdapter() },
] as const;

export type AdapterName = (typeof adapters)[number]['name'];

/**
 * Compiles `module`, boots it on the given adapter and waits until the server
 * accepts requests. `setup` runs before `init()`, for global pipes, filters
 * and plugin registration.
 */
export async function createApp(
  adapter: AdapterName,
  module: Type<unknown>,
  options: {
    override?: (builder: TestingModuleBuilder) => TestingModuleBuilder;
    setup?: (app: INestApplication) => void | Promise<void>;
  } = {},
): Promise<INestApplication> {
  let builder = Test.createTestingModule({ imports: [module] });
  if (options.override) builder = options.override(builder);
  const moduleRef = await builder.compile();
  const app = moduleRef.createNestApplication(
    adapters.find((a) => a.name === adapter)!.create() as any,
  );
  await options.setup?.(app);
  // Listen on loopback up front. Handed a server that isn't listening,
  // supertest binds `::` on a random port per request and connects to
  // 127.0.0.1, which on macOS can reach another process holding that port.
  await app.listen(0, '127.0.0.1');
  return app;
}
