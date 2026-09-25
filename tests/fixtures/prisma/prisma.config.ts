// `prisma generate` only: the tests apply the migrations themselves, as `prisma migrate deploy` would.
import { defineConfig } from 'prisma/config';

export default defineConfig({
  schema: 'schema.prisma',
  migrations: { path: 'migrations' },
});
