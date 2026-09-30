// `prisma generate` only: the tests create the tables themselves.
import { defineConfig } from 'prisma/config';

export default defineConfig({
  schema: 'schema.prisma',
});
