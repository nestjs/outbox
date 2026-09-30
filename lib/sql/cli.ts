#!/usr/bin/env node
// The `nest-outbox` command: the SQL stores' migrations from a shell or a CI step. The kit's command line picks a
// store's schema by the database URL's scheme (postgres://, mysql://), and `sql` by `--dialect` (PostgreSQL unless it
// says mysql), so each dialect's schema goes in the list.
import { runStoreCli } from '@nestjs/store-kit';
import { outboxSchemas } from './outbox-schemas.js';

process.exitCode = await runStoreCli(outboxSchemas, process.argv.slice(2));
