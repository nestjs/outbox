#!/usr/bin/env node
// The `nest-outbox` command: the SQL stores' migrations from a shell or a CI step. The kit's command line picks a
// store's schema by the database URL's scheme, so each dialect's schema goes in the list.
import { runStoreCli } from '@nestjs/store-kit';
import { postgresOutboxSchema } from '../postgres/migrations/index.js';

process.exitCode = await runStoreCli([postgresOutboxSchema], process.argv.slice(2));
