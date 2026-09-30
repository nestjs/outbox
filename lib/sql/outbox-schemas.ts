// The SQL stores' schemas (internal): one per dialect, what the `nest-outbox` command serves.
import { mysqlOutboxSchema } from '../mysql/migrations/index.js';
import { postgresOutboxSchema } from '../postgres/migrations/index.js';

/** PostgresOutboxStore's schema and MySqlOutboxStore's: the command picks one by the URL's scheme, or `--dialect`. */
export const outboxSchemas = [postgresOutboxSchema, mysqlOutboxSchema];
