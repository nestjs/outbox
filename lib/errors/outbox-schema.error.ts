import { OutboxError } from './outbox.error.js';

/**
 * A SQL store's schema can't serve this version of the package: it is behind the store's migrations (and `migrate` is
 * off), or applying them failed (`cause`). The store refuses every call until it's fixed, so the application fails to
 * start instead of losing messages.
 *
 * ```ts
 * try {
 *   await app.init();
 * } catch (error) {
 *   if (error instanceof OutboxSchemaError) {
 *     console.error(`Schema "${error.schema}" is at version ${error.version}; run npx nest-outbox migrate`);
 *   }
 *   throw error;
 * }
 * ```
 */
export class OutboxSchemaError extends OutboxError {
  override name = 'OutboxSchemaError';
  /** The store's schema (`nest_outbox` unless the application named another). */
  readonly schema: string;
  /** The schema's version: the last migration applied to it, `0` for none. */
  readonly version: number;
  /** The version this version of the package needs: its last migration. */
  readonly requiredVersion: number;

  constructor(message: string, details: { schema: string; version: number; requiredVersion: number; cause?: unknown }) {
    super(message, details.cause === undefined ? undefined : { cause: details.cause });
    this.schema = details.schema;
    this.version = details.version;
    this.requiredVersion = details.requiredVersion;
  }
}
