const MAX_ERROR_LENGTH = 2_000;

/** A compact, storable description of whatever was thrown. Never throws. */
export function describeError(error: unknown): string {
  const text = describe(error);
  return text.length > MAX_ERROR_LENGTH ? `${text.slice(0, MAX_ERROR_LENGTH)}…` : text;
}

function describe(error: unknown): string {
  try {
    if (error instanceof AggregateError) {
      return `${error.name}: ${error.message} [${error.errors.map(describe).join('; ')}]`;
    }
    if (error instanceof Error) {
      return `${error.name}: ${error.message}`;
    }
    if (typeof error === 'string') {
      return error;
    }
    if (typeof error === 'function') {
      return `[function ${error.name || 'anonymous'}]`;
    }
    if (typeof error === 'symbol' || error === undefined) {
      return String(error);
    }
    return JSON.stringify(error) ?? String(error);
  } catch {
    // A throwing toJSON(), a BigInt, an object without a prototype...
    try {
      return String(error);
    } catch {
      return Object.prototype.toString.call(error);
    }
  }
}
