/** A topic, consumer or transport name: a string with more than whitespace in it. */
export function isName(value: unknown): value is string {
  return typeof value === 'string' && value.trim() !== '';
}
