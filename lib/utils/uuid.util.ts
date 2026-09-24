import { randomBytes, randomInt } from 'node:crypto';

let lastMs = -1;
let sequence = 0;

/**
 * UUIDv7 (RFC 9562): 48-bit Unix ms, then a 12-bit counter in `rand_a` (method 1),
 * then 62 random bits. Monotonic within the process, including many ids per ms and
 * a clock that steps backwards (the timestamp never decreases; on counter overflow it
 * borrows the next ms). Across processes, ids are ordered to the millisecond only.
 */
export function uuidv7(): string {
  const now = Date.now();
  if (now > lastMs) {
    lastMs = now;
    // A random start leaves at least 3072 increments before the counter overflows.
    sequence = randomInt(0, 0x400);
  } else if (++sequence > 0xfff) {
    lastMs += 1;
    sequence = 0;
  }

  const bytes = randomBytes(16);
  let ms = lastMs;
  for (let i = 5; i >= 0; i--) {
    bytes[i] = ms % 256;
    ms = Math.floor(ms / 256);
  }
  bytes[6] = 0x70 | (sequence >> 8);
  bytes[7] = sequence & 0xff;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;

  const hex = bytes.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
