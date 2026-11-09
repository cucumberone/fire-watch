/**
 * Envelope validation and payload compression.
 *
 * The contract is in docs/architecture.md §5 and docs/payload-and-compression.md.
 * Compression is applied to payload fields only, never to the envelope: the
 * gateway must be able to filter without decompressing anything.
 */
import { gzipSync, gunzipSync } from 'node:zlib';

const SEVERITIES = new Set(['FATAL', 'CRITICAL', 'WARNING', 'HARMLESS', 'CLOSE']);
const RETIREMENT = new Set(['CLOSE', 'HARMLESS']);

export class ValidationError extends Error {}

/** gzip magic bytes, the only marker we use. See payload-and-compression.md §4. */
export const GZIP_MAGIC = Buffer.from([0x1f, 0x8b]);

export function isGzip(buf) {
  return Buffer.isBuffer(buf) && buf.length >= 2 && buf[0] === 0x1f && buf[1] === 0x8b;
}

/**
 * Validate the envelope. Throws ValidationError for anything that should be
 * dead-lettered — i.e. anything a retry cannot fix.
 */
export function validateEvent(evt) {
  if (!evt || typeof evt !== 'object') throw new ValidationError('payload is not an object');
  if (typeof evt.id !== 'string' || evt.id === '') throw new ValidationError('missing id');
  if (typeof evt.source !== 'string' || evt.source === '') throw new ValidationError('missing source');
  if (typeof evt.type !== 'string' || evt.type === '') throw new ValidationError('missing type');
  if (!SEVERITIES.has(evt.severity)) {
    throw new ValidationError(`invalid severity: ${JSON.stringify(evt.severity)}`);
  }
  if (evt.payload !== undefined && (typeof evt.payload !== 'object' || evt.payload === null)) {
    throw new ValidationError('payload must be an object');
  }
  return evt;
}

export function isRetirement(evt) {
  return RETIREMENT.has(evt.severity);
}

/**
 * Compress the large string fields of `payload`.
 *
 * Only `msgc` is worth compressing in practice: gzip's ~18 byte framing makes a
 * short `msg` larger, not smaller. The threshold therefore applies per field,
 * and a record can legitimately end up with one compressed field and one plain.
 *
 * Returns { payload, compressedFields, bytesBefore, bytesAfter }.
 */
export function compressPayload(payload, { min, level }) {
  if (!payload || typeof payload !== 'object') {
    return { payload, compressedFields: [], bytesBefore: 0, bytesAfter: 0 };
  }

  const out = {};
  const compressedFields = [];
  let bytesBefore = 0;
  let bytesAfter = 0;

  for (const [name, value] of Object.entries(payload)) {
    if (typeof value !== 'string') {
      out[name] = value;                       // numbers, nested objects: untouched
      continue;
    }
    const raw = Buffer.from(value, 'utf8');
    bytesBefore += raw.length;

    if (raw.length >= min) {
      const gz = gzipSync(raw, { level });
      // Only keep the compressed form if it actually saved bytes. A pathological
      // string (already compressed, or short with a high ratio of unique bytes)
      // can grow; storing that would be strictly worse than plain.
      if (gz.length < raw.length) {
        out[name] = { __gz: gz.toString('base64') };
        compressedFields.push(name);
        bytesAfter += gz.length;
        continue;
      }
    }
    out[name] = value;
    bytesAfter += raw.length;
  }

  return { payload: out, compressedFields, bytesBefore, bytesAfter };
}

/**
 * Inverse of compressPayload, used by tests and by the replay verification.
 * The wire format puts compressed fields in an object with a single `__gz` key
 * so a reader can tell without out-of-band metadata.
 */
export function decompressPayload(payload) {
  const out = {};
  for (const [name, value] of Object.entries(payload || {})) {
    if (value && typeof value === 'object' && typeof value.__gz === 'string') {
      out[name] = gunzipSync(Buffer.from(value.__gz, 'base64')).toString('utf8');
    } else {
      out[name] = value;
    }
  }
  return out;
}

/** Read the broker-assigned timestamp, in ms. Ordering depends on this. */
export function brokerTsMs(msg) {
  const h = msg.properties?.headers || {};
  const original = h['x-original-ts'];
  if (original !== undefined) return Number(original);
  const ms = h.timestamp_in_ms;
  if (ms !== undefined) return Number(ms);
  const secs = msg.properties?.timestamp;
  if (secs !== undefined) return Number(secs) * 1000;
  return null;
}
