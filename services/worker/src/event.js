/**
 * Envelope validation and payload compression.
 *
 * The contract is in docs/architecture.md §5 and docs/payload-and-compression.md.
 * Compression is applied to payload fields only, never to the envelope: the
 * gateway must be able to filter without decompressing anything.
 */
import { gzipSync, gunzipSync } from 'node:zlib';
import { createHash } from 'node:crypto';

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

/**
 * Content fingerprint: what makes two events "the same event".
 *
 * Dedup has to key on content, not on the event id. The same logical alarm
 * routinely arrives with a fresh id each time (an upstream system re-reporting a
 * condition), so an id-only check counts every copy — which is exactly how a
 * duplicate storm inflated `count`. The id is still the identity of a MESSAGE; the
 * fingerprint is the identity of the CONDITION.
 *
 * What is included: `source`, `type`, `severity` and the payload. Those are the
 * facts a producer reports about the world.
 *
 * What is deliberately EXCLUDED, and why each one would break dedup:
 *
 *   id           differs per delivery — the whole point
 *   ts           the producer's clock, also differs per delivery
 *   brokerTs     set on arrival by the ingress; differs every time
 *
 * Payload keys are sorted before hashing so key order cannot make two identical
 * payloads look different. Nested objects are sorted too, recursively — a naive
 * JSON.stringify is order-sensitive at every level.
 *
 * A retirement event (CLOSE/HARMLESS) is included in the same space as the
 * severities it retires: a CLOSE fingerprint differs from the CRITICAL
 * fingerprint for the same source and type, so retirements never collapse into
 * the occurrence they are ending.
 */
export function contentFingerprint(evt) {
  const canonical = stableStringify({
    source: evt.source,
    type: evt.type,
    severity: evt.severity,
    payload: evt.payload ?? {},
  });
  return createHash('sha256').update(canonical).digest('hex').slice(0, 32);
}

/** JSON.stringify with object keys sorted at every depth. */
function stableStringify(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value) ?? 'null';
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  const keys = Object.keys(value).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(value[k])}`).join(',')}}`;
}
