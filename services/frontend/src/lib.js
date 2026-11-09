/**
 * Runtime configuration resolution and payload decoding.
 *
 * Pure functions, no DOM — which is what makes them testable in Node.
 *
 * Two behaviours here are requirements from the design, not conveniences:
 *
 *  1. `resolveWsHostFromPage` — the plain-Kubernetes lab overlay cannot know the
 *     node address, so the frontend substitutes location.hostname and keeps the
 *     port (docs/kustomize.md §3.0).
 *
 *  2. `decodePayload` — `msgc` may be gzip + base64. A decode failure must
 *     degrade to a visible marker, never blank an alert
 *     (docs/payload-and-compression.md §5.2).
 */

/**
 * Resolve the WebSocket URL.
 *
 * @param {object} config      parsed config.json
 * @param {string} pageHost    location.hostname
 * @param {boolean} pageSecure location.protocol === 'https:'
 */
export function resolveWsUrl(config, pageHost, pageSecure) {
  const raw = String(config?.wsUrl || '');
  if (raw === '') return '';

  let url;
  try {
    url = new URL(raw);
  } catch {
    return '';
  }

  if (config.resolveWsHostFromPage) {
    url.hostname = pageHost;
  }
  // A wss:// URL on an https page is correct. A ws:// URL on an https page will
  // be blocked by the browser as mixed content, so upgrade it rather than
  // failing silently.
  if (pageSecure && url.protocol === 'ws:') {
    url.protocol = 'wss:';
  }
  return url.toString();
}

/** True when a payload field is in the compressed wire form. */
export function isCompressedField(value) {
  return Boolean(value && typeof value === 'object' && typeof value.__gz === 'string');
}

const DECODE_FAILED = '[undecodable payload]';

/**
 * Decode a single payload field.
 *
 * `gunzip` is injected so this module has no browser-only dependency and can be
 * tested under Node.
 */
export async function decodeField(value, gunzip, maxBytes) {
  if (!isCompressedField(value)) return value;
  try {
    const bytes = base64ToBytes(value.__gz);
    const out = await gunzip(bytes);
    const text = new TextDecoder().decode(out);
    if (text.length > maxBytes) {
      return `${text.slice(0, maxBytes)}\n…[truncated at ${maxBytes} bytes]`;
    }
    return text;
  } catch {
    return DECODE_FAILED;
  }
}

/** Decode every compressed field of an event payload. Never throws. */
export async function decodePayload(payload, gunzip, maxBytes) {
  if (!payload || typeof payload !== 'object') return payload;
  const out = {};
  for (const [k, v] of Object.entries(payload)) {
    out[k] = await decodeField(v, gunzip, maxBytes);
  }
  return out;
}

/** Browser/Node base64 → bytes. */
export function base64ToBytes(b64) {
  if (typeof Buffer !== 'undefined') return new Uint8Array(Buffer.from(b64, 'base64'));
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i += 1) bytes[i] = bin.charCodeAt(i);
  return bytes;
}

/** Severity ranking for sorting and display. Retirement events rank last. */
export const SEVERITY_RANK = { FATAL: 0, CRITICAL: 1, WARNING: 2, HARMLESS: 3, CLOSE: 4 };

export function isRetirement(severity) {
  return severity === 'CLOSE' || severity === 'HARMLESS';
}

/**
 * Apply a fold verdict to the local alert map.
 *
 * The UI must honour `applied` or it will show a close/reopen flicker on stale
 * CLOSEs and a count that increments 1000 times a minute
 * (docs/event-lifecycle.md §9).
 *
 * Returns a NEW map; never mutates the input.
 */
export function applyAlert(map, { key, applied, event }) {
  const next = new Map(map);
  if (applied === 0 || applied === null || applied === undefined) {
    return next;                       // stale or backfill: no state change
  }
  if (applied === 3) {
    next.delete(key);
    return next;
  }
  if (applied === 1) {
    const prev = next.get(key);
    next.set(key, {
      key,
      severity: event?.severity ?? prev?.severity,
      firstSeen: prev?.firstSeen ?? event?.brokerTs ?? Date.now(),
      lastSeen: event?.brokerTs ?? Date.now(),
      count: (prev?.count ?? 0) + 1,
    });
    return next;
  }
  if (applied === 2) {
    const prev = next.get(key);
    if (!prev) return next;            // coalescing with no local state: ignore
    // The fold overwrites `severity` on every applied event, coalesced or not
    // (alert-fold.lua HSETs severity unconditionally). Mirror that here, or an
    // escalation WARNING -> CRITICAL would not show up.
    next.set(key, {
      ...prev,
      severity: event?.severity ?? prev.severity,
      lastSeen: event?.brokerTs ?? prev.lastSeen,
    });
    return next;
  }
  return next;
}

/**
 * Insert an event into a bounded, id-sorted list.
 *
 * Sorting is by broker timestamp, not arrival order: cross-worker delivery can
 * invert (docs/frontend.md §4).
 */
export function insertEvent(list, evt, limit) {
  if (!evt?.id) return list;
  if (list.some((e) => e.id === evt.id)) return list;
  const next = [...list, evt];
  next.sort((a, b) => (a.brokerTs ?? 0) - (b.brokerTs ?? 0));
  return next.length > limit ? next.slice(next.length - limit) : next;
}

/** Exponential backoff with jitter, capped. */
export function backoffDelay(attempt, { base = 500, cap = 30000 } = {}) {
  const exp = Math.min(cap, base * 2 ** Math.max(0, attempt));
  return Math.round(exp * (0.5 + Math.random() * 0.5));
}
