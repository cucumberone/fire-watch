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

/**
 * Wire field names → the names the UI uses.
 *
 * Dragonfly, the message contract and the audit WAL all say `msg` / `msgc`
 * (docs/payload-and-compression.md §1). The UI calls them **Summary** and
 * **MsgCompleteString**, because "msgc" explains nothing to the person reading
 * the screen and "Message" would collide with the whole event.
 *
 * The rename lives here, in one pure function, for two reasons: the components
 * never touch the wire names, so the vocabulary cannot drift field by field,
 * and the mapping is testable without a DOM.
 */
export const WIRE_FIELD = { summary: 'msg', msgCompleteString: 'msgc' };

/**
 * Fields the workers enrich onto an event before it reaches Dragonfly.
 *
 * `wire` lists every spelling accepted, because these come from an upstream
 * alarm system and the casing has not been normalised: `CIF` and `AlarmIDKey`
 * are acronym-bearing and keep `ID` uppercase, `SupportGroupName` is plain
 * PascalCase. Matching is explicit rather than case-insensitive so a genuine
 * `cif` (some unrelated field) can never be mistaken for the enriched one.
 */
export const ENRICHED_FIELDS = [
  { key: 'device', label: 'Device', wire: ['EVT_Device', 'device'] },
  { key: 'agent', label: 'Agent', wire: ['EVT_Agent', 'agent'] },
  { key: 'agentOwner', label: 'Agent owner', wire: ['EVT_AgentOwner', 'agentOwner'] },
  { key: 'eventKey', label: 'Event key', wire: ['EVT_EventKey', 'eventKey'] },
  { key: 'environment', label: 'Environment', wire: ['EVT_Environment', 'environment'] },
];

/** gzip marker, matching services/worker/src/event.js. */
const GZ_MARK = '__gz';

/** First non-empty value of the given keys, or undefined. */
function firstPresent(source, names) {
  if (!source || typeof source !== 'object') return undefined;
  for (const name of names) {
    const value = source[name];
    if (value !== undefined && value !== null && value !== '') return value;
  }
  return undefined;
}

/** Display text for a leaf value. Booleans read as yes/no, not true/false. */
function asText(value) {
  if (value === undefined || value === null) return '';
  if (typeof value === 'boolean') return value ? 'yes' : 'no';
  return String(value);
}

/** Parse a timestamp into epoch ms, falling back when it is unusable. */
function asTime(value, fallback) {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value !== '') {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return fallback;
}

/** Positive integer, else the fallback. Rejects 0, negatives and NaN. */
function asCount(value, fallback) {
  const n = typeof value === 'number' ? value : Number.parseInt(String(value ?? ''), 10);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

/**
 * Map a wire event to its view shape. Never mutates, never throws; a missing
 * payload yields empty strings rather than `undefined`, so a row can never
 * render `undefined` as text.
 *
 * Occurrence fields come from the worker's enrichment when present. Without
 * enrichment a single raw event is its own first and last occurrence with a
 * count of 1 — which is the honest answer, not a guess.
 */
export function toViewEvent(evt) {
  const payload = evt?.payload;
  const fallbackTs = asTime(evt?.brokerTs, asTime(evt?.ts, undefined));

  const view = {
    ...evt,
    // The untouched wire record. The inspector shows this rather than the view
    // event, or every field would appear twice: once as `payload.CIF` and once
    // as the derived `cif`.
    wire: evt,
    summary: asText(firstPresent(payload, [WIRE_FIELD.summary])),
    // Only a non-empty complete string is worth a disclosure toggle.
    msgCompleteString: asText(firstPresent(payload, [WIRE_FIELD.msgCompleteString])),
    firstOccurrence: asTime(
      firstPresent(payload, ['FirstOccurrence', 'firstOccurrence', 'first_occurrence']),
      fallbackTs,
    ),
    lastOccurrence: asTime(
      firstPresent(payload, ['LastOccurrence', 'lastOccurrence', 'last_occurrence']),
      fallbackTs,
    ),
    count: asCount(firstPresent(payload, ['Count', 'count']), 1),
  };

  for (const field of ENRICHED_FIELDS) {
    view[field.key] = asText(firstPresent(payload, field.wire));
  }
  return view;
}

/**
 * Flatten a record into ordered `[key, value]` pairs for the inspector.
 *
 * Nested objects and arrays are walked with dotted / indexed paths rather than
 * being stringified, because the point of right-clicking is to see the fields
 * *inside* `payload`, not `[object Object]`. `undefined` keys are omitted (an
 * absent field is noise); `null` is shown, because absence and explicit null
 * are different facts.
 */
export function recordEntries(record) {
  const out = [];
  // A cycle is not expected on the wire, but right-clicking a record must never
  // be able to blow the stack — one malformed payload would blank the whole UI.
  const seen = new WeakSet();

  const walk = (value, path) => {
    if (value === undefined) return;
    // A missing record and an empty record both mean "nothing to show"; an
    // entry with a blank key would render as a row of empty cells.
    if (path === '' && value === null) return;
    if (value === null) {
      out.push([path, 'null']);
      return;
    }
    if (typeof value === 'object') {
      if (seen.has(value)) {
        out.push([path, '[circular]']);
        return;
      }
      seen.add(value);
      if (typeof value[GZ_MARK] === 'string') {
        out.push([path, `[gzip, ${value[GZ_MARK].length} base64 chars]`]);
        return;
      }
      if (Array.isArray(value)) {
        if (value.length === 0) out.push([path, '[]']);
        else value.forEach((item, i) => walk(item, `${path}[${i}]`));
        return;
      }
      const keys = Object.keys(value);
      if (keys.length === 0) {
        // Same for a root-level empty object: no key to attach it to.
        if (path !== '') out.push([path, '{}']);
        return;
      }
      for (const k of keys) walk(value[k], path === '' ? k : `${path}.${k}`);
      return;
    }
    out.push([path, String(value)]);
  };
  walk(record, '');
  return out;
}

/** Epoch ms → locale clock time, or '' when the value is not a usable time. */
/**
 * Format an epoch-ms value as `YYYY-MM-DD HH:MM:SS`, 24-hour.
 *
 * Deliberately hand-rolled rather than `toLocaleTimeString()`:
 *
 * - `toLocale*` output depends on the browser's locale and OS settings, so two
 *   operators looking at the same alert could see different strings, and the
 *   same page could render differently after a locale change. An operations
 *   board needs one unambiguous format.
 * - The 12-hour clock lost the AM/PM distinction off the edge of a narrow
 *   column, which made 01:05 and 13:05 look identical.
 * - The date was missing entirely, so events from yesterday were
 *   indistinguishable from today's.
 *
 * Local time, not UTC: an operator correlates against the wall clock in front of
 * them. The inspector shows the raw ISO/epoch values for anyone who needs UTC.
 */
export function formatClock(ms) {
  if (!Number.isFinite(ms)) return '';
  const d = new Date(ms);
  if (Number.isNaN(d.getTime())) return '';
  const p = (n, w = 2) => String(n).padStart(w, '0');
  return (
    `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ` +
    `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`
  );
}

/** Severity ranking for sorting and display. Retirement events rank last. */
/**
 * Sort order for the alert table: most urgent first, retirements last.
 *
 * These are the INTERNAL severities. A producer's `MAJOR` arrives as `CRITICAL`
 * and `MINOR`/`INDETERMINATE` as `WARNING`, mapped once in the ingress, so the UI
 * never has to know the source scale.
 */
export const SEVERITY_RANK = { FATAL: 0, CRITICAL: 1, WARNING: 2, HARMLESS: 3, CLOSE: 4 };

/** Retirement signals: these end an alert episode rather than starting one. */
export function isRetirement(severity) {
  return severity === 'CLOSE' || severity === 'HARMLESS';
}

/**
 * Resolve the colour scheme actually in effect.
 *
 * `preference` is the user's stored choice ('auto' | 'light' | 'dark');
 * `systemDark` is the OS setting. Pure, so the switch logic is testable without
 * a DOM.
 */
export function resolveTheme(preference, systemDark) {
  if (preference === 'dark' || preference === 'light') return preference;
  return systemDark ? 'dark' : 'light';
}

/** CSS class marking a row with its severity, used by the row-colour mode. */
export function severityRowClass(severity, extra) {
  const base = `row-${String(severity || 'unknown').toLowerCase()}`;
  return extra ? `${base} ${extra}` : base;
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
    // Stale, backfill, or a DUPLICATE the fold dropped. Duplicates are removed
    // server-side by the (timestamp, id) watermark, so they never reach the
    // counter and must not touch the row here either.
    return next;
  }
  if (applied === 3) {
    next.delete(key);
    return next;
  }
  if (applied === 1 || applied === 2) {
    // 2 is accepted as an alias for 1 so a gateway still running the previous
    // build (which emitted 2 for a coalesced repeat) cannot freeze a row during
    // a rolling update. Both now mean the same thing: the event was applied.
    //
    // A CONTENT duplicate arrives on this same path with NO event attached: the
    // copy was discarded by the worker, so only the rollup exists. `lastSeen`
    // then falls back to `Date.now()`, which is correct — the fold recorded the
    // arrival at roughly now, and the alternative (keeping the old value) would
    // make Last occurrence appear frozen, which is the bug this replaced.
    const prev = next.get(key);
    next.set(key, {
      key,
      severity: event?.severity ?? prev?.severity,
      firstSeen: prev?.firstSeen ?? event?.brokerTs ?? Date.now(),
      lastSeen: event?.brokerTs ?? Date.now(),
      count: (prev?.count ?? 0) + 1,
      // The latest applied event is the record worth inspecting: right-clicking
      // an alert row must show the enriched fields of the occurrence that put
      // the row there, not a synthetic merge of every event ever folded in.
      event: event ?? prev?.event,
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
