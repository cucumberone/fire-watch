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

  // The badge must show the PRODUCER's severity, not the internal one.
  //
  // `severity` is the internal value after the ingress mapped it down onto five
  // levels (docs/event-schema.md §3). A producer never sends `CLOSE` — it sends
  // `CLEAR` — so rendering the mapped value told an operator their event had a
  // severity that does not exist. The original is always preserved in
  // `payload.EVT_Severity`, so prefer it whenever the record came through the
  // `EVT_*` contract.
  //
  // Retirement signals keep their internal name visible through `isRetirement`,
  // which drives the row styling, so the meaning is not lost: the row still dims
  // as a retirement, and the badge says what was actually reported.
  view.displaySeverity = asText(firstPresent(payload, ['EVT_Severity'])) || view.severity;

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
/**
 * Field names that hold an absolute point in time.
 *
 * The Information panel used to show these raw, which produced a list mixing
 * three formats: `1794319171950` (epoch ms), `2026-11-10T10:00:00.000Z` (ISO) and
 * the human clock used everywhere else in the UI. Comparing two timestamps by eye
 * in that list is the whole reason an operator opens it.
 *
 * Matched by NAME rather than by sniffing values, because a bare number is
 * ambiguous — a count of 1794319171950 and a timestamp of the same value are
 * indistinguishable, and guessing wrong would rewrite a legitimate number.
 */
const TIME_KEYS = new Set([
  'ts', 'timestamp', 'brokerts', 'broker_ts', 'closedat', 'closed_at',
  'firstseen', 'first_seen', 'lastseen', 'last_seen', 'ackedat', 'acked_at',
  'firstoccurrence', 'lastoccurrence', 'closedatms', 'episodestarted',
]);

/** Is this field name a timestamp? Case- and separator-insensitive. */
export function isTimeField(name) {
  if (typeof name !== 'string') return false;
  // Strip separators so `brokerTs`, `broker_ts` and `broker-ts` are one key.
  return TIME_KEYS.has(name.toLowerCase().replace(/[_.-]/g, ''));
}

/**
 * Render a timestamp value as `YYYY-MM-DD HH:MM:SS`, keeping the raw form beside
 * it so nothing is lost.
 *
 * Accepts epoch ms and ISO strings. Returns null when the value is not a usable
 * time, so the caller can fall back to the raw rendering rather than printing
 * "Invalid Date".
 */
export function formatTimeValue(value) {
  if (value === null || value === undefined) return null;

  let ms;
  if (typeof value === 'number') {
    // Seconds vs milliseconds: an epoch in seconds is ~1e9, ms ~1e12. Anything
    // below 1e11 cannot be a millisecond timestamp in this century, so it is
    // read as seconds rather than silently resolving to 1970.
    ms = value < 1e11 ? value * 1000 : value;
  } else if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed === '') return null;
    // A pure-digit string is an epoch, not a date string. `new Date('17...')`
    // would parse it as a year.
    if (/^-?\d+$/.test(trimmed)) {
      const n = Number(trimmed);
      ms = n < 1e11 ? n * 1000 : n;
    } else {
      ms = Date.parse(trimmed);
    }
  } else {
    return null;
  }

  if (!Number.isFinite(ms)) return null;
  const human = formatClock(ms);
  if (!human) return null;
  // The raw value is kept in parentheses: the exact instant still matters when
  // correlating with a producer's log, and hiding it would be a lossy "fix".
  return `${human}  (${value})`;
}

export function recordEntries(record, { formatTimes = true } = {}) {
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
    // Leaf value. `String(value)` for everything, except a field whose NAME says
    // it is a timestamp — then show the human clock with the raw value kept in
    // parentheses. The leaf key is the last path segment, so `payload.firstSeen`
    // is recognised without the container name mattering.
    if (formatTimes) {
      const leaf = path.slice(path.lastIndexOf('.') + 1).replace(/\[\d+\]$/, '');
      if (isTimeField(leaf)) {
        const pretty = formatTimeValue(value);
        if (pretty) {
          out.push([path, pretty]);
          return;
        }
      }
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
/**
 * Fold one `op: alert` frame into the alert map.
 *
 * @param {Map} map
 * @param {object} frame
 * @param {string} frame.key
 * @param {number} frame.applied  1/2 applied, 3 retired, 0/undef ignored
 * @param {object} [frame.event]  the event that caused it, when there is one
 * @param {object} [frame.row]    the alert's CURRENT state from the server:
 *   `{ severity, firstSeen, lastSeen, count, event }`. Sent for a content
 *   duplicate, which has no event of its own — without it a client that never saw
 *   the original has nothing to render a row from.
 */
export function applyAlert(map, { key, applied, event, row }) {
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
    // A CONTENT duplicate arrives on this same path with NO event of its own: the
    // copy was discarded by the worker, so only the rollup moved. Since a recent
    // change it carries `row`, the server's view of the alert, so even a client
    // that has never seen this key can build the row.
    const prev = next.get(key);

    // A row we have never seen, with no event and no server-supplied state: there
    // is nothing to render — no severity, no summary, no timestamp. Creating one
    // anyway is what produced a `severity: undefined` row, and rendering that
    // threw inside Badge and unmounted the whole tree (the blank page). The next
    // `resync` supplies the real row, so ignoring this frame is safe: `count` is
    // owned by the fold, so only this client is briefly stale.
    if (!prev && !event && !row) return next;

    // The server's row is authoritative where it exists, because it knows the
    // fold's real count. Only its counts are used when we already have the row,
    // so a repeat cannot make the visible count jump around.
    //
    // `row.count` ALREADY INCLUDES this occurrence: the fold incremented it
    // before the gateway read the hash. Adding one on top double-counts — the
    // symptom was a CLEAR followed by a re-send showing Count 2 instead of 1,
    // because the new episode had count 1 and the client made it 2.
    next.set(key, {
      key,
      // Never downgrade a known severity to undefined because a later frame had
      // nothing to read it from.
      severity: event?.severity ?? row?.severity ?? prev?.severity,
      firstSeen: prev?.firstSeen ?? row?.firstSeen ?? event?.brokerTs ?? Date.now(),
      // A repeat IS an occurrence, so lastSeen moves even without an event: the
      // fold recorded the arrival at roughly now.
      lastSeen: event?.brokerTs ?? row?.lastSeen ?? Date.now(),
      count: row?.count ?? ((prev?.count ?? 0) + 1),
      // The latest applied event is the record worth inspecting: right-clicking
      // an alert row must show the enriched fields of the occurrence that put
      // the row there, not a synthetic merge of every event ever folded in.
      event: event ?? prev?.event ?? row?.event,
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
 *
 * De-duplication is on `id` **plus `brokerTs`**, not `id` alone. An absorbed
 * repeat keeps the original's id — the copy was never stored, so there is no
 * second record — but it is a distinct ARRIVAL and the tail exists to show
 * arrivals. Deduping on id alone hid every repeat after the first, so two runs of
 * the same probe produced one row. A genuine redelivery carries the same
 * `brokerTs`, so it is still dropped.
 */
export function insertEvent(list, evt, limit) {
  if (!evt?.id) return list;
  const dup = list.some((e) => e.id === evt.id && (e.brokerTs ?? null) === (evt.brokerTs ?? null));
  if (dup) return list;
  const next = [...list, evt];
  next.sort((a, b) => (a.brokerTs ?? 0) - (b.brokerTs ?? 0));
  return next.length > limit ? next.slice(next.length - limit) : next;
}

/** Exponential backoff with jitter, capped. */
export function backoffDelay(attempt, { base = 500, cap = 30000 } = {}) {
  const exp = Math.min(cap, base * 2 ** Math.max(0, attempt));
  return Math.round(exp * (0.5 + Math.random() * 0.5));
}

/**
 * Does this `bad_op` error mean the SERVER is too old to know an op we send?
 *
 * Only resolvable by redeploying the gateway, so the client has to stop asking
 * rather than retrying forever. This is a deploy-skew case: the frontend bundle
 * and the ws-gateway image are separate artifacts, so a newer page can talk to an
 * older gateway. The symptom without this guard is `bad_op: unknown op resync`
 * every resync interval, and again on every reconnect, for as long as the two
 * versions disagree.
 *
 * Matching on the message text is not ideal, but `bad_op` is generic — it covers
 * any unrecognised op — and the gateway does not distinguish "unknown" from
 * "unsupported". Narrowing to an exact `unknown op <name>` match keeps a genuine
 * malformed-frame bug visible instead of silently disarming the resync.
 *
 * @param {object} frame  an `op: 'error'` frame
 * @param {string} op     the op we sent
 */
export function isUnsupportedOpError(frame, op) {
  if (!frame || frame.op !== 'error' || frame.code !== 'bad_op') return false;
  if (typeof frame.message !== 'string') return false;
  return frame.message.trim() === `unknown op ${op}`;
}


/**
 * Filter the Closed tab's rows.
 *
 * Pure and exported so it can be tested without a DOM — the matching rules have
 * enough edge cases (case, absent fields, the producer-vs-internal severity) that
 * asserting them through a rendered table would be both slow and indirect.
 *
 * @param {Array} rows      closed alerts as the gateway sent them
 * @param {string} text     free text, matched case-insensitively across every
 *                          column the operator can see plus the summary
 * @param {string} severity exact internal severity, or '' for all
 */
export function filterClosedAlerts(rows, text, severity) {
  const needle = String(text || '').trim().toLowerCase();
  const out = (rows || []).filter((r) => {
    if (severity && r.severity !== severity) return false;
    if (!needle) return true;
    // Both the internal severity and the producer's own value are searchable:
    // an operator searching "CLEAR" should find a row the badge shows as CLEAR,
    // even though the stored value is CLOSE.
    const hay = [
      r.key, r.severity, r.event?.displaySeverity, r.event?.summary,
      r.event?.device, r.event?.agent, r.event?.agentOwner,
      r.event?.eventKey, r.event?.environment, r.closedBy,
    ].filter(Boolean).join(' ').toLowerCase();
    return hay.includes(needle);
  });
  // Newest closure first, matching the server's order so filtering cannot silently
  // reorder the table.
  out.sort((a, b) => (b.closedAt ?? 0) - (a.closedAt ?? 0));
  return out;
}

/** Distinct severities present in the closed set, for the filter's options. */
export function closedSeverities(rows) {
  const set = new Set();
  for (const r of rows || []) if (r.severity) set.add(r.severity);
  return [...set].sort();
}

/**
 * Filter the All Events rows.
 *
 * Same shape as `filterClosedAlerts` and for the same reason: pure and exported so
 * the matching rules can be tested without a DOM. It searches the fields an
 * operator can actually see plus the summary, because "which switch was flapping
 * last Tuesday" is a free-text question.
 *
 * @param {Array} events   view events (already through `toViewEvent`)
 * @param {string} text    free text, case-insensitive
 */
export function filterEvents(events, text) {
  const needle = String(text || '').trim().toLowerCase();
  const list = events || [];
  if (!needle) return list;
  return list.filter((e) => {
    const hay = [
      e.id, e.source, e.type, e.severity, e.displaySeverity,
      e.summary, e.device, e.agent, e.agentOwner, e.eventKey, e.environment,
    ].filter(Boolean).join(' ').toLowerCase();
    return hay.includes(needle);
  });
}

/**
 * The count shown in a tab's badge.
 *
 * Exists because this was a chain of ternaries ending in `: closed.count`, so when
 * the All events tab was added it silently inherited the CLOSED count — the tab
 * showed a number while its table was empty, which reads as a broken query rather
 * than a wrong badge.
 *
 * Explicit per tab, and it THROWS on an unknown id: a new tab must decide what its
 * badge means instead of quietly borrowing another tab's number.
 *
 * @param {string} tabId
 * @param {{alerts:number, events:number, allEvents:number, closed:number}} counts
 */
export function tabBadgeCount(tabId, counts) {
  switch (tabId) {
    case 'alerts': return counts.alerts;
    case 'events': return counts.events;
    // Loaded rows, not a live total: this tab is a bounded query, so its badge is
    // "how many I currently hold". The Closed tab is different — it tracks closures
    // observed, because a closure happens without being fetched.
    case 'all': return counts.allEvents;
    case 'closed': return counts.closed;
    default:
      throw new Error(`tabBadgeCount: unknown tab "${tabId}" — give it an explicit count`);
  }
}
