/**
 * Operator actions: validation and frame rendering.
 *
 * Pure functions only, so the rules that protect the write path are testable without
 * a socket or a Valkey.
 *
 * Specified by docs/acknowledge.md §3.
 *
 * WHAT AN OPERATOR ACTS ON
 *
 * An ack or a close targets a CONDITION — one record at `event:<32 hex>` — not an
 * individual delivery. That is now the same thing as "what the UI shows as a row":
 * with one hash per condition there is no separate alert object to reconcile, so
 * `key` is the record key and it is what the worker published.
 *
 * The bare key (no `event:` prefix) is what travels on the wire and what the zsets
 * store as a member. The prefix is added exactly once, in `valkey.js`.
 */

/** The generated key is `event:` + 32 hex; the bare form is 32 hex. */
export const KEY_LEN = 32;
export const MAX_BY_LEN = 128;
export const MAX_ALERT_QUERY_KEYS = 200;

/** Default operator label until SSO is in front of the Route. */
export const ANONYMOUS = 'anonymous';

/**
 * Strip anything that would corrupt a log line or a JSON frame, and collapse
 * whitespace. Control characters are removed rather than escaped because the value
 * ends up in a hash field that an operator reads on screen.
 */
function cleanText(value, maxLen) {
  if (typeof value !== 'string') return '';
  // eslint-disable-next-line no-control-regex
  const stripped = value.replace(/[\u0000-\u001f\u007f]/g, ' ');
  return stripped.replace(/\s+/g, ' ').trim().slice(0, maxLen);
}

/**
 * Is this a usable record key?
 *
 * STRICT, and it rejects the legacy `source:type` shape on purpose. A stale browser
 * that still sends `sw1:link.down` would otherwise HSET an ack onto a key no fold
 * ever created: the write succeeds, nothing errors, and the operator sees a row that
 * is "acked" in the UI and still firing everywhere else. Rejecting it produces a
 * `bad_key` frame instead, which is a bug report rather than silent data.
 */
export function isValidKey(key) {
  if (typeof key !== 'string' || key.length !== KEY_LEN) return false;
  return /^[0-9a-f]{32}$/.test(key);
}

/** Backwards-compatible alias: older code and tests call it an alert key. */
export const isValidAlertKey = isValidKey;

/**
 * Validate an inbound `op: ack` frame.
 *
 * `by` is optional and defaults to `anonymous`, because there is no authentication
 * in front of the Route yet. When SSO lands it will supply the real identity and this
 * default should become a rejection instead — see docs/acknowledge.md §7.
 */
export function validateAck(frame) {
  if (!frame || frame.op !== 'ack') {
    return { ok: false, code: 'bad_op', message: 'expected {"op":"ack"}' };
  }
  if (!isValidKey(frame.key)) {
    return {
      ok: false,
      code: 'bad_key',
      message: `key must be a ${KEY_LEN}-character lowercase hex record key`,
    };
  }
  const by = cleanText(frame.by, MAX_BY_LEN) || ANONYMOUS;
  return { ok: true, key: frame.key, by };
}

/**
 * Validate an inbound `op: alerts` frame — a bulk state read.
 *
 * The client sends the keys it is currently displaying. This is deliberate: a
 * server-side "list all open records" would be the unbounded `SCAN event:*` that the
 * design refuses everywhere else (websocket-server.md §3).
 */
export function validateAlertQuery(frame, maxKeys = MAX_ALERT_QUERY_KEYS) {
  if (!frame || frame.op !== 'alerts') {
    return { ok: false, code: 'bad_op', message: 'expected {"op":"alerts"}' };
  }
  if (!Array.isArray(frame.keys)) {
    return { ok: false, code: 'bad_keys', message: 'keys must be an array' };
  }
  if (frame.keys.length > maxKeys) {
    return { ok: false, code: 'too_many_keys', message: `limit is ${maxKeys}` };
  }
  const keys = [];
  for (const k of frame.keys) {
    if (!isValidKey(k)) {
      return { ok: false, code: 'bad_key', message: 'every key must be a valid record key' };
    }
    if (!keys.includes(k)) keys.push(k);
  }
  return { ok: true, keys };
}

/**
 * Normalise a stored ack into the wire shape.
 *
 * `ackedAt` absent means not acked. A field left over from an older build (or a
 * partial write) is treated as NOT acked: a timestamp is what makes the ack
 * meaningful, so without one there is nothing an operator could be shown.
 *
 * Field names are camelCase now, matching the hash the fold writes. The old
 * snake_case (`acked_by`) is gone rather than aliased — two spellings of the same
 * field is how a reader ends up looking at one and writing the other.
 */
export function normaliseAck(hash) {
  if (!hash) return null;
  const at = Number(hash.ackedAt ?? hash.acked_at);
  if (!Number.isFinite(at) || at <= 0) return null;
  const by = hash.ackedBy ?? hash.acked_by;
  return {
    by: typeof by === 'string' && by !== '' ? by : ANONYMOUS,
    at,
  };
}

/** Frame broadcast to every client when an ack changes. */
export function ackFrame(key, ack) {
  return JSON.stringify({ op: 'alert', key, ack });
}

/**
 * Validate an inbound `op: close` frame.
 *
 * Same shape as `ack`, and the same key rules: closing targets a record, because that
 * is what the fold maintains and what the UI shows as a row.
 */
export function validateClose(frame) {
  if (!frame || frame.op !== 'close') {
    return { ok: false, code: 'bad_op', message: 'expected {"op":"close"}' };
  }
  if (!isValidKey(frame.key)) {
    return {
      ok: false,
      code: 'bad_key',
      message: `key must be a ${KEY_LEN}-character lowercase hex record key`,
    };
  }
  const by = cleanText(frame.by, MAX_BY_LEN) || ANONYMOUS;
  return { ok: true, key: frame.key, by };
}

/**
 * Frame telling clients a record is retired.
 *
 * Reuses `op: alert` with `applied: 3`, which is exactly what a CLOSE event from a
 * source produces. A separate op would mean two code paths in every client for one
 * state transition (docs/event-lifecycle.md §5.2).
 */
export function closeFrame(key) {
  return JSON.stringify({ op: 'alert', key, applied: 3, closedBy: 'operator' });
}

/**
 * Validate an inbound `op: delete` frame.
 *
 * Deletion targets one record. Unlike ack/close it removes the record AND its index
 * members, so the row disappears from both timelines — "delete" in the operator's
 * sense, not "retire". The key is still the record key, because with one hash per
 * condition a record and a condition are the same object.
 */
export function validateDelete(frame) {
  if (!frame || frame.op !== 'delete') {
    return { ok: false, code: 'bad_op', message: 'expected {"op":"delete"}' };
  }
  if (!isValidKey(frame.id)) {
    return {
      ok: false,
      code: 'bad_id',
      message: `id must be a ${KEY_LEN}-character lowercase hex record key`,
    };
  }
  const by = cleanText(frame.by, MAX_BY_LEN) || ANONYMOUS;
  return { ok: true, id: frame.id, by };
}

/** Kept for older call sites; both id and key are the same string now. */
export function isValidEventId(id) {
  return isValidKey(id);
}

/**
 * Frame telling clients an event was deleted.
 *
 * A distinct op rather than an `alert` frame, because the two clients that care need
 * different things: the live tail removes the row, while the tables must re-read. A
 * dedicated op lets each decide, instead of overloading `applied: 3` (which means
 * "this condition is retired", a different statement from "this record no longer
 * exists").
 */
export function deletedFrame(id) {
  return JSON.stringify({ op: 'deleted', id });
}

/**
 * Largest number of closed records one request may ask for.
 *
 * The retired set spans a 7-day window; at a realistic incident rate that is a few
 * hundred rows, so 500 covers it without letting one client ask a pod to build a
 * response of arbitrary size.
 */
export const MAX_CLOSED_LIMIT = 500;
/** Default window for the retired set: 7 days, matching event retention. */
export const DEFAULT_CLOSED_MS = 7 * 24 * 60 * 60 * 1000;

/**
 * Validate `{"op":"closed"}`.
 *
 * Both fields are optional: an empty frame means "the default window, default
 * limit", which is what the UI sends. Bounds are CLAMPED rather than rejected for
 * `limit` and `sinceMs` — a client asking for too much wants as much as exists, and
 * refusing the whole request would leave it with an empty table for no reason. A
 * malformed TYPE is rejected, because that is a bug rather than an over-ask.
 */
export function validateClosedQuery(frame, { maxLimit = MAX_CLOSED_LIMIT, now = Date.now() } = {}) {
  if (!frame || frame.op !== 'closed') {
    return { ok: false, code: 'bad_op', message: 'expected {"op":"closed"}' };
  }

  let limit = maxLimit;
  if (frame.limit !== undefined) {
    if (!Number.isFinite(frame.limit)) {
      return { ok: false, code: 'bad_limit', message: 'limit must be a number' };
    }
    limit = Math.max(1, Math.min(maxLimit, Math.floor(frame.limit)));
  }

  // `sinceMs` may be an absolute epoch ms or a window in ms. Either way it must be
  // finite, and a value in the future would silently return nothing.
  let sinceMs = now - DEFAULT_CLOSED_MS;
  if (frame.sinceMs !== undefined) {
    if (!Number.isFinite(frame.sinceMs)) {
      return { ok: false, code: 'bad_since', message: 'sinceMs must be a number' };
    }
    sinceMs = Math.min(frame.sinceMs, now);
  }

  return { ok: true, limit, sinceMs };
}

/**
 * Ranges offered by the All Events tab, in milliseconds.
 *
 * Presets rather than free date pickers: the tab asks "what happened in the last N",
 * and a preset makes that one click with no timezone ambiguity. Filters are applied
 * server-side, so a shorter range is strictly less work.
 */
export const ALL_EVENTS_RANGES = {
  '1h': 60 * 60 * 1000,
  '6h': 6 * 60 * 60 * 1000,
  '24h': 24 * 60 * 60 * 1000,
  '7d': 7 * 24 * 60 * 60 * 1000,
};

/** Default page size for All Events. Larger than the live tail: it is a query. */
export const MAX_ALL_EVENTS_LIMIT = 1000;

/**
 * Ranges offered by the History tab.
 *
 * Capped at THREE DAYS because that is the audit retention (`FW_HISTORY_TTL`).
 * Offering `7d` here would be offering a window whose older half cannot exist, and
 * the badge would then report fewer rows than the label promises for a reason the
 * operator cannot see.
 */
export const HISTORY_RANGES = {
  '15m': 15 * 60 * 1000,
  '1h': 60 * 60 * 1000,
  '6h': 6 * 60 * 60 * 1000,
  '24h': 24 * 60 * 60 * 1000,
  '3d': 3 * 24 * 60 * 60 * 1000,
};

/** Default page size for History. A page, not the window — `total` says that. */
export const MAX_HISTORY_LIMIT = 1000;

/**
 * Validate `{"op":"history"}`.
 *
 * Same contract as `validateAllEventsQuery`: an unknown `range` falls back to the
 * default rather than erroring (the tab only ever sends a known value, and a stray
 * one must not blank the table), `limit` is clamped because a client asking for
 * more wants what exists, and a non-numeric value IS rejected because that is a bug
 * rather than an over-ask.
 *
 * `key` optionally narrows to one condition's deliveries — the question "show me
 * every time this particular incident fired", which is the main reason to open the
 * audit trail at all.
 */
export function validateHistoryQuery(frame, { maxLimit = MAX_HISTORY_LIMIT, now = Date.now() } = {}) {
  if (!frame || frame.op !== 'history') {
    return { ok: false, code: 'bad_op', message: 'expected {"op":"history"}' };
  }

  const range = typeof frame.range === 'string' && HISTORY_RANGES[frame.range]
    ? frame.range
    : '24h';

  let limit = maxLimit;
  if (frame.limit !== undefined) {
    if (!Number.isFinite(frame.limit)) {
      return { ok: false, code: 'bad_limit', message: 'limit must be a number' };
    }
    limit = Math.max(1, Math.min(maxLimit, Math.floor(frame.limit)));
  }

  // `key` is a record key (32 hex). Anything else is rejected rather than ignored:
  // silently returning the unfiltered window would show an operator rows that do
  // not belong to the incident they asked about.
  let key = null;
  if (frame.key !== undefined) {
    if (typeof frame.key !== 'string' || !/^[0-9a-f]{32}$/.test(frame.key)) {
      return { ok: false, code: 'bad_key', message: 'key must be 32 lowercase hex characters' };
    }
    key = frame.key;
  }

  return {
    ok: true,
    range,
    limit,
    key,
    sinceMs: now - HISTORY_RANGES[range],
    untilMs: now,
  };
}

/**
 * Validate `{"op":"allEvents"}`.
 *
 * `range` is one of the preset keys; anything else falls back to `24h` rather than
 * erroring, because the tab always sends a known value and a stray one should not
 * blank the table. `limit` is clamped for the same reason.
 */
export function validateAllEventsQuery(frame, { maxLimit = MAX_ALL_EVENTS_LIMIT, now = Date.now() } = {}) {
  if (!frame || frame.op !== 'allEvents') {
    return { ok: false, code: 'bad_op', message: 'expected {"op":"allEvents"}' };
  }

  const range = typeof frame.range === 'string' && ALL_EVENTS_RANGES[frame.range]
    ? frame.range
    : '24h';

  let limit = maxLimit;
  if (frame.limit !== undefined) {
    if (!Number.isFinite(frame.limit)) {
      return { ok: false, code: 'bad_limit', message: 'limit must be a number' };
    }
    limit = Math.max(1, Math.min(maxLimit, Math.floor(frame.limit)));
  }

  // An explicit `before` lets the UI page backwards without overlapping the page it
  // already has. Defaults to now.
  let untilMs = now;
  if (frame.before !== undefined) {
    if (!Number.isFinite(frame.before)) {
      return { ok: false, code: 'bad_before', message: 'before must be a number' };
    }
    untilMs = Math.min(frame.before, now);
  }

  return {
    ok: true,
    range,
    limit,
    untilMs,
    sinceMs: untilMs - ALL_EVENTS_RANGES[range],
  };
}
