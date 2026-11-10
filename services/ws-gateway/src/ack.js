/**
 * Acknowledgement: validation and frame rendering.
 *
 * Pure functions only, so the rules that protect the write path are testable
 * without a socket or a Dragonfly.
 *
 * Specified by docs/acknowledge.md §3.
 *
 * An ack targets the ALERT (`source:type`), not an individual event: the alert
 * hash `fw:alert:{key}` is what the fold maintains and what the UI shows as a
 * row. Acking an event id would leave the alert row still firing.
 */

/** Alert keys are `source:type`. Bounded so a client cannot grow the keyspace. */
export const MAX_KEY_LEN = 256;
export const MAX_BY_LEN = 128;
export const MAX_ALERT_QUERY_KEYS = 200;

/** Default operator label until SSO is in front of the Route. */
export const ANONYMOUS = 'anonymous';

/**
 * Strip anything that would corrupt a log line or a JSON frame, and collapse
 * whitespace. Control characters are removed rather than escaped because the
 * value ends up in a hash field that an operator reads on screen.
 */
function cleanText(value, maxLen) {
  if (typeof value !== 'string') return '';
  // eslint-disable-next-line no-control-regex
  const stripped = value.replace(/[\u0000-\u001f\u007f]/g, ' ');
  return stripped.replace(/\s+/g, ' ').trim().slice(0, maxLen);
}

/**
 * Is this a usable alert key?
 *
 * Rejects control characters and length overflow. `*` and `?` are allowed:
 * HSET does no glob matching, so they are inert here, and a rejection would be
 * a lie about what is safe.
 */
export function isValidAlertKey(key) {
  if (typeof key !== 'string') return false;
  if (key.length === 0 || key.length > MAX_KEY_LEN) return false;
  // eslint-disable-next-line no-control-regex
  if (/[\u0000-\u001f\u007f]/.test(key)) return false;
  return true;
}

/**
 * Validate an inbound `op: ack` frame.
 *
 * `by` is optional and defaults to `anonymous`, because there is no
 * authentication in front of the Route yet. When SSO lands it will supply the
 * real identity and this default should become a rejection instead — see
 * docs/acknowledge.md §7.
 */
export function validateAck(frame) {
  if (!frame || frame.op !== 'ack') {
    return { ok: false, code: 'bad_op', message: 'expected {"op":"ack"}' };
  }
  if (!isValidAlertKey(frame.key)) {
    return {
      ok: false,
      code: 'bad_key',
      message: `key must be a non-empty string of at most ${MAX_KEY_LEN} chars`,
    };
  }
  const by = cleanText(frame.by, MAX_BY_LEN) || ANONYMOUS;
  return { ok: true, key: frame.key, by };
}

/**
 * Validate an inbound `op: alerts` frame — a bulk state read.
 *
 * The client sends the keys it is currently displaying. This is deliberate: a
 * server-side "list all open alerts" would be the unbounded `SCAN fw:alert:*`
 * that the design refuses everywhere else (websocket-server.md §3).
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
    if (!isValidAlertKey(k)) {
      return { ok: false, code: 'bad_key', message: 'every key must be a valid alert key' };
    }
    if (!keys.includes(k)) keys.push(k);
  }
  return { ok: true, keys };
}

/** Dragonfly hash key for an alert. One place, so the prefix cannot drift. */
export function ackAlertKey(key) {
  return `fw:alert:${key}`;
}

/**
 * Normalise a stored ack into the wire shape.
 *
 * `acked_at` absent means not acked. A hash with `acked_by` but no `acked_at`
 * (a partial write, or a field left over from an older build) is treated as NOT
 * acked: a timestamp is what makes the ack meaningful, so without one there is
 * nothing an operator could be shown.
 */
export function normaliseAck(hash) {
  if (!hash) return null;
  const at = Number(hash.acked_at);
  if (!Number.isFinite(at) || at <= 0) return null;
  return {
    by: typeof hash.acked_by === 'string' && hash.acked_by !== '' ? hash.acked_by : ANONYMOUS,
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
 * Same shape as `ack`, and the same key rules: closing targets the ALERT
 * (`source:type`), because that is what the fold maintains and what the UI shows
 * as a row. Closing an event id would leave the alert row still firing.
 */
export function validateClose(frame) {
  if (!frame || frame.op !== 'close') {
    return { ok: false, code: 'bad_op', message: 'expected {"op":"close"}' };
  }
  if (!isValidAlertKey(frame.key)) {
    return {
      ok: false,
      code: 'bad_key',
      message: `key must be a non-empty string of at most ${MAX_KEY_LEN} chars`,
    };
  }
  const by = cleanText(frame.by, MAX_BY_LEN) || ANONYMOUS;
  return { ok: true, key: frame.key, by };
}

/**
 * Frame telling clients an alert is gone.
 *
 * Reuses `op: alert` with `applied: 3`, which is exactly what a CLOSE event from
 * a source produces. A separate op would mean two code paths in every client for
 * one state transition (docs/event-lifecycle.md §5.2).
 */
export function closeFrame(key) {
  return JSON.stringify({ op: 'alert', key, applied: 3, closedBy: 'operator' });
}

/**
 * Validate an inbound `op: delete` frame.
 *
 * Deletion targets an EVENT id, not an alert key — unlike ack and close. The
 * operator is removing one record from the timeline, and an alert may be backed
 * by many events, so an alert-shaped frame would be ambiguous about what is being
 * destroyed.
 *
 * Ids are bounded the same way as alert keys, and by the same limit the ingress
 * accepts, so a valid event can always be deleted and nothing else can.
 */
export const MAX_ID_LEN = MAX_KEY_LEN;

export function isValidEventId(id) {
  if (typeof id !== 'string') return false;
  if (id.length === 0 || id.length > MAX_ID_LEN) return false;
  // eslint-disable-next-line no-control-regex
  if (/[\u0000-\u001f\u007f]/.test(id)) return false;
  return true;
}

export function validateDelete(frame) {
  if (!frame || frame.op !== 'delete') {
    return { ok: false, code: 'bad_op', message: 'expected {"op":"delete"}' };
  }
  if (!isValidEventId(frame.id)) {
    return {
      ok: false,
      code: 'bad_id',
      message: `id must be a non-empty string of at most ${MAX_ID_LEN} chars`,
    };
  }
  const by = cleanText(frame.by, MAX_BY_LEN) || ANONYMOUS;
  return { ok: true, id: frame.id, by };
}

/** Event record key. Mirrors the worker's `keys.event()`. */
export function eventKey(id) {
  return `fw:evt:${id}`;
}

/**
 * Frame telling clients an event was deleted.
 *
 * A distinct op rather than an `alert` frame, because the two clients that care
 * need different things: the live tail removes the row by id, while the alert
 * table must re-derive its count. Sending `op: deleted` lets each decide, instead
 * of overloading `applied: 3` (which means "this alert is retired", a different
 * statement from "this one event record no longer exists").
 */
export function deletedFrame(id) {
  return JSON.stringify({ op: 'deleted', id });
}
