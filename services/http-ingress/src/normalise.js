/**
 * HTTP body → event envelope.
 *
 * Pure functions. The HTTP ingress is the only place where an untrusted client
 * can create an event, so normalisation and validation both happen here rather
 * than relying on downstream components.
 *
 * Accepted shapes (all three, because producers vary):
 *
 *   1. a single event object        { "id": ..., "source": ... }
 *   2. an array of events           [ {...}, {...} ]
 *   3. a wrapped batch              { "events": [ {...}, {...} ] }
 *
 * Anything else is a 400 — never silently dropped.
 */

const SEVERITIES = new Set(['FATAL', 'CRITICAL', 'WARNING', 'HARMLESS', 'CLOSE']);
const MAX_SOURCE_LEN = 256;
const MAX_TYPE_LEN = 256;
const MAX_ID_LEN = 128;

export class BadRequest extends Error {
  constructor(message, status = 400) {
    super(message);
    this.status = status;
  }
}

/** Extract an event array from whatever the client sent. */
export function extractEvents(body) {
  if (Array.isArray(body)) return body;
  if (body && typeof body === 'object') {
    if (Array.isArray(body.events)) return body.events;
    return [body];
  }
  throw new BadRequest('body must be a JSON object, an array, or {"events":[...]}');
}

/**
 * Normalise and validate one event.
 *
 * Missing fields that can be inferred are filled; missing fields that cannot be
 * are an error. `id` and `ts` are generated when absent because an external
 * producer may be a dumb shipper that only forwards text.
 */
export function normaliseEvent(raw, { now = Date.now(), randomId = defaultId } = {}) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new BadRequest('each event must be an object');
  }

  const id = raw.id === undefined || raw.id === null ? randomId(now) : String(raw.id);
  if (id.length === 0 || id.length > MAX_ID_LEN) {
    throw new BadRequest(`id must be 1..${MAX_ID_LEN} characters`);
  }

  const source = raw.source === undefined ? 'http-ingress' : String(raw.source);
  if (source.length === 0 || source.length > MAX_SOURCE_LEN) {
    throw new BadRequest(`source must be 1..${MAX_SOURCE_LEN} characters`);
  }

  const type = raw.type === undefined ? 'http.event' : String(raw.type);
  if (type.length === 0 || type.length > MAX_TYPE_LEN) {
    throw new BadRequest(`type must be 1..${MAX_TYPE_LEN} characters`);
  }

  const severity = raw.severity === undefined ? 'WARNING' : String(raw.severity);
  if (!SEVERITIES.has(severity)) {
    throw new BadRequest(`severity must be one of ${[...SEVERITIES].join(', ')}`);
  }

  // payload: accept an explicit object, or a bare `msg`/`msgc` pair, or nothing.
  let payload = raw.payload;
  if (payload === undefined || payload === null) {
    payload = {};
    if (raw.msg !== undefined) payload.msg = String(raw.msg);
    if (raw.msgc !== undefined) payload.msgc = String(raw.msgc);
  } else if (typeof payload !== 'object' || Array.isArray(payload)) {
    throw new BadRequest('payload must be an object');
  }

  return {
    id,
    source,
    type,
    severity,
    // Producer time. Ordering uses the broker timestamp, not this
    // (docs/event-lifecycle.md §2); it is kept for display only.
    ts: raw.ts !== undefined ? String(raw.ts) : new Date(now).toISOString(),
    payload,
  };
}

/** Validate a whole batch. Either all events are valid or the request is 400. */
export function normaliseBatch(body, { maxBatch, now = Date.now(), randomId = defaultId } = {}) {
  const raw = extractEvents(body);
  if (raw.length === 0) throw new BadRequest('no events in request');
  if (raw.length > maxBatch) {
    throw new BadRequest(`batch too large: ${raw.length} > ${maxBatch}`, 413);
  }
  return raw.map((e, i) => {
    try {
      return normaliseEvent(e, { now, randomId });
    } catch (err) {
      throw new BadRequest(`event[${i}]: ${err.message}`);
    }
  });
}

let counter = 0;
function defaultId(now) {
  counter = (counter + 1) % 0x10000;
  return `${now.toString(36)}-${counter.toString(36).padStart(4, '0')}-${Math.random().toString(36).slice(2, 8)}`;
}

/**
 * The AMQP message properties we set.
 *
 * `persistent` matters: the queue is a quorum queue and the event must survive a
 * broker restart. Without deliveryMode 2 the message can be dropped from memory
 * on restart even in a durable queue.
 */
export function messageProperties(event, correlationId) {
  return {
    persistent: true,
    deliveryMode: 2,
    contentType: 'application/json',
    contentEncoding: 'identity',
    type: event.type,
    messageId: event.id,
    correlationId,
    timestamp: Math.floor(Date.now() / 1000),
    appId: 'http-ingress',
    headers: {
      'x-ingress-source': 'http',
      'x-event-source': event.source,
      'x-event-severity': event.severity,
    },
  };
}
