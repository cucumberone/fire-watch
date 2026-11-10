/**
 * HTTP body → event envelope.
 *
 * Pure functions. The HTTP ingress is the only place where an untrusted client
 * can create an event, so normalisation and validation both happen here rather
 * than relying on downstream components.
 *
 * Accepted shapes:
 *
 *   1. one event in the upstream schema   { "EVT_Severity": ..., "EVT_EventKey": ... }
 *   2. an array of those                  [ {...}, {...} ]
 *   3. a wrapped batch                    { "events": [ {...}, {...} ] }
 *
 * The upstream field names are mapped here, once, at the boundary. Everything
 * downstream — the worker, the fold, the gateway, the UI — speaks the canonical
 * envelope (`id`/`source`/`type`/`severity`/`payload`), so a schema change is a
 * change to this file alone.
 */

/**
 * Source severities, as the producer reports them.
 *
 * These are NOT the severities the alert board uses. They are mapped onto an
 * internal scale by SEVERITY_MAP below, because "MINOR" and "MAJOR" say nothing
 * about whether an operator must act, while "this is resolved" does.
 */
const SOURCE_SEVERITIES = new Set([
  'CLEAR',
  'HARMLESS',
  'INFORMATION',
  'INDETERMINATE',
  'WARNING',
  'MINOR',
  'MAJOR',
  'CRITICAL',
]);

/**
 * Source severity -> internal severity.
 *
 * The internal scale is deliberately tiny: three levels of urgency plus a
 * retirement. Anything finer is a property of the source system, not something an
 * operator triages differently at 3am.
 *
 * CLEAR and HARMLESS both RETIRE the alert, matching the fold's existing rule.
 * That is the important mapping here: the upstream schema has no "CLOSE", so
 * `CLEAR` is the resolution signal and must not be treated as just another
 * severity — otherwise a resolved alarm would stay on the board forever.
 */
const SEVERITY_MAP = {
  CRITICAL: 'CRITICAL',
  MAJOR: 'CRITICAL',
  MINOR: 'WARNING',
  WARNING: 'WARNING',
  INDETERMINATE: 'WARNING',
  INFORMATION: 'HARMLESS',
  CLEAR: 'CLOSE',
  HARMLESS: 'CLOSE',
};

/** Internal severities the rest of the system accepts. */
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

/** Trim a source field and reject anything unusable. */
function text(value, field, { required = false, max = 256 } = {}) {
  if (value === undefined || value === null) {
    if (required) throw new BadRequest(`${field} is required`);
    return '';
  }
  const s = String(value).trim();
  if (required && s === '') throw new BadRequest(`${field} is required`);
  if (s.length > max) throw new BadRequest(`${field} must be at most ${max} characters`);
  return s;
}

/**
 * Map one event in the upstream schema onto the canonical envelope.
 *
 * Field mapping, and the reasoning for the non-obvious ones:
 *
 *   EVT_EventKey  -> source:type   (split back apart below)
 *   EVT_Severity  -> severity      (via SEVERITY_MAP)
 *   EVT_Summary   -> payload.msg   (the summary line)
 *   EVT_Details   -> payload.msgc  (the long text, compressed downstream)
 *   EVT_Identifier-> id            (or generated when blank)
 *   rest          -> payload.EVT_* (kept as-is for the inspector)
 *
 * The alert key is `source:type`, which is what the fold groups on. `EVT_EventKey`
 * is the only stable identifier the producer guarantees, so it is what those two
 * fields are derived from: `device:eventKey`. Two events with the same event key
 * for the same device are the same alert — which is exactly the semantics the
 * upstream schema implies by calling it a key.
 */
export function fromUpstream(raw, { now = Date.now(), randomId = defaultId } = {}) {
  const device = text(raw.EVT_Device, 'EVT_Device', { required: true, max: MAX_SOURCE_LEN });
  const eventKey = text(raw.EVT_EventKey, 'EVT_EventKey', { required: true, max: MAX_TYPE_LEN });
  const agent = text(raw.EVT_Agent, 'EVT_Agent', { required: true, max: MAX_SOURCE_LEN });
  const agentOwner = text(raw.EVT_AgentOwner, 'EVT_AgentOwner', { required: true, max: MAX_SOURCE_LEN });
  const summary = text(raw.EVT_Summary, 'EVT_Summary', { required: true });

  const rawSeverity = text(raw.EVT_Severity, 'EVT_Severity', { required: true });
  const severity = SEVERITY_MAP[rawSeverity];
  if (!severity) {
    throw new BadRequest(
      `EVT_Severity must be one of ${[...SOURCE_SEVERITIES].join(', ')} (got ${JSON.stringify(rawSeverity)})`,
    );
  }

  // `EVT_Identifier` is optional. When present it is the message identity; when
  // blank a random one is generated, because the duplicate gate needs an id.
  const givenId = text(raw.EVT_Identifier, 'EVT_Identifier', { max: MAX_ID_LEN });
  const id = givenId || randomId(now);

  // The summary is the row's display text; the details are the expandable body.
  // Keeping them as msg/msgc means the existing compression and UI work unchanged.
  const payload = {
    msg: summary,
    msgc: text(raw.EVT_Details, 'EVT_Details'),
    // Everything else is carried through untouched, so the inspector can show it
    // without this file needing to know what each field means.
    EVT_Agent: agent,
    EVT_AgentOwner: agentOwner,
    EVT_EventKey: eventKey,
    EVT_Device: device,
  };
  const environment = text(raw.EVT_Environment, 'EVT_Environment');
  if (environment) payload.EVT_Environment = environment;
  const url = text(raw.EVT_URL, 'EVT_URL');
  if (url) payload.EVT_URL = url;
  // The producer's original severity is kept for display: the operator may want
  // to know it was MINOR rather than the WARNING it maps to.
  payload.EVT_Severity = rawSeverity;

  return {
    id,
    // The alert key is device + event key: the two things the upstream schema
    // guarantees identify an alarm. Using the agent would split one device's
    // alarm across every tool that reports it.
    source: device,
    type: eventKey,
    severity,
    ts: raw.ts !== undefined ? String(raw.ts) : new Date(now).toISOString(),
    payload,
  };
}

/**
 * Normalise and validate one event.
 *
 * Accepts either the upstream schema (detected by `EVT_EventKey`) or the canonical
 * envelope. Both paths converge on the same output, so a legacy producer keeps
 * working while the documented schema is what new producers send.
 */
export function normaliseEvent(raw, { now = Date.now(), randomId = defaultId } = {}) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new BadRequest('each event must be an object');
  }

  // Detect the upstream schema by ANY EVT_ field, not by EVT_EventKey alone.
  // Keying the dispatch on EVT_EventKey meant a request missing that one field
  // silently fell through to the legacy path, which invents a default `type` and
  // accepted an event that should have been a 400.
  if (Object.keys(raw).some((k) => k.startsWith('EVT_'))) {
    return fromUpstream(raw, { now, randomId });
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
