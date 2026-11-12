/**
 * HTTP body → event envelope.
 *
 * Pure functions. The HTTP ingress is the only place where an untrusted client
 * can create an event, so normalisation and validation both happen here rather
 * than relying on downstream components.
 *
 * Accepted shapes:
 *
 *   1. one event in the ingest schema   { "EVT_Severity": ..., "EVT_EventKey": ... }
 *   2. an array of those                [ {...}, {...} ]
 *   3. a wrapped batch                  { "events": [ {...}, {...} ] }
 *
 * The schema itself lives at schemas/event-ingest.schema.json and is documented in
 * docs/event-schema.md. Validation and the mapping below are both driven by it, so
 * the published contract and the implementation cannot drift: adding a required
 * field to the JSON requires changing the mapping too, because the test suite
 * asserts that every property in the schema is handled.
 *
 * Everything downstream — the worker, the fold, the gateway, the UI — speaks the
 * canonical envelope (`id`/`source`/`type`/`severity`/`payload`), so a schema
 * change is a change to this file plus the schema alone.
 */
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));
/** Repo-root `schemas/`, resolved from this file so cwd does not matter. */
const SCHEMA_PATH = join(HERE, '..', '..', '..', 'schemas', 'event-ingest.schema.json');

let cachedSchema = null;

/**
 * The ingest schema, read from disk once.
 *
 * Read rather than duplicated as a literal: a second copy in code is exactly how
 * a published contract ends up describing something the code no longer does.
 */
export function ingestSchema() {
  if (!cachedSchema) {
    cachedSchema = JSON.parse(readFileSync(SCHEMA_PATH, 'utf8'));
  }
  return cachedSchema;
}

/** Required ingest fields, straight from the schema. */
export function requiredFields() {
  return ingestSchema().required;
}

/** Every property name the schema defines. */
export function schemaFields() {
  return Object.keys(ingestSchema().properties);
}

/**
 * Source severities, as the producer reports them — taken from the schema's
 * `enum` so an added value cannot be accepted by one and rejected by the other.
 */
function sourceSeverities() {
  return new Set(ingestSchema().properties.EVT_Severity.enum);
}

/**
 * Source severity -> internal severity.
 *
 * The internal scale is deliberately tiny: three levels of urgency plus a
 * retirement. Anything finer is a property of the source system, not something an
 * operator triages differently at 3am.
 *
 * CLEAR and HARMLESS both RETIRE the alert, matching the fold's existing rule.
 * That is the important mapping here: the ingest schema has no "CLOSE", so `CLEAR`
 * is the resolution signal and must not be treated as just another severity —
 * otherwise a resolved alarm would stay on the board forever.
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
// The details field is the expandable body, not a label. Without its own bound it
// would inherit the 256 default, which rejects any producer that actually explains
// what happened. The worker's gzip threshold is 512 bytes, so shorter values are
// stored uncompressed but still worth accepting.
const MAX_DETAILS_LEN = 8192;

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
 *   EVT_EventKey  -> type          (the device is `source`)
 *   EVT_Severity  -> severity      (via SEVERITY_MAP)
 *   EVT_Summary   -> payload.msg   (the summary line)
 *   EVT_Details   -> payload.msgc  (the long text, compressed downstream)
 *   EVT_Identifier-> id            (or generated when blank)
 *   rest          -> payload.EVT_* (kept as-is for the inspector)
 *
 * The record key is derived from `EVT_Device` + `EVT_Summary`, NOT from
 * `EVT_EventKey`: it is
 * `event:<sha256(device + 0x1f + summary)[0:32]>`, computed by the worker
 * (services/worker/src/keygen.js). `EVT_EventKey` still becomes `type`, which the
 * UI shows and the subscription filter matches on, but it is not part of the
 * identity — two different event keys with the same words on screen are one row
 * for an operator (docs/valkey.md §2).
 */
export function fromUpstream(raw, { now = Date.now(), randomId = defaultId } = {}) {
  // Required fields come from the schema, not from this function. Adding one
  // there is enough to start rejecting requests that omit it — and the test suite
  // fails if it is not also mapped below, so the two cannot drift apart.
  const required = new Set(requiredFields());
  const req = (name, max) => text(raw[name], name, { required: required.has(name), max });
  const opt = (name, max) => text(raw[name], name, { required: false, max });

  const device = req('EVT_Device', MAX_SOURCE_LEN);
  const eventKey = req('EVT_EventKey', MAX_TYPE_LEN);
  const agent = req('EVT_Agent', MAX_SOURCE_LEN);
  const agentOwner = req('EVT_AgentOwner', MAX_SOURCE_LEN);
  const summary = req('EVT_Summary');

  const rawSeverity = req('EVT_Severity');
  const severity = SEVERITY_MAP[rawSeverity];
  if (!severity) {
    throw new BadRequest(
      `EVT_Severity must be one of ${[...sourceSeverities()].join(', ')} (got ${JSON.stringify(rawSeverity)})`,
    );
  }

  // `EVT_Identifier` is optional. When present it is the message identity; when
  // blank a random one is generated, because the duplicate gate needs an id.
  const givenId = opt('EVT_Identifier', MAX_ID_LEN);
  const id = givenId || randomId(now);

  // The summary is the row's display text; the details are the expandable body.
  // Keeping them as msg/msgc means the existing compression and UI work unchanged.
  const payload = {
    msg: summary,
    msgc: opt('EVT_Details', MAX_DETAILS_LEN),
    // Everything else is carried through untouched, so the inspector can show it
    // without this file needing to know what each field means.
    EVT_Agent: agent,
    EVT_AgentOwner: agentOwner,
    EVT_EventKey: eventKey,
    EVT_Device: device,
  };
  const environment = opt('EVT_Environment');
  if (environment) payload.EVT_Environment = environment;
  const url = opt('EVT_URL');
  if (url) payload.EVT_URL = url;
  // The producer's original severity is kept for display: the operator may want
  // to know it was MINOR rather than the WARNING it maps to.
  payload.EVT_Severity = rawSeverity;

  return {
    id,
    // The alert key is device + event key: the two things the ingest schema
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
