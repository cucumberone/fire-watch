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
import {
  compileSchema,
  loadDefaultSchema,
  identifierHash,
  isBareKey,
  SCHEMA_PATH,
} from './schema.js';

let cachedSchema = null;

/**
 * The ingest schema as shipped in the image, read from disk once.
 *
 * Read rather than duplicated as a literal: a second copy in code is exactly how
 * a published contract ends up describing something the code no longer does. It is
 * also the fallback the live contract degrades to (services/http-ingress/src/
 * schema.js), which is why it must stay readable even when Valkey is not.
 */
export function ingestSchema() {
  if (!cachedSchema) cachedSchema = loadDefaultSchema();
  return cachedSchema;
}

/** Where the default contract is read from. Reported at startup. */
export { SCHEMA_PATH };

/** Required ingest fields, straight from the FILE schema. */
export function requiredFields() {
  return ingestSchema().required;
}

/** Every property name the FILE schema defines. */
export function schemaFields() {
  return Object.keys(ingestSchema().properties);
}

/**
 * The compiled file contract, built once.
 *
 * Used as the default when a caller does not pass one, which keeps every existing
 * call site and test working unchanged. The live contract is normally supplied by
 * `SchemaSource` (see schema.js), which is what makes an operator's edit take
 * effect without a restart.
 */
let cachedCompiled = null;
export function defaultCompiledSchema() {
  if (!cachedCompiled) cachedCompiled = compileSchema(loadDefaultSchema());
  return cachedCompiled;
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
// Keys an operator added to the schema have no declared bound, so they get one.
// Without it a producer could park an arbitrarily large value on the hash that the
// inspector then has to render.
const MAX_EXTRA_LEN = 1024;

/**
 * Keys this file maps explicitly.
 *
 * Listed so the "extra keys" loop can skip them: a key with a mapping — or one
 * deliberately left out of the payload — must not be re-read against the generic
 * bound. `EVT_Details` is the one that matters, because its own bound is 8192 and
 * the generic one is 1024.
 */
const MAPPED_KEYS = new Set([
  'EVT_Device',
  'EVT_EventKey',
  'EVT_Severity',
  'EVT_Summary',
  'EVT_Agent',
  'EVT_AgentOwner',
  'EVT_Identifier',
  'EVT_Details',
  'EVT_Environment',
  'EVT_URL',
]);
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
 * THE RECORD KEY, and the one thing that changed with the configurable contract.
 *
 *   Without an identifier policy the record is identified by its CONDITION:
 *   `event:<sha256(device + 0x1f + summary)[0:32]>`, derived by the WORKER
 *   (services/worker/src/keygen.js). `EVT_EventKey` becomes `type`, which the UI
 *   shows and the subscription filter matches on, but it is not part of the
 *   identity — two different event keys with the same words on screen are one row
 *   for an operator (docs/valkey.md §2).
 *
 *   When an operator selects identifier keys, the envelope carries `key` and the
 *   record is identified by the hash those keys produce. That is the requested
 *   behaviour: one record per derived identity, computed HERE, before the event
 *   reaches the stream, so every worker and the gateway agree without deriving it
 *   a second time.
 *
 *   The `key` field is the ONLY thing that changes shape downstream, and it is
 *   optional on the envelope — an install that never opens the page produces
 *   byte-identical events to before.
 */
export function fromUpstream(raw, {
  now = Date.now(),
  randomId = defaultId,
  compiled = defaultCompiledSchema(),
  identifierKeys = [],
} = {}) {
  // Required fields come from the CONTRACT, not from this function. Adding one in
  // the GUI is enough to start rejecting requests that omit it.
  const required = compiled.required;
  const req = (name, max) => text(raw[name], name, { required: required.has(name), max });
  const opt = (name, max) => text(raw[name], name, { required: false, max });

  const device = req('EVT_Device', MAX_SOURCE_LEN);
  const eventKey = req('EVT_EventKey', MAX_TYPE_LEN);
  const agent = req('EVT_Agent', MAX_SOURCE_LEN);
  const agentOwner = req('EVT_AgentOwner', MAX_SOURCE_LEN);
  const summary = req('EVT_Summary');

  // Required keys an operator ADDED. Checked here rather than in the loop below,
  // because a missing required field must be a 400 naming the field — the loop skips
  // blanks so an optional key costs no hash field, which would silently swallow it.
  for (const name of compiled.required) {
    if (MAPPED_KEYS.has(name)) continue;
    if (text(raw[name], name, { required: true, max: MAX_EXTRA_LEN }) === '') {
      // `text` already throws for an empty required value; this branch is unreachable
      // and exists only to keep the intent obvious next to the skip above.
      throw new BadRequest(`${name} is required`);
    }
  }

  const rawSeverity = req('EVT_Severity');
  const severity = SEVERITY_MAP[rawSeverity];
  if (!severity) {
    throw new BadRequest(
      `EVT_Severity must be one of ${[...compiled.severities].join(', ')} (got ${JSON.stringify(rawSeverity)})`,
    );
  }

  // `EVT_Identifier` is optional. When present it is the message identity and the
  // record key stays the CONDITION key (device + summary); when blank, the keys an
  // operator selected mint a hash that becomes the record key.
  //
  // Only the blank case consults the picker, and that is not a shortcut: a producer
  // that sends a unique id per delivery would otherwise turn every repeat into a
  // new record, which is the storage explosion the one-record-per-condition model
  // exists to prevent. The selection is a fallback identity for producers that have
  // no id, not an override for producers that do.
  const givenId = opt('EVT_Identifier', MAX_ID_LEN);
  let derivedKey = '';
  let id = givenId;

  if (!id && identifierKeys.length > 0) {
    // A hash of the selected values. Returns '' when they are ALL blank, and that
    // is a deliberate refusal: a hash of "everything empty" is a constant, so
    // every such event in the fleet would collapse onto one record. Falling
    // through to a random id keeps them separate, which is visible; one merged
    // row is not.
    const hash = identifierHash(raw, identifierKeys);
    if (hash) {
      derivedKey = hash;
      id = `ih-${hash}`;
    }
  }
  if (!id) id = randomId(now);

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
  // The identifier that was actually accepted, so the inspector shows the same
  // value the record was keyed by rather than the producer's blank.
  if (givenId) payload.EVT_Identifier = givenId;
  // The producer's original severity is kept for display: the operator may want
  // to know it was MINOR rather than the WARNING it maps to.
  payload.EVT_Severity = rawSeverity;

  // Keys an operator ADDED to the contract. Carried through with a declared bound
  // and no validation beyond "it is text", because the whole point of adding a key
  // is to accept a value the code does not know about.
  //
  // Iterated from the contract, never from the request: a producer posting an
  // unknown `EVT_Whatever` must not be able to mint hash fields on the record.
  //
  // The keys with a mapping of their own are skipped. Without that, the built-in
  // `EVT_Details` — which is not a CORE key, so it counts as "extra" — would be
  // read a second time against the generic bound and a legitimate 4000-character
  // detail body would be refused for being longer than a field nobody added.
  for (const name of compiled.extraFields) {
    if (MAPPED_KEYS.has(name) || name in payload) continue;
    const value = opt(name, MAX_EXTRA_LEN);
    if (value) payload[name] = value;
  }

  const event = {
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
  // Only present when an identifier policy produced it. Absent is the old wire
  // format, which is why the worker's fallback still matters.
  if (derivedKey) event.key = derivedKey;
  return event;
}

/**
 * Normalise and validate one event.
 *
 * Accepts either the upstream schema (detected by `EVT_EventKey`) or the canonical
 * envelope. Both paths converge on the same output, so a legacy producer keeps
 * working while the documented schema is what new producers send.
 */
export function normaliseEvent(raw, {
  now = Date.now(),
  randomId = defaultId,
  compiled = defaultCompiledSchema(),
  identifierKeys = [],
} = {}) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new BadRequest('each event must be an object');
  }

  // Detect the upstream schema by ANY EVT_ field, not by EVT_EventKey alone.
  // Keying the dispatch on EVT_EventKey meant a request missing that one field
  // silently fell through to the legacy path, which invents a default `type` and
  // accepted an event that should have been a 400.
  if (Object.keys(raw).some((k) => k.startsWith('EVT_'))) {
    return fromUpstream(raw, { now, randomId, compiled, identifierKeys });
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

  const event = {
    id,
    source,
    type,
    severity,
    // Producer time. Ordering uses the broker timestamp, not this
    // (docs/event-lifecycle.md §2); it is kept for display only.
    ts: raw.ts !== undefined ? String(raw.ts) : new Date(now).toISOString(),
    payload,
  };
  // A canonical envelope may carry the record key itself — the operator close does,
  // because it must land on the condition it is closing. Only a 32-hex value is
  // accepted: anything else is a producer guessing at an internal format.
  if (isBareKey(raw.key)) event.key = raw.key;
  return event;
}

/** Validate a whole batch. Either all events are valid or the request is 400. */
export function normaliseBatch(body, {
  maxBatch,
  now = Date.now(),
  randomId = defaultId,
  compiled = defaultCompiledSchema(),
  identifierKeys = [],
} = {}) {
  const raw = extractEvents(body);
  if (raw.length === 0) throw new BadRequest('no events in request');
  if (raw.length > maxBatch) {
    throw new BadRequest(`batch too large: ${raw.length} > ${maxBatch}`, 413);
  }
  return raw.map((e, i) => {
    try {
      return normaliseEvent(e, { now, randomId, compiled, identifierKeys });
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
 * The field/value pairs of one ingest stream entry (`XADD fw:stream:ingest * ...`).
 *
 * `evt` is the canonical envelope as JSON; the worker parses only that field.
 * `src` and `cid` are for debugging (`XRANGE`) and are never used for routing.
 * No timestamp is sent: the arrival time is the entry ID, which Valkey assigns
 * (docs/event-lifecycle.md §2).
 */
export function streamFields(event, { src = 'http', correlationId } = {}) {
  const fields = ['evt', JSON.stringify(event), 'src', src];
  if (correlationId) fields.push('cid', String(correlationId));
  return fields;
}
