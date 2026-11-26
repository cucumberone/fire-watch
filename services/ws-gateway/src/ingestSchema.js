/**
 * Ingest-schema configuration — the gateway's half.
 *
 * Stores and validates an operator's edit to the ingest contract. The DEFAULT
 * contract is not here: the gateway image does not ship `schemas/`, and the client
 * needs the defaults anyway to render the form before the first reply. So this
 * module answers `{schema: null, source: 'file'}` for an untouched install and the
 * client falls back to the document it was built with.
 *
 * WHY THE VALIDATION IS DUPLICATED FROM services/http-ingress/src/schema.js
 *
 *   The two images share nothing at runtime — different build contexts, different
 *   `COPY` lines — so a shared module would mean restructuring both Dockerfiles for
 *   one file. Instead the rules live in both, and
 *   `services/ws-gateway/src/ingestSchema.test.js` imports the ingress copy and
 *   asserts the two give the same verdict on a corpus of documents. That is the
 *   established pattern here: `columns.test.js` does the same against the schema
 *   file.
 *
 *   The duplication has to be EXACT, because the split is user-visible in the worst
 *   way: a document the gateway accepted but the ingress refuses is stored, reported
 *   as saved, and then silently ignored at ingest time.
 *
 * STORAGE. `input-schema`, one hash, two fields:
 *
 *   schema          JSON of the JSON Schema document
 *   identifierKeys  JSON array of `EVT_*` keys used to derive a missing identifier
 */

/** Valkey hash holding the live contract. */
export const INPUT_SCHEMA_KEY = 'input-schema';
export const SCHEMA_FIELD = 'schema';
export const IDENTIFIER_KEYS_FIELD = 'identifierKeys';

/**
 * Keys that cannot be removed and cannot stop being required.
 *
 * The pipeline depends on every one of them: device + summary IS the condition key,
 * severity drives the mapping and the enum, event key is the type column and the
 * subscription filter, and agent/agent owner are on every row.
 */
export const CORE_KEYS = [
  'EVT_Severity',
  'EVT_Summary',
  'EVT_Device',
  'EVT_EventKey',
  'EVT_Agent',
  'EVT_AgentOwner',
];

/** Longest key name. A hash field, a table cell and a JSON property at once. */
export const MAX_KEY_LEN = 64;
/** Longest identifier-key selection. More than this is a fingerprint, not an id. */
export const MAX_IDENTIFIER_KEYS = 8;
/** Bound on the stored document. A schema is a few KiB; this is a sanity stop. */
export const MAX_SCHEMA_BYTES = 64 * 1024;

/** Trim and bound a key name. Returns '' when unusable. */
function cleanKey(value) {
  if (typeof value !== 'string') return '';
  const s = value.trim();
  if (!/^EVT_[A-Za-z0-9_]+$/.test(s)) return '';
  return s.length > MAX_KEY_LEN ? '' : s;
}

/**
 * Validate a candidate schema document.
 *
 * Rejects the WHOLE document on any problem rather than repairing it: a half-applied
 * contract would accept some payloads and reject others for reasons no producer
 * could see, and the operator's form would show something the ingress does not use.
 *
 * Only `type: "string"` properties are accepted. That is a property of this
 * pipeline, not of JSON Schema: every value is stored as text in a hash field and
 * rendered as text in a table cell.
 */
export function validateInputSchema(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    return { ok: false, message: 'schema must be an object' };
  }
  if (raw.type !== 'object') {
    return { ok: false, message: 'schema.type must be "object"' };
  }
  if (!raw.properties || typeof raw.properties !== 'object' || Array.isArray(raw.properties)) {
    return { ok: false, message: 'schema.properties must be an object' };
  }

  const properties = {};
  for (const [name, def] of Object.entries(raw.properties)) {
    const key = cleanKey(name);
    if (!key) {
      return {
        ok: false,
        message: `${JSON.stringify(name)} is not a usable key (EVT_ prefix, A-Z 0-9 _, at most ${MAX_KEY_LEN} characters)`,
      };
    }
    if (!def || typeof def !== 'object' || Array.isArray(def)) {
      return { ok: false, message: `${key} must be an object` };
    }
    if (def.type !== 'string') {
      return { ok: false, message: `${key}.type must be "string"` };
    }
    properties[key] = def;
  }

  if (Object.keys(properties).length === 0) {
    return { ok: false, message: 'schema.properties must not be empty' };
  }

  const required = Array.isArray(raw.required) ? raw.required : [];
  for (const name of required) {
    if (!properties[name]) {
      return { ok: false, message: `required key ${JSON.stringify(name)} is not a property` };
    }
  }
  for (const core of CORE_KEYS) {
    if (!properties[core]) {
      return { ok: false, message: `${core} cannot be removed; the pipeline depends on it` };
    }
    if (!required.includes(core)) {
      return { ok: false, message: `${core} must stay required; the pipeline depends on it` };
    }
  }

  // Severity is an enum, and the ingress's mapping is driven by it. An empty or
  // missing enum would make every event either unmappable or acceptable.
  const sev = properties.EVT_Severity;
  if (!Array.isArray(sev.enum) || sev.enum.length === 0
      || sev.enum.some((v) => typeof v !== 'string' || v.trim() === '')) {
    return { ok: false, message: 'EVT_Severity needs a non-empty enum of strings' };
  }
  if (sev.enum.length !== new Set(sev.enum).size) {
    return { ok: false, message: 'EVT_Severity.enum has duplicate values' };
  }

  const schema = {
    ...raw,
    type: 'object',
    additionalProperties: raw.additionalProperties !== false,
    properties,
    required: [...new Set(required)],
  };

  let serialised;
  try {
    serialised = JSON.stringify(schema);
  } catch {
    return { ok: false, message: 'schema is not serialisable' };
  }
  const bytes = Buffer.byteLength(serialised, 'utf8');
  if (bytes > MAX_SCHEMA_BYTES) {
    return { ok: false, message: `schema is larger than ${MAX_SCHEMA_BYTES} bytes` };
  }

  return { ok: true, schema, bytes };
}

/**
 * Validate the identifier-key selection against a schema.
 *
 * An empty selection is valid and means "keep the legacy identity" — an install that
 * never opens the page behaves exactly as it did before.
 */
export function validateIdentifierKeys(value, schema) {
  if (value === undefined || value === null) return { ok: true, keys: [] };
  if (!Array.isArray(value)) {
    return { ok: false, message: 'identifierKeys must be an array' };
  }
  const keys = [];
  for (const item of value) {
    const key = cleanKey(item);
    if (!key) return { ok: false, message: `${JSON.stringify(item)} is not a usable key` };
    if (!schema.properties[key]) {
      return { ok: false, message: `${key} is not in the schema` };
    }
    if (!keys.includes(key)) keys.push(key);
  }
  if (keys.length > MAX_IDENTIFIER_KEYS) {
    return { ok: false, message: `at most ${MAX_IDENTIFIER_KEYS} identifier keys` };
  }
  return { ok: true, keys };
}

/**
 * Validate a whole `{"op":"setIngestSchema"}` frame.
 *
 * Both halves are validated together and rejected together: a schema saved without
 * its selection, or a selection naming a key the new schema dropped, would leave the
 * ingress hashing fields that no longer exist — a constant hash for every event,
 * which collapses the whole fleet onto one record.
 */
export function validateSetIngestSchema(frame) {
  if (!frame || frame.op !== 'setIngestSchema') {
    return { ok: false, code: 'bad_op', message: 'expected {"op":"setIngestSchema"}' };
  }

  const schemaVerdict = validateInputSchema(frame.schema);
  if (!schemaVerdict.ok) {
    return { ok: false, code: 'bad_schema', message: schemaVerdict.message };
  }

  const keysVerdict = validateIdentifierKeys(frame.identifierKeys, schemaVerdict.schema);
  if (!keysVerdict.ok) {
    return { ok: false, code: 'bad_identifier_keys', message: keysVerdict.message };
  }

  const by = typeof frame.by === 'string' && frame.by.trim() !== ''
    ? frame.by.replace(/\s+/g, ' ').trim().slice(0, 128)
    : 'anonymous';

  return { ok: true, schema: schemaVerdict.schema, identifierKeys: keysVerdict.keys, by };
}

/**
 * The wire frame carrying the live contract.
 *
 * `source` distinguishes "nothing stored, use your defaults" (`file`) from an
 * operator's document (`valkey`). `schema` is null on `file`, because the gateway
 * does not ship the default document — the client renders it.
 *
 * `identifierKeyChoices` is the list the picker must always offer even when it is
 * not selected, so an operator can turn a key on after turning it off.
 */
export function ingestSchemaFrame({ schema, identifierKeys, source, version }) {
  return JSON.stringify({
    op: 'ingestSchema',
    schema: schema ?? null,
    identifierKeys: Array.isArray(identifierKeys) ? identifierKeys : [],
    identifierKeyChoices: CORE_KEYS,
    source,
    version: version ?? null,
  });
}
