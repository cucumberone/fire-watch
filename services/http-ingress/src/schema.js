/**
 * The ingest contract, as the ingress compiles and applies it.
 *
 * The VALIDATION RULES live in this file, and `services/ws-gateway/src/ingestSchema.js`
 * carries a copy, because the two images share no runtime module tree (different
 * build contexts, different `COPY` lines). A test in the gateway imports this file
 * and asserts the two give the same verdict on a corpus of documents, which is what
 * keeps the duplication honest — the same pattern `columns.test.js` already uses
 * against `schemas/event-ingest.schema.json`.
 *
 * The duplication has to be EXACT, because the split is user-visible in the worst
 * way: a document the gateway accepted but the ingress refuses is stored, reported as
 * saved, and then silently ignored at ingest time.
 *
 * This module additionally owns what needs the filesystem or a driver:
 *
 *   - `loadDefaultSchema()`  the contract shipped in the image, used as the default
 *     and as what a corrupt stored document degrades to;
 *   - `SchemaSource`         the poll loop that makes an operator's edit live.
 *
 * STORAGE. A Valkey hash at `input-schema`, two fields:
 *
 *   schema          JSON of the whole JSON Schema document
 *   identifierKeys  JSON array of `EVT_*` keys used to derive a missing identifier
 *
 * A hash rather than two keys, because the two fields describe one decision and a
 * key-by-key write could be read between the halves.
 */
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));
/** Repo-root `schemas/`, resolved from this file so cwd does not matter. */
export const SCHEMA_PATH = join(HERE, '..', '..', '..', 'schemas', 'event-ingest.schema.json');

/** Valkey hash holding the live contract. */
export const INPUT_SCHEMA_KEY = 'input-schema';
export const SCHEMA_FIELD = 'schema';
export const IDENTIFIER_KEYS_FIELD = 'identifierKeys';

/**
 * Keys that cannot be removed and cannot stop being required.
 *
 * Listed here rather than derived from the file, because the point is that the
 * guarantee survives an operator editing the schema: the file is what we start
 * from, this is what we refuse to lose.
 *
 *   EVT_Device, EVT_Summary   the condition key (sha256 of the two)
 *   EVT_Severity              the mapping onto the internal scale, and the enum
 *   EVT_EventKey              the table's type column and the subscription filter
 *   EVT_Agent, EVT_AgentOwner who is reporting it, shown on every row
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

/** Separator for the identifier hash. Cannot occur in a normalised value. */
const SEP = '\u001f';

/** 32 hex characters — the shape every record key in this system has. */
export function isBareKey(value) {
  return typeof value === 'string' && /^[0-9a-f]{32}$/.test(value);
}

/** Trim and bound a key name. Returns '' when unusable. */
export function cleanKey(value) {
  if (typeof value !== 'string') return '';
  const s = value.trim();
  if (!/^EVT_[A-Za-z0-9_]+$/.test(s)) return '';
  return s.length > MAX_KEY_LEN ? '' : s;
}

/** Collapse whitespace so a padded value cannot mint a different identity. */
function component(value) {
  return String(value ?? '').replace(/\s+/g, ' ').trim();
}

/** The contract as shipped in the image. Throws when the file is missing. */
export function loadDefaultSchema() {
  return JSON.parse(readFileSync(SCHEMA_PATH, 'utf8'));
}

/**
 * Validate a candidate schema document.
 *
 * Rejects the WHOLE document on any problem rather than repairing it: a
 * half-applied contract would accept some payloads and reject others for reasons no
 * producer could see, and the operator's form would show something the ingress does
 * not use.
 *
 * Only `type: "string"` properties are accepted. That is a property of this
 * pipeline, not of JSON Schema: every value is stored as text in a hash field and
 * rendered as text in a table cell, so a `number`/`object` property would silently be
 * stringified and a `required` check on it would compare against "undefined".
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

  // Severity is an enum, and the mapping below is driven by it. An empty or missing
  // enum would make every event either unmappable or acceptable.
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
 * Compile a validated schema into the shape the normaliser walks.
 *
 * Done once per change rather than per event: the hot path only does a `Set.has` per
 * required field.
 */
export function compileSchema(schema) {
  const properties = schema.properties;
  const fields = Object.keys(properties);
  return {
    schema,
    properties,
    fields,
    required: new Set(schema.required ?? []),
    severities: new Set(properties.EVT_Severity.enum),
    /** Keys that are not part of the core six — what an operator added. */
    extraFields: fields.filter((f) => !CORE_KEYS.includes(f)),
  };
}

/**
 * Derive an identifier from the selected keys: `sha256(join(0x1f))[0:32]`.
 *
 * Returns '' when every selected value is empty. That is deliberate and is the
 * safety rule of the whole feature: a hash of "everything blank" is a constant, so
 * every such event in the fleet would collapse onto ONE record. The caller falls back
 * to a random id, which keeps them separate and visible.
 */
export function identifierHash(raw, keys) {
  if (!Array.isArray(keys) || keys.length === 0) return '';
  const parts = keys.map((k) => component(raw?.[k]));
  if (parts.every((p) => p === '')) return '';
  return createHash('sha256').update(parts.join(SEP)).digest('hex').slice(0, 32);
}

/**
 * The record key for a producer-supplied identifier.
 *
 * Not used by the ingest path — an event WITH an `EVT_Identifier` keeps the condition
 * key (device + summary), because keying on a per-delivery id would turn every repeat
 * into a new record. This exists for the read side: the verification scripts digest a
 * supplied id the same way when they need to ask "which record did this identifier
 * land on", and they must not invent a second formula.
 */
export function keyForIdentifier(id) {
  return createHash('sha256').update(`id${SEP}${component(id)}`).digest('hex').slice(0, 32);
}

/**
 * Read `input-schema` and compile it, once.
 *
 * Used by the verification scripts and by the tests, which need to know what the
 * ingress would accept without running the server.
 */
export function compileStored(record) {
  const parsed = JSON.parse(record[SCHEMA_FIELD]);
  const verdict = validateInputSchema(parsed);
  if (!verdict.ok) return { ok: false, message: verdict.message };
  let storedKeys = [];
  if (record[IDENTIFIER_KEYS_FIELD]) {
    try {
      storedKeys = JSON.parse(record[IDENTIFIER_KEYS_FIELD]);
    } catch {
      storedKeys = [];
    }
  }
  const keys = validateIdentifierKeys(storedKeys, verdict.schema);
  return {
    ok: true,
    schema: compileSchema(verdict.schema),
    identifierKeys: keys.ok ? keys.keys : [],
  };
}

/**
 * The live contract: the file default, overridden by `input-schema` when present.
 *
 * POLLED, not subscribed. A poll is one HGETALL per interval on a key with two
 * fields, and it needs no second connection, no reconnect handling and no
 * missed-message recovery — a subscription that drops silently would leave the
 * ingress running a contract nobody can see it is not honouring. The refresh runs on
 * the same timer as the Valkey health probe, so it also recovers by itself after an
 * outage.
 */
export class SchemaSource {
  constructor(valkey, log) {
    this.valkey = valkey;
    this.log = log;
    // Left null until `loadFallback()` runs, so a missing `schemas/` file surfaces
    // inside the startup try/catch (an obvious crash loop) instead of as a module
    // load error with no hint attached.
    this.fallback = null;
    this.current = null;
  }

  /**
   * Read and compile the file contract. Called once, from `main()`.
   *
   * Throws when the file is missing or malformed — that is the fail-fast the startup
   * path turns into a crash loop, rather than an image that starts fine and rejects
   * every event with ENOENT.
   */
  loadFallback() {
    this.fallback = compileSchema(loadDefaultSchema());
    this.current = { schema: this.fallback, identifierKeys: [], source: 'file', version: 'file' };
    return this.fallback;
  }

  /** What the normaliser should use. Never null once `loadFallback()` has run. */
  get() {
    return this.current;
  }

  /**
   * Read `input-schema` and swap the compiled contract if it changed.
   *
   * Any failure keeps the previous contract, so a transient read error or a
   * half-written hash cannot stop ingestion. Returns true when something changed.
   */
  async refresh() {
    let raw;
    try {
      raw = await this.valkey.write((c) => c.hgetall(INPUT_SCHEMA_KEY));
    } catch (err) {
      if (this.current.source !== 'file') {
        this.log.warn('input-schema read failed, keeping the previous contract', { err: String(err) });
      }
      return false;
    }

    if (!raw || !raw[SCHEMA_FIELD]) {
      // Absent means "never configured". Not an error: the file default is the
      // documented contract.
      return this._swap('file', 'file', this.fallback, []);
    }

    const compiled = compileStored(raw);
    if (!compiled.ok) {
      this.log.error('stored input-schema is invalid, keeping the current contract', {
        reason: compiled.message,
      });
      return false;
    }

    const version = createHash('sha256')
      .update(`${JSON.stringify(compiled.schema.schema)}|${JSON.stringify(compiled.identifierKeys)}`)
      .digest('hex')
      .slice(0, 12);

    return this._swap('valkey', version, compiled.schema, compiled.identifierKeys);
  }

  _swap(source, version, schema, identifierKeys) {
    if (this.current.version === version && this.current.source === source) return false;
    this.log.info('ingest contract loaded', {
      source,
      version,
      fields: schema.fields.length,
      extra: schema.extraFields.join(',') || '-',
      required: [...schema.required].join(','),
      identifierKeys: identifierKeys.join(',') || '-',
    });
    this.current = { schema, identifierKeys, source, version };
    return true;
  }
}
