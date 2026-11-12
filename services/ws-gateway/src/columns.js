/**
 * Data-to-columns configuration.
 *
 * Maps each `EVT_*` key the ingress accepts onto the name it is shown under in the
 * GUI, and records whether it deserves a COLUMN of its own or should only appear in
 * the Information panel.
 *
 * GLOBAL, not per-user. It lives in Valkey (`config:columns`) so every browser and
 * every pod agrees, and a change is broadcast so nobody has to reload. A
 * per-browser preference would mean two operators looking at the same incident and
 * seeing different columns — and no way to tell whose view was the intended one.
 *
 * THE ALLOWLIST IS THE SCHEMA'S. `COLUMN_KEYS` mirrors
 * `schemas/event-ingest.schema.json`, and a test reads that file and asserts the two
 * sets are equal — so a key added to the contract cannot quietly go missing here,
 * and a key invented here cannot be stored. The list is duplicated rather than read
 * at runtime because the gateway image does not ship `schemas/`; a test is the
 * cheaper way to keep them honest than copying a directory in for one feature.
 */

/** Hash holding the configuration. Field = EVT key, value = JSON. */
export const COLUMNS_KEY = 'config:columns';

/**
 * One entry per ingest-schema key, in the order the form should present them.
 *
 * `label` is the DEFAULT column name. It matches what the UI already showed before
 * this became configurable, so an untouched install looks exactly as it did.
 *
 * `asColumn` is the DEFAULT visibility. The six the tables already rendered are on;
 * `EVT_Summary`, `EVT_Severity` and `EVT_Device` are core columns with their own
 * rendering (badge, summary cell) and are NOT offered as generic columns — they are
 * listed so the form can show them as fixed, and `locked: true` says so.
 */
export const COLUMN_DEFAULTS = [
  // --- core columns, rendered by the table itself, not by this configuration.
  { key: 'EVT_Severity', label: 'Severity', asColumn: true, locked: true },
  { key: 'EVT_Summary', label: 'Summary', asColumn: true, locked: true },
  // --- the configurable ones.
  { key: 'EVT_Device', label: 'Device', asColumn: true },
  { key: 'EVT_EventKey', label: 'Event key', asColumn: true },
  { key: 'EVT_Agent', label: 'Agent', asColumn: true },
  { key: 'EVT_AgentOwner', label: 'Agent owner', asColumn: true },
  { key: 'EVT_Environment', label: 'Environment', asColumn: true },
  { key: 'EVT_Identifier', label: 'Identifier', asColumn: false },
  { key: 'EVT_Details', label: 'Details', asColumn: false },
  { key: 'EVT_URL', label: 'URL', asColumn: false },
];

/** The keys this system is willing to store, in presentation order. */
export const COLUMN_KEYS = COLUMN_DEFAULTS.map((c) => c.key);

const KEY_SET = new Set(COLUMN_KEYS);

/** Longest accepted column name. A header is one line; beyond this it truncates. */
export const MAX_LABEL_LEN = 40;

/** Defaults as a plain map, which is what the wire carries. */
export function defaultColumns() {
  const out = {};
  for (const c of COLUMN_DEFAULTS) {
    out[c.key] = { label: c.label, asColumn: c.asColumn, locked: Boolean(c.locked) };
  }
  return out;
}

/**
 * Merge stored overrides onto the defaults.
 *
 * A key with no stored value keeps its default, which is what makes the feature
 * safe to add to a running system: an empty hash means "all defaults", not "no
 * columns". It also means a key ADDED to the schema later appears automatically
 * rather than being invisible until someone edits the config.
 *
 * `locked` always comes from the DEFAULTS, never from storage: it describes what the
 * table can render, not a preference, so a stored `locked: false` must not be able
 * to unlock a core column.
 */
export function mergeColumns(stored) {
  const out = defaultColumns();
  if (!stored || typeof stored !== 'object') return out;
  for (const [key, value] of Object.entries(stored)) {
    if (!KEY_SET.has(key) || !value || typeof value !== 'object') continue;
    const label = cleanLabel(value.label);
    if (label) out[key].label = label;
    if (typeof value.asColumn === 'boolean') out[key].asColumn = value.asColumn;
  }
  return out;
}

/** Trim and bound a column name. Returns '' when unusable. */
function cleanLabel(value) {
  if (typeof value !== 'string') return '';
  const s = value.replace(/\s+/g, ' ').trim();
  return s.length > MAX_LABEL_LEN ? s.slice(0, MAX_LABEL_LEN) : s;
}

/**
 * Validate an inbound `{"op":"setColumns","columns":{…}}`.
 *
 * Rejects the WHOLE frame on any unknown key rather than dropping it silently. A
 * configuration that half-applied would leave the form showing one thing and the
 * tables another, which is worse than refusing and naming the key.
 */
export function validateSetColumns(frame) {
  if (!frame || frame.op !== 'setColumns') {
    return { ok: false, code: 'bad_op', message: 'expected {"op":"setColumns"}' };
  }
  if (!frame.columns || typeof frame.columns !== 'object' || Array.isArray(frame.columns)) {
    return { ok: false, code: 'bad_columns', message: 'columns must be an object' };
  }

  const out = {};
  for (const [key, value] of Object.entries(frame.columns)) {
    if (!KEY_SET.has(key)) {
      return {
        ok: false,
        code: 'unknown_key',
        message: `${JSON.stringify(key)} is not an ingest-schema key; allowed: ${COLUMN_KEYS.join(', ')}`,
      };
    }
    if (!value || typeof value !== 'object') {
      return { ok: false, code: 'bad_value', message: `${key} must be an object` };
    }
    const label = cleanLabel(value.label);
    if (!label) {
      // An empty header is unusable, and defaulting it silently would look like
      // the rename did not save.
      return { ok: false, code: 'bad_label', message: `${key} needs a non-empty name` };
    }
    if (typeof value.asColumn !== 'boolean') {
      return { ok: false, code: 'bad_value', message: `${key}.asColumn must be true or false` };
    }
    out[key] = { label, asColumn: value.asColumn };
  }

  if (Object.keys(out).length === 0) {
    return { ok: false, code: 'bad_columns', message: 'columns must not be empty' };
  }

  // Who changed it. Not authentication — it is a preference, not a permission — but
  // a configuration change that cannot be attributed is not auditable, and this one
  // rewrites what every other operator sees.
  const by = typeof frame.by === 'string' && frame.by.trim() !== ''
    ? frame.by.replace(/\s+/g, ' ').trim().slice(0, 128)
    : 'anonymous';

  return { ok: true, columns: out, by };
}

/** Frame carrying the current configuration. */
export function columnsFrame(columns) {
  return JSON.stringify({ op: 'columns', columns });
}
