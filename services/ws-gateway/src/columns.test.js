/**
 * Tests for the data-to-columns configuration.
 *
 * The important one is the schema cross-check: this list is duplicated rather than
 * read at runtime (the gateway image does not ship `schemas/`), so a test is what
 * keeps it honest. Without it a key added to the contract would simply never appear
 * in the configuration form.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import {
  COLUMN_KEYS,
  COLUMN_DEFAULTS,
  MAX_LABEL_LEN,
  defaultColumns,
  mergeColumns,
  validateSetColumns,
  columnsFrame,
} from './columns.js';

const schema = JSON.parse(
  readFileSync(new URL('../../../schemas/event-ingest.schema.json', import.meta.url), 'utf8'),
);

test('every ingest-schema key is configurable, and no others', () => {
  const schemaKeys = Object.keys(schema.properties).filter((k) => k.startsWith('EVT_')).sort();
  assert.deepEqual([...COLUMN_KEYS].sort(), schemaKeys);
});

test('defaults cover every key with a usable label', () => {
  const d = defaultColumns();
  for (const key of COLUMN_KEYS) {
    assert.ok(d[key], `${key} has a default`);
    assert.equal(typeof d[key].label, 'string');
    assert.ok(d[key].label.length > 0, `${key} needs a non-empty label`);
    assert.equal(typeof d[key].asColumn, 'boolean');
  }
  assert.equal(Object.keys(d).length, COLUMN_KEYS.length);
});

test('the core columns are locked and cannot be hidden', () => {
  const d = defaultColumns();
  assert.equal(d.EVT_Severity.locked, true);
  assert.equal(d.EVT_Summary.locked, true);
  // Everything else is a preference.
  assert.equal(d.EVT_Agent.locked, false);
});

test('mergeColumns fills in defaults for an empty store', () => {
  const merged = mergeColumns({});
  assert.deepEqual(Object.keys(merged).sort(), [...COLUMN_KEYS].sort());
  assert.equal(merged.EVT_Agent.label, 'Agent');
});

test('mergeColumns applies stored overrides and ignores unknown keys', () => {
  const merged = mergeColumns({
    EVT_Agent: { label: 'Tool', asColumn: false },
    EVT_NotInSchema: { label: 'x', asColumn: true },
  });
  assert.equal(merged.EVT_Agent.label, 'Tool');
  assert.equal(merged.EVT_Agent.asColumn, false);
  assert.equal(merged.EVT_NotInSchema, undefined);
});

test('mergeColumns cannot unlock a locked column', () => {
  // `locked` is a fact about the table, not a preference.
  const merged = mergeColumns({ EVT_Severity: { label: 'Sev', asColumn: false, locked: false } });
  assert.equal(merged.EVT_Severity.locked, true);
});

test('validateSetColumns accepts a well-formed frame', () => {
  const v = validateSetColumns({
    op: 'setColumns',
    by: 'alice',
    columns: { EVT_Agent: { label: 'Tool', asColumn: true } },
  });
  assert.equal(v.ok, true);
  assert.equal(v.by, 'alice');
  assert.deepEqual(v.columns.EVT_Agent, { label: 'Tool', asColumn: true });
});

test('validateSetColumns rejects an unknown key instead of dropping it', () => {
  // A half-applied configuration would leave the form showing one thing and the
  // tables another, which is worse than refusing.
  const v = validateSetColumns({
    op: 'setColumns',
    columns: { EVT_Agent: { label: 'Tool', asColumn: true }, NOPE: { label: 'x', asColumn: true } },
  });
  assert.equal(v.ok, false);
  assert.equal(v.code, 'unknown_key');
});

test('validateSetColumns rejects a blank label', () => {
  const v = validateSetColumns({
    op: 'setColumns',
    columns: { EVT_Agent: { label: '   ', asColumn: true } },
  });
  assert.equal(v.code, 'bad_label');
});

test('validateSetColumns rejects a non-boolean asColumn', () => {
  const v = validateSetColumns({
    op: 'setColumns',
    columns: { EVT_Agent: { label: 'Tool', asColumn: 'yes' } },
  });
  assert.equal(v.code, 'bad_value');
});

test('validateSetColumns rejects an empty selection and a bad op', () => {
  assert.equal(validateSetColumns({ op: 'setColumns', columns: {} }).code, 'bad_columns');
  assert.equal(validateSetColumns({ op: 'setColumns' }).code, 'bad_columns');
  assert.equal(validateSetColumns({ op: 'nope', columns: {} }).code, 'bad_op');
  assert.equal(validateSetColumns(null).code, 'bad_op');
});

test('a label is trimmed, collapsed and bounded', () => {
  const long = 'x'.repeat(MAX_LABEL_LEN + 50);
  const v = validateSetColumns({
    op: 'setColumns',
    columns: {
      EVT_Agent: { label: '  Tool   Name  ', asColumn: true },
      EVT_URL: { label: long, asColumn: false },
    },
  });
  assert.equal(v.ok, true);
  assert.equal(v.columns.EVT_Agent.label, 'Tool Name', 'whitespace collapsed');
  assert.equal(v.columns.EVT_URL.label.length, MAX_LABEL_LEN, 'length bounded');
});

test('an unnamed author falls back rather than storing a blank', () => {
  const v = validateSetColumns({
    op: 'setColumns',
    by: '   ',
    columns: { EVT_Agent: { label: 'Tool', asColumn: true } },
  });
  assert.equal(v.by, 'anonymous');
});

test('the frame carries the whole configuration', () => {
  const frame = JSON.parse(columnsFrame({ EVT_Agent: { label: 'Tool', asColumn: true } }));
  assert.equal(frame.op, 'columns');
  assert.equal(frame.columns.EVT_Agent.label, 'Tool');
});

test('COLUMN_DEFAULTS and COLUMN_KEYS agree', () => {
  assert.deepEqual(COLUMN_KEYS, COLUMN_DEFAULTS.map((c) => c.key));
});
