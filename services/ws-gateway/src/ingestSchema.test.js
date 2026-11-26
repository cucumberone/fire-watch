/**
 * Tests for the ingest-contract configuration.
 *
 * The important one is PARITY with the ingress. The gateway validates a submission
 * and stores it; the ingress validates the same document again on every poll and
 * keeps its previous contract when it disagrees. If the two copies drift, the split
 * is invisible until an operator saves a document that is then silently ignored —
 * reported as saved, never applied.
 *
 * So this file imports the ingress copy directly and compares verdicts over a corpus
 * of documents. That is the only way the duplication can stay honest without
 * restructuring both Dockerfiles into one build context.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import {
  CORE_KEYS,
  MAX_IDENTIFIER_KEYS,
  MAX_KEY_LEN,
  MAX_SCHEMA_BYTES,
  validateInputSchema,
  validateIdentifierKeys,
  validateSetIngestSchema,
  ingestSchemaFrame,
} from './ingestSchema.js';
import * as ingress from '../../http-ingress/src/schema.js';

const fileSchema = JSON.parse(
  readFileSync(new URL('../../../schemas/event-ingest.schema.json', import.meta.url), 'utf8'),
);

/** A minimal valid document, with the core six present and required. */
function validSchema(over = {}) {
  const properties = {
    EVT_Device: { type: 'string' },
    EVT_EventKey: { type: 'string' },
    EVT_Severity: { type: 'string', enum: ['CRITICAL', 'CLEAR'] },
    EVT_Summary: { type: 'string' },
    EVT_Agent: { type: 'string' },
    EVT_AgentOwner: { type: 'string' },
    ...(over.properties || {}),
  };
  return {
    type: 'object',
    properties,
    required: over.required || [...CORE_KEYS],
    ...(over.extra || {}),
  };
}

// ------------------------------------------------------------------- parity
test('the two validators agree on a corpus of documents', () => {
  // Each case names the failure mode it is pinning, so a divergence points at the
  // reason rather than at "case 7 differs".
  const corpus = {
    'the file schema': fileSchema,
    'a minimal valid document': validSchema(),
    'an extra key': validSchema({ properties: { EVT_Extra: { type: 'string' } } }),
    'not an object': 'nope',
    'null': null,
    'an array': [],
    'a wrong type': { ...validSchema(), type: 'array' },
    'no properties': { type: 'object', properties: {}, required: [] },
    'properties as an array': { type: 'object', properties: [] },
    'a property that is not an object': validSchema({ properties: { EVT_X: 'string' } }),
    'a non-string property': validSchema({ properties: { EVT_X: { type: 'integer' } } }),
    'a key with no prefix': validSchema({ properties: { Nope: { type: 'string' } } }),
    'a key with a dash': validSchema({ properties: { 'EVT_With-Dash': { type: 'string' } } }),
    'a key that is too long': validSchema({ properties: { ['EVT_' + 'X'.repeat(MAX_KEY_LEN)]: { type: 'string' } } }),
    'a required key that is not a property': validSchema({ required: [...CORE_KEYS, 'EVT_Ghost'] }),
    'a missing core key': (() => {
      const s = validSchema();
      delete s.properties.EVT_Agent;
      s.required = s.required.filter((k) => k !== 'EVT_Agent');
      return s;
    })(),
    'a core key that stopped being required': validSchema({
      required: CORE_KEYS.filter((k) => k !== 'EVT_Device'),
    }),
    'a severity enum that is empty': validSchema({
      properties: { EVT_Severity: { type: 'string', enum: [] } },
    }),
    'a severity enum with a blank': validSchema({
      properties: { EVT_Severity: { type: 'string', enum: ['CRITICAL', '  '] } },
    }),
    'a severity enum with duplicates': validSchema({
      properties: { EVT_Severity: { type: 'string', enum: ['CRITICAL', 'CRITICAL'] } },
    }),
    'a severity enum missing entirely': validSchema({
      properties: { EVT_Severity: { type: 'string' } },
    }),
    'additionalProperties false': validSchema({ extra: { additionalProperties: false } }),
  };

  for (const [name, doc] of Object.entries(corpus)) {
    const mine = validateInputSchema(doc);
    const theirs = ingress.validateInputSchema(doc);
    assert.equal(
      mine.ok,
      theirs.ok,
      `"${name}": gateway says ok=${mine.ok} (${mine.message || ''}), `
      + `ingress says ok=${theirs.ok} (${theirs.message || ''})`,
    );
    if (mine.ok) {
      // The MESSAGES may word things differently; the normalised documents must not.
      assert.deepEqual(mine.schema, theirs.schema, `"${name}": normalised schemas differ`);
    } else {
      assert.equal(mine.message, theirs.message, `"${name}": rejection messages differ`);
    }
  }
});

test('the two identifier validators agree', () => {
  const schema = validSchema({ properties: { EVT_Extra: { type: 'string' } } });
  const cases = [
    undefined, null, [], ['EVT_Device'], ['EVT_Extra', 'EVT_Agent'],
    'EVT_Device',                          // not an array
    ['EVT_NotInSchema'],                   // not a property
    ['NotPrefixed'],                       // not a usable name
    ['EVT_Device', 'EVT_Device'],          // duplicates collapse, both agree
    Array.from({ length: MAX_IDENTIFIER_KEYS + 1 }, (_, i) => `EVT_K${i}`),  // too many
  ];
  // The corpus above needs EVT_K0.. to exist for the "too many" case to test the
  // count rule rather than the membership rule.
  const wide = validSchema({
    properties: Object.fromEntries(
      Array.from({ length: MAX_IDENTIFIER_KEYS + 1 }, (_, i) => [`EVT_K${i}`, { type: 'string' }]),
    ),
  });
  for (const value of cases) {
    const mine = validateIdentifierKeys(value, schema);
    const theirs = ingress.validateIdentifierKeys(value, schema);
    assert.equal(mine.ok, theirs.ok, `ok differs for ${JSON.stringify(value)}`);
    if (mine.ok) assert.deepEqual(mine.keys, theirs.keys);
  }
  const many = validateIdentifierKeys(cases[cases.length - 1], wide);
  assert.equal(many.ok, false, 'more than the maximum is refused');
});

// --------------------------------------------------------- document validation
test('the file schema is accepted unchanged', () => {
  const verdict = validateInputSchema(fileSchema);
  assert.equal(verdict.ok, true, verdict.message);
  assert.deepEqual(Object.keys(verdict.schema.properties).sort(),
    Object.keys(fileSchema.properties).sort());
});

test('a document is rejected as a WHOLE, never repaired', () => {
  // A half-applied contract would accept some payloads and reject others for reasons
  // no producer can see from the response.
  const bad = validSchema({ properties: { EVT_Ok: { type: 'string' }, EVT_Bad: { type: 'number' } } });
  const verdict = validateInputSchema(bad);
  assert.equal(verdict.ok, false);
  assert.match(verdict.message, /EVT_Bad\.type must be "string"/);
});

test('the core six cannot be removed or made optional', () => {
  for (const core of CORE_KEYS) {
    const removed = validSchema();
    delete removed.properties[core];
    removed.required = removed.required.filter((k) => k !== core);
    const a = validateInputSchema(removed);
    assert.equal(a.ok, false, `${core} must not be removable`);
    assert.match(a.message, /cannot be removed/);

    const optional = validSchema({ required: CORE_KEYS.filter((k) => k !== core) });
    const b = validateInputSchema(optional);
    assert.equal(b.ok, false, `${core} must not become optional`);
    assert.match(b.message, /must stay required/);
  }
});

test('a blank or duplicate severity enum is refused', () => {
  // The ingress's severity mapping is driven by this enum. Empty means every event is
  // unmappable; duplicates mean an operator cannot tell which entry is authoritative.
  assert.equal(validateInputSchema(validSchema({
    properties: { EVT_Severity: { type: 'string', enum: [] } },
  })).ok, false);
  assert.equal(validateInputSchema(validSchema({
    properties: { EVT_Severity: { type: 'string', enum: ['A', 'A'] } },
  })).ok, false);
});

test('an oversized document is refused', () => {
  // A schema is a few KiB. Anything near this bound is a mistake — someone pasting a
  // whole OpenAPI document — and storing it would make every poll expensive.
  const fat = validSchema({ properties: { EVT_Fat: { type: 'string', description: 'x'.repeat(MAX_SCHEMA_BYTES) } } });
  const verdict = validateInputSchema(fat);
  assert.equal(verdict.ok, false);
  assert.match(verdict.message, /larger than/);
});

test('additionalProperties is normalised to true unless explicitly false', () => {
  // The ingress carries unknown keys through, so an omitted flag must not silently
  // start rejecting producers that send one.
  assert.equal(validateInputSchema(validSchema()).schema.additionalProperties, true);
  assert.equal(validateInputSchema(validSchema({
    extra: { additionalProperties: false },
  })).schema.additionalProperties, false);
});

// ------------------------------------------------------ whole-frame validation
test('setIngestSchema validates both halves together', () => {
  const good = validateSetIngestSchema({
    op: 'setIngestSchema',
    schema: validSchema({ properties: { EVT_Extra: { type: 'string' } } }),
    identifierKeys: ['EVT_Extra'],
    by: 'gunnar',
  });
  assert.equal(good.ok, true, good.message);
  assert.deepEqual(good.identifierKeys, ['EVT_Extra']);
  assert.equal(good.by, 'gunnar');
});

test('a selection naming a key the schema does not have is refused', () => {
  // This is the dangerous case: the ingress would hash a field that never arrives,
  // producing the SAME hash for every event and collapsing the fleet onto one record.
  const verdict = validateSetIngestSchema({
    op: 'setIngestSchema',
    schema: validSchema(),
    identifierKeys: ['EVT_Gone'],
  });
  assert.equal(verdict.ok, false);
  assert.equal(verdict.code, 'bad_identifier_keys');
});

test('a bad op or a bad schema is refused before anything is stored', () => {
  assert.equal(validateSetIngestSchema({ op: 'nope' }).code, 'bad_op');
  assert.equal(validateSetIngestSchema({
    op: 'setIngestSchema', schema: { type: 'object' },
  }).code, 'bad_schema');
});

test('an anonymous save is allowed but not attributed to nobody', () => {
  const v = validateSetIngestSchema({ op: 'setIngestSchema', schema: validSchema(), by: '   ' });
  assert.equal(v.ok, true);
  assert.equal(v.by, 'anonymous');
});

// ------------------------------------------------------------------- the frame
test('the read frame carries the contract, its source and the picker choices', () => {
  const frame = JSON.parse(ingestSchemaFrame({
    schema: validSchema(), identifierKeys: ['EVT_Device'], source: 'valkey', version: 'stored',
  }));
  assert.equal(frame.op, 'ingestSchema');
  assert.equal(frame.source, 'valkey');
  assert.deepEqual(frame.identifierKeys, ['EVT_Device']);
  assert.deepEqual(frame.identifierKeyChoices, CORE_KEYS);
});

test('an unconfigured store answers with a null schema and source file', () => {
  // Not an error: the client carries the default document and the ingress is running
  // the copy shipped in its own image.
  const frame = JSON.parse(ingestSchemaFrame({
    schema: null, identifierKeys: [], source: 'file', version: null,
  }));
  assert.equal(frame.schema, null);
  assert.equal(frame.source, 'file');
  assert.deepEqual(frame.identifierKeys, []);
});
