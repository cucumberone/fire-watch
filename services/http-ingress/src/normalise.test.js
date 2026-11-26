/**
 * Tests for HTTP → event normalisation. No broker.
 *   node --test src/
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import {
  extractEvents,
  normaliseEvent,
  normaliseBatch,
  streamFields,
  BadRequest,
  ingestSchema,
  requiredFields,
  schemaFields,
} from './normalise.js';

const opts = { maxBatch: 10, now: 1762699200000, randomId: () => 'gen-id' };

// ------------------------------------------------------------------- shapes
test('a single event object becomes a one-element batch', () => {
  assert.equal(extractEvents({ id: 'a' }).length, 1);
});

test('a bare array is taken as the batch', () => {
  assert.equal(extractEvents([{ id: 'a' }, { id: 'b' }]).length, 2);
});

test('a wrapped events array is unwrapped', () => {
  assert.equal(extractEvents({ events: [{ id: 'a' }] }).length, 1);
});

test('a non-object body is rejected', () => {
  assert.throws(() => extractEvents('string'), BadRequest);
  assert.throws(() => extractEvents(null), BadRequest);
  assert.throws(() => extractEvents(42), BadRequest);
});

// --------------------------------------------------------------- normalising
test('a complete event passes through unchanged', () => {
  const raw = {
    id: 'abc',
    source: 'sensor-a',
    type: 'temp',
    severity: 'CRITICAL',
    ts: '2026-11-09T00:00:00.000Z',
    payload: { msg: 'hot', msgc: 'detail' },
  };
  assert.deepEqual(normaliseEvent(raw, opts), raw);
});

test('a missing id is generated rather than rejected', () => {
  const e = normaliseEvent({ source: 'a' }, opts);
  assert.equal(e.id, 'gen-id');
});

test('missing source and type get defaults', () => {
  const e = normaliseEvent({ id: 'x' }, opts);
  assert.equal(e.source, 'http-ingress');
  assert.equal(e.type, 'http.event');
});

test('a missing severity defaults to WARNING, not CRITICAL', () => {
  // Defaulting to a high severity would make every unknown shipper look like an
  // outage; WARNING is visible without being an alarm.
  assert.equal(normaliseEvent({ id: 'x' }, opts).severity, 'WARNING');
});

test('a missing ts is filled with now, not left empty', () => {
  const e = normaliseEvent({ id: 'x' }, opts);
  assert.equal(e.ts, new Date(opts.now).toISOString());
});

test('bare msg/msgc fields are folded into payload', () => {
  const e = normaliseEvent({ id: 'x', msg: 'short', msgc: 'long detail' }, opts);
  assert.deepEqual(e.payload, { msg: 'short', msgc: 'long detail' });
});

test('an explicit payload wins over bare msg fields', () => {
  const e = normaliseEvent({ id: 'x', msg: 'ignored', payload: { a: 1 } }, opts);
  assert.deepEqual(e.payload, { a: 1 });
});

test('no payload yields an empty object, never undefined', () => {
  assert.deepEqual(normaliseEvent({ id: 'x' }, opts).payload, {});
});

test('an invalid severity is rejected with the allowed list', () => {
  assert.throws(() => normaliseEvent({ id: 'x', severity: 'ERROR' }, opts), /severity must be one of/);
});

test('lowercase severity is rejected: the contract is exact', () => {
  assert.throws(() => normaliseEvent({ id: 'x', severity: 'critical' }, opts), BadRequest);
});

test('an array payload is rejected', () => {
  assert.throws(() => normaliseEvent({ id: 'x', payload: [1, 2] }, opts), BadRequest);
});

test('an oversized id is rejected', () => {
  assert.throws(() => normaliseEvent({ id: 'z'.repeat(200) }, opts), /id must be/);
});

test('an oversized source is rejected', () => {
  assert.throws(() => normaliseEvent({ source: 's'.repeat(300) }, opts), /source must be/);
});

test('a nested event object is rejected', () => {
  assert.throws(() => normaliseEvent([{ id: 'x' }], opts), BadRequest);
});

// -------------------------------------------------------------------- batches
test('an empty batch is rejected', () => {
  assert.throws(() => normaliseBatch([], opts), /no events/);
});

test('an oversized batch is rejected as 413', () => {
  try {
    normaliseBatch(Array.from({ length: 11 }, (_, i) => ({ id: `e${i}` })), opts);
    assert.fail('should have thrown');
  } catch (err) {
    assert.equal(err.status, 413);
  }
});

test('the failing index is named in the error, so a producer can fix it', () => {
  assert.throws(
    () => normaliseBatch([{ id: 'ok' }, { id: 'bad', severity: 'NOPE' }], opts),
    /event\[1\]/,
  );
});

test('a valid batch is all-or-nothing', () => {
  const out = normaliseBatch([{ id: 'a' }, { id: 'b' }], opts);
  assert.equal(out.length, 2);
  assert.deepEqual(out.map((e) => e.id), ['a', 'b']);
});

// --------------------------------------------------------------- stream entry
test('stream fields carry the envelope as JSON under `evt`', () => {
  const evt = { id: 'x', type: 't', source: 's', severity: 'CRITICAL', payload: { msg: 'm' } };
  const f = streamFields(evt, { correlationId: 'corr-1' });
  assert.deepEqual(f, ['evt', JSON.stringify(evt), 'src', 'http', 'cid', 'corr-1']);
  assert.deepEqual(JSON.parse(f[1]), evt);
});

test('stream fields omit `cid` when there is no correlation id', () => {
  const f = streamFields({ id: 'x' }, { src: 'operator' });
  assert.deepEqual(f.filter((_, i) => i % 2 === 0), ['evt', 'src']);
  assert.equal(f[3], 'operator');
});

// ------------------------------------------------- upstream schema (EVT_*)
// The producer contract. Field names are mapped once, here, so the rest of the
// system speaks one language and a schema change touches this file only.
const upstream = (over = {}) => ({
  EVT_Device: 'core-sw-07',
  EVT_EventKey: 'net.link.down',
  EVT_Identifier: 'own-id-1',
  EVT_Severity: 'CRITICAL',
  EVT_Summary: 'link is down',
  EVT_Details: 'Gi0/24 lost carrier',
  EVT_Agent: 'Prometheus',
  EVT_AgentOwner: 'AM-000042',
  EVT_Environment: 'production',
  ...over,
});

test('the upstream schema maps onto the canonical envelope', () => {
  const e = normaliseEvent(upstream(), opts);
  assert.equal(e.id, 'own-id-1');
  // The alert key is device + event key: what the producer guarantees identifies
  // an alarm.
  assert.equal(e.source, 'core-sw-07');
  assert.equal(e.type, 'net.link.down');
  assert.equal(e.severity, 'CRITICAL');
  assert.equal(e.payload.msg, 'link is down');
  assert.equal(e.payload.msgc, 'Gi0/24 lost carrier');
  assert.equal(e.payload.EVT_Agent, 'Prometheus');
  assert.equal(e.payload.EVT_AgentOwner, 'AM-000042');
  assert.equal(e.payload.EVT_Environment, 'production');
});

test('severity is mapped from the producer scale onto the internal one', () => {
  const map = {
    CRITICAL: 'CRITICAL',
    MAJOR: 'CRITICAL',
    MINOR: 'WARNING',
    WARNING: 'WARNING',
    INDETERMINATE: 'WARNING',
    INFORMATION: 'HARMLESS',
    // The resolution signals. These RETIRE, which is the important mapping: the
    // schema has no CLOSE, so CLEAR is what ends an episode.
    CLEAR: 'CLOSE',
    HARMLESS: 'CLOSE',
  };
  for (const [from, to] of Object.entries(map)) {
    const e = normaliseEvent(upstream({ EVT_Severity: from }), opts);
    assert.equal(e.severity, to, `${from} -> ${to}`);
  }
});

test('the producer severity is preserved for display', () => {
  const e = normaliseEvent(upstream({ EVT_Severity: 'MINOR' }), opts);
  assert.equal(e.severity, 'WARNING', 'internal severity');
  assert.equal(e.payload.EVT_Severity, 'MINOR', 'original kept for the inspector');
});

test('every required field is enforced', () => {
  for (const field of ['EVT_Device', 'EVT_EventKey', 'EVT_Severity', 'EVT_Summary', 'EVT_AgentOwner', 'EVT_Agent']) {
    const raw = upstream();
    delete raw[field];
    assert.throws(() => normaliseEvent(raw, opts), BadRequest, `${field} must be required`);
  }
});

test('a blank required field is rejected, not treated as present', () => {
  assert.throws(() => normaliseEvent(upstream({ EVT_Device: '   ' }), opts), BadRequest);
});

test('an unknown severity is rejected with the allowed list', () => {
  assert.throws(
    () => normaliseEvent(upstream({ EVT_Severity: 'SPICY' }), opts),
    (err) => err instanceof BadRequest && /EVT_Severity must be one of/.test(err.message),
  );
});

test('a blank identifier is generated rather than rejected', () => {
  const e = normaliseEvent(upstream({ EVT_Identifier: '' }), opts);
  assert.equal(e.id, 'gen-id');
});

test('optional fields are omitted from the payload when blank', () => {
  const e = normaliseEvent(upstream({ EVT_Environment: '', EVT_URL: undefined }), opts);
  assert.equal('EVT_Environment' in e.payload, false);
  assert.equal('EVT_URL' in e.payload, false);
});

test('a batch of upstream events is normalised together', () => {
  const batch = normaliseBatch([upstream(), upstream({ EVT_EventKey: 'other' })], opts);
  assert.equal(batch.length, 2);
  assert.equal(batch[1].type, 'other');
});

// -------------------------------------------- schema file is the source of truth
// These tests exist so the PUBLISHED contract and the implementation cannot drift.
// The schema is read from disk, so editing schemas/event-ingest.schema.json
// without updating the mapping breaks the build rather than production.
test('the schema file is loadable and well-formed', () => {
  const s = ingestSchema();
  assert.equal(s.type, 'object');
  assert.ok(Array.isArray(s.required), 'required is an array');
  assert.ok(s.properties && typeof s.properties === 'object');
});

test('every property in the schema is handled by the mapping', () => {
  // A field added to the schema but not consumed here would be silently dropped,
  // which is the exact failure this test is meant to prevent.
  const handled = new Set([
    'EVT_Device',
    'EVT_EventKey',
    'EVT_Identifier',
    'EVT_Severity',
    'EVT_Summary',
    'EVT_Details',
    'EVT_Agent',
    'EVT_AgentOwner',
    'EVT_Environment',
    'EVT_URL',
  ]);
  const declared = schemaFields();
  const missing = declared.filter((f) => !handled.has(f));
  assert.deepEqual(missing, [], `schema fields with no mapping: ${missing.join(', ')}`);
  const extra = [...handled].filter((f) => !declared.includes(f));
  assert.deepEqual(extra, [], `mapping handles fields the schema does not declare: ${extra.join(', ')}`);
});

test('every required field in the schema is rejected when missing', () => {
  for (const field of requiredFields()) {
    const raw = upstream();
    delete raw[field];
    assert.throws(() => normaliseEvent(raw, opts), BadRequest, `${field} must be required`);
  }
});

test('every schema-declared severity is accepted and mapped', () => {
  const declared = ingestSchema().properties.EVT_Severity.enum;
  assert.ok(declared.length > 0, 'the schema declares severities');
  for (const sev of declared) {
    const e = normaliseEvent(upstream({ EVT_Severity: sev }), opts);
    assert.ok(e.severity, `${sev} maps to an internal severity`);
  }
});

test('EVT_Details accepts a realistic detail body, not just a label', () => {
  const long = 'x'.repeat(4000);
  const e = normaliseEvent(upstream({ EVT_Details: long }), opts);
  assert.equal(e.payload.msgc.length, 4000);
});

test('EVT_Details above its bound is refused rather than truncated', () => {
  assert.throws(
    () => normaliseEvent(upstream({ EVT_Details: 'x'.repeat(9000) }), opts),
    BadRequest,
  );
});

// ------------------------------------------------ configurable contract + key
// The schema is now a runtime object, and an operator can add keys to it from the
// GUI. These tests pin the two behaviours that change with it: extra keys are
// carried through, and a chosen identifier policy mints the RECORD key before the
// event reaches the stream.
import { compileSchema, identifierHash, keyForIdentifier } from './schema.js';

/** The compiled form of the file contract, which is what a fresh install runs. */
const compiled = compileSchema(ingestSchema());

test('an extra key added by an operator is carried into the payload', () => {
  const withExtra = compileSchema({
    ...ingestSchema(),
    properties: { ...ingestSchema().properties, EVT_Ticket: { type: 'string' } },
  });
  const e = normaliseEvent(upstream({ EVT_Ticket: 'INC-1234' }), { ...opts, compiled: withExtra });
  assert.equal(e.payload.EVT_Ticket, 'INC-1234');
});

test('a key NOT in the contract is never put on the payload', () => {
  // Iterating the contract rather than the request is what stops a producer minting
  // arbitrary hash fields on the record.
  const e = normaliseEvent(upstream({ EVT_Unknown: 'nope' }), { ...opts, compiled });
  assert.equal('EVT_Unknown' in e.payload, false);
});

test('an operator-added key that is missing is simply absent', () => {
  const withExtra = compileSchema({
    ...ingestSchema(),
    properties: { ...ingestSchema().properties, EVT_Ticket: { type: 'string' } },
  });
  const e = normaliseEvent(upstream(), { ...opts, compiled: withExtra });
  assert.equal('EVT_Ticket' in e.payload, false);
});

test('a required key added by an operator is enforced', () => {
  const withExtra = compileSchema({
    ...ingestSchema(),
    properties: { ...ingestSchema().properties, EVT_Ticket: { type: 'string' } },
    required: [...ingestSchema().required, 'EVT_Ticket'],
  });
  assert.throws(
    () => normaliseEvent(upstream(), { ...opts, compiled: withExtra }),
    /EVT_Ticket is required/,
  );
});

test('no identifier policy means no record key on the envelope', () => {
  // This is the wire format an install that never opens the page produces, and the
  // worker's device+summary fallback depends on it staying absent.
  const e = normaliseEvent(upstream(), { ...opts, compiled });
  assert.equal('key' in e, false);
});

test('a chosen policy mints a 32-hex record key from the selected values', () => {
  const e = normaliseEvent(
    upstream({ EVT_Identifier: '' }),
    { ...opts, compiled, identifierKeys: ['EVT_Device', 'EVT_EventKey'] },
  );
  assert.match(e.key, /^[0-9a-f]{32}$/);
  assert.equal(e.key, identifierHash(upstream(), ['EVT_Device', 'EVT_EventKey']));
  assert.equal(e.id, `ih-${e.key}`);
});

test('the same selected values always mint the same key', () => {
  // Otherwise every delivery is a new record, which is the storage explosion the
  // one-record-per-condition model exists to prevent.
  const a = normaliseEvent(upstream({ EVT_Identifier: '' }), { ...opts, compiled, identifierKeys: ['EVT_Device'] });
  const b = normaliseEvent(upstream({ EVT_Identifier: '' }), { ...opts, compiled, identifierKeys: ['EVT_Device'] });
  assert.equal(a.key, b.key);
});

test('padding in a selected value does not mint a different key', () => {
  const a = normaliseEvent(upstream({ EVT_Identifier: '', EVT_Device: 'core-sw-07' }), { ...opts, compiled, identifierKeys: ['EVT_Device'] });
  const b = normaliseEvent(upstream({ EVT_Identifier: '', EVT_Device: '  core-sw-07  ' }), { ...opts, compiled, identifierKeys: ['EVT_Device'] });
  assert.equal(a.key, b.key);
});

test('a supplied identifier wins and no hash key is produced', () => {
  // Keying on a per-delivery id would turn every repeat into a new record.
  const e = normaliseEvent(upstream({ EVT_Identifier: 'own-id-1' }), {
    ...opts, compiled, identifierKeys: ['EVT_Device'],
  });
  assert.equal('key' in e, false);
  assert.equal(e.id, 'own-id-1');
});

test('an all-blank selection falls back to a random id, not a constant hash', () => {
  // A hash of "everything empty" is a constant, so every such event in the fleet
  // would collapse onto ONE record. Visible and separate beats merged and invisible.
  const e = normaliseEvent(
    upstream({ EVT_Identifier: '', EVT_Environment: '' }),
    { ...opts, compiled, identifierKeys: ['EVT_Environment'] },
  );
  assert.equal('key' in e, false);
  assert.equal(e.id, 'gen-id');
});

test('the compiled key set distinguishes core from operator-added keys', () => {
  // The form pins the first list and allows removing the rest, so the split has to be
  // the same on both sides.
  assert.deepEqual(compiled.extraFields.sort(), [
    'EVT_Details', 'EVT_Environment', 'EVT_Identifier', 'EVT_URL',
  ]);
});

test('keyForIdentifier digests a free-text id into the record-key shape', () => {
  const k = keyForIdentifier('own-recipe-of-unique-identifier');
  assert.match(k, /^[0-9a-f]{32}$/);
  assert.equal(k, keyForIdentifier('  own-recipe-of-unique-identifier  '));
});
