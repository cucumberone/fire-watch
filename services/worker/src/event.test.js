/**
 * Tests for the worker's pure logic: validation, compression, timestamp
 * extraction. No broker, no Valkey.
 *
 *   node --test src/
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { gunzipSync } from 'node:zlib';
import {
  validateEvent,
  ValidationError,
  compressPayload,
  decompressPayload,
  entryTsMs,
  isGzip,
  isRetirement,
} from './event.js';
import { eventKeyFor, bareKeyFor } from './keygen.js';

const valid = {
  id: 'abc-123',
  source: 'sensor-a',
  type: 'telemetry.temperature',
  severity: 'CRITICAL',
  ts: '2026-11-09T12:00:00.000Z',
  payload: { msg: 'disk failing', msgc: 'x' },
};

// ------------------------------------------------------------------ validation
test('accepts a well-formed envelope', () => {
  assert.doesNotThrow(() => validateEvent({ ...valid }));
});

test('rejects each missing required field', () => {
  for (const field of ['id', 'source', 'type', 'severity']) {
    const evt = { ...valid };
    delete evt[field];
    assert.throws(() => validateEvent(evt), ValidationError, `missing ${field} should fail`);
  }
});

test('rejects an unknown severity', () => {
  assert.throws(() => validateEvent({ ...valid, severity: 'ERROR' }), ValidationError);
  assert.throws(() => validateEvent({ ...valid, severity: 'critical' }), ValidationError, 'case matters');
});

test('accepts every documented severity', () => {
  for (const s of ['FATAL', 'CRITICAL', 'WARNING', 'HARMLESS', 'CLOSE']) {
    assert.doesNotThrow(() => validateEvent({ ...valid, severity: s }));
  }
});

test('retirement severities are CLOSE and HARMLESS only', () => {
  assert.equal(isRetirement({ severity: 'CLOSE' }), true);
  assert.equal(isRetirement({ severity: 'HARMLESS' }), true);
  for (const s of ['FATAL', 'CRITICAL', 'WARNING']) {
    assert.equal(isRetirement({ severity: s }), false, s);
  }
});

test('rejects a non-object payload', () => {
  assert.throws(() => validateEvent({ ...valid, payload: 'string' }), ValidationError);
  assert.throws(() => validateEvent({ ...valid, payload: null }), ValidationError);
});

// ----------------------------------------------------------------- compression
test('a large field is compressed and round-trips exactly', () => {
  const msgc = 'stack trace line\n'.repeat(500);        // ~8.5 KiB, highly compressible
  const { payload, compressedFields, bytesBefore, bytesAfter } = compressPayload(
    { msg: 'disk failing', msgc },
    { min: 1024, level: 6 },
  );
  assert.deepEqual(compressedFields, ['msgc']);
  assert.ok(bytesAfter < bytesBefore, 'compression must save bytes');
  assert.equal(payload.msg, 'disk failing', 'short field stays plain');

  const back = decompressPayload(payload);
  assert.equal(back.msgc, msgc, 'round trip must be byte-identical');
  assert.equal(back.msg, 'disk failing');
});

test('a field below the threshold is left alone', () => {
  const msgc = 'short detail';
  const { payload, compressedFields } = compressPayload({ msgc }, { min: 1024, level: 6 });
  assert.deepEqual(compressedFields, []);
  assert.equal(payload.msgc, msgc);
});

test('incompressible data is kept plain rather than made larger', () => {
  // Random bytes do not compress; gzip output plus framing exceeds the input.
  const noise = Array.from({ length: 2048 }, () => String.fromCharCode(33 + Math.floor(Math.random() * 90))).join('');
  const { payload, compressedFields, bytesBefore, bytesAfter } = compressPayload(
    { msgc: noise },
    { min: 1024, level: 9 },
  );
  if (compressedFields.length === 0) {
    assert.equal(payload.msgc, noise, 'kept as-is');
    assert.equal(bytesAfter, bytesBefore);
  } else {
    assert.ok(bytesAfter < bytesBefore, 'only compressed when it actually helps');
  }
});

test('non-string payload values are untouched', () => {
  const { payload, compressedFields } = compressPayload(
    { value: 21.4, unit: 'C', nested: { a: 1 } },
    { min: 1, level: 6 },
  );
  assert.deepEqual(compressedFields, []);
  assert.equal(payload.value, 21.4);
  assert.deepEqual(payload.nested, { a: 1 });
});

test('per-field threshold: one compressed, one plain, same record', () => {
  const { payload, compressedFields } = compressPayload(
    { msg: 'a'.repeat(50), msgc: 'b'.repeat(4000) },
    { min: 1024, level: 6 },
  );
  assert.deepEqual(compressedFields, ['msgc']);
  assert.equal(typeof payload.msg, 'string');
  assert.equal(typeof payload.msgc, 'object');
});

test('a compressed field carries the gzip magic so readers can detect it', () => {
  const { payload } = compressPayload({ msgc: 'c'.repeat(4000) }, { min: 1, level: 6 });
  const raw = Buffer.from(payload.msgc.__gz, 'base64');
  assert.ok(isGzip(raw), 'stored bytes must start with 0x1f 0x8b');
  assert.ok(gunzipSync(raw).toString('utf8').startsWith('cccc'));
});

test('isGzip rejects plain JSON and short buffers', () => {
  assert.equal(isGzip(Buffer.from('{"id":"x"}')), false);
  assert.equal(isGzip(Buffer.from([0x1f])), false);
  assert.equal(isGzip(null), false);
});

// ------------------------------------------------------------------- timestamps
test('entryTsMs reads the millisecond part of a stream entry ID', () => {
  assert.equal(entryTsMs('1764079564311-0'), 1764079564311);
  assert.equal(entryTsMs('1764079564311-17'), 1764079564311);
});

test('entryTsMs returns null for anything that is not an entry ID', () => {
  assert.equal(entryTsMs(undefined), null);
  assert.equal(entryTsMs(''), null);
  assert.equal(entryTsMs('*'), null);
  assert.equal(entryTsMs('1764079564311'), null);
});

// ------------------------------------------------------------------ event keys
test('the event key covers device + summary and ignores the type field', () => {
  // Deliberate. The board groups on what an operator reads — the device and the
  // summary line — not on the producer's internal event key. Two different
  // EVT_EventKey values with the same words on screen are one row, which is what
  // an operator means by "the same alarm".
  const a = eventKeyFor({ source: 'sensor-a', type: 'temp', payload: { msg: 'too hot' } });
  const b = eventKeyFor({ source: 'sensor-a', type: 'other', payload: { msg: 'too hot' } });
  assert.equal(a, b);
  assert.notEqual(
    eventKeyFor({ source: 'sensor-a', payload: { msg: 'too hot' } }),
    eventKeyFor({ source: 'sensor-a', payload: { msg: 'too cold' } }),
  );
});

test('bareKeyFor strips the prefix, because zsets store the bare key', () => {
  const evt = { source: 's', payload: { msg: 'm' } };
  assert.equal(bareKeyFor(evt), eventKeyFor(evt).slice('event:'.length));
});
