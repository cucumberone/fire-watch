/**
 * Tests for the worker's pure logic: validation, compression, timestamp
 * extraction. No broker, no Dragonfly.
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
  brokerTsMs,
  isGzip,
  isRetirement,
} from './event.js';
import { alertKey } from './config.js';

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
test('brokerTsMs prefers x-original-ts over the broker header', () => {
  // A republished message must keep the original ordering position
  // (event-lifecycle.md §6).
  assert.equal(
    brokerTsMs({ properties: { headers: { 'x-original-ts': 1500, timestamp_in_ms: 9999 } } }),
    1500,
  );
});

test('brokerTsMs falls back to timestamp_in_ms, then to seconds', () => {
  assert.equal(brokerTsMs({ properties: { headers: { timestamp_in_ms: 2000 } } }), 2000);
  assert.equal(brokerTsMs({ properties: { timestamp: 3 } }), 3000);
});

test('brokerTsMs returns null when no timestamp exists at all', () => {
  assert.equal(brokerTsMs({ properties: {} }), null);
  assert.equal(brokerTsMs({}), null);
});

// ------------------------------------------------------------------ alert keys
test('alertKey is source:type, so different types never share state', () => {
  assert.equal(alertKey({ source: 'sensor-a', type: 'temp' }), 'sensor-a:temp');
  assert.notEqual(
    alertKey({ source: 'sensor-a', type: 'temp' }),
    alertKey({ source: 'sensor-a', type: 'humidity' }),
  );
});
