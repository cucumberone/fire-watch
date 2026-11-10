/**
 * Tests for ack validation and rendering.
 *   node --test "src/ack.test.js"
 *
 * These guard the write path: the gateway is now able to modify Dragonfly, so
 * every rule that keeps that write narrow is pinned here.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import {
  ANONYMOUS,
  MAX_ALERT_QUERY_KEYS,
  MAX_KEY_LEN,
  MAX_ID_LEN,
  ackAlertKey,
  ackFrame,
  closeFrame,
  isValidAlertKey,
  normaliseAck,
  validateAck,
  validateAlertQuery,
  validateClose,
  validateDelete,
  deletedFrame,
  eventKey,
} from './ack.js';

// ------------------------------------------------------------------ key rules
test('accepts a normal source:type alert key', () => {
  assert.equal(isValidAlertKey('sensor-a:telemetry.temperature'), true);
  assert.equal(isValidAlertKey('a'), true);
});

test('rejects empty, non-string and over-long keys', () => {
  for (const bad of ['', null, undefined, 42, {}, []]) {
    assert.equal(isValidAlertKey(bad), false, JSON.stringify(bad));
  }
  assert.equal(isValidAlertKey('x'.repeat(MAX_KEY_LEN)), true);
  assert.equal(isValidAlertKey('x'.repeat(MAX_KEY_LEN + 1)), false);
});

test('rejects keys containing control characters', () => {
  // A newline in a key would forge a second log line; a NUL would truncate a
  // hash field in some clients.
  assert.equal(isValidAlertKey('a\nb'), false);
  assert.equal(isValidAlertKey('a\u0000b'), false);
  assert.equal(isValidAlertKey('a\u007fb'), false);
});

test('allows glob characters because HSET does no pattern matching', () => {
  // Rejecting these would be a lie about what is dangerous here.
  assert.equal(isValidAlertKey('sensor:*'), true);
  assert.equal(isValidAlertKey('a?b'), true);
  assert.equal(isValidAlertKey('a[b]'), true);
});

// -------------------------------------------------------------- validateAck
test('validateAck accepts a valid frame', () => {
  const v = validateAck({ op: 'ack', key: 'sw1:link.down', by: 'gunnar' });
  assert.deepEqual(v, { ok: true, key: 'sw1:link.down', by: 'gunnar' });
});

test('validateAck defaults the operator to anonymous', () => {
  assert.equal(validateAck({ op: 'ack', key: 'a:b' }).by, ANONYMOUS);
  assert.equal(validateAck({ op: 'ack', key: 'a:b', by: '' }).by, ANONYMOUS);
  assert.equal(validateAck({ op: 'ack', key: 'a:b', by: '   ' }).by, ANONYMOUS);
  assert.equal(validateAck({ op: 'ack', key: 'a:b', by: 42 }).by, ANONYMOUS);
});

test('validateAck sanitises the operator label', () => {
  // The label is displayed to other operators, so a newline or a control char
  // must not survive, and a very long name must be truncated.
  assert.equal(validateAck({ op: 'ack', key: 'a:b', by: 'g u n\nnar' }).by, 'g u n nar');
  assert.equal(validateAck({ op: 'ack', key: 'a:b', by: 'x'.repeat(500) }).by.length, 128);
});

test('validateAck rejects a bad op and a bad key', () => {
  assert.equal(validateAck({ op: 'subscribe' }).code, 'bad_op');
  assert.equal(validateAck(null).code, 'bad_op');
  assert.equal(validateAck({ op: 'ack' }).code, 'bad_key');
  assert.equal(validateAck({ op: 'ack', key: 42 }).code, 'bad_key');
  assert.equal(validateAck({ op: 'ack', key: '' }).code, 'bad_key');
});

// ------------------------------------------------------- validateAlertQuery
test('validateAlertQuery accepts and de-duplicates keys', () => {
  const v = validateAlertQuery({ op: 'alerts', keys: ['a:b', 'a:b', 'c:d'] });
  assert.equal(v.ok, true);
  assert.deepEqual(v.keys, ['a:b', 'c:d']);
});

test('validateAlertQuery accepts an empty list', () => {
  // A UI with no alerts open yet sends an empty array; that is not an error.
  assert.deepEqual(validateAlertQuery({ op: 'alerts', keys: [] }), { ok: true, keys: [] });
});

test('validateAlertQuery refuses an unbounded read', () => {
  // This is the guard that stops a client asking the pod to scan the keyspace.
  const v = validateAlertQuery({ op: 'alerts', keys: Array(MAX_ALERT_QUERY_KEYS + 1).fill('a:b') });
  assert.equal(v.ok, false);
  assert.equal(v.code, 'too_many_keys');
});

test('validateAlertQuery rejects a non-array and an invalid key', () => {
  assert.equal(validateAlertQuery({ op: 'alerts', keys: 'a:b' }).code, 'bad_keys');
  assert.equal(validateAlertQuery({ op: 'alerts', keys: ['a:b', 'bad\nkey'] }).code, 'bad_key');
});

// ------------------------------------------------------------------ rendering
test('ackAlertKey adds the fw:alert prefix', () => {
  assert.equal(ackAlertKey('sw1:link.down'), 'fw:alert:sw1:link.down');
});

test('normaliseAck returns null when not acked', () => {
  assert.equal(normaliseAck(null), null);
  assert.equal(normaliseAck({}), null);
  // A by-field without a timestamp is a partial write, not an ack.
  assert.equal(normaliseAck({ acked_by: 'gunnar' }), null);
  assert.equal(normaliseAck({ acked_by: 'gunnar', acked_at: '0' }), null);
  assert.equal(normaliseAck({ acked_by: 'gunnar', acked_at: 'nope' }), null);
});

test('normaliseAck returns a usable ack, defaulting the operator', () => {
  assert.deepEqual(normaliseAck({ acked_by: 'gunnar', acked_at: '1762699200000' }), {
    by: 'gunnar',
    at: 1762699200000,
  });
  assert.deepEqual(normaliseAck({ acked_at: '1762699200000' }), {
    by: ANONYMOUS,
    at: 1762699200000,
  });
});

test('ackFrame is a JSON alert frame carrying the ack', () => {
  const frame = JSON.parse(ackFrame('a:b', { by: 'gunnar', at: 1 }));
  assert.equal(frame.op, 'alert');
  assert.equal(frame.key, 'a:b');
  assert.deepEqual(frame.ack, { by: 'gunnar', at: 1 });
});

// --------------------------------------------------------------------- close
// A close retires the alert, so its validation must be exactly as strict as ack:
// a loose key here would let a client target a hash it should not touch.
test('validateClose accepts a normal alert key and defaults the operator', () => {
  const v = validateClose({ op: 'close', key: 'shop:check-001' });
  assert.equal(v.ok, true);
  assert.equal(v.key, 'shop:check-001');
  assert.equal(v.by, ANONYMOUS);
});

test('validateClose rejects a wrong op or a bad key', () => {
  assert.equal(validateClose({ op: 'ack', key: 'a:b' }).ok, false);
  assert.equal(validateClose({ op: 'close', key: '' }).ok, false);
  assert.equal(validateClose({ op: 'close', key: 'x'.repeat(MAX_KEY_LEN + 1) }).ok, false);
  assert.equal(validateClose({ op: 'close', key: 'a\u0000b' }).ok, false);
});

test('closeFrame is an alert frame with applied 3', () => {
  // Reusing the CLOSE-event frame shape means clients need no new code path to
  // retire a row: it is the same frame a source-sent CLOSE produces.
  const f = JSON.parse(closeFrame('a:b'));
  assert.equal(f.op, 'alert');
  assert.equal(f.key, 'a:b');
  assert.equal(f.applied, 3);
  assert.equal(f.closedBy, 'operator');
});

// -------------------------------------------------------------------- delete
// Delete is the one destructive op, so its validation is the strictest: a loose
// id would let a client remove a record it has no business touching.
test('validateDelete accepts an id and defaults the operator', () => {
  const v = validateDelete({ op: 'delete', id: 'evt-123' });
  assert.equal(v.ok, true);
  assert.equal(v.id, 'evt-123');
  assert.equal(v.by, ANONYMOUS);
});

test('validateDelete rejects a wrong op, empty id and over-long id', () => {
  assert.equal(validateDelete({ op: 'ack', id: 'x' }).ok, false);
  assert.equal(validateDelete({ op: 'delete' }).ok, false);
  assert.equal(validateDelete({ op: 'delete', id: '' }).ok, false);
  assert.equal(validateDelete({ op: 'delete', id: 'x'.repeat(MAX_ID_LEN + 1) }).ok, false);
  assert.equal(validateDelete({ op: 'delete', id: 'a\u0000b' }).ok, false);
});

test('validateDelete rejects an alert key passed as an id only when malformed', () => {
  // `source:type` is a legitimate id shape — ids are opaque strings — so it must
  // be accepted. The distinction between an id and an alert key is what the
  // DELETE targets, not a character rule.
  assert.equal(validateDelete({ op: 'delete', id: 'shop:check-001' }).ok, true);
});

test('eventKey mirrors the worker key shape', () => {
  assert.equal(eventKey('abc'), 'fw:evt:abc');
});

test('deletedFrame is a distinct op carrying the id', () => {
  // Distinct from `alert`, because removing one event is a different statement
  // from retiring an alert.
  const f = JSON.parse(deletedFrame('evt-1'));
  assert.equal(f.op, 'deleted');
  assert.equal(f.id, 'evt-1');
});
