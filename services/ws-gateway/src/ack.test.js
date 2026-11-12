/**
 * Tests for the operator-action validators.
 *
 * The important rule here is that the key SHAPE is enforced, not just its length.
 * A legacy `source:type` key would otherwise reach Valkey and write an ack onto a
 * record no fold ever created — the write succeeds, nothing errors, and the operator
 * sees a row that is "acked" in the UI and still firing everywhere else.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import {
  ANONYMOUS,
  MAX_ALERT_QUERY_KEYS,
  KEY_LEN,
  isValidKey,
  isValidAlertKey,
  validateAck,
  validateAlertQuery,
  validateClose,
  validateDelete,
  normaliseAck,
  closeFrame,
  ackFrame,
  validateHistoryQuery,
  HISTORY_RANGES,
} from './ack.js';

const key = (c = 'a') => c.repeat(KEY_LEN);
const GOOD = key('a');

test('the valid key shape is 32 lowercase hex chars', () => {
  assert.equal(isValidKey(GOOD), true);
  assert.equal(isValidKey('0123456789abcdef0123456789abcdef'), true);
  assert.equal(isValidAlertKey(GOOD), true, 'alias still works for older callers');
});

test('legacy source:type keys are REJECTED, not tolerated', () => {
  // The regression this pins: a stale browser posting `sw1:link.down` used to be
  // accepted, and the ack landed on a key nothing reads.
  assert.equal(isValidKey('sw1:link.down'), false);
  assert.equal(isValidKey('selftest:dupcheck-123'), false);
});

test('wrong length, case or alphabet is rejected', () => {
  assert.equal(isValidKey('a'.repeat(KEY_LEN - 1)), false);
  assert.equal(isValidKey('a'.repeat(KEY_LEN + 1)), false);
  assert.equal(isValidKey('A'.repeat(KEY_LEN)), false, 'uppercase hex is not the generated shape');
  assert.equal(isValidKey('g'.repeat(KEY_LEN)), false);
  assert.equal(isValidKey(''), false);
  assert.equal(isValidKey(null), false);
  assert.equal(isValidKey('event:' + 'a'.repeat(KEY_LEN)), false, 'the prefix is not on the wire');
});

test('control characters are impossible by construction', () => {
  // The strict regex makes every control-character case unreachable, which is
  // stronger than filtering them.
  assert.equal(isValidKey('a\nb'), false);
  assert.equal(isValidKey('a\u0000b'), false);
});

test('a valid ack passes and defaults the operator to anonymous', () => {
  const r = validateAck({ op: 'ack', key: GOOD });
  assert.deepEqual(r, { ok: true, key: GOOD, by: ANONYMOUS });
});

test('an ack with a bad key is rejected with a reason', () => {
  const r = validateAck({ op: 'ack', key: 'sw1:link.down' });
  assert.equal(r.ok, false);
  assert.equal(r.code, 'bad_key');
  assert.match(r.message, /record key/);
});

test('the wrong op is rejected', () => {
  assert.equal(validateAck({ op: 'close', key: GOOD }).ok, false);
  assert.equal(validateAck(null).ok, false);
});

test('a bulk query deduplicates and enforces the cap', () => {
  const r = validateAlertQuery({ op: 'alerts', keys: [GOOD, GOOD] });
  assert.equal(r.ok, true);
  assert.deepEqual(r.keys, [GOOD], 'duplicates collapse');

  const tooMany = validateAlertQuery({
    op: 'alerts',
    keys: Array.from({ length: MAX_ALERT_QUERY_KEYS + 1 }, (_, i) => i.toString(16).padStart(KEY_LEN, '0')),
  });
  assert.equal(tooMany.ok, false);
  assert.equal(tooMany.code, 'too_many_keys');
});

test('a bulk query with one bad key rejects the whole frame', () => {
  const r = validateAlertQuery({ op: 'alerts', keys: [GOOD, 'legacy:key'] });
  assert.equal(r.ok, false);
  assert.equal(r.code, 'bad_key');
});

test('close uses the same key rules as ack', () => {
  assert.equal(validateClose({ op: 'close', key: GOOD }).ok, true);
  assert.equal(validateClose({ op: 'close', key: 'x'.repeat(KEY_LEN + 1) }).ok, false);
});

test('delete validates the id as a record key', () => {
  assert.equal(validateDelete({ op: 'delete', id: GOOD }).ok, true);
  assert.equal(validateDelete({ op: 'delete', id: 'some-uuid' }).ok, false);
});

test('normaliseAck requires a timestamp, in either spelling', () => {
  assert.equal(normaliseAck(null), null);
  assert.equal(normaliseAck({}), null);
  // A `by` with no timestamp is a partial write: nothing to show an operator.
  assert.equal(normaliseAck({ ackedBy: 'alice' }), null);
  assert.deepEqual(normaliseAck({ ackedBy: 'alice', ackedAt: '1500' }), { by: 'alice', at: 1500 });
  assert.deepEqual(normaliseAck({ acked_by: 'bob', acked_at: 2000 }), { by: 'bob', at: 2000 });
  // An empty name falls back rather than rendering a blank cell.
  assert.deepEqual(normaliseAck({ ackedBy: '', ackedAt: 1 }), { by: ANONYMOUS, at: 1 });
});

test('frames are JSON with the wire key', () => {
  assert.deepEqual(JSON.parse(closeFrame(GOOD)), {
    op: 'alert', key: GOOD, applied: 3, closedBy: 'operator',
  });
  assert.deepEqual(JSON.parse(ackFrame(GOOD, { by: 'a', at: 1 })), {
    op: 'alert', key: GOOD, ack: { by: 'a', at: 1 },
  });
});

// ------------------------------------------------------------------- history
test('history defaults to 24h and the max page', () => {
  const now = 1_800_000_000_000;
  const v = validateHistoryQuery({ op: 'history' }, { now });
  assert.equal(v.ok, true);
  assert.equal(v.range, '24h');
  assert.equal(v.untilMs - v.sinceMs, 24 * 60 * 60 * 1000);
  assert.equal(v.key, null);
});

test('history offers no range beyond the audit retention', () => {
  // Offering `7d` would promise rows that cannot exist: the audit hashes and their
  // index members expire at three days. Longest preset must be that.
  const longest = Math.max(...Object.values(HISTORY_RANGES));
  assert.equal(longest, 3 * 24 * 60 * 60 * 1000);
});

test('history falls back to 24h for an unknown range rather than erroring', () => {
  // The tab only ever sends a known value; a stray one must not blank the table.
  assert.equal(validateHistoryQuery({ op: 'history', range: '9y' }).range, '24h');
});

test('history clamps the limit and rejects a non-numeric one', () => {
  assert.equal(validateHistoryQuery({ op: 'history', limit: 1e9 }).limit, 1000);
  assert.equal(validateHistoryQuery({ op: 'history', limit: 0 }).limit, 1);
  assert.equal(validateHistoryQuery({ op: 'history', limit: 'x' }).code, 'bad_limit');
});

test('history accepts an optional condition key and rejects a bad one', () => {
  assert.equal(validateHistoryQuery({ op: 'history', key: GOOD }).key, GOOD);
  // Rejected, not ignored: silently returning the whole window would show rows
  // that do not belong to the incident the operator asked about.
  assert.equal(validateHistoryQuery({ op: 'history', key: 'a:b' }).code, 'bad_key');
  assert.equal(validateHistoryQuery({ op: 'history', key: 'A'.repeat(32) }).code, 'bad_key');
  assert.equal(validateHistoryQuery({ op: 'history', key: 42 }).code, 'bad_key');
});

test('history rejects the wrong op', () => {
  assert.equal(validateHistoryQuery({ op: 'allEvents' }).code, 'bad_op');
  assert.equal(validateHistoryQuery(null).code, 'bad_op');
});
