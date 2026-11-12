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
  operatorCloseEvent,
  operatorDeleteRow,
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

// ------------------------------------------------- operator close AS AN EVENT
test('the injected close event hashes back onto the SAME condition', () => {
  // THE PROPERTY THIS WHOLE FEATURE RESTS ON. The record key is
  // sha256(source + 0x1f + payload.msg)[0:32], so the injected event must echo
  // `source` and `payload.msg` verbatim. Re-deriving either one would open a SECOND
  // condition instead of retiring the one the operator clicked.
  const record = { source: 'db-node-03', type: 'hw.psu.voltage', summary: 'PSU 2 input voltage out of range' };
  const evt = operatorCloseEvent(record, 'alice', 1_800_000_000_000);

  assert.equal(evt.source, record.source, 'the device must be identical');
  assert.equal(evt.payload.msg, record.summary, 'and so must the summary the key hashes');
  assert.equal(evt.payload.EVT_Device, record.source);
  assert.equal(evt.payload.EVT_Summary, record.summary);
});

test('the injected close carries the internal severity and the CLOSED label', () => {
  const evt = operatorCloseEvent(
    { source: 's', type: 't', summary: 'x' }, 'bob', 1_800_000_000_000,
  );
  // CLOSE is what the fold retires on; CLOSED is only what the badge shows, so an
  // operator can tell a human-ended incident from a source-sent CLEAR.
  assert.equal(evt.severity, 'CLOSE', 'the worker only accepts the internal scale');
  assert.equal(evt.payload.EVT_Severity, 'CLOSED');
  assert.equal(evt.payload.EVT_ClosedBy, 'bob');
});

test('the injected close is a valid worker envelope', () => {
  // The same required fields services/worker/src/event.js validates, checked here
  // because a close that the worker DLQs is a closure nobody can prove happened.
  const evt = operatorCloseEvent({ source: 's', type: 't', summary: 'x' }, 'alice', 1);
  for (const field of ['id', 'source', 'type', 'severity']) {
    assert.equal(typeof evt[field], 'string', field);
    assert.ok(evt[field].length > 0, `${field} must not be empty`);
  }
  assert.ok(evt.id.startsWith('operator-close-'), 'the id marks the author');
  assert.equal(typeof evt.payload, 'object');
});

test('each operator close gets its own event id', () => {
  const r = { source: 's', type: 't', summary: 'x' };
  const a = operatorCloseEvent(r, 'alice', 1000);
  const b = operatorCloseEvent(r, 'alice', 2000);
  assert.notEqual(a.id, b.id, 'two closes are two events, not one overwriting the other');
});

// ------------------------------------------------- operator DELETE gets a row
test('the delete row describes what was erased', () => {
  // A delete removes the record and its index members, so the worker never sees a
  // delivery and can never fork this into the audit trail. The gateway writes the
  // row from the record it read immediately before deleting.
  const record = {
    key: GOOD, source: 'db-node-03', type: 'hw.psu.voltage',
    summary: 'PSU 2 input voltage out of range',
    severity: 'CLOSE', displaySeverity: 'MAJOR',
  };
  const row = operatorDeleteRow(record, 'alice', 1_800_000_000_000);

  assert.equal(row.key, GOOD, 'the row points at the condition it erased');
  assert.equal(row.source, 'db-node-03');
  assert.equal(row.msg, 'PSU 2 input voltage out of range',
    'the summary survives only in this row');
  assert.equal(row.severity, 'DELETED');
  assert.equal(row.payload.EVT_DeletedBy, 'alice');
});

test('the delete row records the severity the event HAD', () => {
  // Without this, deleting a CRITICAL and deleting a WARNING leave identical rows
  // and the trail stops being an audit.
  const row = operatorDeleteRow(
    { key: GOOD, source: 's', type: 't', summary: 'x', severity: 'CLOSE', displaySeverity: 'MAJOR' },
    'bob', 1000,
  );
  assert.equal(row.payload.EVT_WasSeverity, 'MAJOR',
    'the PRODUCER label is what an operator recognises');
});

test('the delete row falls back to the internal severity when there is no label', () => {
  const row = operatorDeleteRow(
    { key: GOOD, source: 's', type: 't', summary: 'x', severity: 'CRITICAL' },
    'bob', 1000,
  );
  assert.equal(row.payload.EVT_WasSeverity, 'CRITICAL');
});

test('every field the history reader expects is present', () => {
  // HISTORY_FIELDS in valkey.js: key, id, source, type, severity, displaySeverity,
  // msg, payload, brokerTs, ts. A missing one is a blank column, not an error.
  const row = operatorDeleteRow(
    { key: GOOD, source: 's', type: 't', summary: 'x', severity: 'WARNING' },
    'alice', 1000,
  );
  for (const f of ['key', 'source', 'type', 'severity', 'displaySeverity', 'msg', 'payload', 'brokerTs', 'ts']) {
    assert.ok(row[f] !== undefined && row[f] !== null && row[f] !== '', `${f} must be set`);
  }
  assert.equal(typeof row.payload, 'object', 'payload is stored as a value; the writer stringifies');
});
