/**
 * Tests for subscription matching, validation, dedup and backfill planning.
 *   node --test src/
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import {
  matchesSubscription,
  matchesAlertSubscription,
  validateSubscribe,
  RecentIds,
  backfillPlan,
  eventFrame,
  alertFrame,
  shouldFanOut,
  foldDbProbe,
} from './subscription.js';

const limits = { maxSubscriptions: 20, existing: 0 };

// ------------------------------------------------------------------- matching
test('empty filters match everything', () => {
  const sub = { sources: [], types: [] };
  assert.equal(matchesSubscription(sub, { source: 'a', type: 't' }), true);
});

// --------------------------------------------- snapshot-row matching
// Snapshot rows are record objects now (they carry `source` and `type` as real
// fields). They used to be bare `source:type` strings, which is why this had to
// split on a colon. Getting this wrong silently hides every open row from a
// filtered client, which looks identical to "nothing is firing".
const row = (source, type) => ({ source, type, key: 'a'.repeat(32) });

test('an unfiltered subscription matches every snapshot row', () => {
  const sub = { sources: [], types: [] };
  assert.equal(matchesAlertSubscription(sub, row('shop', 'check-001')), true);
});

test('a type containing colons is matched whole, not split', () => {
  // The old implementation split the KEY on the first colon, which mangled
  // `net:if:eth0`. With real fields there is nothing to split.
  assert.equal(matchesAlertSubscription({ sources: ['net'], types: [] }, row('net', 'if:eth0')), true);
  assert.equal(matchesAlertSubscription({ sources: [], types: ['if:eth0'] }, row('net', 'if:eth0')), true);
  assert.equal(matchesAlertSubscription({ sources: [], types: ['eth0'] }, row('net', 'if:eth0')), false);
});

test('snapshot matching filters by source and by type', () => {
  assert.equal(matchesAlertSubscription({ sources: ['shop'], types: [] }, row('shop', 'a')), true);
  assert.equal(matchesAlertSubscription({ sources: ['shop'], types: [] }, row('db', 'a')), false);
  assert.equal(matchesAlertSubscription({ sources: [], types: ['a'] }, row('shop', 'a')), true);
  assert.equal(matchesAlertSubscription({ sources: [], types: ['a'] }, row('shop', 'b')), false);
});

test('snapshot matching ANDs source and type', () => {
  const sub = { sources: ['shop'], types: ['a'] };
  assert.equal(matchesAlertSubscription(sub, row('shop', 'a')), true);
  assert.equal(matchesAlertSubscription(sub, row('shop', 'b')), false);
  assert.equal(matchesAlertSubscription(sub, row('db', 'a')), false);
});

test('an opaque key or missing row matches nothing when a filter is set', () => {
  // Safe direction: a row is hidden rather than shown to a client that asked for
  // something else. This is what a caller passing `a.key` instead of `a` hits.
  assert.equal(matchesAlertSubscription({ sources: ['shop'], types: [] }, 'a'.repeat(32)), false);
  assert.equal(matchesAlertSubscription({ sources: ['shop'], types: [] }, null), false);
});

test('source filter matches only that source', () => {
  const sub = { sources: ['sensor-a'], types: [] };
  assert.equal(matchesSubscription(sub, { source: 'sensor-a', type: 't' }), true);
  assert.equal(matchesSubscription(sub, { source: 'sensor-b', type: 't' }), false);
});

test('type filter matches only that type', () => {
  const sub = { sources: [], types: ['temp'] };
  assert.equal(matchesSubscription(sub, { source: 'a', type: 'temp' }), true);
  assert.equal(matchesSubscription(sub, { source: 'a', type: 'humidity' }), false);
});

test('source and type filters are ANDed', () => {
  const sub = { sources: ['sensor-a'], types: ['temp'] };
  assert.equal(matchesSubscription(sub, { source: 'sensor-a', type: 'temp' }), true);
  assert.equal(matchesSubscription(sub, { source: 'sensor-a', type: 'humidity' }), false);
  assert.equal(matchesSubscription(sub, { source: 'sensor-b', type: 'temp' }), false);
});

test('missing sources/types behaves as an empty filter, not a crash', () => {
  assert.equal(matchesSubscription({}, { source: 'a', type: 't' }), true);
});

// ----------------------------------------------------------------- validation
test('a valid subscribe frame is accepted and normalised', () => {
  const r = validateSubscribe({ op: 'subscribe', sources: ['a'], types: ['t'] }, limits);
  assert.equal(r.ok, true);
  assert.deepEqual(r.sub.sources, ['a']);
  assert.deepEqual(r.sub.types, ['t']);
  assert.equal(r.sub.since, null);
});

test('a non-subscribe op is rejected', () => {
  const r = validateSubscribe({ op: 'foo' }, limits);
  assert.equal(r.ok, false);
  assert.equal(r.code, 'bad_op');
});

test('exceeding the subscription limit is rejected', () => {
  const r = validateSubscribe({ op: 'subscribe' }, { maxSubscriptions: 2, existing: 2 });
  assert.equal(r.ok, false);
  assert.equal(r.code, 'too_many_subscriptions');
});

test('non-array filters are rejected rather than coerced', () => {
  assert.equal(validateSubscribe({ op: 'subscribe', sources: 'a' }, limits).code, 'bad_filter');
  assert.equal(validateSubscribe({ op: 'subscribe', types: 5 }, limits).code, 'bad_filter');
});

test('non-string entries are dropped, not rejected', () => {
  const r = validateSubscribe({ op: 'subscribe', sources: ['a', 1, null, 'b'] }, limits);
  assert.equal(r.ok, true);
  assert.deepEqual(r.sub.sources, ['a', 'b']);
});

test('an unparseable since is rejected', () => {
  const r = validateSubscribe({ op: 'subscribe', since: 'yesterday' }, limits);
  assert.equal(r.ok, false);
  assert.equal(r.code, 'bad_since');
});

test('a valid ISO since is parsed to ms', () => {
  const r = validateSubscribe({ op: 'subscribe', since: '2026-11-09T00:00:00.000Z' }, limits);
  assert.equal(r.ok, true);
  assert.equal(r.sub.since, Date.parse('2026-11-09T00:00:00.000Z'));
});

// ---------------------------------------------------------------------- dedup
test('RecentIds reports the first sighting as new and repeats as old', () => {
  const r = new RecentIds(10);
  assert.equal(r.add('a'), true);
  assert.equal(r.add('a'), false);
  assert.equal(r.add('b'), true);
});

test('RecentIds evicts in insertion order and forgets the evicted id', () => {
  const r = new RecentIds(2);
  r.add('a'); r.add('b');
  r.add('c');                       // evicts 'a'
  assert.equal(r.size, 2);
  assert.equal(r.add('a'), true, 'evicted id is treated as new again');
});

test('RecentIds tolerates missing ids without consuming capacity', () => {
  const r = new RecentIds(2);
  assert.equal(r.add(null), true);
  assert.equal(r.size, 0);
});

// ------------------------------------------------------------------- backfill
test('no since means live-only, never an unbounded scan', () => {
  assert.equal(backfillPlan({ since: null }, Date.now(), 100), null);
  assert.equal(backfillPlan({}, Date.now(), 100), null);
});

test('a since in the future yields no backfill', () => {
  const now = Date.now();
  assert.equal(backfillPlan({ since: now + 1000 }, now, 100), null);
});

test('a since in the past yields a bounded window', () => {
  const now = Date.now();
  const plan = backfillPlan({ since: now - 5000 }, now, 100);
  assert.equal(plan.min, now - 5000);
  assert.equal(plan.max, now);
  assert.equal(plan.limit, 100);
});

// --------------------------------------------------------------------- frames
test('eventFrame carries the applied code through unchanged', () => {
  const evt = { id: 'x', source: 'a', type: 't' };
  for (const code of [0, 1, 2, 3]) {
    const f = JSON.parse(eventFrame(evt, code));
    assert.equal(f.op, 'event');
    assert.equal(f.applied, code);
    assert.equal(f.event.id, 'x');
  }
});

test('eventFrame allows a null applied for backfilled events', () => {
  const f = JSON.parse(eventFrame({ id: 'x' }, null));
  assert.equal(f.applied, null);
});

test('alertFrame is keyed and carries the applied code', () => {
  const f = JSON.parse(alertFrame('a:t', 3));
  assert.equal(f.op, 'alert');
  assert.equal(f.key, 'a:t');
  assert.equal(f.applied, 3);
});

test('alertFrame stays minimal when no row is supplied', () => {
  // The hot path: every applied event already carries its own `op: event` frame,
  // so attaching a row here would duplicate the payload for every event.
  const f = JSON.parse(alertFrame('a:t', 1));
  assert.deepEqual(Object.keys(f).sort(), ['applied', 'key', 'op']);
});

test('alertFrame attaches the row when given one', () => {
  // A content duplicate has no event of its own, so the row is the only way a
  // client that never saw the original can render anything.
  const row = {
    key: 'a:t', severity: 'WARNING', firstSeen: 1, lastSeen: 2, count: 7,
    event: { id: 'e1', source: 'a', type: 't' },
  };
  const f = JSON.parse(alertFrame('a:t', 1, row));
  assert.equal(f.op, 'alert');
  assert.equal(f.applied, 1);
  assert.equal(f.row.count, 7);
  assert.equal(f.row.severity, 'WARNING');
  assert.equal(f.row.event.id, 'e1');
});

// ---------- RecentIds is now keyed on the FRAME, not the event id alone
test('a repeat with a new brokerTs is not suppressed as a duplicate', () => {
  // The gateway used to key this on `event.id`. An absorbed repeat keeps the
  // original id, so every arrival after the first was dropped and the live tail
  // showed one row for two runs of the same probe. The key now includes brokerTs.
  const r = new RecentIds(10);
  assert.equal(r.add('p1|1000|0'), true, 'first arrival is new');
  assert.equal(r.add('p1|2000|0'), true, 'later arrival with a new brokerTs is new');
});

test('an identical frame is still suppressed', () => {
  const r = new RecentIds(10);
  assert.equal(r.add('p1|1000|0'), true);
  assert.equal(r.add('p1|1000|0'), false, 'a true redelivery is dropped');
});

// ---------------------------------------------- heartbeat liveness contract
/**
 * The heartbeat loop reads and writes `ws.__awaitingPong`; the pong handlers must
 * clear exactly that flag.
 *
 * This is a contract test rather than an integration test because the bug it
 * guards was one of NAMING, not logic: the pong handler set `conn.alive` while the
 * loop tested `ws.__awaitingPong`, so every socket was terminated one
 * `pongTimeoutMs` after its first heartbeat — measured at 14.97 s with close code
 * 1006 — and every client reconnected in a 15-second cycle. Nothing failed, no
 * error was raised, and at the default log level nothing was even logged.
 *
 * So the source is asserted directly: the loop must test the same property the
 * handler clears, and the handler must not maintain a second, unread one.
 */
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const SRC = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), 'index.js'), 'utf8',
);

test('both pong paths clear __awaitingPong', () => {
  // The protocol-level handler and the JSON `{"op":"pong"}` frame.
  const clears = SRC.match(/__awaitingPong\s*=\s*false/g) || [];
  assert.ok(clears.length >= 2,
    `expected the heartbeat flag to be cleared in both pong paths, found ${clears.length}`);
});

test('the heartbeat loop sets and tests the same flag it clears', () => {
  assert.match(SRC, /if\s*\(ws\.__awaitingPong\)/, 'loop must TEST __awaitingPong');
  assert.match(SRC, /ws\.__awaitingPong\s*=\s*true/, 'loop must SET __awaitingPong');
});

test('no second, unread liveness flag exists', () => {
  // `conn.alive` was the unread half. It must not come back: a property that is
  // written and never read is indistinguishable from a working heartbeat.
  assert.doesNotMatch(SRC, /conn\.alive\s*=\s*true/);
});

test('terminating a socket for liveness is logged above debug', () => {
  // At `debug` (with the default `info` level) a heartbeat bug that dropped every
  // connection produced no log output at all.
  const terminatelines = SRC.split('\n').filter((l) => l.includes('ws.terminate()'));
  const logged = SRC.split('\n').filter((l) => /log\.(warn|error|info)\(/.test(l));
  assert.ok(terminatelines.length >= 1, 'sockets are still terminated on timeout');
  assert.ok(logged.some((l) => /unresponsive|no pong/i.test(l)),
    'the termination must be logged at warn or above');
});

// ---------------------- a fan-out must not echo back to its own publisher
/**
 * `close` and `ack` are confirmed DIRECTLY to the requesting client, then
 * broadcast. The broadcast comes back to the publishing pod, so without an origin
 * tag that pod delivers the same frame twice — which made one operator close count
 * as two closures on the Closed badge.
 *
 * Asserted on the source because it is a property of the wiring, not of a pure
 * function: the handler must compare the frame's origin with this pod's name.
 */
test('the ack and close fan-outs skip their own broadcast', () => {
  // Both broadcasts must be origin-tagged...
  const tags = SRC.match(/origin: POD_NAME/g) || [];
  assert.ok(tags.length >= 2,
    `ack and close broadcasts must be origin-tagged, found ${tags.length}`);
  // ...and both handlers must consult the shared rule rather than re-deriving it.
  const uses = SRC.match(/if \(!shouldFanOut\(parsed, POD_NAME\)\) return;/g) || [];
  assert.ok(uses.length >= 2,
    `both fan-outs must use shouldFanOut, found ${uses.length}`);
});

test('shouldFanOut drops our own echo and keeps everyone else\'s', () => {
  assert.equal(shouldFanOut({ kind: 'close', origin: 'pod-a' }, 'pod-a'), false,
    'own broadcast must not be re-delivered');
  assert.equal(shouldFanOut({ kind: 'close', origin: 'pod-b' }, 'pod-a'), true,
    "a peer's broadcast must be delivered");
});

test('an untagged frame is still delivered', () => {
  // A peer running an older build sends no origin. Duplicate is recoverable; a
  // missed close would leave a row on screen that nobody can clear.
  assert.equal(shouldFanOut({ kind: 'close' }, 'pod-a'), true);
});

test('a malformed payload is not fanned out', () => {
  assert.equal(shouldFanOut(null, 'pod-a'), false);
  assert.equal(shouldFanOut('nonsense', 'pod-a'), false);
});

test('POD_NAME is defined once and used by the hello frame too', () => {
  assert.match(SRC, /const POD_NAME = process\.env\.POD_NAME \|\| 'local'/);
  assert.doesNotMatch(SRC, /process\.env\.POD_NAME \|\| 'local'\}`/,
    'the hello frame must reuse the constant, not re-read the env var');
});

// ------------------------------------------- Dragonfly health reporting rules
test('a single failed probe does NOT report the database as down', () => {
  // One failed PING is indistinguishable from a demoted master or a network blip.
  // Reporting it would make the indicator flicker on noise.
  let s = { up: true, fails: 0 };
  s = foldDbProbe(s, false, 3, 1);
  assert.equal(s.up, true, 'still up after 1 failure');
  assert.equal(s.changed, false, 'no frame worth sending');
  assert.equal(s.fails, 1);
});

test('the third consecutive failure flips to DOWN and reports a change', () => {
  let s = { up: true, fails: 0 };
  s = foldDbProbe(s, false, 3, 1);
  s = foldDbProbe(s, false, 3, 2);
  assert.equal(s.up, true, 'two failures is not yet an outage');
  s = foldDbProbe(s, false, 3, 3);
  assert.equal(s.up, false, 'three failures is DOWN');
  assert.equal(s.changed, true, 'this is the frame that must be pushed');
});

test('further failures while DOWN do not re-send the frame', () => {
  let s = { up: true, fails: 0 };
  for (let i = 0; i < 3; i += 1) s = foldDbProbe(s, false, 3, i);
  assert.equal(s.up, false);
  const again = foldDbProbe(s, false, 3, 9);
  assert.equal(again.up, false);
  assert.equal(again.changed, false, 'still down is not a state change');
});

test('one success recovers immediately and resets the counter', () => {
  let s = { up: true, fails: 0 };
  for (let i = 0; i < 3; i += 1) s = foldDbProbe(s, false, 3, i);
  const back = foldDbProbe(s, true, 3, 99);
  assert.equal(back.up, true);
  assert.equal(back.changed, true, 'recovery is a transition worth pushing');
  assert.equal(back.fails, 0, 'the counter resets, or the next outage is misjudged');
});

test('a success while already up is not a change', () => {
  const s = foldDbProbe({ up: true, fails: 2 }, true, 3, 1);
  assert.equal(s.up, true);
  assert.equal(s.changed, false, 'healthy to healthy sends nothing');
});

test('a threshold of 1 reports the first failure', () => {
  const s = foldDbProbe({ up: true, fails: 0 }, false, 1, 1);
  assert.equal(s.up, false);
  assert.equal(s.changed, true);
});
