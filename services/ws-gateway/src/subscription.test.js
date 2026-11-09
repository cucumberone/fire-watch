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
} from './subscription.js';

const limits = { maxSubscriptions: 20, existing: 0 };

// ------------------------------------------------------------------- matching
test('empty filters match everything', () => {
  const sub = { sources: [], types: [] };
  assert.equal(matchesSubscription(sub, { source: 'a', type: 't' }), true);
});

// ------------------------------------------------- alert-key matching (snapshot)
// Snapshot rows arrive as bare `source:type` keys, not events, so they cannot be
// matched by matchesSubscription. Getting this wrong silently hides every open
// alert from a filtered client, which looks identical to "nothing is firing".
test('an unfiltered subscription matches every alert key', () => {
  const sub = { sources: [], types: [] };
  assert.equal(matchesAlertSubscription(sub, 'shop:check-001'), true);
});

test('alert-key matching splits on the FIRST colon only', () => {
  // A type may contain colons; a source may not. Splitting on the last colon
  // would mangle `net:if:eth0` into type `eth0`.
  const sub = { sources: ['net'], types: [] };
  assert.equal(matchesAlertSubscription(sub, 'net:if:eth0'), true);

  const typed = { sources: [], types: ['if:eth0'] };
  assert.equal(matchesAlertSubscription(typed, 'net:if:eth0'), true);
});

test('alert-key matching filters by source and by type', () => {
  assert.equal(matchesAlertSubscription({ sources: ['shop'], types: [] }, 'shop:a'), true);
  assert.equal(matchesAlertSubscription({ sources: ['shop'], types: [] }, 'db:a'), false);
  assert.equal(matchesAlertSubscription({ sources: [], types: ['a'] }, 'shop:a'), true);
  assert.equal(matchesAlertSubscription({ sources: [], types: ['a'] }, 'shop:b'), false);
});

test('alert-key matching ANDs source and type', () => {
  const sub = { sources: ['shop'], types: ['a'] };
  assert.equal(matchesAlertSubscription(sub, 'shop:a'), true);
  assert.equal(matchesAlertSubscription(sub, 'shop:b'), false);
  assert.equal(matchesAlertSubscription(sub, 'db:a'), false);
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
