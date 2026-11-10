/**
 * Tests for the worker's write path, with Dragonfly stubbed.
 *
 * Regression covered: duplicates were only detected by the fold's
 * (timestamp, id) watermark, which cannot catch a re-sent event id — every
 * broker delivery carries a fresh timestamp, so the repeat always looked newer
 * and was counted again. `persistEvent` now gates on the event record itself
 * (`SET ... NX`), so a duplicate is dropped before it can touch the fold, the
 * index or the stored record.
 *
 *   node --test src/
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { persistEvent, missingFromGlobal, reconcileEventIndex } from './store.js';

/**
 * Minimal Dragonfly stub. `set` returns null on a second call for the same key,
 * which is exactly what real `SET NX` does.
 */
function makeDfl() {
  const store = new Map();
  const calls = { set: [], zadd: [], eval: [], get: [] };
  return {
    store,
    calls,
    async write(fn) {
      const c = {
        async set(key, value, ...rest) {
          calls.set.push({ key, rest });
          const nx = rest.includes('NX');
          if (nx && store.has(key)) return null;
          store.set(key, value);
          return 'OK';
        },
        async get(key) {
          calls.get.push({ key });
          return store.has(key) ? store.get(key) : null;
        },
        async zadd(key, score, member) {
          calls.zadd.push({ key, score, member });
          return 1;
        },
        async eval(script, numKeys, ...args) {
          calls.eval.push({ numKeys, args });
          return 1;
        },
      };
      return fn(c);
    },
  };
}

/** Store keys are namespaced, so tests can ask about a specific kind. */
const eventSets = (dfl) => dfl.calls.set.filter((s) => s.key.startsWith('fw:evt:'));
const fpSets = (dfl) => dfl.calls.set.filter((s) => s.key.startsWith('fw:fp:'));

const fold = { get: () => '-- fake fold script' };
const log = { info() {}, warn() {}, debug() {}, error() {} };
const cfg = { eventTtl: 604800, alertDedupWindow: 300000 };

const silent = {
  id: 'evt-1',
  source: 'shop',
  type: 'check-001',
  severity: 'CRITICAL',
  ts: '2026-11-10T00:00:00Z',
  payload: {},
};

test('first delivery is stored, fingerprinted, indexed and folded', async () => {
  const dfl = makeDfl();
  const res = await persistEvent({
    dfl, evt: silent, storedPayload: {}, brokerTs: 1000, cfg, fold, log,
  });

  assert.equal(res.applied, 1);
  assert.equal(res.duplicate, undefined);
  assert.equal(res.contentDuplicate, undefined);
  assert.equal(eventSets(dfl).length, 1, 'the record is written');
  assert.equal(fpSets(dfl).length, 1, 'the content fingerprint is claimed');
  // Two indexes, written together: the per-source timeline and the global one that
  // backs the All Events tab. A missing global member would make an event invisible
  // to "everything in the last N hours" while still appearing under its source.
  assert.equal(dfl.calls.zadd.length, 2, 'both indexes are written');
  const idxKeys = dfl.calls.zadd.map((z) => z.key);
  assert.ok(idxKeys.includes('fw:idx:evt:all'), 'the global index is written');
  assert.ok(idxKeys.some((k) => k.startsWith('fw:idx:src:')), 'the source index is written');
  assert.equal(dfl.calls.eval.length, 1, 'the fold runs');
});

test('a re-sent id still writes one record, and folds once per arrival', async () => {
  const dfl = makeDfl();
  const args = { dfl, evt: silent, storedPayload: {}, brokerTs: 1000, cfg, fold, log };

  const first = await persistEvent(args);
  const zaddsAfterFirst = dfl.calls.zadd.length;
  // A second delivery of the same id, with a NEWER broker timestamp.
  const second = await persistEvent({ ...args, brokerTs: 2000 });

  assert.equal(first.applied, 1);
  assert.equal(second.contentDuplicate, true, 'recognised as the same condition');
  // The ARRIVAL is still stored — that is the reversal: "all events" must not omit
  // events. What dedup owns is the ROLLUP, not whether the delivery exists.
  assert.equal(eventSets(dfl).length, 2, 'every delivery is an event record');
  // Two more index writes (source + global) for the second arrival. Compared
  // against the state after the FIRST write rather than a hard-coded number, so
  // adding an index does not require editing this assertion.
  assert.equal(dfl.calls.zadd.length, zaddsAfterFirst * 2, 'the copy is indexed too');
  assert.equal(dfl.calls.eval.length, 2, 'the rollup still took the occurrence');
});

// ------------------------------------------------------- content dedup (§5.5)
// The same CONDITION reported again, arriving as a new message with a new id:
// the ALERT ROLLUP stays deduplicated (one alert, one count, last_id -> owner),
// but the ARRIVAL is stored and indexed under its own id. Dedup owns the rollup,
// not whether the delivery exists — "all events" that omits events is wrong.
test('a content duplicate is stored, and the rollup names the owner', async () => {
  const dfl = makeDfl();
  const args = { dfl, storedPayload: {}, brokerTs: 1000, cfg, fold, log };

  const first = await persistEvent({ ...args, evt: silent });
  const zaddsAfterFirst = dfl.calls.zadd.length;
  // Same source/type/severity/payload, DIFFERENT id and a later arrival time.
  const copy = await persistEvent({
    ...args, evt: { ...silent, id: 'evt-2' }, brokerTs: 2000,
  });

  assert.equal(first.applied, 1);
  assert.equal(copy.contentDuplicate, true, 'reported as a content duplicate');
  assert.equal(copy.ownerId, silent.id, 'the original record is named as owner');
  assert.equal(copy.applied, 1, 'the rollup still counts the occurrence');

  assert.equal(eventSets(dfl).length, 2, 'the arrival is stored under its own id');
  assert.equal(dfl.calls.zadd.length, zaddsAfterFirst * 2, 'the arrival is indexed in both timelines');
  assert.equal(dfl.calls.eval.length, 2, 'the fold runs for both arrivals');
});

test('the arrival reaches the record and the index under its own id', async () => {
  // What the All events tab depends on: after two identical arrivals there are TWO
  // stored events, so the tab can show both. It used to show one, which is the
  // "all events is not all events" report.
  const dfl = makeDfl();
  const args = { dfl, storedPayload: {}, brokerTs: 1000, cfg, fold, log };

  await persistEvent({ ...args, evt: silent });
  await persistEvent({ ...args, evt: { ...silent, id: 'copy-1' }, brokerTs: 2000 });

  const storedIds = [...dfl.store.keys()].filter((k) => k.startsWith('fw:evt:')).sort();
  assert.deepEqual(storedIds, [`fw:evt:${silent.id}`, 'fw:evt:copy-1'].sort());
  const idxMembers = dfl.calls.zadd
    .filter((z) => z.key === 'fw:idx:evt:all')
    .map((z) => z.member);
  assert.ok(idxMembers.includes('copy-1'), 'the arrival is in the global timeline');
});

test('a different payload is a different condition and gets its own record', async () => {
  const dfl = makeDfl();
  const args = { dfl, storedPayload: {}, brokerTs: 1000, cfg, fold, log };

  await persistEvent({ ...args, evt: silent });
  const other = await persistEvent({
    ...args,
    evt: { ...silent, id: 'evt-2', payload: { msg: 'a different message' } },
    brokerTs: 1005,
  });

  assert.equal(other.contentDuplicate, undefined, 'different content is not a duplicate');
  assert.equal(eventSets(dfl).length, 2, 'both records are stored');
});

test('SET is issued with NX so the checks are atomic', async () => {
  const dfl = makeDfl();
  await persistEvent({ dfl, evt: silent, storedPayload: {}, brokerTs: 1000, cfg, fold, log });

  for (const call of dfl.calls.set) {
    assert.ok(call.rest.includes('NX'), `NX is what makes ${call.key} a compare-and-set`);
    assert.ok(call.rest.includes('EX'));
    assert.equal(Number(call.rest[call.rest.indexOf('EX') + 1]), cfg.eventTtl);
  }
});

test('an id that differs but content that matches stores every arrival', async () => {
  // This is the case that used to inflate the board with one ROW per delivery; now
  // the board shows one ALERT (deduped rollup) while storage keeps one RECORD per
  // delivery, so All events and the live tail agree.
  const dfl = makeDfl();
  const args = { dfl, storedPayload: {}, brokerTs: 1000, cfg, fold, log };

  let last;
  for (let i = 0; i < 5; i += 1) {
    last = await persistEvent({ ...args, evt: { ...silent, id: `evt-${i}` }, brokerTs: 1000 + i });
  }

  assert.equal(eventSets(dfl).length, 5, 'five deliveries, five records');
  assert.equal(dfl.calls.eval.length, 5, 'and the count moved five times');
  assert.equal(last.contentDuplicate, true);
});

test('a record that already exists absorbs the repeat when the fingerprint is absent', async () => {
  // THE BUG THIS PINS.
  //
  // State: a stored record for this event id, but NO fingerprint (it expired, it
  // was never written because the record predates content dedup, or a partial
  // reset cleared it). The old code claimed the fingerprint, then hit the record's
  // NX, and concluded "transport duplicate" — so it was dropped silently: no fold,
  // no count, no last_seen, and the alert could never recover.
  //
  // It must instead be treated as a content duplicate: the existing record owns
  // this condition, so it absorbs the arrival.
  const dfl = makeDfl();
  // Pre-seed only the record, mimicking a fingerprint-less event.
  dfl.store.set(`fw:evt:${silent.id}`, '{"id":"evt-1"}');

  const res = await persistEvent({
    dfl, evt: silent, storedPayload: {}, brokerTs: 5000, cfg, fold, log,
  });

  assert.equal(res.contentDuplicate, true, 'treated as a content duplicate');
  assert.equal(res.duplicate, undefined, 'NOT silently dropped as transport noise');
  assert.equal(res.applied, 1, 'the fold ran, so count and last_seen move');
  assert.equal(dfl.calls.eval.length, 1, 'the rollup was applied');
  // The NX attempt still happens — that is how the existing record is detected —
  // so assert on the store, not the call log: the original bytes must be intact.
  assert.equal(
    dfl.store.get(`fw:evt:${silent.id}`),
    '{"id":"evt-1"}',
    'the original record is not rewritten',
  );
});

test('absorbing a fingerprint-less repeat repairs the fingerprint', async () => {
  // Otherwise every subsequent delivery falls through the slow path forever.
  const dfl = makeDfl();
  dfl.store.set(`fw:evt:${silent.id}`, '{"id":"evt-1"}');

  await persistEvent({ dfl, evt: silent, storedPayload: {}, brokerTs: 5000, cfg, fold, log });

  const fpKeys = [...dfl.store.keys()].filter((k) => k.startsWith('fw:fp:'));
  assert.equal(fpKeys.length, 1, 'the fingerprint exists again');
  assert.equal(dfl.store.get(fpKeys[0]), silent.id, 'and points at the record');
});

// --------------------------- the fold must be told about a record that EXISTS
test('an absorb records the OWNER id in the fold, not the discarded copy', async () => {
  // Regression: `last_id` was set to the copy's id, but the copy is never stored.
  // The alert then pointed at a record that does not exist, so anything resolving
  // a summary from it rendered an empty cell (the blank Summary column and the
  // "Live events: Nothing yet" report share this cause).
  const dfl = makeDfl();
  const args = { dfl, storedPayload: {}, brokerTs: 1000, cfg, fold, log };

  await persistEvent({ ...args, evt: silent });
  await persistEvent({ ...args, evt: { ...silent, id: 'evt-2' }, brokerTs: 2000 });

  // `eval` is called as (script, numKeys, ...keys, ...argv), so `args` is
  // [4 keys, brokerTs, id, severity, window, bareKey] — the id is index 5.
  const last = dfl.calls.eval[dfl.calls.eval.length - 1];
  const idGiven = last.args[5];
  assert.equal(idGiven, silent.id, 'the owner id, which has a stored record');
  assert.notEqual(idGiven, 'evt-2', 'not the id of the copy that was discarded');

  // And that id really does resolve, i.e. it matches the stored record's id.
  const stored = eventSets(dfl).map((s) => JSON.parse(dfl.store.get(s.key)));
  const ids = stored.map((e) => e.id);
  assert.ok(ids.includes(idGiven), `fold id ${idGiven} must exist in the store`);
});

test('the fold id on the fingerprint-absent path also names a real record', async () => {
  // The other absorb branch: record present, fingerprint missing.
  const dfl = makeDfl();
  const args = { dfl, storedPayload: {}, brokerTs: 1000, cfg, fold, log };

  await persistEvent({ ...args, evt: silent });
  // Remove just the fingerprint, simulating an expired/never-written one.
  for (const k of [...dfl.store.keys()]) if (k.startsWith('fw:fp:')) dfl.store.delete(k);

  await persistEvent({ ...args, evt: { ...silent, id: 'evt-3' }, brokerTs: 3000 });

  const last = dfl.calls.eval[dfl.calls.eval.length - 1];
  const idGiven = last.args[5];
  const ids = eventSets(dfl).map((s) => JSON.parse(dfl.store.get(s.key)).id);
  assert.ok(ids.includes(idGiven), `fold id ${idGiven} must exist in the store`);
});

// ------------------------------- the fold is told about the closed index too
test('the fold receives the closed index as its fourth key', async () => {
  // Without it the Closed tab has nothing to enumerate: retiring an alert removes
  // it from the open index, so the only alternative is an unbounded SCAN.
  const dfl = makeDfl();
  await persistEvent({
    dfl, evt: silent, storedPayload: {}, brokerTs: 1000, cfg, fold, log,
  });
  const last = dfl.calls.eval[dfl.calls.eval.length - 1];
  assert.equal(last.numKeys, 4, 'numKeys must match the 4 keys passed');
  assert.equal(last.args[0], 'fw:alert:' + `${silent.source}:${silent.type}`);
  assert.equal(last.args[3], 'fw:idx:alert:closed', 'fourth key is the closed index');
});

// ------------------------------------------------------------ index reconcile
// These pin the repair for "All events is empty while the live tail has rows":
// the global index was missing records that storage held, and nothing in the read
// path can detect that.

test('missingFromGlobal reports only ids the global index lacks', () => {
  assert.deepEqual(
    missingFromGlobal(['a', 'b', 'c'], new Set(['a', 'c'])),
    ['b'],
  );
  // Complete index: nothing to do, which is the normal case after one run.
  assert.deepEqual(missingFromGlobal(['a', 'b'], new Set(['a', 'b'])), []);
  // Empty index: everything is missing.
  assert.deepEqual(missingFromGlobal(['a', 'b'], []), ['a', 'b']);
});

test('missingFromGlobal ignores blanks, duplicates and non-strings', () => {
  // These come from a zset read, so a blank or a null would be a corrupt member
  // rather than a real id. Writing one into the index creates an entry that can
  // never resolve to a record.
  assert.deepEqual(
    missingFromGlobal(['a', '', null, undefined, 'a', 'b'], new Set()),
    ['a', 'b'],
  );
  assert.deepEqual(missingFromGlobal(null, new Set()), []);
  assert.deepEqual(missingFromGlobal([], new Set()), []);
});

test('missingFromGlobal accepts an array for the global set', () => {
  // The caller may hand back an array from a pipeline rather than a Set.
  assert.deepEqual(missingFromGlobal(['a', 'b', 'c'], ['a', 'b']), ['c']);
});

/** A client stub good enough for the reconcile: an in-memory key/value store. */
function makeReconcileDfl(initial = {}) {
  const store = new Map(Object.entries(initial));
  const indexes = new Map();   // key -> Map(member -> score)
  const dfl = {
    store,
    indexes,
    async write(fn) {
      const c = {
        scan: async (_cursor, _m, pattern) => {
          // One page, everything that matches the prefix.
          const prefix = pattern.replace('*', '');
          const keys = [...indexes.keys()].filter((k) => k.startsWith(prefix));
          return ['0', keys];
        },
        zrange: async (key) => [...(indexes.get(key)?.keys() ?? [])],
        mget: async (keys) => keys.map((k) => store.get(k) ?? null),
        zadd: (key, score, member) => {
          if (!indexes.has(key)) indexes.set(key, new Map());
          indexes.get(key).set(member, Number(score));
          return 1;
        },
      };
      return fn(c);
    },
  };
  return dfl;
}

test('reconcile adds unindexed records and scores them by brokerTs', async () => {
  const dfl = makeReconcileDfl({
    'fw:evt:a': JSON.stringify({ id: 'a', brokerTs: 1000 }),
    'fw:evt:b': JSON.stringify({ id: 'b', brokerTs: 2000 }),
  });
  dfl.indexes.set('fw:idx:src:shop', new Map([['a', 1000], ['b', 2000]]));
  dfl.indexes.set('fw:idx:evt:all', new Map([['a', 1000]]));

  const res = await reconcileEventIndex(dfl, {}, { info() {}, warn() {} });
  assert.equal(res.missing, 1);
  assert.equal(res.added, 1);
  // Scored by the RECORD's brokerTs, not by "now": scoring now would put a week-old
  // event at the top of the All events list.
  assert.equal(dfl.indexes.get('fw:idx:evt:all').get('b'), 2000);
  assert.equal(dfl.indexes.get('fw:idx:evt:all').get('a'), 1000, 'existing member untouched');
});

test('reconcile does nothing when the index is already complete', async () => {
  const dfl = makeReconcileDfl({
    'fw:evt:a': JSON.stringify({ id: 'a', brokerTs: 1000 }),
  });
  dfl.indexes.set('fw:idx:src:shop', new Map([['a', 1000]]));
  dfl.indexes.set('fw:idx:evt:all', new Map([['a', 1000]]));

  const res = await reconcileEventIndex(dfl, {}, { info() {}, warn() {} });
  assert.equal(res.missing, 0);
  assert.equal(res.added, 0);
});

test('reconcile skips a missing or timestamp-less record instead of inventing a score', async () => {
  const dfl = makeReconcileDfl({
    // 'gone' is in the index but its record has expired
    'fw:evt:nots': JSON.stringify({ id: 'nots' }),
    'fw:evt:bad': 'not json',
  });
  dfl.indexes.set('fw:idx:src:shop', new Map([['gone', 1], ['nots', 2], ['bad', 3]]));
  dfl.indexes.set('fw:idx:evt:all', new Map());

  const res = await reconcileEventIndex(dfl, {}, { info() {}, warn() {} });
  assert.equal(res.added, 0);
  assert.equal(res.skipped, 3, 'all three are unplaceable on a timeline');
});

test('reconcile is capped and reports that it was capped', async () => {
  // A pathological history must not stall startup. A partial reconcile is progress:
  // the next restart compares the same two sets again.
  const initial = {};
  const src = new Map();
  for (let i = 0; i < 10; i += 1) {
    initial[`fw:evt:e${i}`] = JSON.stringify({ id: `e${i}`, brokerTs: 1000 + i });
    src.set(`e${i}`, 1000 + i);
  }
  const dfl = makeReconcileDfl(initial);
  dfl.indexes.set('fw:idx:src:shop', src);
  dfl.indexes.set('fw:idx:evt:all', new Map());

  const res = await reconcileEventIndex(dfl, {}, { info() {}, warn() {} }, { maxToAdd: 3 });
  assert.equal(res.added, 3);
  assert.equal(res.capped, true);
});
