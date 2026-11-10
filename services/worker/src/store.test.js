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
import { persistEvent } from './store.js';

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
  assert.equal(dfl.calls.zadd.length, 1, 'the event is indexed');
  assert.equal(dfl.calls.eval.length, 1, 'the fold runs');
});

test('a re-sent id is dropped before it can create a second record', async () => {
  const dfl = makeDfl();
  const args = { dfl, evt: silent, storedPayload: {}, brokerTs: 1000, cfg, fold, log };

  const first = await persistEvent(args);
  // A second delivery of the same id, with a NEWER broker timestamp — which is
  // what a re-send looks like and why the timestamp watermark alone missed it.
  //
  // It is caught by the CONTENT gate, not the id gate: identical content has
  // identical fingerprints, so the content check fires first. That is correct —
  // from the system's point of view it is the same condition, and the outcome is
  // the one that matters: no second record.
  const second = await persistEvent({ ...args, brokerTs: 2000 });

  assert.equal(first.applied, 1);
  assert.equal(second.contentDuplicate, true, 'caught as a content duplicate');
  assert.equal(eventSets(dfl).length, 1, 'no second record is written');
  assert.equal(dfl.calls.zadd.length, 1, 'the index is not touched again');
  assert.equal(dfl.calls.eval.length, 2, 'the rollup still took the occurrence');
});

// ------------------------------------------------------- content dedup (§5.5)
// The requested behaviour: the same CONDITION reported again, arriving as a new
// message with a new id, must NOT create a second record. The original record
// stays, its count is incremented and its Last occurrence moves, and the new copy
// is discarded.
test('a content duplicate is absorbed by the original record', async () => {
  const dfl = makeDfl();
  const args = { dfl, storedPayload: {}, brokerTs: 1000, cfg, fold, log };

  const first = await persistEvent({ ...args, evt: silent });
  // Same source/type/severity/payload, DIFFERENT id and a later arrival time.
  const copy = await persistEvent({
    ...args, evt: { ...silent, id: 'evt-2' }, brokerTs: 2000,
  });

  assert.equal(first.applied, 1);
  assert.equal(copy.contentDuplicate, true, 'reported as a content duplicate');
  assert.equal(copy.ownerId, silent.id, 'the original record is named as owner');
  assert.equal(copy.applied, 1, 'the rollup still counts the occurrence');

  assert.equal(eventSets(dfl).length, 1, 'the copy is never stored');
  assert.equal(dfl.calls.zadd.length, 1, 'the copy is not indexed');
  assert.equal(dfl.calls.eval.length, 2, 'the fold runs for both arrivals');
});

test('the copy never reaches the record or the index', async () => {
  // The strongest statement of the requirement: after two identical arrivals
  // there is exactly ONE stored event, and it is the original id.
  const dfl = makeDfl();
  const args = { dfl, storedPayload: {}, brokerTs: 1000, cfg, fold, log };

  await persistEvent({ ...args, evt: silent });
  await persistEvent({ ...args, evt: { ...silent, id: 'copy-1' }, brokerTs: 2000 });

  const storedIds = [...dfl.store.keys()].filter((k) => k.startsWith('fw:evt:'));
  assert.deepEqual(storedIds, [`fw:evt:${silent.id}`]);
  assert.equal(dfl.store.has(`fw:evt:copy-1`), false);
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

test('an id that differs but content that matches never writes a second record', async () => {
  // This is the case that used to inflate the board: a producer re-reporting a
  // condition with a fresh id on every delivery.
  const dfl = makeDfl();
  const args = { dfl, storedPayload: {}, brokerTs: 1000, cfg, fold, log };

  let last;
  for (let i = 0; i < 5; i += 1) {
    last = await persistEvent({ ...args, evt: { ...silent, id: `evt-${i}` }, brokerTs: 1000 + i });
  }

  assert.equal(eventSets(dfl).length, 1, 'five deliveries, one record');
  assert.equal(dfl.calls.eval.length, 5, 'but the count moved five times');
  assert.equal(last.contentDuplicate, true);
});
