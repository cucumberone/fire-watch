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
  const calls = { set: [], zadd: [], eval: [] };
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

test('first delivery is stored, indexed and folded', async () => {
  const dfl = makeDfl();
  const res = await persistEvent({
    dfl, evt: silent, storedPayload: {}, brokerTs: 1000, cfg, fold, log,
  });

  assert.equal(res.applied, 1);
  assert.equal(res.duplicate, undefined);
  assert.equal(dfl.calls.set.length, 1);
  assert.equal(dfl.calls.zadd.length, 1, 'the event is indexed');
  assert.equal(dfl.calls.eval.length, 1, 'the fold runs');
});

test('a re-sent id is dropped before the fold', async () => {
  const dfl = makeDfl();
  const args = { dfl, evt: silent, storedPayload: {}, brokerTs: 1000, cfg, fold, log };

  const first = await persistEvent(args);
  // A second delivery of the same id, with a NEWER broker timestamp — which is
  // what a re-send looks like and why the timestamp watermark alone missed it.
  const second = await persistEvent({ ...args, brokerTs: 2000 });

  assert.equal(first.applied, 1);
  assert.equal(second.duplicate, true, 'the duplicate is reported');
  assert.equal(second.applied, 0, 'and contributes no occurrence');
  assert.equal(dfl.calls.eval.length, 1, 'the fold must not run for a duplicate');
  assert.equal(dfl.calls.zadd.length, 1, 'the index must not be touched again');
});

test('SET is issued with NX so the check is atomic', async () => {
  const dfl = makeDfl();
  await persistEvent({ dfl, evt: silent, storedPayload: {}, brokerTs: 1000, cfg, fold, log });

  const { rest } = dfl.calls.set[0];
  assert.ok(rest.includes('NX'), 'NX is what makes this a compare-and-set');
  // Order matters for SET: EX <ttl> NX.
  assert.ok(rest.includes('EX'));
  assert.equal(Number(rest[rest.indexOf('EX') + 1]), cfg.eventTtl);
});

test('a distinct id on the same alert key is counted', async () => {
  const dfl = makeDfl();
  const args = { dfl, storedPayload: {}, brokerTs: 1000, cfg, fold, log };

  await persistEvent({ ...args, evt: silent });
  const res = await persistEvent({ ...args, evt: { ...silent, id: 'evt-2' }, brokerTs: 1005 });

  assert.equal(res.duplicate, undefined, 'a new id is not a duplicate');
  assert.equal(res.applied, 1);
  assert.equal(dfl.calls.eval.length, 2, 'both events reach the fold');
});
