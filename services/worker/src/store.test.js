/**
 * Tests for the worker's write path, with Valkey stubbed.
 *
 * WHAT THIS PINS
 *
 * Valkey replaced a two-key model (`fw:evt:{id}` per delivery + `fw:alert:{source}:
 * {type}` rollup) with ONE hash per condition at `event:<32hex>`. The consequences
 * worth testing:
 *
 *   1. two deliveries of the same CONDITION hit the SAME key, so the second cannot
 *      create a record;
 *   2. a redelivery is rejected by the fold's (ts, id) watermark, not by a
 *      fingerprint set — the set is gone because the key IS the fingerprint;
 *   3. the record, both timelines and the state rollup are ONE `EVAL`, so no
 *      interleaved writer can leave them disagreeing.
 *
 *   node --test src/
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { persistEvent, missingFromGlobal, reconcileEventIndex } from './store.js';
import { eventKeyFor, bareKeyFor, isValidEventKey, summaryOf, deviceOf } from './keygen.js';

/**
 * Minimal Valkey stub. `eval` records the call and applies the one rule the tests
 * care about: the watermark guard. A second call with an older-or-equal (ts, id)
 * pair returns 0, exactly like the real script.
 */
function makeValkey() {
  const store = new Map();     // key -> Map(field -> value)
  const calls = { eval: [], zadd: [], zremrangebyscore: [], scan: [] };

  const hashGet = (key, field) => store.get(key)?.get(field);
  const hashSet = (key, obj) => {
    if (!store.has(key)) store.set(key, new Map());
    const m = store.get(key);
    for (const [f, v] of Object.entries(obj)) m.set(f, String(v));
  };

  return {
    store,
    calls,
    async write(fn) {
      const c = {
        async eval(script, numKeys, ...args) {
          calls.eval.push({ numKeys, args });
          const keys = args.slice(0, numKeys);
          const argv = args.slice(numKeys);
          const recordKey = keys[0];
          const ts = Number(argv[0]);
          const arrivalId = String(argv[1]);
          const sev = String(argv[2]);

          const wmTs = Number(hashGet(recordKey, 'watermark') ?? 0);
          const wmId = String(hashGet(recordKey, 'watermarkId') ?? '');
          if (ts < wmTs || (ts === wmTs && arrivalId <= wmId)) return 0;

          const existed = store.has(recordKey);
          const state = String(hashGet(recordKey, 'state') ?? '');

          // The record's own fields are applied BEFORE the branch, exactly as the
          // real script does: a retirement can arrive for a condition with no
          // stored record yet, and it must still store `msg`, `source` and
          // `payload` or the Closed tab renders blanks.
          const fields = {};
          for (let i = 6; i + 1 < argv.length; i += 2) fields[argv[i]] = argv[i + 1];
          if (Object.keys(fields).length > 0) hashSet(recordKey, fields);

          if (sev === 'CLOSE' || sev === 'HARMLESS') {
            hashSet(recordKey, {
              state: 'closed',
              severity: sev,
              closedBy: arrivalId,
              closedAt: ts,
              lastSeen: ts,
              watermark: ts,
              watermarkId: arrivalId,
            });
            store.get(recordKey).delete('ackedBy');
            store.get(recordKey).delete('ackedAt');
            return 3;
          }

          let count = Number(hashGet(recordKey, 'count') ?? 0);
          const episodeStarted = !existed || state === 'closed' || state === '';
          count = episodeStarted ? 1 : count + 1;

          const patch = {
            state: 'open',
            severity: sev,
            count,
            lastSeen: ts,
            lastId: arrivalId,
            watermark: ts,
            watermarkId: arrivalId,
          };
          if (episodeStarted) patch.firstSeen = ts;
          hashSet(recordKey, patch);
          if (episodeStarted) {
            store.get(recordKey).delete('ackedBy');
            store.get(recordKey).delete('ackedAt');
            store.get(recordKey).delete('closedBy');
            store.get(recordKey).delete('closedAt');
          }
          return 1;
        },
        async zadd(key, score, member) {
          calls.zadd.push({ key, score, member });
          return 1;
        },
        async zremrangebyscore(key, min, max) {
          calls.zremrangebyscore.push({ key, min, max });
          return 0;
        },
        async hmget(key, ...fields) {
          const h = store.get(key);
          return fields.map((f) => (h && h.has(f) ? h.get(f) : null));
        },
        async scan() {
          return ['0', []];
        },
        async zrange(key) {
          return [...store.keys()].filter((k) => k === key);
        },
        pipeline() {
          const queued = [];
          const p = {
            hmget(key, ...fields) { queued.push([key, fields]); return p; },
            async exec() { return queued.map(([k, f]) => [null, this._read(k, f)]); },
            _read(k, f) {
              const h = store.get(k);
              return f.map((x) => (h && h.has(x) ? h.get(x) : null));
            },
          };
          return p;
        },
      };
      return fn(c);
    },
  };
}

const fold = { get: () => '-- fake fold script' };
const log = { info() {}, warn() {}, debug() {}, error() {} };
const cfg = { eventTtl: 604800 };

const silent = {
  id: 'evt-1',
  source: 'shop',
  type: 'check-001',
  severity: 'CRITICAL',
  ts: '2026-11-10T00:00:00Z',
  payload: { msg: 'disk almost full' },
};

const storedPayload = (evt) => evt.payload ?? {};

// ---------------------------------------------------------------- key generation
test('the key is derived from device + summary, not from the event id', () => {
  const a = eventKeyFor(silent);
  const b = eventKeyFor({ ...silent, id: 'a-completely-different-id' });
  assert.equal(a, b, 'a new message id must not change the key');
  assert.match(a, /^event:[0-9a-f]{32}$/);
});

test('whitespace differences in the summary collapse to one key', () => {
  // An upstream tool that pads or re-wraps its summary would otherwise mint a new
  // key per report — the duplicate storm this key exists to prevent.
  const a = eventKeyFor(silent);
  const b = eventKeyFor({ ...silent, payload: { msg: '  disk   almost  full  ' } });
  assert.equal(a, b);
});

test('a different summary or device is a different condition', () => {
  const base = eventKeyFor(silent);
  assert.notEqual(eventKeyFor({ ...silent, payload: { msg: 'disk full' } }), base);
  assert.notEqual(eventKeyFor({ ...silent, source: 'other' }), base);
});

test('a different severity is the SAME condition', () => {
  // Deliberate, and the opposite of the old fingerprint. Severity is a property of
  // the report, not of the condition: escalating WARNING -> CRITICAL must update the
  // standing row rather than open a second one beside it.
  assert.equal(eventKeyFor({ ...silent, severity: 'WARNING' }), eventKeyFor(silent));
});

test('summaryOf and deviceOf read the documented fields with fallbacks', () => {
  assert.equal(summaryOf({ payload: { msg: 'x' } }), 'x');
  assert.equal(summaryOf({ payload: { EVT_Summary: 'y' } }), 'y');
  assert.equal(deviceOf({ source: 'd1' }), 'd1');
  assert.equal(deviceOf({ payload: { EVT_Device: 'd2' } }), 'd2');
  // No summary anywhere must not silently collapse the whole fleet onto one key.
  assert.notEqual(eventKeyFor({ source: 'a', payload: {} }), eventKeyFor({ source: 'b', payload: {} }));
});

test('isValidEventKey accepts only the generated shape', () => {
  assert.equal(isValidEventKey(eventKeyFor(silent)), true);
  // Legacy `source:type` keys are rejected rather than tolerated: a stale browser
  // would otherwise write an ack onto a key no fold ever created.
  assert.equal(isValidEventKey('shop:check-001'), false);
  assert.equal(isValidEventKey('event:not-hex'), false);
  assert.equal(isValidEventKey('event:'), false);
  assert.equal(isValidEventKey(null), false);
});

// --------------------------------------------------------------------- write path
test('the first delivery writes one record, both timelines and one fold call', async () => {
  const valkey = makeValkey();
  const res = await persistEvent({
    valkey, evt: silent, storedPayload: storedPayload(silent), brokerTs: 1000, cfg, fold, log,
  });

  assert.equal(res.applied, 1);
  assert.equal(valkey.calls.eval.length, 1, 'record + indexes + rollup are ONE call');
  assert.equal(valkey.calls.eval[0].numKeys, 5, 'five keys: record, timeline, open, closed, source');

  const bare = bareKeyFor(silent);
  const h = valkey.store.get(`event:${bare}`);
  assert.ok(h, 'the record exists at the content key');
  assert.equal(h.get('count'), '1');
  assert.equal(h.get('state'), 'open');
  assert.equal(h.get('msg'), 'disk almost full', 'the summary is stored as a field');
  assert.equal(h.get('source'), 'shop');
});

test('a repeat of the same condition updates the SAME record and creates no second one', async () => {
  const valkey = makeValkey();
  const args = { valkey, storedPayload: storedPayload(silent), cfg, fold, log };

  await persistEvent({ ...args, evt: silent, brokerTs: 1000 });
  const second = await persistEvent({ ...args, evt: { ...silent, id: 'evt-2' }, brokerTs: 2000 });

  assert.equal(second.applied, 1, 'an occurrence, not a duplicate');
  assert.equal(valkey.store.size, 1, 'exactly one record — the duplicate was folded in');

  const h = valkey.store.get(`event:${bareKeyFor(silent)}`);
  assert.equal(h.get('count'), '2', 'the count moved');
  assert.equal(h.get('lastSeen'), '2000', 'last occurrence moved');
  assert.equal(h.get('firstSeen'), '1000', 'first occurrence did NOT move');
  assert.equal(h.get('lastId'), 'evt-2', 'the newest arrival id is recorded');
});

test('a redelivery of the same message is dropped without touching anything', async () => {
  const valkey = makeValkey();
  const args = { valkey, storedPayload: storedPayload(silent), cfg, fold, log };

  await persistEvent({ ...args, evt: silent, brokerTs: 1000 });
  const again = await persistEvent({ ...args, evt: silent, brokerTs: 1000 });

  assert.equal(again.applied, 0, 'the (ts, id) watermark rejected it');
  const h = valkey.store.get(`event:${bareKeyFor(silent)}`);
  assert.equal(h.get('count'), '1', 'count unchanged');
  assert.equal(h.get('lastSeen'), '1000', 'last occurrence unchanged');
});

test('a retirement closes the record and keeps it addressable', async () => {
  const valkey = makeValkey();
  const args = { valkey, storedPayload: {}, cfg, fold, log };

  await persistEvent({ ...args, evt: silent, brokerTs: 1000 });
  const closed = await persistEvent({
    ...args,
    evt: { ...silent, id: 'close-1', severity: 'CLOSE', payload: { msg: 'disk almost full' } },
    brokerTs: 2000,
  });

  assert.equal(closed.applied, 3);
  const h = valkey.store.get(`event:${bareKeyFor(silent)}`);
  assert.equal(h.get('state'), 'closed');
  assert.equal(h.get('closedBy'), 'close-1');
  assert.equal(h.get('closedAt'), '2000');
});

test('a CLOSE with no stored record still stores the row, not a blank shell', async () => {
  // An occurrence and its CLEAR are two messages. A redelivery, a replay or a
  // restart can deliver the CLEAR first — and if the fields were only written on
  // the occurrence path, the record would be `state=closed` with no `msg`, no
  // `source` and no `payload`: a row that is blank in every column and unfindable
  // in the Closed tab.
  const valkey = makeValkey();
  const res = await persistEvent({
    valkey,
    evt: { ...silent, id: 'lonely-close', severity: 'CLOSE' },
    storedPayload: storedPayload(silent),
    brokerTs: 1000, cfg, fold, log,
  });

  assert.equal(res.applied, 3);
  const h = valkey.store.get(`event:${bareKeyFor(silent)}`);
  assert.equal(h.get('state'), 'closed');
  assert.equal(h.get('msg'), 'disk almost full', 'the summary is stored');
  assert.equal(h.get('source'), 'shop', 'the device is stored');
  assert.ok(h.get('payload'), 'the payload is stored');
});

test('a re-fire after retirement starts a new episode with count 1', async () => {
  const valkey = makeValkey();
  const args = { valkey, storedPayload: storedPayload(silent), cfg, fold, log };
  const key = `event:${bareKeyFor(silent)}`;

  await persistEvent({ ...args, evt: silent, brokerTs: 1000 });
  await persistEvent({ ...args, evt: { ...silent, id: 'e2' }, brokerTs: 2000 });
  assert.equal(valkey.store.get(key).get('count'), '2');

  await persistEvent({
    ...args, evt: { ...silent, id: 'c1', severity: 'CLOSE' }, brokerTs: 3000,
  });
  await persistEvent({ ...args, evt: { ...silent, id: 'e3' }, brokerTs: 4000 });

  const h = valkey.store.get(key);
  assert.equal(h.get('count'), '1', 'a new episode counts from 1, not from the old total');
  assert.equal(h.get('firstSeen'), '4000', 'first occurrence is the new episode');
  assert.equal(h.get('state'), 'open');
});

test('an ack is cleared when the episode ends and kept while it continues', async () => {
  const valkey = makeValkey();
  const args = { valkey, storedPayload: storedPayload(silent), cfg, fold, log };
  const key = `event:${bareKeyFor(silent)}`;

  await persistEvent({ ...args, evt: silent, brokerTs: 1000 });
  valkey.store.get(key).set('ackedBy', 'alice');
  valkey.store.get(key).set('ackedAt', '1500');

  // Still firing: the ack belongs to this episode and must survive.
  await persistEvent({ ...args, evt: { ...silent, id: 'e2' }, brokerTs: 2000 });
  assert.equal(valkey.store.get(key).get('ackedBy'), 'alice', 'an ongoing episode keeps its ack');

  // Retired: the ack goes with the episode.
  await persistEvent({ ...args, evt: { ...silent, id: 'c1', severity: 'CLOSE' }, brokerTs: 3000 });
  assert.equal(valkey.store.get(key).has('ackedBy'), false, 'retirement clears the ack');
});

test('an older event cannot move the record backwards', async () => {
  // The scenario that matters: a redelivered CLOSE from before a newer CRITICAL must
  // not retire the active alarm.
  const valkey = makeValkey();
  const args = { valkey, storedPayload: storedPayload(silent), cfg, fold, log };

  await persistEvent({ ...args, evt: silent, brokerTs: 2000 });
  const stale = await persistEvent({
    ...args, evt: { ...silent, id: 'old-close', severity: 'CLOSE' }, brokerTs: 1500,
  });

  assert.equal(stale.applied, 0, 'stale CLOSE dropped');
  assert.equal(valkey.store.get(`event:${bareKeyFor(silent)}`).get('state'), 'open');
});

test('a same-millisecond CLOSE with a higher id wins, deterministically', async () => {
  const valkey = makeValkey();
  const args = { valkey, storedPayload: {}, cfg, fold, log };

  await persistEvent({ ...args, evt: { ...silent, id: 'aaa' }, brokerTs: 5000 });
  const close = await persistEvent({
    ...args, evt: { ...silent, id: 'zzz', severity: 'CLOSE' }, brokerTs: 5000,
  });
  assert.equal(close.applied, 3, 'the id breaks the tie, so every worker agrees');
});

// -------------------------------------------------------------- index reconcile
test('missingFromGlobal reports only keys absent from the global set', () => {
  assert.deepEqual(missingFromGlobal(['a', 'b', 'c'], new Set(['b'])), ['a', 'c']);
  assert.deepEqual(missingFromGlobal([], new Set()), []);
  assert.deepEqual(missingFromGlobal(['a', 'a'], new Set()), ['a'], 'duplicates collapse');
});

test('reconcile is a no-op when there are no source indexes', async () => {
  const valkey = makeValkey();
  const res = await reconcileEventIndex(valkey, cfg, log);
  assert.deepEqual(res, { scanned: 0, missing: 0, added: 0, skipped: 0, capped: false });
});
