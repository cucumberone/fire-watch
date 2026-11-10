/**
 * Tests for the gateway's READER against a fake ioredis client.
 *
 * These exist because `readClosedAlerts` shipped broken in a way nothing else
 * caught: it called `c.pipeline([...])` with an array, which ioredis does not
 * accept, and the resulting `TypeError` was reported to the browser as the
 * generic `closed_failed`. The Closed tab simply said "nothing has closed",
 * which is indistinguishable from an empty week — no test, no log, no
 * visible symptom pointing at the real cause.
 *
 * So the fake below implements the API exactly as ioredis does, including
 * `pipeline()` taking NO arguments. A call that passes the wrong shape now fails
 * here instead of in production.
 *
 *   node --test src/
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { DragonflyReader } from './dragonfly.js';

const log = { info() {}, warn() {}, error() {}, debug() {} };
const cfg = { sentinels: [{ host: 'x', port: 1 }], masterName: 'm',
              dragonflyPassword: 'p', alertTtlMs: 1000 };

/**
 * Minimal ioredis stand-in.
 *
 * `evalsha` returns the flat SNAPSHOT_FIELDS array for each key it is given, so
 * the hash decode path is exercised rather than bypassed.
 */
function fakeClient({ closedKeys = [], hashes = {}, events = {}, zscores = {} } = {}) {
  const calls = { pipeline: 0, hmget: [] };
  const client = {
    calls,
    async zrevrangebyscore(_key, _max, _min, _limit, _offset, _count) {
      return closedKeys;
    },
    async evalsha(_sha, numKeys, ...keys) {
      const out = [];
      for (let i = 0; i < numKeys; i += 1) {
        const h = hashes[keys[i]] || {};
        // severity, first_seen, last_seen, count, last_id, acked_by, acked_at
        out.push(
          h.severity ?? 'CLOSE',
          String(h.first_seen ?? 0),
          String(h.last_seen ?? 0),
          String(h.count ?? 1),
          h.last_id ?? '',
          '',
          '',
        );
      }
      return out;
    },
    async script(_sub, _src) { return 'sha'; },
    async mget(keys) { return keys.map((k) => events[k] ?? null); },
    async zscore(_key, member) { return zscores[member] ?? null; },
    async get(key) { return events[key] ?? null; },
    // ioredis: pipeline() takes NO arguments and returns a queueing builder.
    pipeline(...args) {
      calls.pipeline += 1;
      if (args.length > 0) {
        throw new TypeError("Cannot read properties of undefined (reading 'apply')");
      }
      const queued = [];
      const builder = {
        hmget(key, ...fields) { calls.hmget.push({ key, fields }); queued.push({ key, fields }); return builder; },
        async exec() {
          return queued.map(({ key, fields }) => {
            const h = hashes[key] || {};
            return [null, fields.map((f) => (h[f] === undefined ? null : String(h[f])))];
          });
        },
      };
      return builder;
    },
  };
  return client;
}

/** Reader wired to a fake client, bypassing Sentinel resolution. */
function readerWith(client) {
  const r = new DragonflyReader(cfg, log);
  r.connect = async () => client;
  r._snapshotSha = async () => 'sha';
  return r;
}

test('readClosedAlerts returns rows with closure metadata attached', async () => {
  const key = 'probe-device:static.probe';
  const client = fakeClient({
    closedKeys: [key],
    hashes: {
      [`fw:alert:${key}`]: {
        severity: 'CLOSE', first_seen: 100, last_seen: 200, count: 3,
        last_id: 'evt-1', state: 'closed', closed_at: 300, closed_by: 'evt-1',
      },
    },
    events: { 'fw:evt:evt-1': JSON.stringify({ id: 'evt-1', source: 'probe-device', type: 'static.probe' }) },
  });

  const rows = await readerWith(client).readClosedAlerts({ sinceMs: 0, limit: 10 });

  assert.equal(rows.length, 1);
  assert.equal(rows[0].key, key);
  assert.equal(rows[0].severity, 'CLOSE');
  assert.equal(rows[0].count, 3);
  // The fields that only the second read provides.
  assert.equal(rows[0].state, 'closed');
  assert.equal(rows[0].closedAt, 300, 'closedAt is a number, not a string');
  assert.equal(rows[0].closedBy, 'evt-1');
  assert.equal(rows[0].event.id, 'evt-1', 'the retiring event is attached');
});

test('readClosedAlerts uses pipeline() with no arguments', async () => {
  // Regression: an array argument threw inside ioredis and surfaced only as
  // `closed_failed`, so the tab looked like an empty week.
  const key = 'a:b';
  const client = fakeClient({ closedKeys: [key], hashes: { [`fw:alert:${key}`]: {} } });
  await readerWith(client).readClosedAlerts({ sinceMs: 0, limit: 10 });
  assert.equal(client.calls.pipeline, 1);
  assert.equal(client.calls.hmget.length, 1);
  assert.deepEqual(client.calls.hmget[0].fields, ['state', 'closed_at', 'closed_by']);
});

test('readClosedAlerts returns [] without touching Redis when nothing closed', async () => {
  const client = fakeClient({ closedKeys: [] });
  const rows = await readerWith(client).readClosedAlerts({ sinceMs: 0, limit: 10 });
  assert.deepEqual(rows, []);
  assert.equal(client.calls.pipeline, 0);
});

test('readClosedAlerts tolerates an expired event record', async () => {
  // The alert hash survives 7 days; its event record may already be gone, so the
  // row must still render (Summary falls back to a dash in the UI).
  const key = 'a:b';
  const client = fakeClient({
    closedKeys: [key],
    hashes: { [`fw:alert:${key}`]: { last_id: 'gone', state: 'closed', closed_at: 5 } },
    events: {},
  });
  const rows = await readerWith(client).readClosedAlerts({ sinceMs: 0, limit: 10 });
  assert.equal(rows.length, 1);
  assert.equal(rows[0].event, null);
  assert.equal(rows[0].closedAt, 5);
});

test('readAlert returns null when the key is not in the open index', async () => {
  // A retired alert must never be resurrected as a live row by the occurrence
  // fan-out.
  const client = fakeClient({ zscores: {} });
  const row = await readerWith(client).readAlert('a:b');
  assert.equal(row, null);
});

// ---------------------------------------- operator close must be a real event
/**
 * Fake client that records the EVAL keys/argv, so the close script's contract can
 * be asserted. The previous version of closeAlert wrote only the hash and the open
 * index, so an operator close was invisible in the Closed tab AND in the retention
 * window — it existed only as a state flag.
 */
function fakeEvalClient(result = ['closed', 'alice', '1000']) {
  const calls = [];
  return {
    calls,
    async eval(script, numKeys, ...args) {
      calls.push({ script, numKeys, args });
      return result;
    },
  };
}

function evalReader(client) {
  const r = new DragonflyReader(cfg, log);
  r.connect = async () => client;
  return r;
}

test('closeAlert writes the closed index, the event record and the source index', async () => {
  const client = fakeEvalClient();
  const key = 'probe-device:static.probe';
  await evalReader(client).closeAlert(`fw:alert:${key}`, 'alice', 1000, 604800000);

  const call = client.calls[0];
  assert.equal(call.numKeys, 6, 'six keys: hash, open, open-index, closed-index, event, source-index');
  assert.equal(call.args[0], `fw:alert:${key}`);
  assert.equal(call.args[3], 'fw:idx:alert:closed', 'the Closed tab enumerates from this');
  assert.match(call.args[4], /^fw:evt:operator-clear-/, 'a real event record id');
  assert.equal(call.args[5], 'fw:idx:src:probe-device', 'indexed by source, like any event');
});

test('the close event record is a CLEAR carrying the operator as author', async () => {
  const client = fakeEvalClient();
  await evalReader(client).closeAlert('fw:alert:a:b', 'alice', 1000, 604800000);

  // `eval` is called as (script, numKeys, ...keys, ...argv), so `args` is
  // [6 keys, by, nowMs, watermarkId, alertKey, ttl, eventId, eventRecord] —
  // the JSON record is the LAST element.
  const record = JSON.parse(client.calls[0].args[12]);
  assert.equal(record.severity, 'CLOSE', 'internal severity, so the fold treats it as retirement');
  assert.equal(record.payload.EVT_Severity, 'CLEAR', 'the badge shows what a producer would send');
  assert.equal(record.payload.EVT_Agent, 'alice', 'a human closed it, and that is recorded');
  assert.equal(record.source, 'a');
  assert.equal(record.type, 'b');
  assert.equal(record.operatorClose, true);
});

test('the event id is deterministic, so a retried close cannot duplicate', async () => {
  const k = 'fw:alert:a:b';
  const c1 = fakeEvalClient();
  const c2 = fakeEvalClient();
  await evalReader(c1).closeAlert(k, 'alice', 1000, 604800000);
  await evalReader(c2).closeAlert(k, 'alice', 1000, 604800000);
  assert.equal(c1.calls[0].args[4], c2.calls[0].args[4]);
});

test('a type containing a colon splits on the FIRST colon only', async () => {
  const client = fakeEvalClient();
  await evalReader(client).closeAlert('fw:alert:dev:hw:psu', 'bob', 5, 1000);
  const record = JSON.parse(client.calls[0].args[12]);
  assert.equal(record.source, 'dev');
  assert.equal(record.type, 'hw:psu');
  assert.equal(client.calls[0].args[5], 'fw:idx:src:dev');
});

test('an already-closed alert reports who closed it instead of overwriting', async () => {
  const client = fakeEvalClient(['already', 'alice', '900']);
  const res = await evalReader(client).closeAlert('fw:alert:a:b', 'bob', 1000, 1000);
  assert.equal(res.status, 'already');
  assert.equal(res.by, 'alice', 'the original author is preserved');
  assert.equal(res.at, 900);
});
