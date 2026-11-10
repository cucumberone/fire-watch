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
  assert.equal(call.numKeys, 7, 'seven keys: hash, open, open-idx, closed-idx, event, source-idx, global-idx');
  assert.equal(call.args[0], `fw:alert:${key}`);
  assert.equal(call.args[3], 'fw:idx:alert:closed', 'the Closed tab enumerates from this');
  assert.match(call.args[4], /^fw:evt:operator-clear-/, 'a real event record id');
  assert.equal(call.args[5], 'fw:idx:src:probe-device', 'indexed by source, like any event');
  assert.equal(call.args[6], 'fw:idx:evt:all', 'and on the global timeline, so All Events finds it');
});

test('the close event record is a CLEAR carrying the operator as author', async () => {
  const client = fakeEvalClient();
  await evalReader(client).closeAlert('fw:alert:a:b', 'alice', 1000, 604800000);

  // `eval` is called as (script, numKeys, ...keys, ...argv), so `args` is
  // [6 keys, by, nowMs, watermarkId, alertKey, ttl, eventId, eventRecord] —
  // the JSON record is the LAST element.
  const record = JSON.parse(client.calls[0].args[13]);
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
  const record = JSON.parse(client.calls[0].args[13]);
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

// ------------------------------------------------ All Events reader contract
function allEventsClient({ ids = [], events = {}, total = ids.length, states = {} } = {}) {
  const calls = { range: null, mget: [], count: null, hmget: [] };
  return {
    calls,
    async zrevrangebyscore(key, max, min, ...rest) {
      calls.range = { key, max, min, rest };
      return ids;
    },
    async zcount(key, min, max) {
      calls.count = { key, min, max };
      return total;
    },
    async mget(keys) {
      calls.mget.push(keys);
      return keys.map((k) => events[k] ?? null);
    },
    pipeline() {
      const queued = [];
      const pipe = {
        hmget(key, ...fields) {
          calls.hmget.push({ key, fields });
          queued.push(key);
          return pipe;
        },
        async exec() {
          return queued.map((key) => {
            const s = states[key];
            if (!s) return [null, [null, null]];
            return [null, [s.state ?? null, s.closedAt != null ? String(s.closedAt) : null]];
          });
        },
      };
      return pipe;
    },
  };
}

test('readAllEvents reads the GLOBAL index, newest first, windowed', async () => {
  const client = allEventsClient({
    ids: ['e1'],
    events: { 'fw:evt:e1': JSON.stringify({ id: 'e1' }) },
  });
  const r = new DragonflyReader(cfg, log);
  r.connect = async () => client;

  const res = await r.readAllEvents({ sinceMs: 5000, untilMs: 9000, limit: 100 });
  assert.deepEqual(res.events.map((e) => e.id), ['e1']);

  assert.equal(client.calls.range.key, 'fw:idx:evt:all',
    'must read the global timeline, not a per-source index');
  assert.equal(client.calls.range.max, '9000');
  // Exclusive lower bound, so two overlapping windows never list the same event.
  assert.equal(client.calls.range.min, '(5000');
  assert.deepEqual(client.calls.range.rest.slice(-2), [0, 100]);
});

test('readAllEvents joins the ALERT state onto each event', async () => {
  // An event record is immutable and carries no state. "Open or closed" is a
  // property of the alert (`source:type`), so the reader joins it on. The State
  // chip reads `alertState`.
  const client = allEventsClient({
    ids: ['e1', 'e2', 'e3'],
    events: {
      'fw:evt:e1': JSON.stringify({ id: 'e1', source: 'a', type: 'open.one' }),
      'fw:evt:e2': JSON.stringify({ id: 'e2', source: 'a', type: 'closed.one' }),
      'fw:evt:e3': JSON.stringify({ id: 'e3', source: 'a', type: 'gone.one' }),
    },
    states: {
      'fw:alert:a:open.one': { state: 'open', closedAt: null },
      'fw:alert:a:closed.one': { state: 'closed', closedAt: 1700000000000 },
    },
  });
  const r = new DragonflyReader(cfg, log);
  r.connect = async () => client;

  const { events } = await r.readAllEvents({ sinceMs: 0, untilMs: 999, limit: 10 });
  assert.deepEqual(events.map((e) => [e.id, e.alertState]), [
    ['e1', 'open'],
    ['e2', 'closed'],
    ['e3', null],
  ]);
  assert.equal(events[1].alertClosedAt, 1700000000000);
  assert.equal(events[0].alertClosedAt, null);

  // ONE lookup per DISTINCT alert, not per row: this page has three events but the
  // read must not scale with row count.
  assert.equal(client.calls.hmget.length, 3);
  assert.deepEqual(client.calls.hmget.map((h) => h.key).sort(), [
    'fw:alert:a:closed.one', 'fw:alert:a:gone.one', 'fw:alert:a:open.one',
  ]);
});

test('readAllEvents does one alert lookup per distinct key, not per row', async () => {
  // 50 events from 2 incidents costs 2 lookups. Scaling with ROWS here would add a
  // read per row to a query that already pages 1000 of them.
  const ids = Array.from({ length: 50 }, (_, i) => `e${i}`);
  const events = Object.fromEntries(ids.map((id, i) => [
    `fw:evt:${id}`,
    JSON.stringify({ id, source: 's', type: i % 2 ? 'a' : 'b' }),
  ]));
  const client = allEventsClient({
    ids, events,
    states: {
      'fw:alert:s:a': { state: 'open', closedAt: null },
      'fw:alert:s:b': { state: 'closed', closedAt: 1 },
    },
  });
  const r = new DragonflyReader(cfg, log);
  r.connect = async () => client;

  const { events: out } = await r.readAllEvents({ sinceMs: 0, untilMs: 999, limit: 100 });
  assert.equal(out.length, 50);
  assert.equal(client.calls.hmget.length, 2, 'two distinct alerts -> two lookups');
});

test('readAllEvents reports the WINDOW total, not the page size', async () => {
  // The badge reads this. Deriving it from the fetched page made it report "rows I
  // have fetched", which is 0 until the tab is opened — so a browser refresh on
  // another tab showed All events 0 while the window was full.
  const client = allEventsClient({
    ids: ['e1'],
    events: { 'fw:evt:e1': JSON.stringify({ id: 'e1' }) },
    total: 4242,
  });
  const r = new DragonflyReader(cfg, log);
  r.connect = async () => client;

  const res = await r.readAllEvents({ sinceMs: 5000, untilMs: 9000, limit: 1 });
  assert.equal(res.events.length, 1, 'the page is capped');
  assert.equal(res.total, 4242, 'the total is the whole window');
  // Same exclusive lower bound, or a boundary event would be counted twice.
  assert.deepEqual(client.calls.count, { key: 'fw:idx:evt:all', min: '(5000', max: '9000' });
});

test('readAllEvents still reports a total when the page is empty', async () => {
  // A window whose newest page is empty can still hold events beyond the range
  // start; the caller must be able to tell "none in this window" from "did not ask".
  const client = allEventsClient({ ids: [], total: 0 });
  const r = new DragonflyReader(cfg, log);
  r.connect = async () => client;

  const res = await r.readAllEvents({ sinceMs: 0, untilMs: 1, limit: 10 });
  assert.deepEqual(res.events, []);
  assert.equal(res.total, 0);
});

test('readAllEvents returns the records newest-first', async () => {
  const client = allEventsClient({
    ids: ['e2', 'e1'],
    events: {
      'fw:evt:e2': JSON.stringify({ id: 'e2', brokerTs: 200 }),
      'fw:evt:e1': JSON.stringify({ id: 'e1', brokerTs: 100 }),
    },
  });
  const r = new DragonflyReader(cfg, log);
  r.connect = async () => client;

  const res = await r.readAllEvents({ sinceMs: 0, untilMs: 999, limit: 10 });
  assert.deepEqual(res.events.map((e) => e.id), ['e2', 'e1']);
});

test('readAllEvents skips an index member whose record expired', async () => {
  // Otherwise the tab renders a blank row that cannot be opened.
  const client = allEventsClient({ ids: ['gone', 'e1'],
    events: { 'fw:evt:e1': JSON.stringify({ id: 'e1' }) } });
  const r = new DragonflyReader(cfg, log);
  r.connect = async () => client;

  const res = await r.readAllEvents({ sinceMs: 0, untilMs: 999, limit: 10 });
  assert.deepEqual(res.events.map((e) => e.id), ['e1']);
});

test('readAllEvents chunks a large page instead of one huge MGET', async () => {
  const ids = Array.from({ length: 600 }, (_, i) => `e${i}`);
  const events = Object.fromEntries(ids.map((id) => [`fw:evt:${id}`, JSON.stringify({ id })]));
  const client = allEventsClient({ ids, events });
  const r = new DragonflyReader(cfg, log);
  r.connect = async () => client;

  const out = await r.readAllEvents({ sinceMs: 0, untilMs: 999, limit: 1000 });
  assert.equal(out.events.length, 600);
  assert.equal(client.calls.mget.length, 3, '600 ids -> 256 + 256 + 88');
});

test('readAllEvents does no MGET when the page is empty', async () => {
  const client = allEventsClient({ ids: [], total: 42 });
  const r = new DragonflyReader(cfg, log);
  r.connect = async () => client;
  const res = await r.readAllEvents({ sinceMs: 0, untilMs: 1, limit: 10 });
  assert.deepEqual(res.events, []);
  assert.equal(res.total, 42, 'the total is still reported');
  assert.equal(client.calls.mget.length, 0);
});
