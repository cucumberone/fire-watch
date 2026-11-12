/**
 * Tests for the gateway's read path, with a fake Valkey client.
 *
 * WHAT THESE PIN
 *
 *  1. The record is a HASH at `event:<32 hex>`, so a row is read with HMGET of a
 *     named field list — not by GET-ing a JSON blob. A wrong field list renders a
 *     blank column, which is silent.
 *  2. Snapshot and closed reads go through ONE script and ONE key list, so the cost
 *     does not grow with the number of rows.
 *  3. `readAlert` refuses a key that is not in `idx:event:open`, so a retired row
 *     cannot be resurrected as a live one.
 *  4. The one write path (ack/close/journal) names its keys explicitly, because
 *     Valkey refuses a script that touches an undeclared key.
 *
 *   node --test src/
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { ValkeyReader, journalKey } from './valkey.js';

const log = { info() {}, warn() {}, debug() {}, error() {} };
const cfg = { valkeyPassword: 'p', alertTtlMs: 1000, sentinels: [] };

/** A reader whose connection is a hand-rolled client. */
function readerWith(client) {
  const r = new ValkeyReader(cfg, log);
  r.connect = async () => client;
  r.resolveMaster = async () => ({ host: 'fake', port: 6379 });
  return r;
}

const KEY = 'a'.repeat(32);

/** A hash as HMGET returns it, in SNAPSHOT_FIELDS order. */
function hashValues(over = {}) {
  const base = {
    severity: 'CRITICAL',
    displaySeverity: 'MAJOR',
    source: 'core-sw-07',
    type: 'net.link.down',
    msg: 'uplink flapping',
    payload: '{"msg":"uplink flapping"}',
    brokerTs: '1000',
    lastTs: '2026-11-10T00:00:00Z',
    firstSeen: '1000',
    lastSeen: '2000',
    count: '3',
    lastId: 'evt-9',
    state: 'open',
    closedAt: '',
    closedBy: '',
    ackedBy: '',
    ackedAt: '',
    ...over,
  };
  return Object.values(base);
}

test('readOpenAlerts reads one hash per key and returns renderable rows', async () => {
  const client = {
    async zrevrange() { return [KEY]; },
    async script() { return 'sha'; },
    async evalsha(_sha, numKeys, ...keys) {
      assert.equal(numKeys, 1);
      assert.equal(keys[0], `event:${KEY}`, 'the record is addressed by its bare key');
      return hashValues();
    },
  };
  const rows = await readerWith(client).readOpenAlerts(10);

  assert.equal(rows.length, 1);
  const r = rows[0];
  assert.equal(r.key, KEY);
  assert.equal(r.source, 'core-sw-07');
  assert.equal(r.type, 'net.link.down');
  assert.equal(r.summary, 'uplink flapping', 'the summary comes from the hash field');
  assert.equal(r.count, 3);
  assert.equal(r.firstOccurrence, 1000);
  assert.equal(r.lastOccurrence, 2000);
  assert.equal(r.alertState, 'open');
  assert.deepEqual(r.payload, { msg: 'uplink flapping' }, 'the payload is parsed');
});

test('a hash that vanished between the index read and the script is skipped', async () => {
  // Otherwise it becomes a row of empty strings: a blank line on the board that
  // nothing can clear.
  const client = {
    async zrevrange() { return [KEY, 'b'.repeat(32)]; },
    async script() { return 'sha'; },
    async evalsha() { return [...hashValues(), ...new Array(17).fill('')]; },
  };
  const rows = await readerWith(client).readOpenAlerts(10);
  assert.equal(rows.length, 1, 'only the row with data survives');
  assert.equal(rows[0].key, KEY);
});

test('an empty open index short-circuits without calling the script', async () => {
  let scripted = false;
  const client = {
    async zrevrange() { return []; },
    async script() { scripted = true; return 'sha'; },
    async evalsha() { scripted = true; return []; },
  };
  const rows = await readerWith(client).readOpenAlerts(10);
  assert.deepEqual(rows, []);
  assert.equal(scripted, false, 'no round trip when there is nothing to read');
});

test('readAllEvents reads the global index and reports the window total', async () => {
  const client = {
    async zrevrangebyscore(key, max, min, _l, _o, limit) {
      assert.equal(key, 'idx:event:all');
      assert.match(min, /^\(/, 'the lower bound is exclusive, so a page boundary is not double-counted');
      assert.ok(limit > 0);
      return [KEY];
    },
    async zcount(key, min, max) {
      assert.equal(key, 'idx:event:all');
      assert.match(min, /^\(/);
      return 42;
    },
    pipeline() {
      return {
        _q: [],
        hmget(k, ...f) { this._q.push([k, f]); return this; },
        async exec() { return this._q.map(() => [null, hashValues()]); },
      };
    },
  };

  const { events, total } = await readerWith(client).readAllEvents({ sinceMs: 100, limit: 10 });
  assert.equal(events.length, 1);
  assert.equal(total, 42, 'the badge is the WINDOW total, not the page size');
});

test('readClosedAlerts reads the closed index, newest first', async () => {
  const client = {
    async zrevrangebyscore(key, max, min) {
      assert.equal(key, 'idx:event:closed');
      assert.equal(max, '+inf');
      return [KEY];
    },
    async script() { return 'sha'; },
    async evalsha() { return hashValues({ state: 'closed', closedAt: '3000', closedBy: 'alice' }); },
  };
  const rows = await readerWith(client).readClosedAlerts({ sinceMs: 0, limit: 10 });
  assert.equal(rows.length, 1);
  assert.equal(rows[0].alertState, 'closed');
  assert.equal(rows[0].alertClosedAt, 3000);
  assert.equal(rows[0].alertClosedBy, 'alice');
});

test('readEvent does not require the record to be open', async () => {
  // The delete path uses it: an operator may delete a CLOSED record, and readAlert
  // would return null for it, making the delete report "no such event".
  const client = {
    async hmget() { return hashValues({ state: 'closed' }); },
  };
  const row = await readerWith(client).readEvent(KEY);
  assert.equal(row.key, KEY);
  assert.equal(row.alertState, 'closed');
});

test('readAlert refuses a record that is not in the open index', async () => {
  // The index is the authority on what is open. Without this check a retired row
  // could be resurrected by an occurrence frame for a stale key.
  const client = {
    async hmget() { return hashValues({ state: 'closed' }); },
    async zscore() { return null; },
  };
  assert.equal(await readerWith(client).readAlert(KEY), null);
});

test('readAlert returns the row when it is in the open index', async () => {
  const client = {
    async hmget() { return hashValues(); },
    async zscore() { return '2000'; },
  };
  const row = await readerWith(client).readAlert(KEY);
  assert.ok(row);
  assert.equal(row.key, KEY);
});

test('ack names its key explicitly and refuses a missing record', async () => {
  let seen;
  const client = {
    async eval(_script, numKeys, ...args) {
      seen = { numKeys, args };
      return ['acked', 'alice', '5000'];
    },
  };

  // The real ack has an EXISTS guard inside the script; this asserts the CALL SHAPE,
  // which is what a declared-key violation would break.
  const real = readerWith(client);
  real.connect = async () => ({
    async eval(script, numKeys, ...args) {
      seen = { numKeys, args, script };
      return ['acked', 'alice', '5000'];
    },
  });
  const res = await real.ackAlert(KEY, 'alice', 5000, 1000);

  assert.equal(seen.numKeys, 1);
  assert.equal(seen.args[0], `event:${KEY}`, 'the bare key is prefixed exactly once');
  assert.deepEqual(res, { status: 'acked', ack: { by: 'alice', at: 5000 } });
});

test('close moves the key between the two state indexes in one script', async () => {
  let seen;
  const client = {
    async eval(script, numKeys, ...args) {
      seen = { numKeys, args, script };
      return ['closed', 'alice', '6000'];
    },
  };
  const res = await readerWith(client).closeAlert(KEY, 'alice', 6000, 1000);

  assert.equal(seen.numKeys, 3);
  assert.equal(seen.args[0], `event:${KEY}`);
  assert.equal(seen.args[1], 'idx:event:open');
  assert.equal(seen.args[2], 'idx:event:closed');
  assert.match(seen.script, /ZREM/);
  assert.match(seen.script, /ZADD/);
  assert.deepEqual(res, { status: 'closed', by: 'alice', at: 6000 });
});

test('delete removes the record and all three index members', async () => {
  let seen;
  const client = {
    async eval(script, numKeys, ...args) {
      seen = { numKeys, args };
      return 'deleted';
    },
  };
  const res = await readerWith(client).deleteEvent(KEY);

  assert.equal(seen.numKeys, 4);
  assert.equal(seen.args[0], `event:${KEY}`);
  assert.deepEqual(seen.args.slice(1, 4), ['idx:event:all', 'idx:event:open', 'idx:event:closed']);
  assert.deepEqual(res, { status: 'deleted' });
});

test('the journal is a separate key, not a field on the record', () => {
  assert.equal(journalKey(KEY), `journal:${KEY}`);
  assert.notEqual(journalKey(KEY), `event:${KEY}`);
});

test('an empty journal read returns an entry per requested key', async () => {
  // So a client can tell "read it, there is nothing" from "never answered".
  const client = {
    pipeline() {
      return {
        _q: [],
        lrange(k) { this._q.push(k); return this; },
        async exec() { return this._q.map(() => [null, []]); },
      };
    },
  };
  const out = await readerWith(client).readJournals([KEY, 'b'.repeat(32)], 10);
  assert.equal(out.length, 2);
  for (const row of out) assert.deepEqual(row.entries, []);
});

// ------------------------------------------------------------------- API surface
test('every method index.js calls exists on the reader', () => {
  // THE BUG THIS PINS. The Valkey rewrite replaced the whole client class, and
  // `ping()` was dropped while `index.js` kept calling `reader.ping()`. Every
  // other method worked, so the gateway looked healthy — but the DB-health probe
  // threw `TypeError: reader.ping is not a function` every 10s and the UI's
  // database chip could never turn red. That removed the only signal that the
  // gateway's reads had failed, which is precisely the signal an outage needs.
  //
  // A list of call sites rather than a reflection trick: adding a method to the
  // gateway without adding it here is the review point, and the assertion is
  // against the real prototype.
  const used = [
    'ackAlert', 'appendJournal', 'closeAlert', 'deleteEvent', 'isSubscribed',
    'ping', 'publish', 'quit', 'readAcks', 'readAlert', 'readAllEvents',
    'readClosedAlerts', 'readEvent', 'readEvents', 'readIndex', 'readIndexRange',
    'readJournals', 'readOpenAlerts', 'subscribe',
  ];
  const r = new ValkeyReader(cfg, log);
  const missing = used.filter((m) => typeof r[m] !== 'function');
  assert.deepEqual(missing, [], `reader is missing: ${missing.join(', ')}`);
});

test('ping reports down instead of throwing when the datastore is unreachable', () => {
  // The probe runs on a timer. A throw here becomes an unhandled rejection every
  // interval for the whole duration of an outage.
  const r = new ValkeyReader(cfg, log);
  r.connect = async () => { throw new Error('ECONNREFUSED'); };
  return r.ping().then((up) => assert.equal(up, false));
});

test('ping reports up when the socket answers', async () => {
  const r = new ValkeyReader(cfg, log);
  r.connect = async () => ({ async ping() { return 'PONG'; } });
  assert.equal(await r.ping(), true);
});
