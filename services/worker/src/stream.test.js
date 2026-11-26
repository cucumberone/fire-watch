/**
 * Stream consumer: reply parsing, consumer GC selection, and the atomic
 * remove / dead-letter writes. No Valkey; a recording fake stands in.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { parseEntries, staleConsumers, StreamConsumer } from './stream.js';

const cfg = {
  streamKey: 'fw:stream:ingest',
  streamGroup: 'workers',
  streamDlqKey: 'fw:stream:dlq',
  consumerName: 'worker-a',
  streamBatch: 32,
  streamBlockMs: 5000,
  claimIdleMs: 60000,
};
const log = { debug() {}, info() {}, warn() {}, error() {} };

/** Records every command; MULTI chains record into one transaction. */
function fakeValkey(replies = {}) {
  const calls = [];
  const client = {
    multi() {
      const tx = [];
      const chain = new Proxy({}, {
        get(_, name) {
          if (name === 'exec') return async () => { calls.push({ multi: tx }); return tx.map(() => [null, 1]); };
          return (...args) => { tx.push([name, ...args]); return chain; };
        },
      });
      return chain;
    },
  };
  for (const name of ['xreadgroup', 'xgroup', 'xautoclaim']) {
    client[name] = async (...args) => {
      calls.push([name, ...args]);
      const r = replies[name];
      if (r instanceof Error) throw r;
      return r ?? null;
    };
  }
  return { calls, write: (fn) => fn(client) };
}

test('parseEntries turns the ioredis reply into id + field map and skips nil entries', () => {
  const out = parseEntries([
    ['1-0', ['evt', '{"id":"a"}', 'src', 'http']],
    null,
    ['2-0', null],
  ]);
  assert.deepEqual(out, [{ id: '1-0', fields: { evt: '{"id":"a"}', src: 'http' } }]);
});

test('staleConsumers keeps anyone with pending entries, recent activity, or our own name', () => {
  const infos = [
    { name: 'worker-a', pending: 0, idle: 9e9 },
    { name: 'old-1', pending: 0, idle: 7200000 },
    { name: 'old-2', pending: 3, idle: 7200000 },
    { name: 'busy', pending: 0, idle: 10 },
  ];
  assert.deepEqual(staleConsumers(infos, 'worker-a', 3600000), ['old-1']);
});

test('ackDelete acknowledges and deletes in ONE transaction', async () => {
  const w = fakeValkey();
  await new StreamConsumer(cfg, w, w, log).ackDelete(['1-0', '2-0']);
  assert.deepEqual(w.calls, [{ multi: [
    ['xack', 'fw:stream:ingest', 'workers', '1-0', '2-0'],
    ['xdel', 'fw:stream:ingest', '1-0', '2-0'],
  ] }]);
});

test('ackDelete with nothing to remove sends nothing', async () => {
  const w = fakeValkey();
  await new StreamConsumer(cfg, w, w, log).ackDelete([]);
  assert.equal(w.calls.length, 0);
});

test('deadLetter copies, acks and deletes atomically, keeping the original id and reason', async () => {
  const w = fakeValkey();
  await new StreamConsumer(cfg, w, w, log).deadLetter({ id: '5-0', fields: { evt: 'nope' } }, 'unparseable');
  const [{ multi }] = w.calls;
  assert.deepEqual(multi[0], ['xadd', 'fw:stream:dlq', '*', 'evt', 'nope', 'origId', '5-0', 'reason', 'unparseable']);
  assert.deepEqual(multi.slice(1), [
    ['xack', 'fw:stream:ingest', 'workers', '5-0'],
    ['xdel', 'fw:stream:ingest', '5-0'],
  ]);
});

test('read(">") blocks; read("0") re-reads the pending list without blocking', async () => {
  const w = fakeValkey({ xreadgroup: [['fw:stream:ingest', [['7-0', ['evt', '{}']]]]] });
  const c = new StreamConsumer(cfg, w, w, log);
  const got = await c.read('>');
  await c.read('0');
  assert.deepEqual(got, [{ id: '7-0', fields: { evt: '{}' } }]);
  assert.ok(w.calls[0].includes('BLOCK'));
  assert.equal(w.calls[0].at(-1), '>');
  assert.ok(!w.calls[1].includes('BLOCK'));
  assert.equal(w.calls[1].at(-1), '0');
});

test('a lost group is recreated from 0 instead of failing the loop', async () => {
  const w = fakeValkey({ xreadgroup: new Error('NOGROUP No such key or consumer group') });
  const c = new StreamConsumer(cfg, w, w, log);
  assert.deepEqual(await c.read('>'), []);
  assert.deepEqual(w.calls[1], ['xgroup', 'CREATE', 'fw:stream:ingest', 'workers', '0', 'MKSTREAM']);
  assert.equal(c.isReady(), true);
});
