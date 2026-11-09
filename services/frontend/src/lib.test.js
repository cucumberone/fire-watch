/**
 * Tests for the frontend's pure logic.
 *   node --test src/
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { gunzipSync } from 'node:zlib';
import {
  resolveWsUrl,
  isCompressedField,
  decodeField,
  decodePayload,
  applyAlert,
  insertEvent,
  backoffDelay,
  isRetirement,
  SEVERITY_RANK,
} from './lib.js';

const gunzip = async (bytes) => gunzipSync(Buffer.from(bytes));
import { gzipSync } from 'node:zlib';
const gzField = (s) => ({ __gz: gzipSync(Buffer.from(s, 'utf8')).toString('base64') });

// ------------------------------------------------------------------ ws url
test('resolveWsUrl returns the configured URL when not resolving from page', () => {
  const url = resolveWsUrl(
    { wsUrl: 'wss://ws.example.com', resolveWsHostFromPage: false },
    '192.168.1.5',
    true,
  );
  assert.equal(url, 'wss://ws.example.com/');
});

test('resolveWsUrl substitutes the page host when asked', () => {
  const url = resolveWsUrl(
    { wsUrl: 'ws://127.0.0.1:30300', resolveWsHostFromPage: true },
    '192.168.50.4',
    false,
  );
  assert.equal(url, 'ws://192.168.50.4:30300/');
});

test('resolveWsUrl upgrades ws to wss on an https page', () => {
  // A ws:// URL on an https page is blocked as mixed content. Upgrading is
  // better than a silent failure.
  const url = resolveWsUrl(
    { wsUrl: 'ws://127.0.0.1:30300', resolveWsHostFromPage: true },
    'example.com',
    true,
  );
  assert.ok(url.startsWith('wss://'), url);
});

test('resolveWsUrl returns empty for a missing or invalid url', () => {
  assert.equal(resolveWsUrl({}, 'h', false), '');
  assert.equal(resolveWsUrl({ wsUrl: 'not a url' }, 'h', false), '');
});

// ------------------------------------------------------------------- decoding
test('isCompressedField recognises the wire shape', () => {
  assert.equal(isCompressedField({ __gz: 'abc' }), true);
  assert.equal(isCompressedField('plain'), false);
  assert.equal(isCompressedField({ other: 1 }), false);
  assert.equal(isCompressedField(null), false);
});

test('decodeField passes plain values straight through', async () => {
  assert.equal(await decodeField('hello', gunzip, 1024), 'hello');
});

test('decodeField decompresses a gzip field', async () => {
  const original = 'line\n'.repeat(100);
  const decoded = await decodeField(gzField(original), gunzip, 1_000_000);
  assert.equal(decoded, original);
});

test('decodeField degrades to a marker on corrupt data instead of throwing', async () => {
  const bad = { __gz: Buffer.from('not gzip at all').toString('base64') };
  const decoded = await decodeField(bad, gunzip, 1024);
  assert.equal(decoded, '[undecodable payload]');
});

test('decodeField truncates an oversized expansion', async () => {
  const big = 'a'.repeat(5000);
  const decoded = await decodeField(gzField(big), gunzip, 100);
  assert.ok(decoded.startsWith('aaaa'));
  assert.ok(decoded.includes('truncated'), decoded.slice(-40));
});

test('decodePayload decodes only the compressed fields', async () => {
  const msgc = 'trace\n'.repeat(500);
  const payload = { msg: 'disk failing', msgc: gzField(msgc) };
  const out = await decodePayload(payload, gunzip, 1_000_000);
  assert.equal(out.msg, 'disk failing');
  assert.equal(out.msgc, msgc);
});

// --------------------------------------------------------------- alert folding
test('applied=0 leaves alert state untouched', () => {
  const map = new Map([['a:t', { key: 'a:t', severity: 'CRITICAL', count: 1 }]]);
  const next = applyAlert(map, { key: 'a:t', applied: 0, event: { severity: 'CLOSE' } });
  assert.deepEqual([...next.keys()], ['a:t']);
  assert.equal(next.get('a:t').severity, 'CRITICAL');
});

test('applied=1 creates and then increments an alert', () => {
  let map = new Map();
  map = applyAlert(map, { key: 'a:t', applied: 1, event: { severity: 'CRITICAL', brokerTs: 1000 } });
  assert.equal(map.get('a:t').count, 1);
  map = applyAlert(map, { key: 'a:t', applied: 1, event: { severity: 'CRITICAL', brokerTs: 2000 } });
  assert.equal(map.get('a:t').count, 2);
  assert.equal(map.get('a:t').lastSeen, 2000);
  assert.equal(map.get('a:t').firstSeen, 1000);
});

test('applied=2 moves lastSeen without incrementing count', () => {
  let map = new Map([['a:t', { key: 'a:t', severity: 'WARNING', count: 5, firstSeen: 1, lastSeen: 100 }]]);
  map = applyAlert(map, { key: 'a:t', applied: 2, event: { severity: 'CRITICAL', brokerTs: 900 } });
  assert.equal(map.get('a:t').count, 5, 'coalescing must not increment');
  assert.equal(map.get('a:t').lastSeen, 900);
  assert.equal(map.get('a:t').severity, 'CRITICAL', 'severity escalation is still shown');
});

test('applied=3 removes the alert', () => {
  const map = new Map([['a:t', { key: 'a:t' }]]);
  const next = applyAlert(map, { key: 'a:t', applied: 3, event: {} });
  assert.equal(next.has('a:t'), false);
});

test('applyAlert never mutates the input map', () => {
  const map = new Map([['a:t', { key: 'a:t', count: 1, severity: 'CRITICAL' }]]);
  const before = map.get('a:t').count;
  applyAlert(map, { key: 'a:t', applied: 1, event: { severity: 'CRITICAL', brokerTs: 5 } });
  assert.equal(map.get('a:t').count, before);
});

// ------------------------------------------------------------------ event list
test('insertEvent keeps the list sorted by broker timestamp', () => {
  let list = [];
  list = insertEvent(list, { id: 'b', brokerTs: 200 }, 10);
  list = insertEvent(list, { id: 'a', brokerTs: 100 }, 10);
  list = insertEvent(list, { id: 'c', brokerTs: 300 }, 10);
  assert.deepEqual(list.map((e) => e.id), ['a', 'b', 'c']);
});

test('insertEvent ignores a duplicate id', () => {
  let list = insertEvent([], { id: 'a', brokerTs: 1 }, 10);
  list = insertEvent(list, { id: 'a', brokerTs: 2 }, 10);
  assert.equal(list.length, 1);
});

test('insertEvent trims to the limit, keeping the newest', () => {
  let list = [];
  for (let i = 0; i < 10; i += 1) list = insertEvent(list, { id: `e${i}`, brokerTs: i }, 3);
  assert.equal(list.length, 3);
  assert.deepEqual(list.map((e) => e.id), ['e7', 'e8', 'e9']);
});

// -------------------------------------------------------------------- severity
test('retirement severities are recognised', () => {
  assert.equal(isRetirement('CLOSE'), true);
  assert.equal(isRetirement('HARMLESS'), true);
  assert.equal(isRetirement('CRITICAL'), false);
});

test('severity ranks order FATAL first and retirement last', () => {
  assert.ok(SEVERITY_RANK.FATAL < SEVERITY_RANK.CRITICAL);
  assert.ok(SEVERITY_RANK.CRITICAL < SEVERITY_RANK.WARNING);
  assert.ok(SEVERITY_RANK.WARNING < SEVERITY_RANK.HARMLESS);
});

// -------------------------------------------------------------------- backoff
test('backoffDelay grows and is capped', () => {
  const first = backoffDelay(0, { base: 500, cap: 30000 });
  assert.ok(first >= 250 && first <= 500, String(first));
  const later = backoffDelay(20, { base: 500, cap: 30000 });
  assert.ok(later <= 30000, String(later));
});
