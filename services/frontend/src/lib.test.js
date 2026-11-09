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
  resolveTheme,
  severityRowClass,
  toViewEvent,
  recordEntries,
  formatClock,
  ENRICHED_FIELDS,
  WIRE_FIELD,
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

// ---------------------------------------------------------------------- theme
test('resolveTheme follows the system setting on auto', () => {
  assert.equal(resolveTheme('auto', true), 'dark');
  assert.equal(resolveTheme('auto', false), 'light');
});

test('resolveTheme honours an explicit choice over the system setting', () => {
  assert.equal(resolveTheme('dark', false), 'dark');
  assert.equal(resolveTheme('light', true), 'light');
});

test('resolveTheme treats an unknown preference as auto', () => {
  assert.equal(resolveTheme(undefined, true), 'dark');
  assert.equal(resolveTheme('purple', false), 'light');
});

// ------------------------------------------------------------- row colouring
test('severityRowClass lowercases the severity', () => {
  assert.equal(severityRowClass('FATAL'), 'row-fatal');
  assert.equal(severityRowClass('HARMLESS'), 'row-harmless');
});

test('severityRowClass appends extra classes and survives a missing severity', () => {
  assert.equal(severityRowClass('CLOSE', 'retirement'), 'row-close retirement');
  // An unknown severity must still get a class, or the row-colour mode would
  // silently leave it unstyled.
  assert.equal(severityRowClass(undefined), 'row-unknown');
});

// --------------------------------------------------------- wire → view rename
test('WIRE_FIELD is the single source of the rename', () => {
  assert.equal(WIRE_FIELD.summary, 'msg');
  assert.equal(WIRE_FIELD.msgCompleteString, 'msgc');
});

test('toViewEvent renames msg and msgc to summary and msgCompleteString', () => {
  const view = toViewEvent({
    id: 'e1',
    severity: 'CRITICAL',
    payload: { msg: 'disk failing', msgc: 'full trace' },
  });
  assert.equal(view.summary, 'disk failing');
  assert.equal(view.msgCompleteString, 'full trace');
  assert.equal(view.id, 'e1');
  assert.equal(view.severity, 'CRITICAL');
});

test('toViewEvent keeps the wire fields so nothing downstream breaks', () => {
  // A caller that still reads evt.payload.msg must not suddenly get undefined.
  const view = toViewEvent({ payload: { msg: 'a', msgc: 'b' } });
  assert.equal(view.payload.msg, 'a');
  assert.equal(view.payload.msgc, 'b');
});

test('toViewEvent yields empty strings, never undefined', () => {
  // A row must never render the literal text "undefined".
  for (const input of [undefined, null, {}, { payload: null }, { payload: {} }]) {
    const view = toViewEvent(input);
    assert.equal(view.summary, '', JSON.stringify(input));
    assert.equal(view.msgCompleteString, '', JSON.stringify(input));
  }
});

test('toViewEvent keeps the untouched wire record for the inspector', () => {
  // Without this the right-click panel would list every field twice: once as
  // `payload.CIF` and once as the derived `cif`.
  const wire = { id: 'e1', payload: { msg: 'a', CIF: 'c1' } };
  const view = toViewEvent(wire);
  assert.equal(view.wire, wire);
  assert.equal(view.wire.payload.CIF, 'c1');
  // The wire record must not itself gain the derived keys.
  assert.ok(!('cif' in view.wire));
});

test('toViewEvent never mutates the input event', () => {
  const evt = { payload: { msg: 'x', msgc: 'y' } };
  const snapshot = JSON.parse(JSON.stringify(evt));
  toViewEvent(evt);
  assert.deepEqual(evt, snapshot);
});

// ------------------------------------------------------ occurrence + enrichment
test('toViewEvent falls back to the event time for both occurrences', () => {
  // One raw event is its own first and last occurrence, count 1. That is the
  // honest answer; inventing a wider window would be a guess.
  const view = toViewEvent({ brokerTs: 1_700_000_000_000, payload: {} });
  assert.equal(view.firstOccurrence, 1_700_000_000_000);
  assert.equal(view.lastOccurrence, 1_700_000_000_000);
  assert.equal(view.count, 1);
});

test('toViewEvent prefers worker-enriched occurrence fields', () => {
  const view = toViewEvent({
    brokerTs: 5000,
    payload: {
      FirstOccurrence: 1000,
      LastOccurrence: 4000,
      Count: 7,
    },
  });
  assert.equal(view.firstOccurrence, 1000);
  assert.equal(view.lastOccurrence, 4000);
  assert.equal(view.count, 7);
});

test('toViewEvent accepts snake_case and camelCase occurrence spellings', () => {
  const snake = toViewEvent({ payload: { first_occurrence: 10, last_occurrence: 20, count: 3 } });
  assert.equal(snake.firstOccurrence, 10);
  assert.equal(snake.lastOccurrence, 20);
  assert.equal(snake.count, 3);

  const camel = toViewEvent({ payload: { firstOccurrence: 11, lastOccurrence: 21, count: 4 } });
  assert.equal(camel.firstOccurrence, 11);
  assert.equal(camel.count, 4);
});

test('toViewEvent parses ISO occurrence strings', () => {
  const view = toViewEvent({
    payload: { FirstOccurrence: '2026-11-09T12:00:00.000Z', LastOccurrence: '2026-11-09T12:05:00.000Z' },
  });
  assert.equal(view.firstOccurrence, Date.parse('2026-11-09T12:00:00.000Z'));
  assert.equal(view.lastOccurrence, Date.parse('2026-11-09T12:05:00.000Z'));
});

test('toViewEvent rejects a nonsense count instead of rendering NaN', () => {
  for (const bad of [0, -3, 'lots', '', null, undefined, NaN]) {
    assert.equal(toViewEvent({ payload: { Count: bad } }).count, 1, JSON.stringify(bad));
  }
});

test('toViewEvent maps the five enriched worker fields', () => {
  const view = toViewEvent({
    payload: {
      CIF: 'cif-991',
      AlarmIDKey: 'key-abc',
      AlarmOwner: 'ops-team',
      AutoClose: true,
      SupportGroupName: 'Network Core',
    },
  });
  assert.equal(view.cif, 'cif-991');
  assert.equal(view.alarmIdKey, 'key-abc');
  assert.equal(view.alarmOwner, 'ops-team');
  assert.equal(view.autoClose, 'yes', 'a boolean must render as yes, not true');
  assert.equal(view.supportGroupName, 'Network Core');
});

test('toViewEvent accepts every AlarmIDKey ID-casing variant', () => {
  // "ID" is an acronym, so upstream may send it capitalised in different ways.
  for (const wire of ['AlarmIDKey', 'AlarmIdKey', 'alarmIDKey', 'alarmIdKey', 'alarm_id_key']) {
    const view = toViewEvent({ payload: { [wire]: 'k1' } });
    assert.equal(view.alarmIdKey, 'k1', wire);
  }
});

test('toViewEvent does not confuse AlarmIDKey with AlarmOwner', () => {
  const view = toViewEvent({ payload: { AlarmIDKey: 'k1' } });
  assert.equal(view.alarmIdKey, 'k1');
  assert.equal(view.alarmOwner, '', 'only the ID key was sent');
});

test('toViewEvent leaves enriched fields empty when the worker did not add them', () => {
  const view = toViewEvent({ payload: { msg: 'x' } });
  for (const f of ENRICHED_FIELDS) {
    assert.equal(view[f.key], '', f.key);
  }
});

test('ENRICHED_FIELDS matches the wire casing the workers emit', () => {
  const view = toViewEvent({
    payload: {
      cif: 'lower',          // not the documented casing, but tolerated
      alarm_id_key: 'snake key',
      alarm_owner: 'snake',
      auto_close: false,
      support_group_name: 'snake group',
    },
  });
  assert.equal(view.cif, 'lower');
  assert.equal(view.alarmIdKey, 'snake key');
  assert.equal(view.alarmOwner, 'snake');
  assert.equal(view.autoClose, 'no');
  assert.equal(view.supportGroupName, 'snake group');
});

// ------------------------------------------------------------------- inspector
test('recordEntries flattens nested payloads with dotted paths', () => {
  const entries = recordEntries({
    id: 'e1',
    payload: { msg: 'disk failing', nested: { depth: 2 } },
  });
  const map = Object.fromEntries(entries);
  assert.equal(map.id, 'e1');
  assert.equal(map['payload.msg'], 'disk failing');
  assert.equal(map['payload.nested.depth'], '2');
  // The nested object itself must NOT appear as "[object Object]".
  assert.ok(!entries.some(([k]) => k === 'payload.nested'), 'container key leaked');
});

test('recordEntries indexes arrays and keeps empty containers visible', () => {
  const map = Object.fromEntries(recordEntries({
    tags: ['a', 'b'],
    empty: [],
    nothing: {},
  }));
  assert.equal(map['tags[0]'], 'a');
  assert.equal(map['tags[1]'], 'b');
  assert.equal(map.empty, '[]');
  assert.equal(map.nothing, '{}');
});

test('recordEntries omits undefined but shows null', () => {
  const entries = recordEntries({ absent: undefined, present: null, zero: 0 });
  const map = Object.fromEntries(entries);
  assert.ok(!('absent' in map), 'undefined should be omitted');
  assert.equal(map.present, 'null');
  assert.equal(map.zero, '0', '0 is a value, not absence');
});

test('recordEntries labels a still-compressed field rather than base64 noise', () => {
  const map = Object.fromEntries(recordEntries({ payload: { msgc: { __gz: 'AAAA' } } }));
  assert.ok(map['payload.msgc'].startsWith('[gzip'), map['payload.msgc']);
});

test('recordEntries survives a circular reference', () => {
  const cyclic = { a: 1 };
  cyclic.self = cyclic;
  // Cycles are not expected on the wire, but a stack overflow here would blank
  // the whole UI on a right-click.
  const map = Object.fromEntries(recordEntries(cyclic));
  assert.equal(map.a, '1');
  assert.equal(map.self, '[circular]');
  assert.deepEqual(recordEntries({}), []);
  assert.deepEqual(recordEntries(null), []);
});

// ---------------------------------------------------------------- formatClock
test('formatClock returns empty for unusable times', () => {
  assert.equal(formatClock(undefined), '');
  assert.equal(formatClock(NaN), '');
  assert.deepEqual(recordEntries(undefined), []);
});
