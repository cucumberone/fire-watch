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
  isUnsupportedOpError,
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

test('applied=2 moves lastSeen and DOES increment count', () => {
  // 2 is kept as an alias for 1 so a gateway on the previous build cannot freeze
  // a row mid-rollout. Both mean "applied"; duplicates never reach this branch
  // because the fold drops them and returns 0.
  let map = new Map([['a:t', { key: 'a:t', severity: 'WARNING', count: 5, firstSeen: 1, lastSeen: 100 }]]);
  map = applyAlert(map, { key: 'a:t', applied: 2, event: { severity: 'CRITICAL', brokerTs: 900 } });
  assert.equal(map.get('a:t').count, 6, 'an applied event is counted');
  assert.equal(map.get('a:t').lastSeen, 900, 'last occurrence always moves');
  assert.equal(map.get('a:t').severity, 'CRITICAL', 'severity escalation is still shown');
});

// --------------------------------------------------- duplicate handling (drop)
// A true duplicate never reaches the client: the fold's (timestamp, id)
// watermark returns 0 for it, so there is no "coalesced" concept and no "+N"
// marker. Count counts events; Last occurrence is the newest event's arrival.
test('applied=0 (a dropped duplicate) changes nothing at all', () => {
  const before = { key: 'a:t', severity: 'CRITICAL', count: 4, firstSeen: 1, lastSeen: 500 };
  const map = applyAlert(new Map([['a:t', before]]), {
    key: 'a:t',
    applied: 0,
    event: { severity: 'CLOSE', brokerTs: 999 },
  });
  const after = map.get('a:t');
  assert.equal(after.count, 4, 'a duplicate must not increment count');
  assert.equal(after.lastSeen, 500, 'a duplicate must not move last occurrence');
  assert.equal(after.severity, 'CRITICAL', 'a duplicate must not change severity');
});

test('every counted event advances last occurrence', () => {
  let map = new Map();
  map = applyAlert(map, { key: 'a:t', applied: 1, event: { severity: 'WARNING', brokerTs: 1000 } });
  map = applyAlert(map, { key: 'a:t', applied: 1, event: { severity: 'WARNING', brokerTs: 2000 } });
  map = applyAlert(map, { key: 'a:t', applied: 1, event: { severity: 'WARNING', brokerTs: 3000 } });
  assert.equal(map.get('a:t').count, 3);
  assert.equal(map.get('a:t').firstSeen, 1000, 'first occurrence never moves');
  assert.equal(map.get('a:t').lastSeen, 3000, 'last occurrence tracks the newest event');
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

test('insertEvent ignores a true duplicate (same id AND brokerTs)', () => {
  // It previously ignored any repeated id, which was wrong: an absorbed repeat
  // keeps the original id and only differs by brokerTs, and the tail must show
  // each arrival. Only an identical frame is a duplicate.
  let list = insertEvent([], { id: 'a', brokerTs: 1 }, 10);
  list = insertEvent(list, { id: 'a', brokerTs: 1 }, 10);
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

test('toViewEvent maps the five enriched producer fields', () => {
  const view = toViewEvent({
    payload: {
      EVT_Device: 'core-sw-07',
      EVT_Agent: 'Prometheus',
      EVT_AgentOwner: 'AM-000042',
      EVT_EventKey: 'net.link.down',
      EVT_Environment: 'production',
    },
  });
  assert.equal(view.device, 'core-sw-07');
  assert.equal(view.agent, 'Prometheus');
  assert.equal(view.agentOwner, 'AM-000042');
  assert.equal(view.eventKey, 'net.link.down');
  assert.equal(view.environment, 'production');
});

test('toViewEvent tolerates lowerCamel spellings of the same fields', () => {
  // A producer that has already normalised its own casing should still render.
  const view = toViewEvent({
    payload: {
      device: 'd1',
      agent: 'a1',
      agentOwner: 'o1',
      eventKey: 'k1',
      environment: 'stage',
    },
  });
  assert.equal(view.device, 'd1');
  assert.equal(view.agent, 'a1');
  assert.equal(view.agentOwner, 'o1');
  assert.equal(view.eventKey, 'k1');
  assert.equal(view.environment, 'stage');
});

test('toViewEvent does not confuse the event key with the agent owner', () => {
  const view = toViewEvent({ payload: { EVT_EventKey: 'k1' } });
  assert.equal(view.eventKey, 'k1');
  assert.equal(view.agentOwner, '', 'only the event key was sent');
});

test('toViewEvent leaves enriched fields empty when the producer did not send them', () => {
  const view = toViewEvent({ payload: { msg: 'x' } });
  for (const f of ENRICHED_FIELDS) {
    assert.equal(view[f.key], '', f.key);
  }
});

test('ENRICHED_FIELDS covers exactly the fields the inspector relies on', () => {
  const view = toViewEvent({
    payload: {
      EVT_Device: 'd',
      EVT_Agent: 'a',
      EVT_AgentOwner: 'o',
      EVT_EventKey: 'k',
      EVT_Environment: 'e',
    },
  });
  for (const f of ENRICHED_FIELDS) {
    assert.ok(f.key in view, `${f.key} is mapped`);
    assert.ok(f.wire.includes(`EVT_${f.key[0].toUpperCase()}${f.key.slice(1)}`),
      `${f.key} lists the EVT_ wire spelling`);
  }
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
// The format is fixed at YYYY-MM-DD HH:MM:SS in 24-hour LOCAL time. It is
// hand-rolled rather than locale-based so every operator sees the same string,
// and so a 12-hour clock cannot hide the AM/PM distinction off the edge of a
// narrow column.
test('formatClock returns empty for unusable times', () => {
  assert.equal(formatClock(undefined), '');
  assert.equal(formatClock(NaN), '');
  assert.equal(formatClock(null), '');
  assert.equal(formatClock('not a number'), '');
});

test('formatClock renders YYYY-MM-DD HH:MM:SS', () => {
  // Built from local components so the assertion is timezone-independent.
  const d = new Date(2026, 10, 9, 13, 5, 7); // 9 Nov 2026, 13:05:07 local
  assert.equal(formatClock(d.getTime()), '2026-11-09 13:05:07');
});

test('formatClock is 24-hour, so 1pm is 13 not 01', () => {
  const am = new Date(2026, 0, 2, 1, 4, 9);
  const pm = new Date(2026, 0, 2, 13, 4, 9);
  assert.equal(formatClock(am.getTime()), '2026-01-02 01:04:09');
  assert.equal(formatClock(pm.getTime()), '2026-01-02 13:04:09');
});

test('formatClock zero-pads every component', () => {
  const d = new Date(2026, 0, 2, 3, 4, 5);
  assert.equal(formatClock(d.getTime()), '2026-01-02 03:04:05');
});

// ---------------------------------------------- deploy skew: unsupported ops
test('an unknown-op rejection is recognised for the op we sent', () => {
  // The exact frame a gateway older than this bundle returns for `resync`.
  assert.equal(
    isUnsupportedOpError({ op: 'error', code: 'bad_op', message: 'unknown op resync' }, 'resync'),
    true,
  );
});

test('a bad_op for a DIFFERENT op is not mistaken for resync', () => {
  // Otherwise an ack bug would silently disable the auto-refresh.
  assert.equal(
    isUnsupportedOpError({ op: 'error', code: 'bad_op', message: 'unknown op ack' }, 'resync'),
    false,
  );
});

test('other error frames are never treated as an unsupported op', () => {
  assert.equal(isUnsupportedOpError({ op: 'error', code: 'rate_limited', message: 'too many frames' }, 'resync'), false);
  assert.equal(isUnsupportedOpError({ op: 'error', code: 'bad_op' }, 'resync'), false);
  assert.equal(isUnsupportedOpError({ op: 'alert', key: 'a:b' }, 'resync'), false);
  assert.equal(isUnsupportedOpError(null, 'resync'), false);
  assert.equal(isUnsupportedOpError(undefined, 'resync'), false);
});

test('a malformed bad_op message does not arm the disable', () => {
  assert.equal(isUnsupportedOpError({ op: 'error', code: 'bad_op', message: 'unknown operation' }, 'resync'), false);
});

// -------------------------------- the blank-page regression (content duplicates)
test('an eventless frame for an UNSEEN key creates no row', () => {
  // This is the exact frame the gateway fans out when the worker absorbs a
  // content duplicate: {kind:'occurrence'} has no event attached. Creating a row
  // from it produced one with `severity: undefined`, and rendering that threw
  // inside Badge, which unmounted the entire tree and left a blank page.
  const out = applyAlert(new Map(), { key: 'probe-device:static.probe', applied: 1 });
  assert.equal(out.size, 0);
});

test('an eventless frame still increments a row that already exists', () => {
  // The count is owned by the fold, so a repeat must move it even though the
  // client has no event to attach.
  const first = applyAlert(new Map(), {
    key: 'd:probe', applied: 1,
    event: { severity: 'WARNING', brokerTs: 1000 },
  });
  const out = applyAlert(first, { key: 'd:probe', applied: 1 });
  assert.equal(out.size, 1);
  assert.equal(out.get('d:probe').count, 2);
  assert.equal(out.get('d:probe').severity, 'WARNING'); // never downgraded
  assert.ok(out.get('d:probe').lastSeen > 0);
});

test('a row is never left without a severity', () => {
  // Whatever sequence of frames arrives, every row must be renderable.
  let map = new Map();
  const frames = [
    { key: 'a:b', applied: 1 },                                  // unseen, no event
    { key: 'a:b', applied: 1, event: { severity: 'FATAL', brokerTs: 1 } },
    { key: 'a:b', applied: 1 },                                  // repeat, no event
    { key: 'a:b', applied: 2 },                                  // coalesced alias
  ];
  for (const f of frames) map = applyAlert(map, f);
  for (const row of map.values()) {
    assert.equal(typeof row.severity, 'string', `row ${row.key} has no severity`);
    assert.ok(row.severity.length > 0);
  }
});

// ------------------- a repeat after a CLEAR must appear without a page refresh
test('an occurrence frame carrying a server row CREATES the missing row', () => {
  // The reported bug: probe -> CLEAR retires the alert -> probe again. The
  // repeat is absorbed as a content duplicate, so the frame has no event. Before
  // the row was attached, a client that had dropped the alert on the CLEAR had
  // nothing to rebuild from and the row only appeared on the next snapshot,
  // i.e. a browser refresh.
  const out = applyAlert(new Map(), {
    key: 'probe-device:static.probe',
    applied: 1,
    row: {
      key: 'probe-device:static.probe',
      severity: 'WARNING',
      firstSeen: 1000,
      lastSeen: 2000,
      count: 4,
      event: { id: 'static-probe-0001', severity: 'WARNING', brokerTs: 2000 },
    },
  });
  assert.equal(out.size, 1);
  const row = out.get('probe-device:static.probe');
  assert.equal(row.severity, 'WARNING');
  assert.equal(row.firstSeen, 1000);
  assert.equal(row.lastSeen, 2000);
  assert.ok(row.count >= 4);              // server's count, plus this occurrence
  assert.ok(row.event);                   // so the Summary column is filled
});

test('a server row never blanks a known severity', () => {
  const first = applyAlert(new Map(), {
    key: 'd:k', applied: 1, event: { severity: 'FATAL', brokerTs: 10 },
  });
  const out = applyAlert(first, { key: 'd:k', applied: 1, row: { count: 9 } });
  assert.equal(out.get('d:k').severity, 'FATAL');
  assert.equal(out.get('d:k').firstSeen, first.get('d:k').firstSeen);
});

test('row-supplied event fills the Summary when the frame has none', () => {
  const out = applyAlert(new Map(), {
    key: 'd:k', applied: 1,
    row: { severity: 'WARNING', count: 1, event: { id: 'e1', brokerTs: 5 } },
  });
  assert.equal(out.get('d:k').event.id, 'e1');
});

// ------------------------ the fold's count is authoritative, never added to
test('a re-fire after a CLEAR shows Count 1, not 2', () => {
  // Reported bug: probe -> CLEAR -> probe again showed Count 2. The fold resets
  // count to 1 at the start of a new episode (alert-fold.lua), and the row the
  // gateway attaches already reflects that, so adding one on the client made it 2.
  const KEY = 'probe-device:static.probe';
  let map = new Map();

  // 1. opens
  map = applyAlert(map, {
    key: KEY, applied: 1,
    event: { severity: 'WARNING', brokerTs: 1000 },
  });
  assert.equal(map.get(KEY).count, 1);

  // 2. retired by a CLEAR
  map = applyAlert(map, { key: KEY, applied: 3 });
  assert.equal(map.has(KEY), false);

  // 3. re-fired: new episode, the row from the server has count 1
  map = applyAlert(map, {
    key: KEY, applied: 1,
    row: { severity: 'WARNING', count: 1, firstSeen: 2000, lastSeen: 2000 },
  });
  assert.equal(map.get(KEY).count, 1, 'a new episode starts at 1');
});

test('a repeat within the same episode follows the server count', () => {
  const KEY = 'd:k';
  let map = applyAlert(new Map(), {
    key: KEY, applied: 1, event: { severity: 'WARNING', brokerTs: 1 },
  });
  assert.equal(map.get(KEY).count, 1);
  // The fold's own count is 2 now, and the row carries it.
  map = applyAlert(map, { key: KEY, applied: 1, row: { count: 2 } });
  assert.equal(map.get(KEY).count, 2, 'not 3 — the row already counted this one');
});

test('without a row the client still increments a known row', () => {
  // A gateway that predates the row attachment sends bare frames; the count must
  // keep moving rather than freezing.
  const KEY = 'd:k';
  let map = applyAlert(new Map(), {
    key: KEY, applied: 1, event: { severity: 'WARNING', brokerTs: 1 },
  });
  map = applyAlert(map, { key: KEY, applied: 1 });
  assert.equal(map.get(KEY).count, 2);
});

// ---------------------- the live tail shows ARRIVALS, not unique records
test('an absorbed repeat with a new brokerTs is a distinct arrival', () => {
  // A repeat keeps the original's id (the copy was never stored), so deduping on
  // id alone hid every repeat after the first: two runs of the same probe showed
  // one row in the tail.
  let list = [];
  list = insertEvent(list, { id: 'p1', brokerTs: 1000 }, 500);
  list = insertEvent(list, { id: 'p1', brokerTs: 2000 }, 500);
  assert.equal(list.length, 2, 'both arrivals are shown');
  assert.deepEqual(list.map((e) => e.brokerTs), [1000, 2000]);
});

test('a redelivered frame (same id and brokerTs) is still dropped', () => {
  let list = [];
  list = insertEvent(list, { id: 'p1', brokerTs: 1000 }, 500);
  list = insertEvent(list, { id: 'p1', brokerTs: 1000 }, 500);
  assert.equal(list.length, 1);
});

test('the limit still bounds a stream of repeats', () => {
  let list = [];
  for (let i = 0; i < 10; i += 1) list = insertEvent(list, { id: 'p1', brokerTs: i + 1 }, 3);
  assert.equal(list.length, 3);
  assert.deepEqual(list.map((e) => e.brokerTs), [8, 9, 10]);
});
