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
  insertAllEvent,
  allEventRowKey,
  backoffDelay,
  isRetirement,
  resolveTheme,
  severityRowClass,
  colourSeverity,
  toViewEvent,
  recordEntries,
  formatClock,
  ENRICHED_FIELDS,
  WIRE_FIELD,
  SEVERITY_RANK,
  isUnsupportedOpError,
  isTimeField,
  filterEvents,
  eventSeverities,
  tabBadgeCount,
  linkState,
  formatTimeValue,
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

test('lastSeen never moves BACKWARDS, whatever the frame carries', () => {
  // THE BUG THIS PINS. One arrival produces TWO frames — `op: event` (carrying
  // brokerTs) and `op: alert` (usually carrying neither `event` nor `row`). If the
  // alert frame lands second with an older timestamp, or with a `row` whose
  // lastSeen predates what the event frame already wrote, the column went
  // backwards: measured 21:51:11 -> 21:51:07 while Count climbed 2 -> 3.
  //
  // "Last occurrence" is read as a monotonic clock. It must never retreat.
  let map = new Map();
  map = applyAlert(map, { key: 'a:t', applied: 1, event: { severity: 'WARNING', brokerTs: 2000 } });
  assert.equal(map.get('a:t').lastSeen, 2000);

  // An `op: alert` frame with a STALE row must not pull it back.
  map = applyAlert(map, { key: 'a:t', applied: 1, row: { severity: 'WARNING', lastSeen: 1000, count: 2 } });
  assert.equal(map.get('a:t').lastSeen, 2000, 'a stale row cannot rewind lastSeen');

  // Nor can an older event brokerTs.
  map = applyAlert(map, { key: 'a:t', applied: 2, event: { severity: 'WARNING', brokerTs: 1500 } });
  assert.equal(map.get('a:t').lastSeen, 2000);

  // A newer value always wins.
  map = applyAlert(map, { key: 'a:t', applied: 1, row: { severity: 'WARNING', lastSeen: 3000, count: 3 } });
  assert.equal(map.get('a:t').lastSeen, 3000);
});

test('an applied frame with no event and no row still moves lastSeen forward', () => {
  // The common case: `op: alert` for a NEW occurrence carries neither. Falling
  // back to Date.now() is what keeps the column alive, so it must not be
  // defeated by the "never rewind" clamp when the stored value is older.
  let map = new Map([['a:t', { key: 'a:t', severity: 'WARNING', count: 1, firstSeen: 1, lastSeen: 1000 }]]);
  const before = Date.now();
  map = applyAlert(map, { key: 'a:t', applied: 1 });
  const got = map.get('a:t').lastSeen;
  assert.ok(got >= before, `lastSeen must advance to now, got ${got}`);
  assert.equal(map.get('a:t').count, 2);
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

// ------------------------------------------------------------- all-events list
test('insertAllEvent de-duplicates on the RECORD KEY, not the message id', () => {
  // The All Events tab is "what is STORED", and storage is ONE record per
  // CONDITION. Two arrivals of the same condition — different message ids, same
  // record key — are one row here, because the query behind this tab returns one
  // row for them. Keeping both would make the list disagree with its own query.
  const KEY = 'a'.repeat(32);
  let list = insertAllEvent([], { id: 'msg-1', key: KEY, brokerTs: 1000 }, 10);
  list = insertAllEvent(list, { id: 'msg-2', key: KEY, brokerTs: 2000 }, 10);
  assert.equal(list.length, 1);
  assert.equal(list[0].brokerTs, 1000);
});

test('a live row and a queried row for one record collapse to ONE row', () => {
  // THE BUG THIS PINS. The two sources carry different identities for the same
  // record:
  //
  //   a queried row (gateway _row())     id === key === <32-hex>
  //   a live row    (worker op: event)   id === <message id>, key === <32-hex>
  //
  // De-duplicating on `id` therefore failed to collapse them, so any record that
  // arrived live and was later returned by an `op: allEvents` query appeared
  // TWICE — once with a message id, once with the record key.
  const KEY = 'b'.repeat(32);
  const queried = { id: KEY, key: KEY, brokerTs: 1000 };
  const live = { id: 'mwdsycxk-0002-mcgau7', key: KEY, brokerTs: 1000 };
  const list = insertAllEvent([queried], live, 10);
  assert.equal(list.length, 1, 'one record, one row, whichever way it arrived');
});

test('a frame from an older worker with no key still de-duplicates on id', () => {
  // Rolling update: an older worker publishes no `key`. Falling back to `id` keeps
  // such a frame from adding a duplicate row on every redelivery.
  let list = insertAllEvent([], { id: 'old-1', brokerTs: 1000 }, 10);
  list = insertAllEvent(list, { id: 'old-1', brokerTs: 2000 }, 10);
  assert.equal(list.length, 1);
});

test('allEventRowKey uses the record key, so every row lookup agrees', () => {
  // The selection list, the React key and the right-click lookup all build this
  // string. When they were three copies (`id` in one place, `key` in another) a
  // freshly-selected live row could not be found again, so Delete acted on
  // nothing.
  const KEY = 'c'.repeat(32);
  const live = { id: 'msg-1', key: KEY, brokerTs: 1000 };
  const queried = { id: KEY, key: KEY, brokerTs: 1000 };
  assert.equal(allEventRowKey(live), allEventRowKey(queried));
  assert.equal(allEventRowKey(live), `${KEY}@1000`);
  // An older frame with no `key` still produces a usable, unique key.
  assert.equal(allEventRowKey({ id: 'old-1', brokerTs: 5 }), 'old-1@5');
});

test('allEventRowKey keeps same-millisecond records distinct', () => {
  // Two conditions reported in the same millisecond share brokerTs. React needs
  // unique keys, so the record key has to stay in the composition.
  const a = { key: 'a'.repeat(32), brokerTs: 1000 };
  const b = { key: 'b'.repeat(32), brokerTs: 1000 };
  assert.notEqual(allEventRowKey(a), allEventRowKey(b));
});

test('insertAllEvent keeps the list sorted and trims the oldest', () => {
  let list = [];
  list = insertAllEvent(list, { id: 'c', brokerTs: 300 }, 10);
  list = insertAllEvent(list, { id: 'a', brokerTs: 100 }, 10);
  list = insertAllEvent(list, { id: 'b', brokerTs: 200 }, 10);
  assert.deepEqual(list.map((e) => e.id), ['a', 'b', 'c']);
  for (let i = 0; i < 10; i += 1) list = insertAllEvent(list, { id: `x${i}`, brokerTs: 1000 + i }, 3);
  assert.equal(list.length, 3);
  assert.deepEqual(list.map((e) => e.id), ['x7', 'x8', 'x9']);
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

test('toViewEvent never stringifies a compressed field into "[object Object]"', () => {
  // A compressed field is `{__gz: "<base64>"}`. `String({})` is "[object Object]",
  // and that is literally what the All events details fold rendered: the QUERY
  // paths mapped events with `toViewEvent` and skipped `decodePayload`, so `msgc`
  // reached this function still compressed. A field with no text yet must yield
  // '' so the row hides its disclosure rather than offering garbage — and a plain
  // object where a string was expected must do the same.
  const gz = { __gz: 'H4sIAAAAAAAAA0tMSgYAAAD//wMA' };
  const view = toViewEvent({ payload: { msg: 'summary ok', msgc: gz } });
  assert.equal(view.msgCompleteString, '');
  assert.equal(view.summary, 'summary ok', 'an uncompressed sibling still renders');

  // A compressed SUMMARY likewise must not render as an object.
  assert.equal(toViewEvent({ payload: { msg: gz } }).summary, '');

  // Any other stray object, e.g. a producer that sent a nested structure.
  assert.equal(toViewEvent({ payload: { msgc: { a: 1 } } }).msgCompleteString, '');
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

test('toViewEvent keeps the RECORD episode bounds when the payload has none', () => {
  // Regression: All events rendered `firstOccurrence` straight from this mapper,
  // which only consulted the payload and then fell back to `brokerTs`. `brokerTs`
  // is rewritten on every delivery, so after three identical events the All events
  // tab showed first == last == the newest delivery while Count was 3 — and the
  // Open alerts tab, which reads `firstSeen` off the row, showed the true start.
  // Two tabs, one record, disagreeing. The fold's values must win over `brokerTs`.
  const view = toViewEvent({
    brokerTs: 3000,          // newest delivery, NOT the episode start
    firstOccurrence: 1000,   // the fold's `firstSeen`
    lastOccurrence: 3000,    // the fold's `lastSeen`
    payload: {},
  });
  assert.equal(view.firstOccurrence, 1000, 'must not use brokerTs as the start');
  assert.equal(view.lastOccurrence, 3000);
});

test('toViewEvent falls back to lastSeen when the row only carries that', () => {
  const view = toViewEvent({ brokerTs: 3000, lastSeen: 2500, payload: {} });
  assert.equal(view.lastOccurrence, 2500);
});

test('a payload occurrence field still beats the record field', () => {
  // A producer that reports its own window is authoritative: the fold bounds are
  // when the BROKER saw the condition, not when the device observed it.
  const view = toViewEvent({
    brokerTs: 3000,
    firstOccurrence: 1000,
    payload: { FirstOccurrence: 500 },
  });
  assert.equal(view.firstOccurrence, 500);
});

test('toViewEvent maps the record count onto alertCount', () => {
  // The All events table renders `alertCount` — a name left over from when that
  // column was a rollup joined in from a separate alert hash. The row IS the record
  // now, so the fold's `count` is that same number, but nothing copied it across and
  // the column read `—` on every row: the `EVT_*` schema has no producer `Count`
  // field, so the payload never supplied one.
  const view = toViewEvent({ brokerTs: 3000, count: 3, payload: {} });
  assert.equal(view.alertCount, 3, 'the row count must reach the column that renders it');
});

test('alertCount is null when nothing has told us the count', () => {
  // `null` renders as `—`, which is the honest answer. Falling back to 1 would
  // claim "reported once" for a record we know nothing about.
  const view = toViewEvent({ brokerTs: 3000, payload: {} });
  assert.equal(view.alertCount, null);
});

test('a producer Count wins over the record count', () => {
  const view = toViewEvent({ brokerTs: 3000, count: 5, payload: { Count: 9 } });
  assert.equal(view.alertCount, 9);
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
  // count to 1 at the start of a new episode (event-fold.lua), and the row the
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

// ------------------------------- the badge shows what the PRODUCER reported
test('a CLEAR retired event is displayed as CLEAR, not the internal CLOSE', () => {
  // The ingress maps a producer's CLEAR onto the internal CLOSE. Showing the
  // mapped name presented a severity the producer never sends.
  const evt = {
    id: 'c1', source: 's', type: 't', severity: 'CLOSE',
    payload: { msg: 'cleared', EVT_Severity: 'CLEAR' },
  };
  const v = toViewEvent(evt);
  assert.equal(v.severity, 'CLOSE', 'internal severity is untouched, for styling');
  assert.equal(v.displaySeverity, 'CLEAR', 'the badge shows the producer value');
});

test('an untouched severity displays as itself', () => {
  const v = toViewEvent({
    id: 'w1', source: 's', type: 't', severity: 'CRITICAL',
    payload: { msg: 'x', EVT_Severity: 'MAJOR' },
  });
  assert.equal(v.displaySeverity, 'MAJOR');
});

test('a legacy event with no producer severity falls back to the internal one', () => {
  const v = toViewEvent({ id: 'l1', source: 's', type: 't', severity: 'WARNING', payload: {} });
  assert.equal(v.displaySeverity, 'WARNING');
});

test('retirement styling still keys off the internal severity', () => {
  const v = toViewEvent({
    id: 'c2', source: 's', type: 't', severity: 'CLOSE',
    payload: { msg: 'cleared', EVT_Severity: 'CLEAR' },
  });
  assert.equal(isRetirement(v.severity), true, 'row still dims as a retirement');
});

// ------------------------------------------- badge colour follows the badge text
test('colourSeverity gives a closed WARNING row an amber colour', () => {
  // THE REPORTED BUG. An operator close writes `severity: CLOSE` on the record but
  // leaves the payload alone, so the badge said WARNING — in GREEN, because the
  // colour was taken from the internal CLOSE. Colour is the primary signal here, so
  // it must agree with the word beside it.
  assert.equal(colourSeverity('CLOSE', 'WARNING'), 'WARNING');
  assert.equal(colourSeverity('CLOSE', 'MAJOR'), 'CRITICAL');
  assert.equal(colourSeverity('CLOSE', 'MINOR'), 'WARNING');
  assert.equal(colourSeverity('CLOSE', 'FATAL'), 'FATAL');
  assert.equal(colourSeverity('CLOSE', 'INDETERMINATE'), 'WARNING');
});

test('a genuine clear still renders green', () => {
  // The retirement colour is not lost by this change: CLEAR and HARMLESS both map
  // onto CLOSE, which is the documented retirement colour (docs/frontend.md §5.1).
  assert.equal(colourSeverity('CLOSE', 'CLEAR'), 'CLOSE');
  assert.equal(colourSeverity('CLOSE', 'HARMLESS'), 'CLOSE');
  assert.equal(colourSeverity('HARMLESS', 'INFORMATION'), 'HARMLESS');
  assert.equal(colourSeverity('CLOSE', 'CLOSE'), 'CLOSE');
});

test('colourSeverity falls back to the stored severity for an unknown label', () => {
  // A producer inventing its own value, a legacy record and an absent label must
  // all keep the internal colour rather than losing it.
  assert.equal(colourSeverity('CRITICAL', 'PANIC'), 'CRITICAL');
  assert.equal(colourSeverity('WARNING', ''), 'WARNING');
  assert.equal(colourSeverity('WARNING', undefined), 'WARNING');
  assert.equal(colourSeverity('WARNING'), 'WARNING');
});

test('colourSeverity is case- and whitespace-insensitive in the label', () => {
  assert.equal(colourSeverity('CLOSE', ' warning '), 'WARNING');
  assert.equal(colourSeverity('CLOSE', 'clear'), 'CLOSE');
});

// --------------------- the Information panel renders every time the same way
test('epoch ms, ISO and nested time fields all render as the local clock', () => {
  const entries = recordEntries({
    id: 'e1', brokerTs: 1794319171950, closedAt: 1794319171950,
    ts: '2026-11-10T10:00:00.000Z',
    payload: { FirstOccurrence: '2026-11-10T09:00:00Z' },
  });
  const by = Object.fromEntries(entries);
  // Each begins with the same YYYY-MM-DD HH:MM:SS shape...
  for (const k of ['brokerTs', 'closedAt', 'ts', 'payload.FirstOccurrence']) {
    assert.match(by[k], /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+\(/, `${k} not formatted: ${by[k]}`);
  }
  // ...and keeps the raw value so nothing is lost.
  assert.match(by.brokerTs, /\(1794319171950\)$/);
  assert.match(by.ts, /\(2026-11-10T10:00:00\.000Z\)$/);
});

test('a NON-time field keeps its raw value even if it looks like an epoch', () => {
  // Guessing by value would rewrite a legitimate number. Matching is by name.
  const by = Object.fromEntries(recordEntries({ count: 1794319171950, size: '1794319171950' }));
  assert.equal(by.count, '1794319171950');
  assert.equal(by.size, '1794319171950');
});

test('separator and case variants of a time field are recognised', () => {
  for (const name of ['brokerTs', 'broker_ts', 'broker-ts', 'BROKERTS']) {
    assert.equal(isTimeField(name), true, `${name} should be a time field`);
  }
  for (const name of ['count', 'severity', 'msg', 'id']) {
    assert.equal(isTimeField(name), false, `${name} should not be a time field`);
  }
});

test('a time field holding something unparseable falls back to the raw string', () => {
  // Better a plain value than "Invalid Date".
  const by = Object.fromEntries(recordEntries({ brokerTs: 'not-a-date' }));
  assert.equal(by.brokerTs, 'not-a-date');
});

test('epoch SECONDS are read as seconds, not as 1970', () => {
  const by = Object.fromEntries(recordEntries({ brokerTs: 1794319171 }));
  assert.match(by.brokerTs, /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/);
  assert.ok(!by.brokerTs.startsWith('1970'), by.brokerTs);
});

test('formatTimes:false leaves the record completely raw', () => {
  const by = Object.fromEntries(recordEntries({ brokerTs: 1794319171950 }, { formatTimes: false }));
  assert.equal(by.brokerTs, '1794319171950');
});

// --------------------------------------------------- All Events free-text search
test('filterEvents matches across the visible fields', () => {
  const list = [
    { id: 'e1', source: 'core-sw-07', type: 'net.link.down', summary: 'uplink flapping', agent: 'Prometheus' },
    { id: 'e2', source: 'db-node-03', type: 'hw.psu.voltage', summary: 'PSU out of range', agent: 'Zabbix' },
  ];
  assert.equal(filterEvents(list, 'flapping').length, 1);
  assert.equal(filterEvents(list, 'zabbix').length, 1);
  assert.equal(filterEvents(list, 'core-sw').length, 1);
  assert.equal(filterEvents(list, 'nothing').length, 0);
});

test('filterEvents is case-insensitive and tolerates empty input', () => {
  const list = [{ id: 'e1', source: 'A', summary: 'X' }];
  assert.equal(filterEvents(list, '  a  ').length, 1);
  assert.equal(filterEvents(list, '').length, 1, 'an empty filter matches everything');
  assert.deepEqual(filterEvents(null, 'x'), []);
  assert.deepEqual(filterEvents([], 'x'), []);
});

test('eventSeverities offers what the badge SHOWS, from the data only', () => {
  // The badge renders displaySeverity, so offering internal names would give the
  // operator `CLOSE` as an option for a row labelled `CLEAR`.
  const list = [
    { id: 'a', severity: 'CLOSE', displaySeverity: 'CLEAR' },
    { id: 'b', severity: 'CRITICAL', displaySeverity: 'MAJOR' },
    { id: 'c', severity: 'CRITICAL', displaySeverity: 'MAJOR' },
    { id: 'd', severity: 'WARNING' },   // no producer value: falls back
  ];
  assert.deepEqual(eventSeverities(list), ['CLEAR', 'MAJOR', 'WARNING']);
  assert.deepEqual(eventSeverities([]), []);
  assert.deepEqual(eventSeverities(null), []);
});

test('filterEvents applies the severity cut and the text cut together', () => {
  const list = [
    { id: 'a', severity: 'CRITICAL', displaySeverity: 'MAJOR', summary: 'psu fault' },
    { id: 'b', severity: 'WARNING', displaySeverity: 'MINOR', summary: 'link flap' },
    { id: 'c', severity: 'CLOSE', displaySeverity: 'CLEAR', summary: 'link restored' },
  ];
  assert.equal(filterEvents(list, '', '').length, 3, 'no cut at all');
  assert.deepEqual(filterEvents(list, '', 'MAJOR').map((e) => e.id), ['a']);
  // Selecting a DISPLAYED label matches only rows showing it. `MAJOR` maps down to
  // internal CRITICAL, so matching the internal field too would have pulled in `a`
  // when the operator asked for CRITICAL — a filter that returns rows labelled
  // something else.
  assert.deepEqual(filterEvents(list, '', 'CRITICAL'), []);
  assert.deepEqual(filterEvents(list, '', 'CLEAR').map((e) => e.id), ['c']);
  // The internal name is not an offered option and does not match either.
  assert.deepEqual(filterEvents(list, '', 'CLOSE'), []);
  // A row with no producer value falls back to the internal name, which is what its
  // badge renders, so that label is selectable.
  assert.deepEqual(filterEvents([{ id: 'x', severity: 'WARNING' }], '', 'WARNING').map((e) => e.id), ['x']);
  assert.deepEqual(filterEvents(list, 'link', 'MINOR').map((e) => e.id), ['b']);
  assert.deepEqual(filterEvents(list, 'psu', 'MINOR'), []);
});

// ------------------------------------------- tab badges are per-tab, never shared
test('each tab reports its OWN count', () => {
  const counts = { alerts: 1, events: 2, allEvents: 3 };
  assert.equal(tabBadgeCount('alerts', counts), 1);
  assert.equal(tabBadgeCount('events', counts), 2);
  assert.equal(tabBadgeCount('all', counts), 3);
});

test('History has no badge, and asking for one throws', () => {
  // Its count would be the operator's own page size (50/100/200), not a fact about
  // the system. A number reporting the limit you just set is noise, so the branch
  // was removed AND the tab opts out. If someone re-adds a `history` branch without
  // re-adding the badge, this test is the tripwire.
  assert.throws(() => tabBadgeCount('history', { alerts: 1, events: 2, allEvents: 3 }),
    /unknown tab/, 'history must not have a badge count');
});

test('an unknown tab throws instead of borrowing another tab count', () => {
  // Regression: the badge was a ternary chain ending in `: closed.count`, so the
  // All events tab silently showed the CLOSED count — a number beside an empty
  // table, which reads as a broken query rather than a wrong badge.
  assert.throws(() => tabBadgeCount('nope', { alerts: 1, events: 2, allEvents: 3 }),
    /unknown tab/);
});

test('the All events badge is 0 when its own list is empty, whatever the others hold', () => {
  // The exact reported symptom: All events showed another tab's number beside an
  // empty table.
  const counts = { alerts: 5, events: 77, allEvents: 0 };
  assert.equal(tabBadgeCount('all', counts), 0);
  assert.equal(tabBadgeCount('events', counts), 77);
});

// ---------------------------------------------- the chip link reflects BOTH chips
test('the link is ok only when the socket AND the database are healthy', () => {
  // The whole point of the pair: a connected socket with failing reads is a stale
  // board behind a green chip.
  assert.equal(linkState('connected', true), 'ok');
  assert.equal(linkState('connected', false), 'bad', 'socket fine, database down');
  assert.equal(linkState('reconnecting', true), 'bad', 'database fine, socket down');
  assert.equal(linkState('reconnecting', false), 'bad');
});

test('an unreported database state is unknown, not ok and not bad', () => {
  // Rendering unknown as red would cry wolf on every page load; rendering it green
  // would be a lie.
  assert.equal(linkState('connected', null), 'unknown');
  assert.equal(linkState('connected', undefined), 'unknown');
  assert.equal(linkState('idle', null), 'unknown');
});

// --------------------------------------------------- row shape and count authority
test('applyAlert takes timestamps from a gateway row (firstOccurrence/lastOccurrence)', () => {
  // The gateway's row is the record itself, and its timestamp fields are
  // `firstOccurrence` / `lastOccurrence`. Reading only `firstSeen` ignored them and
  // fell through to `Date.now()`, so "First occurrence" was the moment the browser
  // got the frame — different on every reload.
  const map = applyAlert(new Map(), {
    key: 'k', applied: 1,
    row: { severity: 'WARNING', firstOccurrence: 1000, lastOccurrence: 5000, count: 4 },
  });
  const row = map.get('k');
  assert.equal(row.firstSeen, 1000);
  assert.equal(row.lastSeen, 5000);
  assert.equal(row.count, 4);
});

test('the server count wins over a local increment', () => {
  // Count 6 for three arrivals was the reported bug. Preferring the fold's own
  // number removes the whole class: two increments can disagree, one number cannot.
  let map = new Map();
  map = applyAlert(map, { key: 'k', applied: 1, row: { count: 1, lastSeen: 100 } });
  // A LOCAL fold (no row) must still advance, for a frame that carries nothing.
  map = applyAlert(map, { key: 'k', applied: 1, event: { brokerTs: 200 } });
  assert.equal(map.get('k').count, 2);
  // But when the server states the number, it is used verbatim — not 3.
  map = applyAlert(map, { key: 'k', applied: 1, row: { count: 3, lastSeen: 300 } });
  assert.equal(map.get('k').count, 3);
});

test('a row with no firstOccurrence falls back to the event broker timestamp', () => {
  // Falling back to Date.now() is only acceptable when there is NO server value and
  // NO event timestamp — otherwise the column drifts on every reload.
  const map = applyAlert(new Map(), { key: 'k', applied: 1, event: { brokerTs: 777 } });
  assert.equal(map.get('k').firstSeen, 777);
});
