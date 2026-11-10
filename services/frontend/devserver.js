/**
 * Dev harness: serves the built frontend and a mock WebSocket that emits a
 * realistic event stream. Used only to take screenshots — not shipped.
 *
 *   node devserver.js
 *   open http://127.0.0.1:5599
 */
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';
import { gzipSync } from 'node:zlib';

const PORT = Number(process.env.PORT || 5599);
// Module-relative, NOT cwd-relative: the terminal may run this from anywhere.
const DIST = fileURLToPath(new URL('./dist/', import.meta.url));

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
};

const server = http.createServer(async (req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/config.json') {
    res.writeHead(200, { 'content-type': MIME['.json'], 'cache-control': 'no-store' });
    res.end(JSON.stringify({ wsUrl: `ws://127.0.0.1:${PORT}/ws`, backfillDefaultMinutes: 15, resolveWsHostFromPage: false }));
    return;
  }
  if (url === '/healthz') { res.writeHead(200).end('ok\n'); return; }
  let file = path.join(DIST, url === '/' ? 'index.html' : url);
  if (!existsSync(file)) file = path.join(DIST, 'index.html');
  try {
    const body = await readFile(file);
    res.writeHead(200, {
      'content-type': MIME[path.extname(file)] || 'application/octet-stream',
      'cache-control': 'no-store',
    });
    res.end(body);
  } catch {
    res.writeHead(404).end();
  }
});

const wss = new WebSocketServer({ server, path: '/ws' });

// ---------------------------------------------------------------- fake stream
const now = Date.now();
const SOURCES = ['edge-gw-01', 'edge-gw-02', 'core-sw-07', 'sensor-rack-a'];
const TYPES = ['link.down', 'power.fault', 'telemetry.temperature', 'disk.smart'];

// The upstream payload fields a real producer sends; the mock must populate them
// or the enriched columns would be empty in every screenshot.
const ENRICH = [
  { EVT_Agent: 'Prometheus', EVT_AgentOwner: 'AM-000042', EVT_Device: 'core-sw-07', EVT_EventKey: 'net.link.down', EVT_Environment: 'production' },
  { EVT_Agent: 'Zabbix', EVT_AgentOwner: 'AM-000117', EVT_Device: 'db-node-03', EVT_EventKey: 'hw.psu.voltage', EVT_Environment: 'production' },
  { EVT_Agent: 'SNMP', EVT_AgentOwner: 'AM-000208', EVT_Device: 'sensor-rack-a', EVT_EventKey: 'env.temperature', EVT_Environment: 'stage' },
  { EVT_Agent: 'SMARTd', EVT_AgentOwner: 'AM-000315', EVT_Device: 'edge-gw-02', EVT_EventKey: 'disk.predictive', EVT_Environment: 'development' },
];

function mkEvent(i, overrides = {}) {
  const first = now - (40 - i) * 45_000;
  // The INTERNAL severity, as the ingress emits it after mapping the producer's
  // value down onto five levels. The producer's own severity rides along in
  // `payload.EVT_Severity`, exactly as the real ingress preserves it — without
  // this the mock showed `CLOSE`, a value no producer ever sends, because the
  // badge falls back to the internal name when the original is absent.
  const internal = ['CRITICAL', 'WARNING', 'FATAL', 'HARMLESS', 'CLOSE'][i % 5];
  const producerSeverity = {
    CRITICAL: 'MAJOR',
    WARNING: 'MINOR',
    FATAL: 'CRITICAL',
    HARMLESS: 'INFORMATION',
    CLOSE: 'CLEAR',
  }[internal];
  return {
    id: `evt-${i}`,
    source: SOURCES[i % SOURCES.length],
    type: TYPES[i % TYPES.length],
    severity: internal,
    ts: new Date(first).toISOString(),
    brokerTs: first,
    payload: {
      EVT_Severity: producerSeverity,
      msg: [
        'uplink flapping on port Gi0/24',
        'PSU 2 input voltage out of range',
        'inlet temperature 41.2C above threshold',
        'SMART predictive failure on /dev/sda',
        'link restored after 3 retries',
        'operator acknowledged and closed',
      ][i % 6],
      FirstOccurrence: new Date(first).toISOString(),
      LastOccurrence: new Date(first + 30_000).toISOString(),
      Count: (i % 5) + 1,
      ...ENRICH[i % ENRICH.length],
      ...(i % 3 === 0
        ? {
            msgc: {
              __gz: gzipSync(
                Buffer.from(
                  `--- device report ---\ndevice: ${SOURCES[i % SOURCES.length]}\ntimestamp: ${new Date(now).toISOString()}\n\n` +
                  `interface Gi0/24\n  state        : flapping\n  flaps (1h)   : 14\n  last change  : 2s ago\n  ` +
                  `speed        : 1000baseT\n  duplex       : full\n  errors in    : 0\n  errors out   : 0\n\n` +
                  `neighbour\n  remote       : core-sw-07 / Gi1/0/12\n  cdp holdtime : 120\n\n` +
                  `recommendation: check SFP and patch cable; if flaps persist, replace the optics.\n`,
                  'utf8',
                ),
              ).toString('base64'),
            },
          }
        : {}),
    },
    ...overrides,
  };
}

// Ack state, shared across sockets the way the real gateway shares it through
// fw:alert:{key} in Dragonfly.
const acks = new Map();

/**
 * What the mock currently considers "open".
 *
 * Tracked state rather than a hard-coded list, because a resync must be
 * idempotent with respect to the live demo stream: if it answered with a fixed
 * set it would resurrect rows the stream had just retired, and the local
 * behaviour would disagree with the real gateway (which derives the set from
 * `fw:idx:alert:open`). That makes the harness useless for the exact bug this
 * feature exists to fix.
 */
const openAlerts = new Map();

/** Apply a fold-like outcome to the mock's open set. */
function foldMock(evt, applied) {
  if (!evt?.source || !evt?.type) return;
  const key = `${evt.source}:${evt.type}`;
  if (applied === 3) { openAlerts.delete(key); return; }
  if (applied === 1 || applied === 2) {
    const prev = openAlerts.get(key);
    const now = evt.brokerTs ?? Date.now();
    openAlerts.set(key, {
      key,
      severity: evt.severity,
      firstSeen: prev ? prev.firstSeen : now,
      lastSeen: now,
      count: prev ? prev.count + 1 : 1,
    });
  }
}

function mockOpenAlerts() {
  return [...openAlerts.values()];
}

/**
 * Retired alerts for the Closed tab.
 *
 * Populated from the same fold path that feeds the open set, so a retirement in
 * the demo moves a key from one tab to the other rather than duplicating it —
 * which is the invariant the real fold maintains by writing both indexes in one
 * script.
 */
const closedMock = new Map();

/**
 * Every event the mock has produced, keyed by id.
 *
 * The real worker writes each accepted event to a global timeline; the mock keeps
 * this Map so `op: allEvents` can answer a time-windowed query without inventing
 * rows. Populated from the same emit path as the live stream, so the two agree.
 */
const mockEventLog = new Map();

/** Record an event for the All Events tab. Bounded like the real index. */
function logMockEvent(evt) {
  if (!evt?.id) return;
  mockEventLog.set(evt.id, evt);
  if (mockEventLog.size > 5000) {
    // Drop the oldest by brokerTs; the real index prunes by age instead.
    const oldest = [...mockEventLog.entries()]
      .sort((a, b) => (a[1].brokerTs ?? 0) - (b[1].brokerTs ?? 0))
      .slice(0, 500);
    for (const [id] of oldest) mockEventLog.delete(id);
  }
}

// Seeded at load, not on subscribe. Seeding inside the subscribe handler made the
// tab's contents depend on whether `op: closed` arrived before or after the first
// subscribe — the app asks for the list when the tab is opened, which can happen
// first. Seeding here makes the mock deterministic.
{
  const seeds = [
    { key: 'db-node-03:hw.psu.voltage', severity: 'CLOSE', at: 6 * 3600e3,
      src: 'db-node-03', type: 'hw.psu.voltage', agent: 'Zabbix', count: 3 },
    { key: 'core-sw-07:net.link.down', severity: 'HARMLESS', at: 26 * 3600e3,
      src: 'core-sw-07', type: 'net.link.down', agent: 'Prometheus', count: 11 },
    { key: 'edge-gw-02:disk.predictive', severity: 'CLOSE', at: 3 * 86400e3,
      src: 'edge-gw-02', type: 'disk.predictive', agent: 'SMARTd', count: 1 },
    { key: 'sensor-rack-a:env.temperature', severity: 'CLOSE', at: 5 * 86400e3,
      src: 'sensor-rack-a', type: 'env.temperature', agent: 'SNMP', count: 6 },
  ];
  for (const seed of seeds) {
    const closedAt = Date.now() - seed.at;
    closedMock.set(seed.key, {
      key: seed.key,
      severity: seed.severity,
      firstSeen: closedAt - 900e3,
      lastSeen: closedAt - 60e3,
      count: seed.count,
      closedAt,
      closedBy: 'seed-' + seed.key,
      state: 'closed',
      event: {
        id: 'seed-' + seed.key, source: seed.src, type: seed.type,
        severity: seed.severity, brokerTs: closedAt,
        payload: {
          EVT_Severity: seed.severity === 'CLOSE' ? 'CLEAR' : 'INFORMATION',
          msg: seed.type + ' cleared on ' + seed.src,
          EVT_Device: seed.src, EVT_EventKey: seed.type, EVT_Agent: seed.agent,
          EVT_AgentOwner: 'AM-000100', EVT_Environment: 'production',
        },
      },
    });
  }
}

function retireMock(evt, applied) {
  if (applied !== 3 || !evt?.source || !evt?.type) return;
  const key = `${evt.source}:${evt.type}`;
  const open = openAlerts.get(key);
  const closedAt = evt.brokerTs ?? Date.now();
  if (!closedMock.has(key)) {
    closedMock.set(key, {
      key,
      severity: evt.severity,
      firstSeen: open?.firstSeen ?? closedAt,
      lastSeen: open?.lastSeen ?? closedAt,
      count: open?.count ?? 1,
      closedAt,
      closedBy: evt.id ?? null,
      state: 'closed',
      event: { ...evt, payload: evt.payload },
    });
  }
  openAlerts.delete(key);
}

wss.on('connection', async (ws) => {
  ws.on('message', async (raw) => {
    let frameIn;
    try {
      frameIn = JSON.parse(raw.toString());
    } catch { return; }

    // ---- op: alerts — bulk ack-state read, mirroring handleAlertQuery.
    if (frameIn.op === 'alerts') {
      const alertsOut = [];
      for (const key of frameIn.keys || []) {
        const ack = acks.get(key);
        alertsOut.push(ack ? { key, ack } : null);
      }
      ws.send(JSON.stringify({ op: 'alerts', alerts: alertsOut.filter(Boolean) }));
      return;
    }

    // ---- op: ack — first one wins, mirroring the Lua in ackAlert().
    if (frameIn.op === 'ack') {
      const key = frameIn.key;
      if (typeof key !== 'string' || key === '') {
        ws.send(JSON.stringify({ op: 'error', code: 'bad_key', message: 'key required' }));
        return;
      }
      const existing = acks.get(key);
      if (existing) {
        ws.send(JSON.stringify({ op: 'alert', key, ack: existing }));
        return;
      }
      const ack = { by: frameIn.by || 'anonymous', at: Date.now() };
      acks.set(key, ack);
      // Broadcast to every socket, like fw:ch:alerts in the real gateway.
      for (const client of wss.clients) {
        if (client.readyState === client.OPEN) {
          client.send(JSON.stringify({ op: 'alert', key, ack }));
        }
      }
      return;
    }

    // ---- op: resync — re-send the authoritative open set, mirroring
    // handleResync in the gateway. The real one reads fw:idx:alert:open; this
    // mock has no alert store, so it reports whatever the demo believes is open.
    // The point of supporting it at all is that the client must not sit waiting
    // for a reply that never comes when developing locally.
    if (frameIn.op === 'resync') {
      ws.send(JSON.stringify({ op: 'snapshot', alerts: mockOpenAlerts() }));
      return;
    }

    // ---- op: closed — the recently retired alerts, mirroring handleClosedQuery.
    // Built from the mock's own retired set so the tab shows something coherent
    // with the rest of the demo, rather than a hard-coded fixture that contradicts
    // the live stream.
    // ---- op: allEvents — every stored event in a window, mirroring
    // handleAllEventsQuery. Built from the mock's own event log so the tab agrees
    // with the live stream rather than showing a fixture that contradicts it.
    if (frameIn.op === 'allEvents') {
      const RANGE_MS = { '1h': 3600e3, '6h': 6 * 3600e3, '24h': 24 * 3600e3,
                         '7d': 7 * 86400e3 };
      const range = RANGE_MS[frameIn.range] ? frameIn.range : '24h';
      const now = Date.now();
      const since = now - RANGE_MS[range];
      const limit = Math.min(frameIn.limit || 1000, 1000);
      const rows = [...mockEventLog.values()]
        .filter((e) => (e.brokerTs ?? 0) > since)
        .sort((a, b) => (b.brokerTs ?? 0) - (a.brokerTs ?? 0))
        .slice(0, limit);
      ws.send(JSON.stringify({
        op: 'allEvents',
        events: rows,
        range,
        truncated: rows.length === limit,
        sinceMs: since,
        untilMs: now,
      }));
      return;
    }

    if (frameIn.op === 'closed') {
      const now = Date.now();
      const rows = [...closedMock.values()]
        .sort((a, b) => b.closedAt - a.closedAt)
        .slice(0, Math.min(frameIn.limit || 500, 500))
        .map((r) => ({ ...r, event: { ...r.event } }));
      ws.send(JSON.stringify({
        op: 'closed',
        alerts: rows,
        truncated: closedMock.size > rows.length,
        sinceMs: now - 7 * 24 * 60 * 60 * 1000,
      }));
      return;
    }

    if (frameIn.op !== 'subscribe') return;

    // Backfill: a mix of open alerts and history.
    const history = Array.from({ length: 14 }, (_, i) => mkEvent(i));
    for (const evt of history) {
      ws.send(JSON.stringify({ op: 'event', event: evt, applied: null }));
      await new Promise((r) => setTimeout(r, 15));
    }
    ws.send(JSON.stringify({ op: 'hello', server: 'ws-gateway-demo', backfill: history.length, truncated: false }));

    // Open alerts, so the top table is populated. Each alert is sent once per
    // occurrence because the UI derives the count from the applied:1 frames.
    // Only non-retirement severities can be open (CLOSE/HARMLESS are folds to
    // applied:3), so the mock must not emit applied:1 for those.
    const alerts = [
      { id: 'a1', source: 'core-sw-07', type: 'link.down', severity: 'FATAL', count: 4 },
      { id: 'a2', source: 'edge-gw-01', type: 'power.fault', severity: 'CRITICAL', count: 12 },
      { id: 'a3', source: 'sensor-rack-a', type: 'telemetry.temperature', severity: 'WARNING', count: 37 },
      { id: 'a4', source: 'edge-gw-02', type: 'power.fault', severity: 'CRITICAL', count: 6 },
    ];
    for (const a of alerts) {
      for (let k = 0; k < a.count; k += 1) {
        const evt = mkEvent(0, { id: `${a.id}-${k}`, source: a.source, type: a.type, severity: a.severity });
        foldMock(evt, 1);
        logMockEvent(evt);
        ws.send(JSON.stringify({ op: 'event', event: evt, applied: 1 }));
      }
      await new Promise((r) => setTimeout(r, 20));
    }

    // A redelivery, to show the dedup path: the same event id twice. The real
    // fold drops the second one (returns 0) because the (timestamp, id)
    // watermark already saw it, so it must not change Count or Last occurrence.
    {
      const dup = mkEvent(0, { id: 'a1-0', source: 'core-sw-07', type: 'link.down', severity: 'FATAL' });
      ws.send(JSON.stringify({ op: 'event', event: { ...dup, brokerTs: Date.now() }, applied: 0 }));
    }
    await new Promise((r) => setTimeout(r, 20));

    // Live tail. Semantics mirror the real fold: applied:1 opens/increments an
    // alert, applied:2 coalesces into an existing one, applied:3 retires it.
    // Retirement severities (CLOSE, HARMLESS) only ever produce applied:3.
    let n = 100;
    let open = true; // toggles a demo key so a close is visible mid-stream
    const tick = setInterval(() => {
      if (ws.readyState !== ws.OPEN) { clearInterval(tick); return; }
      const phase = n % 6;
      let severity = 'WARNING';
      let applied = 1;
      let source = 'edge-gw-02';
      let type = 'link.flap';

      // Every other tick escalates the severity so a live CRITICAL is visible,
      // and one tick per cycle is a dropped duplicate (applied 0) — exactly what
      // the real fold returns for a redelivered event id, and proof that a
      // duplicate does not touch Count or Last occurrence.
      if (phase === 2) { severity = 'CRITICAL'; }
      if (phase === 5) { applied = 0; }
      if (phase === 4) { applied = open ? 3 : 1; severity = open ? 'CLOSE' : 'CRITICAL'; open = !open; }

      const evt = mkEvent(n, {
        id: `live-${n}`,
        source,
        type,
        severity,
        brokerTs: Date.now(),
        ts: new Date().toISOString(),
      });
      foldMock(evt, applied);
      retireMock(evt, applied);
      logMockEvent(evt);
      ws.send(JSON.stringify({ op: 'event', event: evt, applied }));
      n += 1;
    }, 1800);
    ws.on('close', () => clearInterval(tick));
  });
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`dev server on http://127.0.0.1:${PORT}`);
});
