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
  return {
    id: `evt-${i}`,
    source: SOURCES[i % SOURCES.length],
    type: TYPES[i % TYPES.length],
    severity: ['CRITICAL', 'WARNING', 'FATAL', 'HARMLESS', 'CLOSE'][i % 5],
    ts: new Date(first).toISOString(),
    brokerTs: first,
    payload: {
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
      ws.send(JSON.stringify({ op: 'event', event: evt, applied }));
      n += 1;
    }, 1800);
    ws.on('close', () => clearInterval(tick));
  });
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`dev server on http://127.0.0.1:${PORT}`);
});
