/**
 * ws-gateway — stateless WebSocket fan-out.
 *
 * Every pod subscribes to Dragonfly pub/sub and fans out to its own sockets. No
 * pod needs to know about any other pod, so a Route can round-robin and an HPA
 * can add pods freely (docs/websocket-server.md §2).
 *
 * Deliberate non-features:
 *   - no writes to Dragonfly (no method for it in dragonfly.js)
 *   - no decompression (the browser does it; doing it here would cost CPU per
 *     message per pod for no benefit — payload-and-compression.md §5.1)
 *   - no alert computation (that lives in the worker's fold)
 */
import http from 'node:http';
import { WebSocketServer } from 'ws';
import { loadConfig } from './config.js';
import { createLogger } from './log.js';
import { DragonflyReader } from './dragonfly.js';
import {
  matchesSubscription,
  validateSubscribe,
  RecentIds,
  backfillPlan,
  eventFrame,
  alertFrame,
} from './subscription.js';

const cfg = loadConfig();
const log = createLogger(cfg.logLevel);

const reader = new DragonflyReader(cfg, log);
const recent = new RecentIds(cfg.dedupeLru);

let ready = false;
let connCount = 0;
let msgsOut = 0;

// ------------------------------------------------------------------ HTTP health
const httpServer = http.createServer((req, res) => {
  if (req.url === '/livez') {
    res.writeHead(200, { 'content-type': 'text/plain' }).end('ok\n');
    return;
  }
  if (req.url === '/healthz') {
    // False while the Dragonfly subscriber is down: a pod that cannot subscribe
    // should not receive new sockets (websocket-server.md §7).
    const ok = ready;
    res.writeHead(ok ? 200 : 503, { 'content-type': 'text/plain' })
       .end(`${ok ? 'ready' : 'not ready'} conns=${connCount}\n`);
    return;
  }
  if (req.url === '/metrics') {
    res.writeHead(200, { 'content-type': 'text/plain' }).end(
      `# TYPE fw_ws_connections gauge\nfw_ws_connections ${connCount}\n` +
      `# TYPE fw_ws_msgs_out_total counter\nfw_ws_msgs_out_total ${msgsOut}\n` +
      `# TYPE fw_ws_ready gauge\nfw_ws_ready ${ready ? 1 : 0}\n`,
    );
    return;
  }
  res.writeHead(404).end();
});

// -------------------------------------------------------------- WebSocket server
const wss = new WebSocketServer({ server: httpServer, maxPayload: 64 * 1024 });

wss.on('connection', (ws, req) => {
  if (connCount >= cfg.maxConns) {
    log.warn('rejecting connection: at max', { max: cfg.maxConns });
    ws.close(1013, 'at capacity');
    return;
  }
  connCount += 1;

  const conn = {
    subscriptions: [],
    alive: true,
    framesThisSecond: 0,
    frameWindow: Date.now(),
    outbound: 0,
  };

  // Expose the two things the fan-out loop needs directly on the socket, so it
  // does not need a second per-connection registry to stay in sync.
  ws.__subscriptions = conn.subscriptions;
  ws.__send = (payload) => send(payload);

  ws.on('pong', () => { conn.alive = true; });

  // Outbound backpressure: a slow consumer is disconnected, never buffered.
  // Buffering turns one slow browser into a memory incident for every client on
  // the pod (websocket-server.md §4).
  const send = (payload) => {
    if (ws.readyState !== ws.OPEN) return;
    if (conn.outbound >= cfg.maxOutboundQueue) {
      log.warn('disconnecting slow consumer', { outbound: conn.outbound });
      ws.close(1011, 'slow_consumer');
      return;
    }
    conn.outbound += 1;
    ws.send(payload, () => { conn.outbound -= 1; });
    msgsOut += 1;
  };

  ws.on('message', async (data) => {
    // Rate limit inbound frames so one chatty client cannot dominate the pod.
    const now = Date.now();
    if (now - conn.frameWindow > 1000) {
      conn.frameWindow = now;
      conn.framesThisSecond = 0;
    }
    conn.framesThisSecond += 1;
    if (conn.framesThisSecond > cfg.maxFramesPerSec) {
      send(JSON.stringify({ op: 'error', code: 'rate_limited', message: 'too many frames' }));
      return;
    }

    let frame;
    try {
      frame = JSON.parse(data.toString('utf8'));
    } catch {
      send(JSON.stringify({ op: 'error', code: 'bad_json', message: 'frame must be JSON' }));
      return;
    }

    if (frame.op === 'pong') {
      conn.alive = true;
      return;
    }

    if (frame.op !== 'subscribe') {
      send(JSON.stringify({ op: 'error', code: 'bad_op', message: `unknown op ${frame.op}` }));
      return;
    }

    const verdict = validateSubscribe(frame, {
      maxSubscriptions: cfg.maxSubscriptions,
      existing: conn.subscriptions.length,
    });
    if (!verdict.ok) {
      send(JSON.stringify({ op: 'error', code: verdict.code, message: verdict.message }));
      return;
    }

    conn.subscriptions.push(verdict.sub);
    log.debug('subscription added', { subs: conn.subscriptions.length });

    // ---- backfill, bounded
    const plan = backfillPlan(verdict.sub, Date.now(), cfg.backfillMax);
    let backfilled = 0;
    let truncated = false;
    if (plan) {
      try {
        const sources = verdict.sub.sources.length > 0 ? verdict.sub.sources : [];
        if (sources.length === 0) {
          // No source filter: there is no bounded index to read, so live-only.
          // Scanning every source is exactly the unbounded scan we refuse to do.
          log.debug('backfill skipped: no source filter');
        } else {
          for (const source of sources) {
            const ids = await reader.readIndex(source, plan.min, plan.max, plan.limit);
            if (ids.length >= plan.limit) truncated = true;
            const events = await reader.readEvents(ids);
            for (const evt of events) {
              if (!matchesSubscription(verdict.sub, evt)) continue;
              if (!recent.add(evt.id)) continue;
              send(eventFrame(evt, null));
              backfilled += 1;
            }
          }
        }
      } catch (err) {
        log.warn('backfill failed', { err: String(err) });
        send(JSON.stringify({ op: 'error', code: 'backfill_failed', message: 'could not read history' }));
      }
    }

    send(JSON.stringify({
      op: 'hello',
      server: `ws-gateway-${process.env.POD_NAME || 'local'}`,
      backfill: backfilled,
      truncated,
    }));
  });

  ws.on('close', () => {
    connCount -= 1;
    log.debug('connection closed', { remaining: connCount });
  });
  ws.on('error', (err) => log.debug('socket error', { err: String(err) }));
});

// ------------------------------------------------------------------- pub/sub wiring
function handleEventMessage(raw) {
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    log.warn('unparseable pub/sub frame', { err: String(err) });
    return;
  }

  const { event, applied } = parsed;
  if (!event || !event.id) return;

  // De-dup on id: a duplicate frame would double-render in the UI.
  if (!recent.add(event.id)) return;

  const frame = eventFrame(event, applied);
  for (const ws of wss.clients) {
    if (ws.readyState !== ws.OPEN) continue;
    // Each socket has its own subscriptions; we cannot reach `conn` from here,
    // so subscriptions are stored on the ws object instead.
    const subs = ws.__subscriptions;
    if (!subs || subs.length === 0) continue;
    if (!subs.some((s) => matchesSubscription(s, event))) continue;
    if (ws.__send) ws.__send(frame);
  }
}

function handleAlertMessage(raw) {
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return;
  }
  if (!parsed || !parsed.key) return;
  const frame = alertFrame(parsed.key, parsed.applied);
  for (const ws of wss.clients) {
    if (ws.readyState === ws.OPEN && ws.__send) ws.__send(frame);
  }
}

// ---------------------------------------------------------------------- heartbeat
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.readyState !== ws.OPEN) continue;
    if (ws.__awaitingPong) {
      log.debug('closing unresponsive socket');
      ws.terminate();
      continue;
    }
    ws.__awaitingPong = true;
    ws.ping();
    setTimeout(() => {
      if (ws.readyState === ws.OPEN && ws.__awaitingPong) ws.terminate();
    }, cfg.pongTimeoutMs).unref();
  }
}, cfg.heartbeatMs).unref();

// --------------------------------------------------------------------- startup
async function main() {
  log.info('ws-gateway starting', {
    port: cfg.port,
    master: cfg.masterName,
    sentinels: cfg.sentinels.length,
    maxConns: cfg.maxConns,
  });

  httpServer.listen(cfg.port, '0.0.0.0', () => {
    log.info('http+ws listening', { port: cfg.port });
  });

  const connectLoop = async () => {
    while (true) {
      try {
        await reader.subscribe(cfg.channelEvents, handleEventMessage);
        // A second subscription needs its own connection; reuse the same one but
        // add the alerts channel.
        if (reader.subscriber) {
          await reader.subscriber.subscribe(cfg.channelAlerts);
          reader.subscriber.on('message', (ch, msg) => {
            if (ch === cfg.channelAlerts) handleAlertMessage(msg);
          });
        }
        ready = true;
        log.info('subscribed to channels', { events: cfg.channelEvents, alerts: cfg.channelAlerts });
        // Stay here until the subscriber dies.
        await new Promise((resolve) => {
          const check = setInterval(async () => {
            if (!(await reader.isSubscribed())) {
              clearInterval(check);
              resolve();
            }
          }, 2000);
        });
        ready = false;
        log.warn('subscriber lost, reconnecting');
      } catch (err) {
        ready = false;
        log.error('subscribe failed, retrying in 3s', { err: String(err) });
        await new Promise((r) => setTimeout(r, 3000));
      }
    }
  };
  connectLoop();
}

async function shutdown(signal) {
  log.info('shutting down', { signal });
  for (const ws of wss.clients) {
    try {
      ws.send(JSON.stringify({ op: 'shutdown' }));
      ws.close(1001, 'shutting down');
    } catch { /* ignore */ }
  }
  await reader.quit();
  httpServer.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 8000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('unhandledRejection', (e) => log.error('unhandled rejection', { err: String(e) }));
process.on('uncaughtException', (e) => {
  log.error('uncaught exception', { err: String(e), stack: e?.stack });
  process.exit(1);
});

main().catch((e) => {
  log.error('fatal startup error', { err: String(e), stack: e?.stack });
  process.exit(1);
});
