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
  matchesAlertSubscription,
  validateSubscribe,
  RecentIds,
  backfillPlan,
  eventFrame,
  alertFrame,
} from './subscription.js';
import {
  validateAck,
  validateAlertQuery,
  validateClose,
  validateDelete,
  ackAlertKey,
  normaliseAck,
  ackFrame,
  closeFrame,
  deletedFrame,
} from './ack.js';

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

    if (frame.op === 'ack') {
      await handleAck(conn, frame, send);
      return;
    }

    if (frame.op === 'close') {
      await handleClose(conn, frame, send);
      return;
    }

    if (frame.op === 'delete') {
      await handleDelete(conn, frame, send);
      return;
    }

    if (frame.op === 'alerts') {
      await handleAlertQuery(conn, frame, send);
      return;
    }

    if (frame.op === 'resync') {
      await handleResync(conn, frame, send);
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

    // ---- snapshot of open alerts
    // Sent BEFORE the backfill so the board has its rows immediately, and sent
    // for every subscribe regardless of `since` or `sources`: "what is firing
    // right now" is not a time-windowed question. A client that connects to a
    // quiet cluster would otherwise show an empty board forever, because the
    // event replay only produces rows as new events arrive.
    const snapshot = await sendSnapshot(verdict.sub, send);

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
/**
 * Acknowledge an alert on behalf of an operator.
 *
 * Two pods can receive two clicks on the same alert; the Lua in `ackAlert`
 * makes the first one win and the second a no-op, so the attribution cannot be
 * overwritten by a later click.
 *
 * The broadcast is a single `fw:ch:alerts` frame. Every gateway pod receives it
 * through its existing subscriber and fans it out to its own sockets, so the
 * click reaches browsers connected to *other* pods without any pod-to-pod
 * coordination (docs/acknowledge.md §5).
 */
async function handleAck(conn, frame, send) {
  const verdict = validateAck(frame);
  if (!verdict.ok) {
    send(JSON.stringify({ op: 'error', code: verdict.code, message: verdict.message }));
    return;
  }

  const hashKey = ackAlertKey(verdict.key);
  let result;
  try {
    result = await reader.ackAlert(hashKey, verdict.by, Date.now(), cfg.alertTtlMs);
  } catch (err) {
    log.error('ack write failed', { key: verdict.key, err: String(err) });
    send(JSON.stringify({
      op: 'error',
      code: 'ack_failed',
      message: 'could not persist the acknowledgement, retry',
    }));
    return;
  }

  if (result.status === 'missing') {
    // The alert hash has expired, or never existed. Do not create one: an
    // ack-only hash would render as a phantom row with no severity and no count.
    send(JSON.stringify({
      op: 'error',
      code: 'no_such_alert',
      message: 'that alert no longer exists',
    }));
    return;
  }

  // Confirm to the originator so its UI settles even if the pub/sub frame is
  // delayed or lost (a reconnect re-reads the state anyway).
  send(ackFrame(verdict.key, result.ack));

  if (result.status === 'acked') {
    try {
      await reader.publish(cfg.channelAlerts, JSON.stringify({
        kind: 'ack',
        key: verdict.key,
        ack: result.ack,
      }));
    } catch (err) {
      // The write is durable already. Other pods will not see this frame, so
      // their clients keep showing the alert as un-acked until they re-read
      // state on reconnect. Loud, because it is a real divergence.
      log.error('ack broadcast failed after a durable write', {
        key: verdict.key, err: String(err),
      });
    }
  }
}

/**
 * Bulk ack-state read for the alerts a client is currently showing.
 *
 * Client-supplied keys, never a server-side scan: `SCAN fw:alert:*` on every
 * connect is exactly the unbounded read the design forbids
 * (websocket-server.md §3).
 */
/**
 * Close an alert on behalf of an operator.
 *
 * Same shape as handleAck: validate, one narrow Lua write, confirm to the
 * originator, broadcast so other pods' clients see it immediately.
 *
 * The broadcast frame is `op: alert` with `applied: 3` — the same frame a
 * source-sent CLOSE produces — so clients need no new code path to retire a row.
 */
async function handleClose(conn, frame, send) {
  const verdict = validateClose(frame);
  if (!verdict.ok) {
    send(JSON.stringify({ op: 'error', code: verdict.code, message: verdict.message }));
    return;
  }

  const hashKey = ackAlertKey(verdict.key);
  let result;
  try {
    result = await reader.closeAlert(hashKey, verdict.by, Date.now(), cfg.alertTtlMs);
  } catch (err) {
    log.error('close write failed', { key: verdict.key, err: String(err) });
    send(JSON.stringify({
      op: 'error',
      code: 'close_failed',
      message: 'could not close the alert, retry',
    }));
    return;
  }

  if (result.status === 'missing') {
    // Same rule as ack: never create a hash just to close it, or a phantom row
    // with no severity and no count appears on every board.
    send(JSON.stringify({
      op: 'error',
      code: 'no_such_alert',
      message: 'that alert no longer exists',
    }));
    return;
  }

  // Confirm to the originator, then fan out to the other pods.
  send(closeFrame(verdict.key));

  try {
    await reader.publish(cfg.channelAlerts, JSON.stringify({
      kind: 'close',
      key: verdict.key,
    }));
  } catch (err) {
    // The write is durable already; other pods will simply not see the frame and
    // their clients keep the row until they reload. Loud, because it diverges.
    log.error('close broadcast failed after a durable write', { key: verdict.key, err: String(err) });
  }
}

/**
 * Permanently delete one event record.
 *
 * The only DESTRUCTIVE client operation in this service, and the only one that is
 * not recoverable from anything: no AOF, no audit copy for operator deletes.
 * docs/acknowledge.md §8.2 explains why it exists despite the project's "no data
 * loss" rule, and why it is the one place that rule is knowingly bent.
 *
 * The record is read first for two reasons: to answer `missing` honestly instead
 * of reporting a successful delete of something that was never there, and to
 * learn the event's `source`, which names the timeline index that must also be
 * cleaned.
 */
async function handleDelete(conn, frame, send) {
  const verdict = validateDelete(frame);
  if (!verdict.ok) {
    send(JSON.stringify({ op: 'error', code: verdict.code, message: verdict.message }));
    return;
  }

  let evt;
  try {
    evt = await reader.readEvent(verdict.id);
  } catch (err) {
    log.error('delete: read failed', { id: verdict.id, err: String(err) });
    send(JSON.stringify({
      op: 'error',
      code: 'delete_failed',
      message: 'could not read the event, retry',
    }));
    return;
  }

  if (!evt) {
    send(JSON.stringify({
      op: 'error',
      code: 'no_such_event',
      message: 'that event no longer exists',
    }));
    return;
  }

  try {
    const res = await reader.deleteEvent(verdict.id, evt.source || '');
    if (res.status === 'missing') {
      // Lost a race with another delete. Report it rather than claiming success.
      send(JSON.stringify({
        op: 'error',
        code: 'no_such_event',
        message: 'that event no longer exists',
      }));
      return;
    }
  } catch (err) {
    log.error('delete write failed', { id: verdict.id, err: String(err) });
    send(JSON.stringify({
      op: 'error',
      code: 'delete_failed',
      message: 'could not delete the event, retry',
    }));
    return;
  }

  // Loud on purpose: this is the one irreversible action in the system, so it
  // leaves a trace in the gateway log naming who did it and what was destroyed.
  log.warn('event deleted', { id: verdict.id, source: evt.source, by: verdict.by });

  send(deletedFrame(verdict.id));

  try {
    await reader.publish(cfg.channelAlerts, JSON.stringify({
      kind: 'deleted',
      id: verdict.id,
    }));
  } catch (err) {
    log.error('delete broadcast failed after a durable delete', { id: verdict.id, err: String(err) });
  }
}

/**
 * Send the current open-alert set as a `snapshot` frame.
 *
 * Shared by `subscribe` and `resync` so there is exactly one definition of "what
 * the board should contain". Two copies would drift, and the drift would show up
 * as a resync that quietly disagrees with a fresh reload — the hardest kind of
 * bug to notice, because both paths look correct in isolation.
 *
 * An EMPTY result is always sent, not suppressed. The client treats a snapshot as
 * authoritative and replaces its alert map with it, so "nothing is firing" is a
 * meaningful answer that must reach the client; omitting it would leave stale
 * rows that only a browser refresh clears.
 *
 * Returns the number of rows sent.
 */
async function sendSnapshot(sub, send) {
  try {
    // readOpenAlerts already batches its reads: it returns every alert with its
    // event attached, in two round trips. Do NOT loop over the rows calling
    // reader.readEvent() here — that is O(alerts) round trips on every connect
    // and measured ~1.1s for 199 alerts (see the cost note on readOpenAlerts).
    const open = await reader.readOpenAlerts(cfg.alertSnapshotMax);
    const alerts = [];
    for (const a of open) {
      // Only the alerts the client asked about. An unfiltered subscribe means
      // "everything", which is the common case.
      if (matchesAlertSubscription(sub, a.key)) alerts.push(a);
    }
    send(JSON.stringify({ op: 'snapshot', alerts }));
    return alerts.length;
  } catch (err) {
    // A snapshot failure must not break the live stream: the client still gets
    // live frames, it simply opens with an empty board.
    log.warn('alert snapshot failed', { err: String(err) });
    return -1;
  }
}

/**
 * Re-send the snapshot on the client's existing subscription.
 *
 * Why this exists as its own op rather than "just send subscribe again": every
 * subscribe appends to `conn.subscriptions`, and the gateway caps those
 * (`FW_MAX_SUBSCRIPTIONS`, default 20). A client that resynced periodically
 * would exhaust that budget and then be unable to subscribe at all — and it
 * would also re-run the bounded backfill on every resync, re-sending events the
 * client already has.
 *
 * The client-side race that makes this necessary:
 *
 *   T0  client renders row X              (X is open)
 *   T1  worker retires X, publishes the fan-out frame
 *   T2  the frame is lost — a missed
 *       client->server pong, a dropped
 *       pub/sub message on a reconnect,
 *       a laptop sleep, a tab throttled
 *   T3  client still shows X forever
 *
 * Rebuilding the board from the source of truth is the only fix that does not
 * depend on having received every frame. A browser refresh already does exactly
 * that; a resync makes the refresh unnecessary.
 *
 * No subscription required: a client that has not subscribed has nothing to
 * resync, and `sub` would be undefined. Tolerating it (rather than erroring)
 * keeps the op safe to fire blindly from a timer.
 */
async function handleResync(conn, frame, send) {
  const sub = conn.subscriptions[0] || { sources: [], types: [], since: null };
  const rows = await sendSnapshot(sub, send);
  // Echo the count back so the client can log/settle on the result. `-1` means
  // the read failed and the snapshot the client already has is still its best
  // information — deliberately not an error frame, because the board is intact.
  if (rows >= 0 && frame.ack === true) {
    send(JSON.stringify({ op: 'resynced', alerts: rows }));
  }
}

async function handleAlertQuery(conn, frame, send) {
  const verdict = validateAlertQuery(frame);
  if (!verdict.ok) {
    send(JSON.stringify({ op: 'error', code: verdict.code, message: verdict.message }));
    return;
  }

  let hashes;
  try {
    hashes = await reader.readAcks(verdict.keys.map(ackAlertKey));
  } catch (err) {
    log.warn('ack state read failed', { err: String(err) });
    send(JSON.stringify({ op: 'error', code: 'alerts_failed', message: 'could not read ack state' }));
    return;
  }

  const out = [];
  verdict.keys.forEach((key, i) => {
    const ack = normaliseAck(hashes[i]);
    if (ack) out.push({ key, ack });
  });
  send(JSON.stringify({ op: 'alerts', alerts: out }));
}

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

/**
 * Handle a fan-out message on the alerts channel.
 *
 * Async because an `occurrence` needs one Redis read to attach the alert's
 * current row: the worker discarded the duplicate's copy, so the frame has no
 * event and a client that never saw the original has nothing to render.
 *
 * Every rejection is caught by the caller, so a slow or failing read can never
 * break the subscriber loop for every other client on the pod.
 */
async function handleAlertMessage(raw) {
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return;
  }
  if (!parsed || !parsed.key) return;

  // An ack change is broadcast as its own frame, so every pod's clients see the
  // new state without a re-read (docs/acknowledge.md §5).
  if (parsed.kind === 'ack') {
    const frame = ackFrame(parsed.key, parsed.ack);
    for (const ws of wss.clients) {
      if (ws.readyState === ws.OPEN && ws.__send) ws.__send(frame);
    }
    return;
  }

  // An operator close travels on the same channel and fans out as applied: 3,
  // so clients retire the row through the path they already have.
  if (parsed.kind === 'close') {
    const frame = closeFrame(parsed.key);
    for (const ws of wss.clients) {
      if (ws.readyState === ws.OPEN && ws.__send) ws.__send(frame);
    }
    return;
  }

  // A deleted event fans out by id, so every client can drop the same record
  // from its live tail regardless of which pod handled the delete.
  if (parsed.kind === 'deleted') {
    const frame = deletedFrame(parsed.id);
    for (const ws of wss.clients) {
      if (ws.readyState === ws.OPEN && ws.__send) ws.__send(frame);
    }
    return;
  }

  // A CONTENT duplicate: the original record absorbed a repeat, so its count and
  // last_seen moved but no new event exists. Fan out the rollup only — there is no
  // event to show in the tail, which is the point (the copy was discarded).
  //
  // The row is attached anyway, read fresh from the alert hash. A client that has
  // never seen this key has no other way to learn it exists: the frame carries no
  // event, so `applyAlert` would have nothing to build a row from. That is exactly
  // the case after a CLEAR followed by a repeat — the alert reopens, the copy is
  // absorbed, and the only notice is this eventless frame, so the row never
  // appeared until the next snapshot (i.e. a browser refresh).
  //
  // Cost is one hash read plus one event read, and only on an absorbed duplicate.
  // If the alert is not open (already retired, or the read failed) the bare frame
  // is still sent: a client that already has the row still moves its count, and a
  // client without it simply stays as it was rather than inventing a row.
  if (parsed.kind === 'occurrence') {
    let row = null;
    try {
      row = await reader.readAlert(parsed.key);
    } catch (err) {
      log.warn('occurrence row read failed; sending bare frame', {
        key: parsed.key, err: String(err),
      });
    }
    const frame = alertFrame(parsed.key, parsed.applied, row);
    for (const ws of wss.clients) {
      if (ws.readyState === ws.OPEN && ws.__send) ws.__send(frame);
    }
    return;
  }
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
            // The handler is async (an occurrence reads the alert's row), so the
            // rejection must be handled here: an unhandled rejection inside an
            // event emitter would take the process down and drop every client on
            // this pod.
            if (ch === cfg.channelAlerts) {
              handleAlertMessage(msg).catch((err) => {
                log.error('alert message handling failed', { err: String(err) });
              });
            }
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
