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
  shouldFanOut,
  foldDbProbe,
} from './subscription.js';
import {
  validateAck,
  validateAlertQuery,
  validateClose,
  validateDelete,
  validateClosedQuery,
  validateAllEventsQuery,
  ackAlertKey,
  normaliseAck,
  ackFrame,
  closeFrame,
  deletedFrame,
} from './ack.js';
import {
  validateJournalAppend,
  validateJournalQuery,
  journalEntryFrame,
  journalListFrame,
} from './journal.js';
import { AuditPublisher } from './audit.js';

const cfg = loadConfig();
const log = createLogger(cfg.logLevel);

/**
 * This pod's identity, used to recognise our own pub/sub broadcasts.
 *
 * A fan-out published on `fw:ch:alerts` comes back to the pod that published it,
 * because pub/sub has no sender filtering. Any handler that ALSO replies to the
 * caller directly will therefore deliver twice unless it can tell its own echo
 * apart — which is exactly the bug that made one operator close count as two.
 */
const POD_NAME = process.env.POD_NAME || 'local';

const reader = new DragonflyReader(cfg, log);
const recent = new RecentIds(cfg.dedupeLru);
// Replicates journal entries to the audit exchange, because Dragonfly alone cannot
// make a note durable. Logged at startup either way, so a deployment without it
// knows notes are stored-but-not-recoverable rather than assuming they are safe.
const audit = new AuditPublisher(cfg, log);

let ready = false;
let connCount = 0;
let msgsOut = 0;

/**
 * Dragonfly health, as last probed.
 *
 * `dbUp` is what clients render. `dbFails` counts CONSECUTIVE failures: a single
 * failed PING is not proof of an outage (a demoted master, a Sentinel re-resolve and
 * a brief network blip are indistinguishable), and a chip that flickers red on noise
 * trains an operator to ignore it.
 *
 * Starts optimistically `true`: the gateway refuses to become ready until it has
 * subscribed, so a client cannot connect before the first successful read.
 */
let dbUp = true;
let dbFails = 0;
let dbCheckedAt = null;

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
      `# TYPE fw_ws_ready gauge\nfw_ws_ready ${ready ? 1 : 0}\n` +
      // Exported separately from fw_ws_ready on purpose: they answer different
      // questions. `ready` is "can this pod serve sockets"; `db_up` is "can it
      // read". A pod that is ready while its reads fail serves a stale board, and
      // that used to be invisible to monitoring.
      `# TYPE fw_db_up gauge\nfw_db_up ${dbUp ? 1 : 0}\n` +
      `# TYPE fw_db_consecutive_failures gauge\nfw_db_consecutive_failures ${dbFails}\n` +
      // Whether journal notes are being replicated to the audit exchange. A `0`
      // here means the feature works but its durability promise does not, and that
      // must be visible to monitoring rather than living only in a startup log
      // line nobody reads after a deploy (docs/acknowledge.md §11.3).
      `# TYPE fw_journal_audit_enabled gauge\nfw_journal_audit_enabled ${audit.isEnabled() ? 1 : 0}\n`,
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
    framesThisSecond: 0,
    frameWindow: Date.now(),
    outbound: 0,
  };

  // Expose the two things the fan-out loop needs directly on the socket, so it
  // does not need a second per-connection registry to stay in sync.
  ws.__subscriptions = conn.subscriptions;
  ws.__send = (payload) => send(payload);

  /**
   * Liveness. ONE flag, on the socket, cleared by the pong and set by the loop.
   *
   * This used to be two half-finished implementations: the pong handler set
   * `conn.alive`, which nothing ever read, while the heartbeat loop tested and
   * set `ws.__awaitingPong`, which nothing ever cleared. The result was that every
   * socket was terminated one `pongTimeoutMs` after its first heartbeat — measured
   * at 14.97 s, close code 1006 — so each client reconnected forever in a
   * 15-second cycle and the status chip flickered `open → reconnecting → open`.
   *
   * The flag lives on the socket, not on `conn`, because the heartbeat loop only
   * has access to `wss.clients`.
   */
  ws.__awaitingPong = false;
  ws.on('pong', () => { ws.__awaitingPong = false; });

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
      // The application-level pong, for a client that answers heartbeats as JSON
      // rather than with a protocol-level frame. Both must clear the same flag or
      // the connection is killed one heartbeat later.
      ws.__awaitingPong = false;
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

    if (frame.op === 'closed') {
      await handleClosedQuery(conn, frame, send);
      return;
    }

    if (frame.op === 'allEvents') {
      await handleAllEventsQuery(conn, frame, send);
      return;
    }

    if (frame.op === 'journal') {
      await handleJournalAppend(conn, frame, send);
      return;
    }

    if (frame.op === 'journals') {
      await handleJournalQuery(conn, frame, send);
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
      server: `ws-gateway-${POD_NAME}`,
      backfill: backfilled,
      truncated,
      // The database state as of this connect, so a client that joins while
      // Dragonfly is down does not have to wait for the next probe to find out.
      db: { up: dbUp, checkedAt: dbCheckedAt },
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
        // Same reason as the close broadcast: the originator was already sent
        // `ackFrame` directly, and this pod's own echo would deliver it twice.
        origin: POD_NAME,
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

  if (result.status === 'already') {
    // Two operators clicked Close. The first write wins and the second learns who
    // actually closed it, rather than overwriting the author and timestamp. The
    // client still gets a close frame so its row disappears.
    log.debug('alert already closed', { key: verdict.key, by: result.by });
    send(closeFrame(verdict.key));
    return;
  }

  // Confirm to the originator, then fan out to the other pods.
  send(closeFrame(verdict.key));

  // The close wrote a real CLEAR event record, so broadcast it like any other
  // event: the live tail shows the closure next to the CLEAR a source would have
  // sent, and it appears in the retention window with an author. `applied: 3`
  // matches what the fold returns for a retirement, so clients treat it
  // identically.
  //
  // A fan-out failure is logged but not fatal: the record is already durable in
  // Dragonfly and the Closed tab is a query, so it will show there regardless.
  if (result.eventId) {
    try {
      await reader.publish(cfg.channelEvents, JSON.stringify({
        event: {
          id: result.eventId,
          source: verdict.key.slice(0, Math.max(0, verdict.key.indexOf(':'))),
          type: verdict.key.slice(verdict.key.indexOf(':') + 1),
          severity: 'CLOSE',
          ts: new Date(result.at).toISOString(),
          brokerTs: result.at,
          operatorClose: true,
          payload: {
            msg: `Closed by ${result.by}`,
            EVT_Severity: 'CLEAR',
            EVT_Agent: result.by,
          },
        },
        applied: 3,
      }));
    } catch (err) {
      log.error('close event broadcast failed after a durable write', {
        key: verdict.key, id: result.eventId, err: String(err),
      });
    }
  }

  try {
    await reader.publish(cfg.channelAlerts, JSON.stringify({
      kind: 'close',
      // Tagged with this pod so the subscriber can skip re-sending it to clients
      // this pod ALREADY confirmed above. Without the tag the originator got the
      // frame twice: once from `send(closeFrame(...))` and once from its own
      // pub/sub echo, which made the Closed tab's badge count one closure
      // twice.
      origin: POD_NAME,
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

/**
 * `{"op":"closed"}` — the recently retired alerts, newest closure first.
 *
 * Backs the Closed tab, which is why an `op` exists rather than a snapshot field:
 * the window is a query, not part of "what is firing now", and a client that
 * never opens that tab should not pay for it.
 *
 * Read-only and bounded server-side. The response is one `op: closed` frame with
 * the rows; `truncated` tells the client the limit was hit, so the UI can say so
 * instead of silently presenting a partial window as complete.
 */
async function handleClosedQuery(conn, frame, send) {
  const verdict = validateClosedQuery(frame);
  if (!verdict.ok) {
    send(JSON.stringify({ op: 'error', code: verdict.code, message: verdict.message }));
    return;
  }

  let rows;
  try {
    rows = await reader.readClosedAlerts({ sinceMs: verdict.sinceMs, limit: verdict.limit });
  } catch (err) {
    log.warn('closed alert read failed', { err: String(err) });
    send(JSON.stringify({ op: 'error', code: 'closed_failed', message: 'could not read closed alerts' }));
    return;
  }

  send(JSON.stringify({
    op: 'closed',
    alerts: rows,
    // A full page means "there may be more", which is the honest signal. The
    // alternative — silently returning the newest N — would make a busy week look
    // smaller than it is.
    truncated: rows.length >= verdict.limit,
    sinceMs: verdict.sinceMs,
  }));
}

/**
 * `{"op":"allEvents","range":"24h"}` — every event in a time window, newest
 * first, regardless of correlation.
 *
 * Distinct from `resync`/`subscribe`: those are about open alerts, this is the raw
 * event log. Distinct from the live tail, which is a bounded in-memory buffer that
 * dies on reload — this reads the worker's global timeline, so it survives a
 * refresh and reaches the full 7-day retention.
 *
 * The window is applied SERVER-side, so a 1-hour request is cheaper than a 7-day
 * one rather than the server building everything and the client filtering it.
 */
async function handleAllEventsQuery(conn, frame, send) {
  const verdict = validateAllEventsQuery(frame);
  if (!verdict.ok) {
    send(JSON.stringify({ op: 'error', code: verdict.code, message: verdict.message }));
    return;
  }

  let events;
  try {
    events = await reader.readAllEvents({
      sinceMs: verdict.sinceMs,
      untilMs: verdict.untilMs,
      limit: verdict.limit,
    });
  } catch (err) {
    log.warn('all-events read failed', { err: String(err) });
    send(JSON.stringify({ op: 'error', code: 'all_events_failed', message: 'could not read events' }));
    return;
  }

  send(JSON.stringify({
    op: 'allEvents',
    events,
    range: verdict.range,
    // A full page means "there may be older events inside this same window". The
    // client shows that as a notice rather than implying the range is empty.
    truncated: events.length >= verdict.limit,
    sinceMs: verdict.sinceMs,
    untilMs: verdict.untilMs,
  }));
}

/**
 * `{"op":"journal","key":"source:type","text":"…"}` — append one operator note.
 *
 * The only append in the system. Everything else the gateway writes (ack, close)
 * is a single-field overwrite of state that has exactly one correct value; a journal
 * is a LOG, where the earlier entries are the value. So this uses RPUSH and never a
 * SET, and two operators annotating the same incident both keep their text.
 *
 * The note is stored first and replicated second. If the audit publish fails the
 * operator still gets their entry — an annotation is not worth failing over a
 * broker hiccup — but the failure is logged at `warn` rather than swallowed, because
 * it silently weakens a durability promise the operator may be relying on.
 */
async function handleJournalAppend(conn, frame, send) {
  const verdict = validateJournalAppend(frame);
  if (!verdict.ok) {
    send(JSON.stringify({ op: 'error', code: verdict.code, message: verdict.message }));
    return;
  }

  const entry = {
    id: verdict.id,
    by: verdict.by,
    at: Date.now(),
    text: verdict.text,
  };

  let result;
  try {
    result = await reader.appendJournal(
      verdict.key,
      entry,
      cfg.journalTtlMs,
      cfg.journalMaxEntries,
    );
  } catch (err) {
    log.warn('journal append failed', { key: verdict.key, err: String(err) });
    send(JSON.stringify({
      op: 'error',
      code: 'journal_failed',
      message: 'could not save the note, retry',
    }));
    return;
  }

  // Replication is awaited but never fails the request: see audit.js.
  await audit.publishJournal({ key: verdict.key, entry });

  // Echo to the writer, then fan out to everyone else. The echo carries the entry
  // (with the server-assigned id and timestamp) so this client can render it
  // immediately; `shouldFanOut` is what stops the round trip from delivering it a
  // second time.
  send(journalEntryFrame(verdict.key, entry));

  if (result.truncated) {
    send(JSON.stringify({
      op: 'error',
      code: 'journal_truncated',
      message: `this incident keeps the newest ${cfg.journalMaxEntries} notes; older ones were dropped`,
    }));
  }

  try {
    await reader.publish(cfg.channelAlerts, JSON.stringify({
      kind: 'journal',
      key: verdict.key,
      entry,
      origin: POD_NAME,
    }));
  } catch (err) {
    // The note is stored. Another pod's clients will pick it up on their next read,
    // so this is a delay rather than a loss.
    log.warn('journal broadcast failed after a successful write', {
      key: verdict.key, id: entry.id, err: String(err),
    });
  }
}

/**
 * `{"op":"journals","keys":["source:type",…]}` — bulk read of recent notes.
 *
 * Bulk rather than one-key-per-frame for the same reason `op: alerts` is bulk: an
 * operator opening the journal on a table of alerts should not cause one round trip
 * per row. Bounded by `MAX_JOURNAL_QUERY_KEYS`, so one frame cannot ask a pod to do
 * unbounded work (websocket-server.md §4).
 *
 * Every requested key comes back with an entry, even when empty, so a client can
 * distinguish "read it, there is nothing" from "no answer".
 */
async function handleJournalQuery(conn, frame, send) {
  const verdict = validateJournalQuery(frame);
  if (!verdict.ok) {
    send(JSON.stringify({ op: 'error', code: verdict.code, message: verdict.message }));
    return;
  }
  if (verdict.keys.length === 0) {
    send(journalListFrame([]));
    return;
  }

  let journals;
  try {
    journals = await reader.readJournals(verdict.keys, verdict.limit);
  } catch (err) {
    log.warn('journal read failed', { err: String(err) });
    send(JSON.stringify({
      op: 'error',
      code: 'journal_read_failed',
      message: 'could not read the journal',
    }));
    return;
  }

  send(journalListFrame(journals));
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

  // De-dup on id, but ONLY for identical frames.
  //
  // Two different situations both carry the same event id and they must not be
  // conflated:
  //
  //   1. A redelivery — Kafka/Rabbit redelivered the same message, so the worker
  //      published the same frame twice. Re-sending it would double-render.
  //   2. A REPEAT — the same event content sent again later. The worker absorbed
  //      it (no new record) and republished it with a FRESH `brokerTs` so the
  //      live tail shows the arrival. Suppressing that by id hid every repeat
  //      after the first, which is why a tail of `send-same-event.mjs` runs
  //      showed one row.
  //
  // The key includes brokerTs and `applied`, so a redelivery (identical frame) is
  // still dropped while a distinct arrival is shown. The client de-duplicates on
  // id alone for the ROW list, so the row list is unaffected either way.
  const frameKey = `${event.id}|${event.brokerTs ?? ''}|${applied ?? ''}`;
  if (!recent.add(frameKey)) return;

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
    // Skip our own broadcast: this pod already sent `ackFrame` to the client that
    // made the request, so re-delivering it here would double-send.
    if (!shouldFanOut(parsed, POD_NAME)) return;
    const frame = ackFrame(parsed.key, parsed.ack);
    for (const ws of wss.clients) {
      if (ws.readyState === ws.OPEN && ws.__send) ws.__send(frame);
    }
    return;
  }

  // An operator close travels on the same channel and fans out as applied: 3,
  // so clients retire the row through the path they already have.
  if (parsed.kind === 'close') {
    // The publishing pod has already confirmed this close directly to the client
    // that requested it, so it must not deliver its own broadcast as well — that
    // double delivery is what made one closure read as two. Every OTHER pod still
    // fans it out, which is the point of broadcasting.
    if (!shouldFanOut(parsed, POD_NAME)) return;
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

  // A new journal note. Same echo guard as ack and close: the pod that accepted the
  // append already sent the entry to the client that wrote it, so delivering its own
  // broadcast again would show the note twice. A duplicated note is worse than a
  // duplicated ack — an ack is idempotent state, a note is a row in a log.
  if (parsed.kind === 'journal') {
    if (!shouldFanOut(parsed, POD_NAME)) return;
    const frame = journalEntryFrame(parsed.key, parsed.entry);
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

// ------------------------------------------------------------------ db health
//
// Probes Dragonfly and pushes the result to clients when it CHANGES.
//
// Why the gateway probes rather than each client: one PING per pod per interval
// instead of one per browser tab, and it is the only way to show a TRANSITION —
// a client that is merely connected has no idea whether the gateway's reads are
// working.
//
// Why push on change rather than every probe: an unchanged status is not worth a
// frame to every socket every 10 seconds. The initial value arrives in `hello`.
setInterval(async () => {
  const ok = await reader.ping();
  const next = foldDbProbe(
    { up: dbUp, fails: dbFails }, ok, cfg.dbFailThreshold, Date.now(),
  );

  dbFails = next.fails;
  dbCheckedAt = next.checkedAt;

  if (!ok) {
    // Log every failure: a database the gateway cannot reach is never routine.
    // The threshold only decides when to TELL CLIENTS, not when to log.
    log.warn('dragonfly ping failed', {
      consecutive: dbFails, threshold: cfg.dbFailThreshold,
    });
  }

  if (!next.changed) return;

  dbUp = next.up;
  if (dbUp) {
    log.info('dragonfly reachable again');
  } else {
    log.error('dragonfly reported DOWN to clients', { failures: dbFails });
  }
  broadcastDbState();
}, cfg.dbProbeMs).unref();

/** Fan out the current database state to every connected client. */
function broadcastDbState() {
  const frame = JSON.stringify({ op: 'db', up: dbUp, checkedAt: dbCheckedAt });
  for (const ws of wss.clients) {
    if (ws.readyState === ws.OPEN && ws.__send) ws.__send(frame);
  }
}

// ---------------------------------------------------------------------- heartbeat
//
// One flag per socket: `ws.__awaitingPong`. Set here, cleared by either pong path
// (protocol-level and the `{"op":"pong"}` JSON frame). A socket that fails to clear
// it within `FW_PONG_TIMEOUT_MS` is terminated.
//
// The log line is `warn`, not `debug`. This condition means a client is about to be
// disconnected, and at `debug` — the default level is `info` — a heartbeat bug that
// killed every connection every 15 seconds produced NO log output at all. An
// operation that drops connections must be visible without raising the log level.
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.readyState !== ws.OPEN) continue;

    if (ws.__awaitingPong) {
      log.warn('closing unresponsive socket', { awaitingFor: cfg.pongTimeoutMs });
      ws.terminate();
      continue;
    }

    ws.__awaitingPong = true;
    ws.ping();
    setTimeout(() => {
      if (ws.readyState === ws.OPEN && ws.__awaitingPong) {
        log.warn('no pong before timeout, terminating', { timeoutMs: cfg.pongTimeoutMs });
        ws.terminate();
      }
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
