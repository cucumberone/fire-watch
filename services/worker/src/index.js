/**
 * worker — RabbitMQ -> Valkey event pipeline.
 *
 * Pipeline (docs/workers.md §3):
 *
 *   validate -> compress -> record+index+fold (ONE atomic script)
 *            -> audit copy (plain, confirmed) -> publish -> ack
 *
 * The order is load-bearing:
 *   - the write happens before the ack, or a crash loses the event
 *   - the audit copy is confirmed before the ack, or Valkey becomes the only copy
 *   - the ack is last, always
 *
 * The record, its indexes and the state fold are now a single script. They used to
 * be three separate writes because the record was keyed by the MESSAGE id while the
 * fold was keyed by the CONDITION. With a content-derived key both live at
 * `event:<key>`, so one atomic call replaces them (docs/valkey.md §2).
 */
import http from 'node:http';
import { loadConfig, keys } from './config.js';
import { createLogger } from './log.js';
import {
  validateEvent,
  ValidationError,
  compressPayload,
  brokerTsMs,
} from './event.js';
import { ValkeyClient } from './valkey.js';
import { FoldScript, persistEvent, reconcileEventIndex } from './store.js';
import { AmqpConsumer } from './amqp.js';

const cfg = loadConfig();
const log = createLogger(cfg.logLevel);

const valkey = new ValkeyClient(cfg, log);
const fold = new FoldScript(cfg.foldScriptPath, log, cfg.foldReloadMs);
const amqpConsumer = new AmqpConsumer(cfg, log);

let draining = false;
let lastWriteOk = false;
let inflight = 0;

// --------------------------------------------------------------- health server
const server = http.createServer((req, res) => {
  if (req.url === '/livez') {
    res.writeHead(200, { 'content-type': 'text/plain' }).end('ok\n');
    return;
  }
  if (req.url === '/healthz') {
    // Readiness. False when we cannot write or cannot consume: a worker that
    // cannot write must stop consuming, letting the queue build up visibly
    // instead of nacking in a tight loop (workers.md §7).
    const ok = amqpConsumer.isReady() && lastWriteOk && !draining;
    res.writeHead(ok ? 200 : 503, { 'content-type': 'text/plain' })
       .end(`${ok ? 'ready' : 'not ready'} inflight=${inflight}\n`);
    return;
  }
  if (req.url === '/metrics') {
    // Minimal Prometheus output: enough to see the pipeline is alive without a
    // second exporter. Real metrics live in the Valkey/RabbitMQ endpoints.
    res.writeHead(200, { 'content-type': 'text/plain' }).end(
      `# HELP fw_worker_inflight Messages currently being processed\n` +
      `# TYPE fw_worker_inflight gauge\n` +
      `fw_worker_inflight ${inflight}\n` +
      `# HELP fw_worker_write_ok 1 if the last write succeeded\n` +
      `# TYPE fw_worker_write_ok gauge\n` +
      `fw_worker_write_ok ${lastWriteOk ? 1 : 0}\n`,
    );
    return;
  }
  res.writeHead(404).end();
});
server.listen(cfg.healthPort, '0.0.0.0', () => {
  log.info('health server listening', { port: cfg.healthPort });
});

// ------------------------------------------------------------------ the handler
async function handleMessage(msg) {
  inflight += 1;
  try {
    // Parse first: a body that is not JSON can never succeed, so it goes to
    // the DLQ rather than retrying forever.
    let evt;
    try {
      evt = JSON.parse(msg.content.toString('utf8'));
    } catch (err) {
      log.warn('unparseable message, dead-lettering', { err: String(err) });
      return 'dlq';
    }

    try {
      validateEvent(evt);
    } catch (err) {
      if (err instanceof ValidationError) {
        log.warn('validation failed, dead-lettering', { id: evt?.id, reason: err.message });
        return 'dlq';
      }
      throw err;
    }

    const brokerTs = brokerTsMs(msg);
    if (brokerTs === null) {
      // Without a broker timestamp the alert fold cannot order this event. That
      // is a permanent misconfiguration (the interceptor is off), so retrying
      // will not help — but it also is not the producer's fault, so it must be
      // visible rather than silently dropped.
      log.error('message has no broker timestamp, dead-lettering', { id: evt.id });
      return 'dlq';
    }

    // ---- compress payload fields (never the envelope)
    const { payload: storedPayload, compressedFields } = compressPayload(evt.payload, {
      min: cfg.compressMin,
      level: cfg.compressLevel,
    });

    // ---- write + fold. Throws on transient failure -> requeue.
    //
    // ONE call now covers the record, both timeline indexes and the state rollup.
    // `applied` is the fold's verdict (docs/event-lifecycle.md §5.2):
    //
    //   0  stale or redelivered  -> nothing moved
    //   1  occurrence            -> count++, lastSeen moved
    //   3  retired               -> the condition is closed
    const { applied, bareKey } = await persistEvent({
      valkey, evt, storedPayload, brokerTs, cfg, fold, log,
    });
    lastWriteOk = true;

    // A redelivery of the same MESSAGE keeps its broker timestamp and id, so the
    // fold's (ts, id) watermark rejects it. Nothing downstream may see it: no audit
    // copy, because the first delivery already produced one, and no live frame, so
    // the tail does not show the same event twice.
    //
    // Still ACKED: requeueing a message that can never succeed would loop forever.
    if (applied === 0) {
      log.debug('event dropped as stale or redelivered', { id: evt.id, key: bareKey });
      return 'ack';
    }

    // A content duplicate is NOT a separate case any more. It hashes to the same
    // key, so the fold incremented `count` and moved `lastSeen` on the existing
    // record. There is no second record to reconcile and no owner id to chase —
    // which is why the whole `contentDuplicate` branch is gone.

    // ---- audit copy: PLAIN, confirmed, before the ack.
    // Reuse the original buffer: re-serializing would risk a different byte
    // sequence than what the producer sent.
    await amqpConsumer.publishAudit(msg.content);

    // ---- live fan-out, carrying the fold verdict.
    // Compression already applied, so the gateway can pass it through.
    try {
      await valkey.publish(
        keys.channelEvents,
        // brokerTs is included deliberately. It is the only timestamp that
        // reflects WHEN THIS EVENT ARRIVED, and a client needs it to move the
        // row's "Last occurrence". Without it the frontend falls back to
        // `prev.lastSeen` and Last occurrence silently stops advancing.
        JSON.stringify({
          // `key` is the record key the row lives at. It is carried on the event
          // frame because the browser joins the live tail to the open-alert table
          // on it, and it can no longer be derived from `source`+`type` (the key is
          // a hash of device + summary). Without it the client would have to
          // reimplement the hash, which is how the two would drift.
          event: { ...evt, payload: storedPayload, brokerTs, key: bareKey },
          applied,
        }),
      );
      if (applied === 1 || applied === 3) {
        // The wire key is the bare record key, not `source:type`. The client joins
        // on it directly, so the frame carries exactly what the row is keyed by.
        //
        // `brokerTs` is carried for ONE reason: a single arrival produces TWO
        // frames on TWO channels — `op: event` above and this one — and both reach
        // the same browser. The client must treat them as one delivery, and the
        // only field the two frames share is the broker timestamp.
        //
        // WITHOUT IT THE COUNT DOUBLED. Measured on the cluster: three arrivals
        // showed Count 6. The client's pairing guard compares this frame's
        // `brokerTs` against the one the `op: event` frame recorded; with the field
        // absent it saw `undefined` twice, matched nothing, and folded the same
        // arrival twice — once per frame.
        await valkey.publish(
          keys.channelAlerts,
          JSON.stringify({ key: bareKey, applied, brokerTs }),
        );
      }
    } catch (err) {
      // The event is durable already. A missed pub/sub frame is recovered by
      // the gateway's backfill, so this is not worth retrying.
      log.warn('publish to channel failed (durable write already done)', { id: evt.id, err: String(err) });
    }

    if (compressedFields.length > 0) {
      log.debug('compressed fields', { id: evt.id, fields: compressedFields });
    }
    log.debug('event persisted', { id: evt.id, applied, brokerTs });
    return 'ack';
  } catch (err) {
    lastWriteOk = false;
    log.error('write failed, requeueing', { err: String(err), stack: err?.stack });
    return 'requeue';
  } finally {
    inflight -= 1;
  }
}

// ---------------------------------------------------------------------- start
async function main() {
  log.info('worker starting', {
    queue: cfg.amqpQueue,
    sentinels: cfg.sentinels.length,
    masterName: cfg.masterName,
    compressMin: cfg.compressMin,
    eventTtl: cfg.eventTtl,
  });

  // Warm the fold script so a bad mount fails at boot, not on the first event.
  fold.get();

  // Connect Valkey first: if it is unreachable we should not consume at all,
  // because every message would be nacked immediately.
  let connected = false;
  for (let i = 0; i < 30 && !connected; i += 1) {
    connected = await valkey.ping();
    if (!connected) {
      log.warn('valkey not reachable yet, retrying', { attempt: i + 1 });
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  if (!connected) {
    log.error('could not reach valkey through sentinel; starting anyway with readiness false');
  }
  lastWriteOk = connected;

  // Repair a global event index that is missing records.
  //
  // Runs BEFORE consuming, so the All events tab is correct from the first moment
  // this worker is up rather than only after some later event happens to re-index.
  // Non-fatal by design: a reconcile failure must not stop event ingestion, and a
  // partial reconcile is still progress because the next restart re-compares the
  // same two sets.
  if (connected && cfg.indexReconcile) {
    try {
      const res = await reconcileEventIndex(valkey, cfg, log, {
        maxToAdd: cfg.indexReconcileMax,
      });
      if (res.added > 0) {
        log.warn('global event index repaired at startup', res);
      }
    } catch (err) {
      log.warn('index reconcile failed; ingestion continues', { err: String(err) });
    }
  }

  // Keep probing so readiness recovers without a restart.
  setInterval(async () => {
    if (!draining) lastWriteOk = await valkey.ping();
  }, 10000).unref();

  // Consume. Reconnect on channel death rather than exiting: an exit would drop
  // the prefetched messages back into a redelivery burst.
  const consumeLoop = async () => {
    while (!draining) {
      try {
        await amqpConsumer.connect();
        await amqpConsumer.consume(handleMessage);
        // consume() resolves once the subscription is set up; wait for close.
        await new Promise((resolve) => {
          const check = setInterval(() => {
            if (draining || !amqpConsumer.isReady()) {
              clearInterval(check);
              resolve();
            }
          }, 1000);
        });
      } catch (err) {
        log.error('amqp setup failed, retrying in 5s', { err: String(err) });
        await new Promise((r) => setTimeout(r, 5000));
      }
    }
  };
  consumeLoop();
}

// --------------------------------------------------------------------- signals
async function shutdown(signal) {
  log.info('shutting down', { signal });
  draining = true;
  await amqpConsumer.close();
  await valkey.quit();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 8000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('unhandledRejection', (err) => log.error('unhandled rejection', { err: String(err) }));
process.on('uncaughtException', (err) => {
  log.error('uncaught exception', { err: String(err), stack: err?.stack });
  process.exit(1);
});

main().catch((err) => {
  log.error('fatal startup error', { err: String(err), stack: err?.stack });
  process.exit(1);
});
