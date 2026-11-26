/**
 * worker — Valkey ingest stream -> Valkey event pipeline.
 *
 * Pipeline (docs/workers.md §3):
 *
 *   XREADGROUP -> validate -> compress -> history fork
 *              -> record+index+fold (ONE atomic script) -> publish -> XACK+XDEL
 *
 * The order is load-bearing:
 *   - the write happens before the XACK+XDEL, or a crash loses the event
 *   - XACK+XDEL is last, always; until then the entry sits in the pending list
 *     and is retried (or claimed by another worker if this one dies)
 *
 * The record, its indexes and the state fold are a single script: with a
 * content-derived key the record and its rollup both live at `event:<key>`, so
 * one atomic call covers them (docs/valkey.md §2).
 */
import http from 'node:http';
import { loadConfig, keys } from './config.js';
import { createLogger } from './log.js';
import {
  validateEvent,
  ValidationError,
  compressPayload,
  entryTsMs,
} from './event.js';
import { ValkeyClient } from './valkey.js';
import { bareKeyFor } from './keygen.js';
import { FoldScript, persistEvent, reconcileEventIndex, recordDelivery, deliveryIdFor } from './store.js';
import { StreamConsumer } from './stream.js';

const cfg = loadConfig();
const log = createLogger(cfg.logLevel);

const valkey = new ValkeyClient(cfg, log);
// Separate connection: XREADGROUP BLOCK holds its socket for up to streamBlockMs,
// and every fold write queued behind it would wait that long.
const streamConn = new ValkeyClient(cfg, log);
const fold = new FoldScript(cfg.foldScriptPath, log, cfg.foldReloadMs);
const consumer = new StreamConsumer(cfg, streamConn, valkey, log);

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
    // Readiness. False when we cannot write or cannot read the stream: entries
    // then stay in the stream, visibly, instead of failing in a tight loop
    // (workers.md §7).
    const ok = consumer.isReady() && lastWriteOk && !draining;
    res.writeHead(ok ? 200 : 503, { 'content-type': 'text/plain' })
       .end(`${ok ? 'ready' : 'not ready'} inflight=${inflight}\n`);
    return;
  }
  if (req.url === '/metrics') {
    // Minimal Prometheus output: enough to see the pipeline is alive without a
    // second exporter. Real metrics live in the Valkey exporter.
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
/**
 * Process one stream entry. Returns:
 *   'ack'    -> XACK + XDEL
 *   'retry'  -> leave it pending; re-read after retryMs
 *   'dlq'    -> move to the dead-letter stream (can never succeed)
 */
async function handleEntry(entry) {
  inflight += 1;
  try {
    // Parse first: a body that is not JSON can never succeed, so it goes to
    // the DLQ rather than retrying forever.
    let evt;
    try {
      evt = JSON.parse(entry.fields.evt);
    } catch (err) {
      log.warn('unparseable entry, dead-lettering', { entry: entry.id, err: String(err) });
      return { outcome: 'dlq', reason: `unparseable: ${err.message}` };
    }

    try {
      validateEvent(evt);
    } catch (err) {
      if (err instanceof ValidationError) {
        log.warn('validation failed, dead-lettering', { id: evt?.id, reason: err.message });
        return { outcome: 'dlq', reason: `invalid: ${err.message}` };
      }
      throw err;
    }

    // The arrival time is the entry ID, assigned by the master at XADD. It can
    // only be missing if the ID is malformed, which no retry fixes.
    const brokerTs = entryTsMs(entry.id);
    if (brokerTs === null) {
      log.error('entry id carries no timestamp, dead-lettering', { entry: entry.id, id: evt.id });
      return { outcome: 'dlq', reason: 'no timestamp in entry id' };
    }

    // ---- compress payload fields (never the envelope)
    const { payload: storedPayload, compressedFields } = compressPayload(evt.payload, {
      min: cfg.compressMin,
      level: cfg.compressLevel,
    });

    // ---- 1. FORK THE DELIVERY into the audit trail, BEFORE any dedup.
    //
    // This is the raw log: one row per message we were handed, written
    // unconditionally and never revisited. It happens before the fold on purpose,
    // or the trail could only describe the arrivals the fold ACCEPTED — which is
    // the one thing an audit trail must not do.
    //
    // It also has to succeed before the XACK: a delivery that was processed but not
    // recorded is a hole in the log, and holes are what an audit trail exists to
    // rule out. A failure here is retried, exactly like a failed record write.
    //
    // The delivery id is the STREAM ENTRY ID. It is already unique per delivery —
    // the master assigned it at XADD — so there is nothing to mint, and reusing it
    // means an audit row can be traced back to the exact stream entry it came from.
    const deliveryId = deliveryIdFor(entry.id);
    await recordDelivery({
      valkey,
      evt,
      storedPayload,
      brokerTs,
      bareKey: bareKeyFor(evt),
      deliveryId,
      cfg,
      log,
    });

    // ---- 2. write + fold. Throws on transient failure -> retry.
    //
    // ONE call covers the record, both timeline indexes and the state rollup.
    // `applied` is the fold's verdict (docs/event-lifecycle.md §5.2):
    //
    //   0  stale or redelivered  -> nothing moved (but the fork above stands)
    //   1  occurrence            -> count++, lastSeen moved
    //   3  retired               -> the condition is closed
    const { applied, bareKey } = await persistEvent({
      valkey, evt, storedPayload, brokerTs, cfg, fold, log,
      // The arrival identity for this delivery: the stream entry id. Unique per
      // entry, ordered within a millisecond, and the same value the audit row is
      // keyed by — so a record's `lastId` points at an addressable `hist:` row.
      entryId: entry.id,
    });
    lastWriteOk = true;

    // A redelivery of the same ENTRY keeps its ID (so its timestamp) and its event
    // id, so the fold's (ts, id) watermark rejects it. Nothing downstream may see
    // it: no live frame, so the tail does not show the same event twice.
    //
    // Still removed from the stream: retrying an entry that can never apply would
    // loop forever.
    if (applied === 0) {
      log.debug('event dropped as stale or redelivered', { id: evt.id, key: bareKey });
      return { outcome: 'ack' };
    }

    // A content duplicate is NOT a separate case any more. It hashes to the same
    // key, so the fold incremented `count` and moved `lastSeen` on the existing
    // record. There is no second record to reconcile and no owner id to chase —
    // which is why the whole `contentDuplicate` branch is gone.

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
          //
          // `deliveryId` is the stream entry id and is carried for the same reason
          // `brokerTs` is: one arrival produces two frames on two channels, and the
          // client must fold it once. Unlike a timestamp it is exactly unique, so
          // two arrivals in the same millisecond are still told apart.
          event: {
            ...evt, payload: storedPayload, brokerTs, key: bareKey, deliveryId,
          },
          applied,
          deliveryId,
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
          JSON.stringify({ key: bareKey, applied, brokerTs, deliveryId }),
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
    return { outcome: 'ack' };
  } catch (err) {
    lastWriteOk = false;
    log.error('write failed, will retry from the pending list', {
      entry: entry.id, err: String(err), stack: err?.stack,
    });
    return { outcome: 'retry' };
  } finally {
    inflight -= 1;
  }
}

/**
 * Run a batch concurrently, then settle each entry. Returns true if any entry
 * must be retried.
 */
async function processBatch(entries) {
  const results = await Promise.all(entries.map(async (entry) => {
    let r;
    try {
      r = await handleEntry(entry);
    } catch (err) {
      // An unexpected throw is more likely transient than permanent; the opposite
      // assumption silently discards data.
      log.error('handler threw, will retry', { entry: entry.id, err: String(err) });
      r = { outcome: 'retry' };
    }
    if (r.outcome === 'dlq') {
      try {
        await consumer.deadLetter(entry, r.reason);
      } catch (err) {
        log.error('dead-letter failed, will retry', { entry: entry.id, err: String(err) });
        return { entry, outcome: 'retry' };
      }
    }
    return { entry, outcome: r.outcome };
  }));

  const done = results.filter((r) => r.outcome === 'ack').map((r) => r.entry.id);
  try {
    await consumer.ackDelete(done);
  } catch (err) {
    // The writes are done; the entries stay pending and are redelivered, which
    // the fold's watermark absorbs (applied 0). At-least-once by design.
    log.warn('XACK/XDEL failed, entries will be redelivered', { count: done.length, err: String(err) });
    return true;
  }
  return results.some((r) => r.outcome === 'retry');
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * The consume loop.
 *
 * Mostly `XREADGROUP ... >` (new entries, blocking). After a failure it re-reads
 * this consumer's own pending entries with `0` once `retryMs` has passed, and
 * every `claimIntervalMs` it XAUTOCLAIMs entries a dead consumer left behind.
 * A fresh process also starts with one `0` pass, in case its pod name already
 * owns pending entries.
 */
async function consumeLoop() {
  let pendingDueAt = 0;
  let claimDueAt = 0;
  while (!draining) {
    try {
      if (!consumer.isReady()) await consumer.ensureGroup();

      const now = Date.now();
      let entries;
      let fromPending = false;
      if (pendingDueAt !== null && now >= pendingDueAt) {
        pendingDueAt = null;
        fromPending = true;
        entries = await consumer.read('0');
      } else if (now >= claimDueAt) {
        claimDueAt = now + cfg.claimIntervalMs;
        entries = await consumer.claim();
        if (entries.length > 0) log.warn('claimed entries from an idle consumer', { count: entries.length });
        consumer.gcConsumers().catch((err) => log.debug('consumer gc failed', { err: String(err) }));
      } else {
        entries = await consumer.read('>');
      }

      if (entries.length === 0) continue;
      const retry = await processBatch(entries);
      if (retry && pendingDueAt === null) pendingDueAt = Date.now() + cfg.retryMs;
      // A full page of pending entries means there may be more behind it.
      else if (fromPending && entries.length >= cfg.streamBatch && pendingDueAt === null) pendingDueAt = 0;
      // Writes are failing: slow down rather than pull more entries into the
      // pending list at full speed.
      if (retry) await sleep(1000);
    } catch (err) {
      log.error('stream read failed, retrying in 2s', { err: String(err) });
      await streamConn.reset();
      await sleep(2000);
    }
  }
}

// ---------------------------------------------------------------------- start
async function main() {
  log.info('worker starting', {
    stream: cfg.streamKey,
    group: cfg.streamGroup,
    consumer: cfg.consumerName,
    sentinels: cfg.sentinels.length,
    masterName: cfg.masterName,
    compressMin: cfg.compressMin,
    eventTtl: cfg.eventTtl,
  });

  // Warm the fold script so a bad mount fails at boot, not on the first event.
  fold.get();

  // Connect Valkey first: if it is unreachable we should not read at all,
  // because every entry would fail immediately.
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

  // Consume. Stream errors are retried inside the loop rather than exiting: an
  // exit would only push this pod's pending entries onto XAUTOCLAIM.
  loopDone = consumeLoop();
}

// --------------------------------------------------------------------- signals
let loopDone = Promise.resolve();

async function shutdown(signal) {
  log.info('shutting down', { signal });
  draining = true;
  // Hard stop well inside terminationGracePeriodSeconds. Anything still pending is
  // claimed by another worker after claimIdleMs — nothing is lost, only delayed.
  setTimeout(() => process.exit(0), 20000).unref();
  // Let the batch in flight finish its writes and XACK+XDEL; the blocking read
  // returns within streamBlockMs.
  await loopDone;
  await streamConn.quit();
  await valkey.quit();
  server.close(() => process.exit(0));
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
