/**
 * worker — RabbitMQ -> Dragonfly event pipeline.
 *
 * Pipeline (docs/workers.md §3):
 *
 *   validate -> compress -> record -> index -> alert fold
 *            -> audit copy (plain, confirmed) -> publish -> ack
 *
 * The order is load-bearing:
 *   - the write happens before the ack, or a crash loses the event
 *   - the audit copy is confirmed before the ack, or Dragonfly becomes the only
 *     copy and it has no AOF
 *   - the ack is last, always
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
import { DragonflyClient } from './dragonfly.js';
import { FoldScript, persistEvent } from './store.js';
import { AmqpConsumer } from './amqp.js';

const cfg = loadConfig();
const log = createLogger(cfg.logLevel);

const dfl = new DragonflyClient(cfg, log);
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
    // second exporter. Real metrics live in the Dragonfly/RabbitMQ endpoints.
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
    const { applied, duplicate } = await persistEvent({
      dfl, evt, storedPayload, brokerTs, cfg, fold, log,
    });
    lastWriteOk = true;

    // ---- duplicate: dropped, and deliberately silent.
    //
    // The event record already existed for this id, so this is the same event
    // being delivered again (a redelivery, or a producer re-sending an id it
    // already used). Nothing downstream may see it: no fold, so no count and no
    // Last occurrence change; no audit copy, because the first delivery already
    // produced one; no live frame, so the tail does not show the same event
    // twice.
    //
    // Still ACKED. Requeueing would loop forever on a message that can never
    // succeed.
    if (duplicate) {
      log.debug('duplicate event id, dropped', { id: evt.id });
      return 'ack';
    }

    // ---- audit copy: PLAIN, confirmed, before the ack.
    // Reuse the original buffer: re-serializing would risk a different byte
    // sequence than what the producer sent.
    await amqpConsumer.publishAudit(msg.content);

    // ---- live fan-out, carrying the fold verdict.
    // Compression already applied, so the gateway can pass it through.
    try {
      await dfl.publish(
        keys.channelEvents,
        // brokerTs is included deliberately. It is the only timestamp that
        // reflects WHEN THIS EVENT ARRIVED, and a client needs it to move the
        // row's "Last occurrence". Without it the frontend falls back to
        // `prev.lastSeen` and Last occurrence silently stops advancing.
        JSON.stringify({
          event: { ...evt, payload: storedPayload, brokerTs },
          applied,
        }),
      );
      if (applied === 1 || applied === 3) {
        await dfl.publish(keys.channelAlerts, JSON.stringify({ key: `${evt.source}:${evt.type}`, applied }));
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

  // Connect Dragonfly first: if it is unreachable we should not consume at all,
  // because every message would be nacked immediately.
  let connected = false;
  for (let i = 0; i < 30 && !connected; i += 1) {
    connected = await dfl.ping();
    if (!connected) {
      log.warn('dragonfly not reachable yet, retrying', { attempt: i + 1 });
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  if (!connected) {
    log.error('could not reach dragonfly through sentinel; starting anyway with readiness false');
  }
  lastWriteOk = connected;

  // Keep probing so readiness recovers without a restart.
  setInterval(async () => {
    if (!draining) lastWriteOk = await dfl.ping();
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
  await dfl.quit();
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
