/**
 * http-ingress — external nginx (HTTP) → Valkey stream.
 *
 * Position in the system:
 *
 *   producer ──HTTP──> external nginx ──HTTP──> [http-ingress] ──XADD──> fw:stream:ingest
 *
 * This is a *producer*, not a consumer. It validates and normalises, appends the
 * batch to the ingest stream, and does not answer 202 until the master has
 * accepted the XADD (min-replicas-to-write satisfied) and WAIT has seen the
 * replicas acknowledge it.
 *
 * Status codes are a contract with nginx:
 *
 *   202  accepted and replicated           -> nginx may discard its buffer
 *   400  malformed body                    -> not retryable, fix the producer
 *   401  bad or missing token              -> not retryable
 *   405  method other than POST            -> not retryable
 *   413  body too large                    -> not retryable
 *   503  Valkey unavailable or unreplicated -> retryable, THIS IS THE IMPORTANT ONE
 *
 * A 503 means "I did not accept it". nginx must retry. Answering 202 and losing
 * the event would break the no-data-loss requirement.
 */
import http from 'node:http';
import { loadConfig } from './config.js';
import { createLogger } from './log.js';
import { ValkeyClient } from './valkey.js';
import { StreamWriter } from './stream.js';
import { normaliseBatch, streamFields, BadRequest, requiredFields, schemaFields } from './normalise.js';

const cfg = loadConfig();
const log = createLogger(cfg.logLevel);
const valkey = new ValkeyClient(cfg, log);
const stream = new StreamWriter(cfg, valkey, log);
let valkeyUp = false;

let requests = 0;
let accepted = 0;
let rejected = 0;
let unavailable = 0;

function json(res, status, body) {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(payload),
    // A rejected ingest must never be cached by an intermediary.
    'cache-control': 'no-store',
  });
  res.end(payload);
}

/** Constant-time-ish token comparison. Length is not secret. */
function tokenOk(header) {
  if (!cfg.ingestToken) return true;
  if (typeof header !== 'string' || header.length === 0) return false;
  const expected = cfg.ingestToken;
  if (header.length !== expected.length) return false;
  let diff = 0;
  for (let i = 0; i < expected.length; i += 1) diff |= header.charCodeAt(i) ^ expected.charCodeAt(i);
  return diff === 0;
}

function readBody(req, limit) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > limit) {
        const err = new Error(`body exceeds ${limit} bytes`);
        err.status = 413;
        req.destroy();
        reject(err);
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  const started = Date.now();

  // ---- health, on separate paths so nginx's own health check never counts
  if (req.url === '/livez') {
    res.writeHead(200, { 'content-type': 'text/plain' }).end('ok\n');
    return;
  }
  if (req.url === '/healthz') {
    // Ready only when we can durably accept: an ingress that cannot publish
    // must be taken out of the load balancer, or nginx will get 5xx and buffer.
    const ok = valkeyUp;
    res.writeHead(ok ? 200 : 503, { 'content-type': 'text/plain' })
       .end(`${ok ? 'ready' : 'not ready'}\n`);
    return;
  }
  if (req.url === '/metrics') {
    res.writeHead(200, { 'content-type': 'text/plain' }).end(
      `# TYPE fw_ingress_requests_total counter\nfw_ingress_requests_total ${requests}\n` +
      `# TYPE fw_ingress_accepted_total counter\nfw_ingress_accepted_total ${accepted}\n` +
      `# TYPE fw_ingress_rejected_total counter\nfw_ingress_rejected_total ${rejected}\n` +
      `# TYPE fw_ingress_unavailable_total counter\nfw_ingress_unavailable_total ${unavailable}\n` +
      `# TYPE fw_ingress_ready gauge\nfw_ingress_ready ${valkeyUp ? 1 : 0}\n`,
    );
    return;
  }

  // ---- only the configured path accepts events
  const path = (req.url || '').split('?')[0];
  if (path !== cfg.eventPath) {
    json(res, 404, { error: 'not_found', message: `POST ${cfg.eventPath}` });
    return;
  }
  if (req.method !== 'POST') {
    res.writeHead(405, { allow: 'POST' }).end();
    return;
  }

  requests += 1;

  // ---- auth. An unauthenticated ingest endpoint inside the cluster is an open
  // door: anything that can reach the Service can inject events.
  if (!tokenOk(req.headers['x-ingest-token'])) {
    rejected += 1;
    log.warn('rejected request: bad token', { remote: req.socket.remoteAddress });
    json(res, 401, { error: 'unauthorized' });
    return;
  }

  // ---- body
  let body;
  try {
    body = await readBody(req, cfg.maxBodyBytes);
  } catch (err) {
    rejected += 1;
    json(res, err.status || 400, { error: 'bad_body', message: err.message });
    return;
  }

  let parsed;
  try {
    parsed = JSON.parse(body.toString('utf8'));
  } catch (err) {
    rejected += 1;
    json(res, 400, { error: 'bad_json', message: 'body is not valid JSON' });
    return;
  }

  let events;
  try {
    events = normaliseBatch(parsed, { maxBatch: cfg.maxBatch });
  } catch (err) {
    rejected += 1;
    const status = err instanceof BadRequest ? err.status : 400;
    json(res, status, { error: 'invalid_event', message: err.message });
    return;
  }

  // ---- append, and only then answer
  const correlationId = req.headers['x-request-id'] || undefined;
  try {
    await stream.appendBatch(events.map((evt) => streamFields(evt, { src: 'http', correlationId })));
  } catch (err) {
    unavailable += 1;
    log.error('stream append failed', {
      err: String(err),
      count: events.length,
      durationMs: Date.now() - started,
    });
    json(res, 503, {
      error: 'store_unavailable',
      message: 'event was NOT accepted; retry',
    });
    return;
  }

  accepted += 1;
  log.info('accepted batch', {
    count: events.length,
    correlationId,
    durationMs: Date.now() - started,
  });
  // 202, not 201: the event is queued, not yet processed.
  json(res, 202, { accepted: events.length });
});

async function main() {
  // Load the ingest contract BEFORE listening.
  //
  // It is read from disk at runtime (docs/event-schema.md), so an image that was
  // built without schemas/ starts perfectly and then answers every EVT_* request
  // with "400 invalid_event: ENOENT". That is indistinguishable from a producer
  // sending a bad payload, and it cost real debugging time. Failing here turns a
  // silently degraded container into an obvious crash loop that the readiness
  // probe and the pod status both report immediately.
  try {
    log.info('ingest schema loaded', {
      fields: schemaFields().length,
      required: requiredFields().join(','),
    });
  } catch (err) {
    log.error('cannot load the ingest schema; refusing to start', {
      err: String(err),
      hint: 'the image must contain /schemas/event-ingest.schema.json',
    });
    process.exit(1);
  }

  log.info('http-ingress starting', {
    port: cfg.port,
    path: cfg.eventPath,
    stream: cfg.streamKey,
    waitReplicas: cfg.waitReplicas,
    sentinels: cfg.sentinels.length,
    maxBatch: cfg.maxBatch,
    authEnabled: Boolean(cfg.ingestToken),
  });

  server.listen(cfg.port, '0.0.0.0', () => {
    log.info('listening', { port: cfg.port, path: cfg.eventPath });
  });

  // Probe eagerly and keep probing so readiness follows Valkey without a
  // restart: a restart would not help a master that is still failing over.
  const probe = async () => {
    const up = await valkey.ping();
    if (up !== valkeyUp) log[up ? 'info' : 'warn'](up ? 'valkey reachable' : 'valkey unreachable');
    valkeyUp = up;
  };
  await probe();
  setInterval(probe, 5000).unref();
}

async function shutdown(signal) {
  log.info('shutting down', { signal });
  // Stop accepting first; in-flight requests finish their XADD/WAIT and answer.
  server.close(async () => {
    await valkey.quit();
    process.exit(0);
  });
  setTimeout(() => process.exit(0), 15000).unref();
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
