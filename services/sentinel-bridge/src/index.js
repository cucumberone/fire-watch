/**
 * sentinel-bridge — the control plane DragonflyDB does not have.
 *
 * Loop every FW_RECONCILE_INTERVAL:
 *   1. acquire/renew the leader Lease; if not leader, sleep.
 *   2. ask Sentinel who the master is.
 *   3. read every Dragonfly pod's actual ROLE.
 *   4. compute a plan (pure function, see plan.js).
 *   5. apply it: REPLICAOF on stragglers, Endpoints for the master Service.
 *
 * Specified by docs/redis-sentinel.md §4 and docs/dragonfly.md §3.
 *
 * Design rules enforced here:
 *   - Sentinel decides WHO the master is. Pods decide what they currently are.
 *     Disagreements are resolved in Sentinel's favour.
 *   - Write access is published through the Service Endpoints, never by asking
 *     Dragonfly about its own health. A degraded database cannot be trusted to
 *     report its own degradation.
 *   - When the master is not safely writable, the Endpoints are EMPTIED, so
 *     workers fail loudly instead of writing to a minority master.
 *   - No state is kept between cycles. Any replica can take over mid-flight.
 */
import http from 'node:http';
import { loadConfig } from './config.js';
import { createLogger } from './log.js';
import { LeaderElector } from './leader.js';
import {
  connectSentinel,
  getMasterAddr,
  getMasterFlags,
  requestFailover,
} from './sentinel.js';
import {
  createKube,
  listPods,
  readRole,
  applyReplicaOf,
  applyReplicaOfNoOne,
  patchEndpoints,
  setWritableLabel,
} from './kube.js';
import { computePlan } from './plan.js';

const cfg = loadConfig();
const log = createLogger(cfg.logLevel);

const identity = process.env.POD_NAME || `bridge-${process.pid}`;
const labelSelector = `app.kubernetes.io/name=${cfg.statefulSetName}`;

let ready = false;
let lastReconcileOk = false;
let lastFailoverRequest = 0;

// ---------------------------------------------------------------- health server
const server = http.createServer((req, res) => {
  if (req.url === '/livez') {
    res.writeHead(200, { 'content-type': 'text/plain' });
    res.end('ok\n');
    return;
  }
  if (req.url === '/readyz') {
    // Ready means "this process is participating", not "this process is the
    // leader". A standby is fully ready — it is doing its job by waiting.
    if (ready) {
      res.writeHead(200, { 'content-type': 'text/plain' });
      res.end('ready\n');
    } else {
      res.writeHead(503, { 'content-type': 'text/plain' });
      res.end('not ready\n');
    }
    return;
  }
  res.writeHead(404);
  res.end();
});
server.listen(cfg.healthPort, '0.0.0.0', () => {
  log.info('health server listening', { port: cfg.healthPort });
});

// ------------------------------------------------------------------ reconcile
const kube = createKube();
const elector = new LeaderElector({
  kc: kube.kc,
  namespace: cfg.namespace,
  leaseName: cfg.leaderLeaseName,
  identity,
  leaseDurationMs: cfg.leaseDurationMs,
  log,
});

let sentinelClient = null;

async function ensureSentinel() {
  if (sentinelClient && sentinelClient.status === 'ready') return sentinelClient;
  const { client } = await connectSentinel(cfg.sentinelHosts, log);
  sentinelClient = client;
  return client;
}

async function reconcile() {
  const leader = await elector.tryAcquireOrRenew();
  if (!leader) {
    ready = true;   // standby is ready by definition
    return;
  }

  // ---- 1. Sentinel's view ------------------------------------------------
  let sentinel = null;
  try {
    const client = await ensureSentinel();
    const master = await getMasterAddr(client, cfg.masterName, log);
    const masterFlags = await getMasterFlags(client, cfg.masterName, log);
    sentinel = { master, masterFlags };

    // Optional, off by default: ask Sentinel for a failover when the master is
    // unreachable AND Sentinel itself is not already handling it.
    if (cfg.enableFailoverRequest && master && masterFlags.length) {
      const alreadyFailingOver =
        masterFlags.includes('s_down') ||
        masterFlags.includes('odown') ||
        masterFlags.includes('failover_in_progress');
      const cooled = Date.now() - lastFailoverRequest > cfg.failoverCooldownMs;
      if (!alreadyFailingOver && cooled) {
        const reachable = await probeMaster(master);
        if (!reachable) {
          await requestFailover(client, cfg.masterName, log);
          lastFailoverRequest = Date.now();
        }
      }
    }
  } catch (err) {
    log.warn('sentinel unavailable; leaving cluster untouched', { err: String(err) });
    sentinelClient = null;
    sentinel = { master: null, masterFlags: [] };
  }

  // ---- 2. Pods' view -----------------------------------------------------
  const pods = await listPods(kube.core, cfg.namespace, labelSelector);
  if (pods.length === 0) {
    log.warn('no dragonfly pods found', { selector: labelSelector });
    return;
  }
  const podByName = new Map(pods.map((p) => [p.metadata.name, p]));
  const podIps = new Map(pods.map((p) => [p.metadata.name, p.status?.podIP]));
  const states = await Promise.all(pods.map((p) => readRole(p, cfg, log)));

  // ---- 3. Plan -----------------------------------------------------------
  const plan = computePlan({
    sentinel,
    pods: states,
    cfg: { minReplicas: cfg.minReplicas },
  });

  // ---- 4. Apply role fixes ----------------------------------------------
  for (const action of plan.actions) {
    log.info('applying role change', { action: action.type, pod: action.pod, reason: action.reason });
    if (action.type === 'replicaof') {
      await applyReplicaOf(podIps, action.pod, action.master, cfg, log);
    } else if (action.type === 'replicaof-no-one') {
      await applyReplicaOfNoOne(podIps, action.pod, cfg, log);
    }
  }

  // ---- 5. Publish write access ------------------------------------------
  await patchEndpoints(
    kube.core,
    cfg.namespace,
    cfg.dragonflyService,
    plan.endpoints,
    podByName,
    cfg,
    log,
  );

  // ---- 6. Label the pods for humans and alerts --------------------------
  const writableSet = new Set(plan.endpoints || []);
  for (const st of states) {
    const should = writableSet.has(st.name);
    const pod = podByName.get(st.name);
    const current = pod?.metadata?.labels?.[cfg.writableLabel];
    if (current !== String(should)) {
      await setWritableLabel(kube.core, cfg.namespace, st.name, should, cfg.writableLabel, log);
    }
  }

  for (const n of plan.notes) log.debug('plan note', { note: n });
  lastReconcileOk = true;
  ready = true;
}

/** Plain TCP-ish probe: can we AUTH + PING the master? */
async function probeMaster(master) {
  const { default: Redis } = await import('ioredis');
  const c = new Redis({
    host: master.host,
    port: master.port,
    password: cfg.dragonflyPassword,
    lazyConnect: true,
    connectTimeout: 2000,
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false,
    retryStrategy: () => null,
  });
  c.on('error', () => { /* handled below */ });
  try {
    await c.connect();
    await c.ping();
    return true;
  } catch {
    return false;
  } finally {
    try { c.disconnect(); } catch { /* ignore */ }
  }
}

// ---------------------------------------------------------------------- loop
let stopping = false;

async function loop() {
  while (!stopping) {
    const started = Date.now();
    try {
      await reconcile();
    } catch (err) {
      lastReconcileOk = false;
      log.error('reconcile failed', { err: String(err), stack: err?.stack });
      // A failed cycle must not take the process down: a restart would just
      // repeat it, and the leader lease would flap.
      if (sentinelClient) {
        try { sentinelClient.disconnect(); } catch { /* ignore */ }
        sentinelClient = null;
      }
    }
    const elapsed = Date.now() - started;
    const sleep = Math.max(200, cfg.reconcileIntervalMs - elapsed);
    await new Promise((r) => setTimeout(r, sleep));
  }
}

async function shutdown(signal) {
  log.info('shutting down', { signal });
  stopping = true;
  try { await elector.release(); } catch { /* ignore */ }
  if (sentinelClient) {
    try { sentinelClient.disconnect(); } catch { /* ignore */ }
  }
  server.close(() => process.exit(0));
  // Do not hang forever on a stuck socket.
  setTimeout(() => process.exit(0), 5000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

process.on('unhandledRejection', (err) => {
  log.error('unhandled rejection', { err: String(err) });
});
process.on('uncaughtException', (err) => {
  log.error('uncaught exception', { err: String(err), stack: err?.stack });
  // Exit so the runtime restarts us; continuing after an unknown fault in a
  // controller that mutates cluster state is worse than restarting.
  process.exit(1);
});

log.info('sentinel-bridge starting', {
  identity,
  namespace: cfg.namespace,
  masterName: cfg.masterName,
  service: cfg.dragonflyService,
  minReplicas: cfg.minReplicas,
  intervalMs: cfg.reconcileIntervalMs,
  failoverRequestEnabled: cfg.enableFailoverRequest,
});

loop();
