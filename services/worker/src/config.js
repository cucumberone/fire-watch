function required(name) {
  const v = process.env[name];
  if (!v || v.trim() === '') throw new Error(`missing required environment variable ${name}`);
  return v;
}
function optional(name, fallback) {
  const v = process.env[name];
  return v && v.trim() !== '' ? v : fallback;
}
function int(name, fallback) {
  const raw = optional(name, String(fallback));
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 0) throw new Error(`invalid integer for ${name}: ${raw}`);
  return n;
}

export function loadConfig() {
  return {
    // ---- AMQP
    amqpHost: required('AMQP_HOST'),
    amqpPort: int('AMQP_PORT', 5672),
    amqpUser: required('AMQP_USER'),
    amqpPass: required('AMQP_PASS'),
    amqpQueue: required('AMQP_QUEUE'),
    amqpAuditExchange: required('AMQP_AUDIT_EXCHANGE'),
    amqpAuditKey: required('AMQP_AUDIT_KEY'),
    prefetch: int('AMQP_PREFETCH', 32),

    // ---- Dragonfly, discovered through Sentinel. Never a pod IP.
    sentinels: required('SENTINELS')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
      .map((s) => {
        const [host, port] = s.split(':');
        if (!host || !port) throw new Error(`malformed SENTINELS entry: ${s}`);
        return { host, port: Number(port) };
      }),
    masterName: required('DFLY_MASTER_NAME'),
    dragonflyPassword: required('DRAGONFLY_PASSWORD'),

    // ---- behaviour
    eventTtl: int('FW_EVENT_TTL', 604800),
    indexPruneEvery: int('FW_INDEX_PRUNE_EVERY', 1000),
    alertDedupWindow: int('FW_ALERT_DEDUP_WINDOW', 300000),
    compressMin: int('FW_COMPRESS_MIN', 1024),
    compressLevel: int('FW_COMPRESS_LEVEL', 6),
    foldScriptPath: optional('FW_FOLD_SCRIPT', '/opt/fold/alert-fold.lua'),

    // ---- server
    healthPort: int('FW_HEALTH_PORT', 8080),
    logLevel: optional('FW_LOG_LEVEL', 'info'),

    // How long to wait before re-reading the fold script. The ConfigMap can be
    // updated without restarting the pod, so the worker re-checks periodically.
    foldReloadMs: int('FW_FOLD_RELOAD_MS', 60000),
  };
}

/** Redis key names. One place, so a typo cannot silently split the keyspace. */
export const keys = {
  event: (id) => `fw:evt:${id}`,
  index: (source) => `fw:idx:src:${source}`,
  alert: (key) => `fw:alert:${key}`,
  alertOpen: (key) => `fw:alert:open:${key}`,
  // Index of OPEN alerts, so a client that connects later can be told what is
  // already firing. Without it the only way to enumerate open alerts is
  // `SCAN fw:alert:open:*`, which is an unbounded scan on the hot keyspace and
  // was explicitly rejected (docs/websocket-server.md §3). Scores are the last
  // activity timestamp, so a bounded read is "newest N open alerts".
  alertOpenIndex: 'fw:idx:alert:open',
  channelEvents: 'fw:ch:events',
  channelAlerts: 'fw:ch:alerts',
};

/** Alert identity. Default {source}:{type} (event-lifecycle.md §5.1). */
export function alertKey(evt) {
  return `${evt.source}:${evt.type}`;
}
