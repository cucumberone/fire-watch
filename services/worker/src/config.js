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
    // Reconcile fw:idx:evt:all against the per-source indexes at startup.
    //
    // Exists because events written before the global index existed were stored but
    // never indexed, so the All events tab could not list them — measured on the lab
    // as 3 index members for 7 stored records. Nothing in the read path can detect
    // that: it is missing data, not a bad query.
    //
    // On by default, bounded by indexReconcileMax, and it costs only a set comparison
    // when the gap is already zero (the normal case after the first run). Set
    // FW_INDEX_RECONCILE=0 on a very large history where the boot read is not worth it.
    indexReconcile: optional('FW_INDEX_RECONCILE', '1') !== '0',
    indexReconcileMax: int('FW_INDEX_RECONCILE_MAX', 50000),
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
  /**
   * Global event timeline: event id -> broker timestamp.
   *
   * The per-source index cannot answer "every event in the last hour" — there is
   * no way to enumerate the sources without a SCAN, which this design refuses
   * everywhere. So the worker also writes each event here, in the same atomic
   * step as the source index.
   *
   * Pruned on the same cutoff as `fw:idx:src:*`, so it can never list an id whose
   * record has expired — that would render as a blank row.
   */
  eventAllIndex: 'fw:idx:evt:all',
  alert: (key) => `fw:alert:${key}`,
  alertOpen: (key) => `fw:alert:open:${key}`,
  // Index of OPEN alerts, so a client that connects later can be told what is
  // already firing. Without it the only way to enumerate open alerts is
  // `SCAN fw:alert:open:*`, which is an unbounded scan on the hot keyspace and
  // was explicitly rejected (docs/websocket-server.md §3). Scores are the last
  // activity timestamp, so a bounded read is "newest N open alerts".
  alertOpenIndex: 'fw:idx:alert:open',
  /**
   * Index of RETIRED alerts, scored by `closed_at`.
   *
   * Needed because retiring an alert removes it from the open index, so without
   * this there is no way to list what closed — the Closed tab would have to
   * SCAN the whole keyspace, which is the unbounded scan the design forbids
   * everywhere else.
   *
   * Maintained by the fold itself and not by a second write, so a key is in at
   * most one of the two indexes: a crash between the two would otherwise leave
   * an alert listed as both open and closed.
   */
  alertClosedIndex: 'fw:idx:alert:closed',
  /**
   * Content fingerprint -> the id of the event record that owns it.
   *
   * This is the content-dedup index (event-lifecycle.md §5.5). An event whose
   * fingerprint is already present is NOT stored: the original record absorbs it
   * by having its `count` incremented and its `last_seen` moved, and the new copy
   * is discarded.
   *
   * Keyed by fingerprint and not by id because the same logical alarm can arrive
   * with a different id each time — that is the whole reason id-only dedup let a
   * storm inflate the count.
   */
  fingerprint: (fp) => `fw:fp:${fp}`,
  channelEvents: 'fw:ch:events',
  channelAlerts: 'fw:ch:alerts',
};

/** Alert identity. Default {source}:{type} (event-lifecycle.md §5.1). */
export function alertKey(evt) {
  return `${evt.source}:${evt.type}`;
}
