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

    // ---- Valkey, discovered through Sentinel. Never a pod IP.
    sentinels: required('SENTINELS')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
      .map((s) => {
        const [host, port] = s.split(':');
        if (!host || !port) throw new Error(`malformed SENTINELS entry: ${s}`);
        return { host, port: Number(port) };
      }),
    // The Sentinel monitor name (k8s/base/30-sentinel-config.yaml), not a hostname.
    masterName: required('VALKEY_MASTER_NAME'),
    valkeyPassword: required('VALKEY_PASSWORD'),

    // ---- behaviour
    eventTtl: int('FW_EVENT_TTL', 604800),
    indexPruneEvery: int('FW_INDEX_PRUNE_EVERY', 1000),
    // Retention for the per-ARRIVAL audit trail (docs/history.md).
    //
    // Deliberately much shorter than the 7-day record TTL. The record is one hash
    // per CONDITION and is the thing operators work from; history is one hash per
    // DELIVERY and exists to answer "what did we actually receive, and when". A
    // storm at 500/s would make three days of it larger than a week of records, so
    // it gets its own, shorter window.
    historyTtl: int('FW_HISTORY_TTL', 259200),
    // Reconcile idx:event:all against the per-source indexes at startup.
    //
    // Exists because events written before the global index existed were stored but
    // never indexed, so the All events tab could not list them — measured on the lab
    // as 3 index members for 7 stored records. Nothing in the read path can detect
    // that: it is missing data, not a bad query.
    //
    // On by default, bounded by indexReconcileMax, and it costs only a set
    // comparison when the gap is already zero (the normal case after the first run).
    // Set FW_INDEX_RECONCILE=0 on a very large history where the boot read is not
    // worth it.
    indexReconcile: optional('FW_INDEX_RECONCILE', '1') !== '0',
    indexReconcileMax: int('FW_INDEX_RECONCILE_MAX', 50000),
    compressMin: int('FW_COMPRESS_MIN', 1024),
    compressLevel: int('FW_COMPRESS_LEVEL', 6),
    foldScriptPath: optional('FW_FOLD_SCRIPT', '/opt/fold/event-fold.lua'),

    // ---- server
    healthPort: int('FW_HEALTH_PORT', 8080),
    logLevel: optional('FW_LOG_LEVEL', 'info'),

    // How long to wait before re-reading the fold script. The ConfigMap can be
    // updated without restarting the pod, so the worker re-checks periodically.
    foldReloadMs: int('FW_FOLD_RELOAD_MS', 60000),
  };
}

/**
 * Valkey key names. One place, so a typo cannot silently split the keyspace.
 *
 * SHAPE. There is ONE record per CONDITION, not per delivery:
 *
 *   event:<32 hex>        hash  the record (payload fields, count, state, acks)
 *   idx:event:all         zset  everything, scored by last broker ts
 *   idx:event:open        zset  open conditions, scored by last activity
 *   idx:event:closed      zset  retired conditions, scored by closedAt
 *   idx:source:<device>   zset  per-device timeline
 *
 * The record used to be two keys (`fw:evt:<id>` for a delivery and `fw:alert:<key>`
 * for its rollup). They were merged because with a content-derived key a repeat has
 * nowhere to go that is not the same key — so one hash holds the record, the
 * rollup, the state and the acks (docs/valkey.md §2).
 */
export const keys = {
  /** The record. `key` is the BARE key (no prefix). */
  event: (key) => `event:${key}`,

  /** Global timeline: bare key -> last broker timestamp. Backs All events. */
  eventAllIndex: 'idx:event:all',

  /**
   * Index of OPEN conditions, scored by last activity.
   *
   * Without it the only way to enumerate open rows is `SCAN event:*`, an unbounded
   * scan over the hot keyspace, which the design refuses (docs/websocket-server.md
   * §3). Maintained inside the fold so it can never disagree with the records.
   */
  eventOpenIndex: 'idx:event:open',

  /**
   * Index of RETIRED conditions, scored by `closedAt`.
   *
   * Needed because retiring removes a key from the open index, so without this
   * there is no way to list what closed — `op: closed` would have to SCAN.
   * Maintained by the fold and not by a second write, so a key is in at most one of
   * the two: a crash between the two would otherwise list a row as both open and
   * closed.
   */
  eventClosedIndex: 'idx:event:closed',

  /** Per-device timeline: bare key -> last broker timestamp. */
  sourceIndex: (source) => `idx:source:${source}`,

  /**
   * One record per DELIVERY, written BEFORE the fold — the raw audit log.
   *
   * The id is UNIQUE PER DELIVERY, not derived from the message id. Two reasons:
   *
   *   1. Every delivery gets a row. The write happens before the fold, so it cannot
   *      depend on a verdict the fold has not produced yet; nothing downstream may
   *      collapse or overwrite it.
   *   2. A redelivery (the same message arriving twice) is two rows. That is the
   *      point of a pre-dedup fork: the trail records what the broker actually
   *      handed us, so "did this arrive twice" is answerable.
   *
   * The CONDITION key travels inside the hash as a `key` field, not in the key
   * name. Putting it in the name would mean two conditions could collide on one
   * audit hash whenever their delivery ids coincided, and a condition that fires
   * 400 times would overwrite itself down to a single row.
   */
  history: (deliveryId) => `hist:${deliveryId}`,

  /** History index: delivery id -> broker timestamp. Backs the History tab. */
  historyIndex: 'idx:history',

  channelEvents: 'fw:ch:events',
  channelAlerts: 'fw:ch:alerts',
};
