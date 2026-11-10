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

    channelEvents: required('FW_CHANNEL_EVENTS'),
    channelAlerts: required('FW_CHANNEL_ALERTS'),

    dedupeLru: int('FW_DEDUPE_LRU', 10000),
    backfillMax: int('FW_BACKFILL_MAX', 2000),
    // Cap on the open-alert snapshot sent on connect. Bounded for the same reason
    // backfill is: one client must not be able to make a pod do unbounded work.
    alertSnapshotMax: int('FW_ALERT_SNAPSHOT_MAX', 2000),
    maxConns: int('FW_MAX_CONNS', 5000),
    msgMaxBytes: int('FW_MSG_MAX_BYTES', 1048576),

    // per-connection guardrails (websocket-server.md §4)
    maxSubscriptions: int('FW_MAX_SUBSCRIPTIONS', 20),
    // Inbound frames per rolling second. This is a guard against a runaway
    // client, not against an operator: a bulk ack/close of 200 selected alerts
    // is a legitimate burst, and at the original default of 10 it was rejected
    // with `rate_limited: too many frames`. The client also paces its bulk
    // frames (FRAME_BUDGET_PER_SEC), so this is the backstop rather than the
    // mechanism.
    maxFramesPerSec: int('FW_MAX_FRAMES_PER_SEC', 50),
    maxOutboundQueue: int('FW_MAX_OUTBOUND_QUEUE', 1000),
    idleTimeoutMs: int('FW_IDLE_TIMEOUT_MS', 300000),
    heartbeatMs: int('FW_HEARTBEAT_MS', 20000),
    // How often to PING Dragonfly and report its health to clients.
    //
    // This is the only thing that lets the UI show the DATABASE being down rather
    // than just the socket: a client can be perfectly connected to a gateway whose
    // reads are failing. 10s is frequent enough to notice an incident and cheap
    // enough to ignore — one PING per pod per interval, not per client.
    dbProbeMs: int('FW_DB_PROBE_MS', 10000),
    // Consecutive failed probes before the database is reported as down.
    //
    // One failed PING is not proof of an outage: a demoted master, a Sentinel
    // re-resolve and a brief network blip all look identical. Reporting the first
    // failure would make the indicator flicker red on noise, which trains an
    // operator to ignore it — worse than not having the indicator.
    dbFailThreshold: int('FW_DB_FAIL_THRESHOLD', 3),
    pongTimeoutMs: int('FW_PONG_TIMEOUT_MS', 10000),

    // Acks live on the alert hash, so they expire with it. Only extended, never
    // created: an expired alert must not be resurrected as an ack-only entry.
    alertTtlMs: int('FW_ALERT_TTL', 604800) * 1000,

    // Operator journal.
    //
    // Lives on its own key, not on the alert hash: the fold owns that hash and
    // rewrites it atomically, so a journal HSET into it would race the fold.
    journalMaxEntries: int('FW_JOURNAL_MAX', 200),
    // Same retention as the alert it annotates. A journal that outlived its alert
    // would be an orphan list nothing can reach, and one that expired sooner would
    // lose notes about incidents still on the board.
    journalTtlMs: int('FW_JOURNAL_TTL', 604800) * 1000,

    // Publishing each entry to the audit exchange is what makes a note survive a
    // Dragonfly loss. Without it the note lives only in Dragonfly, so a snapshot
    // restore takes it with it — unlike events, which are recoverable from
    // q.audit.events. Optional so the gateway still starts in a dev harness that has
    // no broker; when unset, notes are stored but not replicated and the gateway
    // logs that once at startup rather than pretending they are durable.
    //
    // Assembled from parts rather than a single URL, matching the worker: the
    // RabbitMQ secret carries a password and the user lives in a ConfigMap, so there
    // is no one connection string to reference.
    amqpHost: optional('AMQP_HOST', ''),
    amqpPort: int('AMQP_PORT', 5672),
    amqpUser: optional('AMQP_USER', ''),
    amqpPass: optional('AMQP_PASS', ''),
    amqpAuditExchange: optional('AMQP_AUDIT_EXCHANGE', ''),
    amqpAuditKey: optional('AMQP_AUDIT_KEY', ''),

    port: int('FW_PORT', 8080),
    logLevel: optional('FW_LOG_LEVEL', 'info'),
  };
}

export const keys = {
  index: (source) => `fw:idx:src:${source}`,
  event: (id) => `fw:evt:${id}`,
  // Maintained by the worker's alert fold, scored by `closed_at`. Read-only here:
  // the gateway performs exactly one write in the whole system (an ack).
  alertClosedIndex: 'fw:idx:alert:closed',
};
