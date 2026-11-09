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
    maxConns: int('FW_MAX_CONNS', 5000),
    msgMaxBytes: int('FW_MSG_MAX_BYTES', 1048576),

    // per-connection guardrails (websocket-server.md §4)
    maxSubscriptions: int('FW_MAX_SUBSCRIPTIONS', 20),
    maxFramesPerSec: int('FW_MAX_FRAMES_PER_SEC', 10),
    maxOutboundQueue: int('FW_MAX_OUTBOUND_QUEUE', 1000),
    idleTimeoutMs: int('FW_IDLE_TIMEOUT_MS', 300000),
    heartbeatMs: int('FW_HEARTBEAT_MS', 20000),
    pongTimeoutMs: int('FW_PONG_TIMEOUT_MS', 10000),

    port: int('FW_PORT', 8080),
    logLevel: optional('FW_LOG_LEVEL', 'info'),
  };
}

export const keys = {
  index: (source) => `fw:idx:src:${source}`,
  event: (id) => `fw:evt:${id}`,
};
