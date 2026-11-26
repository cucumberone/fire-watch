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
    // ---- upstream of us: an external nginx speaking plain HTTP
    port: int('FW_PORT', 8080),
    // Path the external nginx posts events to. Anything else is 404.
    eventPath: optional('FW_EVENT_PATH', '/events'),
    // Shared secret the external nginx must send. Empty disables the check —
    // allowed for a lab, never for production.
    ingestToken: optional('FW_INGEST_TOKEN', ''),
    // Refuse bodies larger than this. Must be >= the largest valid event.
    maxBodyBytes: int('FW_MAX_BODY_BYTES', 2 * 1024 * 1024),
    // How long a request may wait for the replicas to acknowledge (WAIT) before
    // we 503. The caller (nginx) must then treat it as retryable.
    confirmTimeoutMs: int('FW_CONFIRM_TIMEOUT_MS', 10000),

    // ---- downstream: the Valkey ingest stream, master found through Sentinel
    sentinels: required('SENTINELS')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
      .map((s) => {
        const [host, port] = s.split(':');
        if (!host || !port) throw new Error(`malformed SENTINELS entry: ${s}`);
        return { host, port: Number(port) };
      }),
    masterName: required('VALKEY_MASTER_NAME'),
    valkeyPassword: required('VALKEY_PASSWORD'),
    streamKey: optional('FW_STREAM_KEY', 'fw:stream:ingest'),
    // Replicas that must acknowledge the XADD before we answer 202. 1 in
    // production (matches min-replicas-to-write); 0 in the single-pod lab.
    waitReplicas: int('FW_WAIT_REPLICAS', 1),
    // Bounded so one request cannot flood the stream.
    maxBatch: int('FW_MAX_BATCH', 500),

    logLevel: optional('FW_LOG_LEVEL', 'info'),
  };
}
