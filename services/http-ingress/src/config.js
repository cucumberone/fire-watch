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
    // How long a request may wait for the broker confirm before we 503. The
    // caller (nginx) must then treat it as retryable.
    confirmTimeoutMs: int('FW_CONFIRM_TIMEOUT_MS', 10000),

    // ---- downstream: RabbitMQ
    amqpHost: required('AMQP_HOST'),
    amqpPort: int('AMQP_PORT', 5672),
    amqpUser: required('AMQP_USER'),
    amqpPass: required('AMQP_PASS'),
    exchange: required('AMQP_EXCHANGE'),
    routingKey: required('AMQP_ROUTING_KEY'),
    // Bounded so one request cannot flood the broker.
    maxBatch: int('FW_MAX_BATCH', 500),
    // Channel pool: confirms are per-channel and serialise, so more than one
    // channel materially raises throughput under concurrency.
    channels: int('FW_CHANNELS', 4),

    logLevel: optional('FW_LOG_LEVEL', 'info'),
  };
}
