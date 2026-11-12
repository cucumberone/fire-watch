/**
 * Configuration for sentinel-bridge.
 *
 * Every value is overridable by environment variable and validated at boot —
 * a bridge that starts with a wrong namespace or master name would silently
 * patch the wrong Service, which is worse than failing to start.
 */

function required(name) {
  const v = process.env[name];
  if (!v || v.trim() === '') {
    throw new Error(`missing required environment variable ${name}`);
  }
  return v;
}

function optional(name, fallback) {
  const v = process.env[name];
  return v && v.trim() !== '' ? v : fallback;
}

function durationMs(name, fallbackMs) {
  const raw = optional(name, `${fallbackMs}ms`);
  const m = /^(\d+)(ms|s|m)$/.exec(raw.trim());
  if (!m) {
    throw new Error(`invalid duration for ${name}: ${raw} (expected e.g. 5s)`);
  }
  const n = Number(m[1]);
  const mult = m[2] === 'ms' ? 1 : m[2] === 's' ? 1000 : 60_000;
  return n * mult;
}

function integer(name, fallback) {
  const raw = optional(name, String(fallback));
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 0) {
    throw new Error(`invalid integer for ${name}: ${raw}`);
  }
  return n;
}

export function loadConfig() {
  return {
    namespace: required('FW_NAMESPACE'),

    // Sentinel
    sentinelHosts: required('SENTINELS')            // "h1:p,h2:p,h3:p"
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
      .map((s) => {
        const [host, port] = s.split(':');
        if (!host || !port) throw new Error(`malformed SENTINELS entry: ${s}`);
        return { host, port: Number(port) };
      }),
    masterName: required('FW_VALKEY_MASTER'),

    // Valkey / Kubernetes topology
    valkeyService: required('FW_VALKEY_SERVICE'),
    valkeyHeadless: optional('FW_VALKEY_HEADLESS', 'valkey-headless'),
    valkeyPort: integer('FW_VALKEY_PORT', 6379),
    valkeyPassword: required('VALKEY_PASSWORD'),
    statefulSetName: optional('FW_VALKEY_STATEFULSET', 'valkey'),

    // Write guard
    minReplicas: integer('FW_WRITE_GUARD_MIN_REPLICAS', 2),
    writableLabel: optional('FW_WRITABLE_LABEL', 'fire-watch.io/writable'),

    // Loop
    reconcileIntervalMs: durationMs('FW_RECONCILE_INTERVAL', 5000),
    leaderLeaseName: optional('FW_LEASE_NAME', 'sentinel-bridge-leader'),
    leaseDurationMs: durationMs('FW_LEASE_DURATION', 15_000),

    // Health endpoint
    healthPort: integer('FW_HEALTH_PORT', 8081),

    // Failover. Off by default: Sentinel must be the component that decides a
    // master is gone. The bridge asking for a failover because ITS probe failed
    // would let a network problem on the bridge pod start an unnecessary
    // promote. Set to "true" only when you want that behaviour.
    enableFailoverRequest: optional('FW_ENABLE_FAILOVER_REQUEST', 'false') === 'true',
    failoverCooldownMs: durationMs('FW_FAILOVER_COOLDOWN', 300_000),

    logLevel: optional('FW_LOG_LEVEL', 'info'),
  };
}
