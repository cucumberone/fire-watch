/** Structured logger — one JSON line per event, so `kubectl logs | jq` works. */
export function createLogger(level = 'info') {
  const order = { debug: 10, info: 20, warn: 30, error: 40 };
  const min = order[level] ?? order.info;
  const emit = (lvl, msg, fields) => {
    if ((order[lvl] ?? 20) < min) return;
    process.stdout.write(JSON.stringify({ ts: new Date().toISOString(), level: lvl, msg, ...(fields || {}) }) + '\n');
  };
  return {
    debug: (m, f) => emit('debug', m, f),
    info: (m, f) => emit('info', m, f),
    warn: (m, f) => emit('warn', m, f),
    error: (m, f) => emit('error', m, f),
  };
}
