/**
 * Sentinel client.
 *
 * Sentinel speaks RESP, so a normal Redis client works. We connect to the
 * sentinel port, not the data port.
 */
import Redis from 'ioredis';

/**
 * Ask Sentinel which pod is currently the master.
 * Returns { host, port } or null when Sentinel cannot answer.
 */
export async function getMasterAddr(client, masterName, log) {
  try {
    const res = await client.send_command('SENTINEL', ['get-master-addr-by-name', masterName]);
    if (!Array.isArray(res) || res.length < 2) return null;
    const [host, port] = res;
    if (!host) return null;
    return { host: String(host), port: Number(port) };
  } catch (err) {
    log.warn('sentinel get-master-addr failed', { masterName, err: String(err) });
    return null;
  }
}

/**
 * Ask Sentinel what it believes about the replicas.
 * Returns [] when Sentinel cannot answer.
 */
export async function getReplicas(client, masterName, log) {
  try {
    const res = await client.send_command('SENTINEL', ['replicas', masterName]);
    if (!Array.isArray(res)) return [];
    // Each entry is a flat alternating array of field/value.
    return res.map((entry) => {
      const obj = {};
      for (let i = 0; i + 1 < entry.length; i += 2) {
        obj[entry[i]] = entry[i + 1];
      }
      return {
        name: obj.name,          // may be undefined
        ip: obj.ip,
        port: Number(obj.port),
        flags: (obj.flags || '').split(','),
        masterLinkStatus: obj['master-link-status'],
      };
    });
  } catch (err) {
    log.warn('sentinel replicas failed', { masterName, err: String(err) });
    return [];
  }
}

/**
 * Sentinel's own liveness judgement for the master. Used only to decide whether
 * we should even look at the write guard: if Sentinel says the master is
 * s_down/odown, a failover is already in progress and removing the endpoint
 * would be a second, conflicting action.
 */
export async function getMasterFlags(client, masterName, log) {
  try {
    const res = await client.send_command('SENTINEL', ['master', masterName]);
    if (!Array.isArray(res)) return [];
    const obj = {};
    for (let i = 0; i + 1 < res.length; i += 2) obj[res[i]] = res[i + 1];
    return (obj.flags || '').split(',').filter(Boolean);
  } catch (err) {
    log.warn('sentinel master failed', { masterName, err: String(err) });
    return [];
  }
}

export async function requestFailover(client, masterName, log) {
  try {
    await client.send_command('SENTINEL', ['failover', masterName]);
    log.warn('requested sentinel failover', { masterName });
    return true;
  } catch (err) {
    log.error('sentinel failover request failed', { masterName, err: String(err) });
    return false;
  }
}

/**
 * Connect to the first Sentinel that answers. Returns { client, endpoint }.
 * Throws only if every sentinel is unreachable, so the caller can retry.
 */
export async function connectSentinel(hosts, log) {
  let lastErr;
  for (const { host, port } of hosts) {
    const client = new Redis({
      host,
      port,
      lazyConnect: true,
      connectTimeout: 3000,
      maxRetriesPerRequest: 1,
      // Sentinel is a control plane: fail fast and let the loop retry rather
      // than buffering commands.
      enableOfflineQueue: false,
      retryStrategy: () => null,
    });
    client.on('error', (e) => log.debug('sentinel socket error', { host, port, err: String(e) }));
    try {
      await client.connect();
      await client.ping();
      log.info('connected to sentinel', { host, port });
      return { client, endpoint: { host, port } };
    } catch (err) {
      lastErr = err;
      try { client.disconnect(); } catch { /* ignore */ }
      log.debug('sentinel unreachable', { host, port, err: String(err) });
    }
  }
  throw new Error(`no sentinel reachable: ${lastErr ? String(lastErr) : 'no hosts'}`);
}
