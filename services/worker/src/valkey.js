/**
 * Sentinel-aware Valkey client.
 *
 * Writes must reach the master only. This module resolves the master through
 * Sentinel, re-resolves on failure, and refuses to talk to a pod IP — that rule is
 * the last line of defence for the split-brain gap documented in
 * docs/fault-tolerance.md §4.
 *
 * Naming note: the class was `DragonflyClient`. It is now `ValkeyClient` because
 * the server is valkey-server; the protocol is unchanged (RESP), so the client
 * library and every command are the same.
 */
import Redis from 'ioredis';

export class ValkeyClient {
  constructor(cfg, log) {
    this.cfg = cfg;
    this.log = log;
    this.client = null;
    this.currentMaster = null;
    this.sentinels = cfg.sentinels.map(({ host, port }) => ({ host, port }));
    this._connecting = null;
  }

  /** Resolve the current master address from any reachable sentinel. */
  async resolveMaster() {
    let lastErr;
    for (const s of this.sentinels) {
      const sentinel = new Redis({
        host: s.host,
        port: s.port,
        lazyConnect: true,
        connectTimeout: 2000,
        maxRetriesPerRequest: 1,
        enableOfflineQueue: false,
        retryStrategy: () => null,
      });
      sentinel.on('error', () => { /* handled by catch */ });
      try {
        await sentinel.connect();
        const res = await sentinel.send_command('SENTINEL', [
          'get-master-addr-by-name',
          this.cfg.masterName,
        ]);
        if (Array.isArray(res) && res.length >= 2 && res[0]) {
          return { host: String(res[0]), port: Number(res[1]) };
        }
        throw new Error(`sentinel ${s.host} returned no master`);
      } catch (err) {
        lastErr = err;
        this.log.debug('sentinel lookup failed', { host: s.host, err: String(err) });
      } finally {
        try { sentinel.disconnect(); } catch { /* ignore */ }
      }
    }
    throw new Error(`no sentinel could resolve the master: ${lastErr ? String(lastErr) : 'none reachable'}`);
  }

  async connect() {
    if (this.client && this.client.status === 'ready') return this.client;
    if (this._connecting) return this._connecting;

    this._connecting = (async () => {
      const master = await this.resolveMaster();
      this.log.info('resolved valkey master', master);

      const client = new Redis({
        host: master.host,
        port: master.port,
        password: this.cfg.valkeyPassword,
        // Connect explicitly instead of letting the constructor dial in the
        // background. With enableOfflineQueue:false (which we need so a write fails
        // loudly rather than queueing forever) any command issued before the socket
        // is ready is rejected outright with
        //   "Stream isn't writeable and enableOfflineQueue options is false"
        // Calling ping() straight after `new Redis(...)` was therefore a race, and
        // it lost every time on a cold start.
        lazyConnect: true,
        maxRetriesPerRequest: 3,
        enableOfflineQueue: false,
        retryStrategy: (times) => Math.min(times * 200, 3000),
      });

      client.on('error', (err) => this.log.warn('valkey socket error', { err: String(err) }));
      client.on('ready', () => this.log.info('valkey connection ready', master));

      try {
        await client.connect();
        await client.ping();
      } catch (err) {
        // Abandon this client; a rejected ioredis instance is not reusable.
        try { client.disconnect(); } catch { /* ignore */ }
        throw err;
      }

      this.client = client;
      this.currentMaster = master;
      return client;
    })();

    try {
      return await this._connecting;
    } finally {
      // Clear the in-flight promise whether we succeeded OR failed. Leaving a
      // rejected promise in this field poisons every later call: connect() would
      // keep handing back the same rejection, so the 30-attempt startup retry burned
      // through its budget against one transient failure and then reported the
      // database unreachable while it was in fact healthy.
      this._connecting = null;
    }
  }

  /**
   * Run a command (or a batch) against the master, retrying once on a demoted node.
   *
   * Named `write` for historical reasons — the first caller was one — but it is the
   * single place that acquires a client and handles READONLY re-resolution, so reads
   * use it too (e.g. the startup index reconcile). On READONLY we are pointed at a
   * demoted node: re-resolve and retry exactly once, then let the caller decide.
   *
   * Valkey also answers `NOREPLICAS` when min-replicas-to-write is unsatisfied. That
   * is NOT retried here: it is not a routing problem, it is the intended refusal,
   * and retrying would spin against a master that is correctly refusing. The caller
   * requeues instead (docs/valkey.md §3).
   */
  async write(fn) {
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const client = await this.connect();
      try {
        return await fn(client);
      } catch (err) {
        const msg = String(err?.message || err);
        const retryable = msg.includes('READONLY') || msg.includes('Connection is closed');
        if (!retryable || attempt === 1) throw err;
        this.log.warn('valkey write failed, re-resolving master', { err: msg });
        await this.reset();
      }
    }
    throw new Error('unreachable');
  }

  /** True when the error is the engine refusing writes for lack of replicas. */
  static isNoReplicas(err) {
    return String(err?.message || err).includes('NOREPLICAS');
  }

  async reset() {
    if (this.client) {
      try { this.client.disconnect(); } catch { /* ignore */ }
    }
    this.client = null;
    this.currentMaster = null;
  }

  async ping() {
    try {
      const c = await this.connect();
      await c.ping();
      return true;
    } catch {
      return false;
    }
  }

  async publish(channel, payload) {
    return this.write((c) => c.publish(channel, payload));
  }

  async quit() {
    if (this.client) {
      try { await this.client.quit(); } catch { /* ignore */ }
    }
  }
}
