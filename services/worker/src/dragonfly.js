/**
 * Sentinel-aware Dragonfly client.
 *
 * Writes must reach the master only. This module resolves the master through
 * Sentinel, re-resolves on failure, and refuses to talk to a pod IP — that rule
 * is the last line of defence for the split-brain gap documented in
 * docs/fault-tolerance.md §4.
 */
import Redis from 'ioredis';

export class DragonflyClient {
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
      this.log.info('resolved dragonfly master', master);

      const client = new Redis({
        host: master.host,
        port: master.port,
        password: this.cfg.dragonflyPassword,
        lazyConnect: false,
        maxRetriesPerRequest: 3,
        // A write must fail loudly, not queue forever: the message stays
        // unacked and is redelivered.
        enableOfflineQueue: false,
        retryStrategy: (times) => Math.min(times * 200, 3000),
      });

      client.on('error', (err) => this.log.warn('dragonfly socket error', { err: String(err) }));
      client.on('ready', () => this.log.info('dragonfly connection ready', master));

      await client.ping();
      this.client = client;
      this.currentMaster = master;
      this._connecting = null;
      return client;
    })();

    return this._connecting;
  }

  /**
   * Run a write. On READONLY we are pointed at a demoted node: re-resolve and
   * retry exactly once, then let the caller requeue.
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
        this.log.warn('dragonfly write failed, re-resolving master', { err: msg });
        await this.reset();
      }
    }
    throw new Error('unreachable');
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
