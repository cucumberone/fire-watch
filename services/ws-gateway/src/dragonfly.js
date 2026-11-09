/**
 * Sentinel-aware READ-ONLY Dragonfly client.
 *
 * The gateway has no write access to Dragonfly by design: a gateway with no
 * write access cannot violate the write-guard invariant even if it is
 * misconfigured (websocket-server.md §5).
 *
 * This module therefore exposes only reads (subscribe, zrangebyscore, mget) and
 * has no `set`, `del`, or `eval` wrapper at all. That is deliberate — the
 * absence of a method is a stronger guarantee than a comment.
 */
import Redis from 'ioredis';

export class DragonflyReader {
  constructor(cfg, log) {
    this.cfg = cfg;
    this.log = log;
    this.client = null;      // reads
    this.subscriber = null;  // pub/sub, must be its own connection
  }

  async resolveMaster() {
    let lastErr;
    for (const s of this.cfg.sentinels) {
      const sentinel = new Redis({
        host: s.host,
        port: s.port,
        lazyConnect: true,
        connectTimeout: 2000,
        maxRetriesPerRequest: 1,
        enableOfflineQueue: false,
        retryStrategy: () => null,
      });
      sentinel.on('error', () => { /* handled below */ });
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
      } finally {
        try { sentinel.disconnect(); } catch { /* ignore */ }
      }
    }
    throw new Error(`no sentinel could resolve the master: ${lastErr ? String(lastErr) : 'none'}`);
  }

  async connect() {
    if (this.client && this.client.status === 'ready') return this.client;
    const master = await this.resolveMaster();
    this.log.info('resolved dragonfly master for reads', master);

    const opts = {
      host: master.host,
      port: master.port,
      password: this.cfg.dragonflyPassword,
      enableOfflineQueue: false,
      maxRetriesPerRequest: 2,
      retryStrategy: (times) => Math.min(times * 200, 3000),
    };
    this.client = new Redis(opts);
    this.client.on('error', (e) => this.log.warn('read socket error', { err: String(e) }));
    await this.client.ping();
    return this.client;
  }

  /** Subscribe to a channel. Callback receives the raw message string. */
  async subscribe(channel, onMessage) {
    if (this.subscriber) {
      this.log.debug('already subscribed', { channel });
      return;
    }
    const master = await this.resolveMaster();
    this.subscriber = new Redis({
      host: master.host,
      port: master.port,
      password: this.cfg.dragonflyPassword,
      enableOfflineQueue: false,
      retryStrategy: (times) => Math.min(times * 200, 3000),
    });
    this.subscriber.on('error', (e) => this.log.warn('subscriber socket error', { err: String(e) }));
    this.subscriber.on('message', (ch, msg) => {
      if (ch === channel) onMessage(msg);
    });
    await this.subscriber.subscribe(channel);
    this.log.info('subscribed', { channel });
  }

  async isSubscribed() {
    return Boolean(this.subscriber && this.subscriber.status === 'ready');
  }

  /** Read event ids in a time window from the per-source index. */
  async readIndex(source, min, max, limit) {
    const c = await this.connect();
    return c.zrangebyscore(`fw:idx:src:${source}`, min, max, 'LIMIT', 0, limit);
  }

  /** Read the records for a set of ids. Missing keys are filtered out. */
  async readEvents(ids) {
    if (ids.length === 0) return [];
    const c = await this.connect();
    const raw = await c.mget(ids.map((id) => `fw:evt:${id}`));
    const out = [];
    for (const item of raw) {
      if (!item) continue;
      try {
        out.push(JSON.parse(item));
      } catch (err) {
        // A record that does not parse is a write bug, not a client problem.
        // Skip it rather than failing the whole backfill.
        this.log.warn('unparseable stored record during backfill', { err: String(err) });
      }
    }
    return out;
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

  async quit() {
    for (const c of [this.client, this.subscriber]) {
      if (c) { try { await c.quit(); } catch { /* ignore */ } }
    }
  }
}
