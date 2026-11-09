/**
 * Sentinel-aware Dragonfly client for the gateway.
 *
 * The gateway's own data path is READ-ONLY: it streams events to browsers and
 * never modifies the event log. There is deliberately no generic `set`, `del`
 * or `eval` wrapper, because the absence of a method is a stronger guarantee
 * than a comment (websocket-server.md §5).
 *
 * The one exception is `ackAlert`: acknowledging an alert is an operator action
 * issued from the UI, and it writes exactly two fields on one alert hash plus a
 * pub/sub frame. It is implemented as a named method with a fixed command, not
 * as a general write escape hatch, so this file still cannot be used to modify
 * events or indexes (docs/acknowledge.md §4).
 */
import Redis from 'ioredis';

export class DragonflyReader {
  constructor(cfg, log) {
    this.cfg = cfg;
    this.log = log;
    this.client = null;      // reads (and the one narrow write)
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
      // lazyConnect + explicit connect(). With enableOfflineQueue:false any
      // command sent before the socket is ready is rejected with
      //   "Stream isn't writeable and enableOfflineQueue options is false"
      // so issuing ping() right after the constructor was a race. The gateway
      // lost it at boot and logged "subscribe failed, retrying in 3s".
      lazyConnect: true,
      enableOfflineQueue: false,
      maxRetriesPerRequest: 2,
      retryStrategy: (times) => Math.min(times * 200, 3000),
    };
    this.client = new Redis(opts);
    this.client.on('error', (e) => this.log.warn('read socket error', { err: String(e) }));
    await this.client.connect();
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
      // Same race as connect(): connect explicitly before issuing commands.
      lazyConnect: true,
      enableOfflineQueue: false,
      retryStrategy: (times) => Math.min(times * 200, 3000),
    });
    this.subscriber.on('error', (e) => this.log.warn('subscriber socket error', { err: String(e) }));
    this.subscriber.on('message', (ch, msg) => {
      if (ch === channel) onMessage(msg);
    });
    await this.subscriber.connect();
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

  /**
   * Read the currently-open alerts, most recently active first.
   *
   * This is the snapshot a client needs on connect. A browser that reloads has no
   * memory of what is already firing, and replaying history returns EVENTS only —
   * the event stream cannot reconstruct which alerts are still open, because "an
   * event was stored" and "this alert is still firing" are different facts.
   * Without this the board is empty on every load until something new fires,
   * which looks exactly like a broken deployment.
   *
   * The index is maintained inside alert-fold.lua, so it never disagrees with
   * `fw:alert:open:*`. Reading it is bounded by `limit`, unlike
   * `SCAN fw:alert:open:*`.
   */
  async readOpenAlerts(limit) {
    const c = await this.connect();
    const keys = await c.zrevrange('fw:idx:alert:open', 0, Math.max(0, limit - 1));
    if (keys.length === 0) return [];

    const hashes = await Promise.all(keys.map((k) => c.hgetall(`fw:alert:${k}`)));
    const out = [];
    keys.forEach((key, i) => {
      const h = hashes[i];
      if (!h || Object.keys(h).length === 0) return;
      // Ignore anything the index is stale about. The fold removes retired
      // alerts, so this only guards against a writer running older code.
      if (h.state && h.state !== 'open') return;
      out.push({
        key,
        severity: h.severity || 'WARNING',
        firstSeen: Number(h.first_seen || 0),
        lastSeen: Number(h.last_seen || 0),
        count: Number(h.count || 0),
        lastId: h.last_id || null,
        ackedBy: h.acked_by || null,
        ackedAt: h.acked_at ? Number(h.acked_at) : null,
      });
    });
    return out;
  }

  /** Read a single stored event by id, so a snapshot can include its Summary. */
  async readEvent(id) {
    if (!id) return null;
    const c = await this.connect();
    const raw = await c.get(`fw:evt:${id}`);
    if (!raw) return null;
    try {
      return JSON.parse(raw);
    } catch (err) {
      this.log.warn('unparseable stored event', { id, err: String(err) });
      return null;
    }
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

  /**
   * The one write the gateway performs.
   *
   * Writes `acked_by` / `acked_at` onto a single alert hash and returns the
   * previous values so the caller can decide whether this was a state change.
   *
   * Safety properties, all deliberate:
   *
   *  - Only these two fields are ever written. `severity`, `count`, `last_seen`
   *    and the watermark belong to the worker's fold; touching them from the
   *    gateway would race the fold and could resurrect an ordering bug.
   *  - `HSETNX`-style guard via the Lua below: the ack is NOT overwritten if one
   *    already exists, so two operators clicking at once cannot fight, and a
   *    second click is a no-op rather than a silent re-attribution.
   *  - The TTL is only extended, never created. An alert hash that has already
   *    expired must not be resurrected as an ack-only entry with no alert data,
   *    which would render as a phantom row in every UI.
   *  - A READONLY reply means we are pointed at a demoted replica after a
   *    failover: re-resolve the master once, then fail. The caller reports the
   *    failure to the client; it must not be swallowed.
   */
  async ackAlert(hashKey, by, nowMs, ttlMs) {
    const script = `
      if redis.call('EXISTS', KEYS[1]) == 0 then
        return {'missing'}
      end
      local prevBy = redis.call('HGET', KEYS[1], 'acked_by')
      local prevAt = redis.call('HGET', KEYS[1], 'acked_at')
      if prevAt and tonumber(prevAt) and tonumber(prevAt) > 0 then
        return {'already', prevBy or '', prevAt}
      end
      redis.call('HSET', KEYS[1], 'acked_by', ARGV[1], 'acked_at', ARGV[2])
      redis.call('PEXPIRE', KEYS[1], ARGV[3])
      return {'acked', ARGV[1], ARGV[2]}
    `;

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const c = await this.connect();
      try {
        const res = await c.eval(script, 1, hashKey, by, String(nowMs), String(ttlMs));
        const [status, ackedBy, ackedAt] = res;
        if (status === 'missing') return { status: 'missing' };
        if (status === 'already') return { status: 'already', ack: { by: ackedBy, at: Number(ackedAt) } };
        return { status: 'acked', ack: { by: ackedBy, at: Number(ackedAt) } };
      } catch (err) {
        const msg = String(err?.message || err);
        const retryable = msg.includes('READONLY') || msg.includes('Connection is closed');
        if (!retryable || attempt === 1) throw err;
        this.log.warn('ack write hit a demoted node, re-resolving master', { err: msg });
        await this.reset();
      }
    }
    throw new Error('unreachable');
  }

  /** Bulk read of ack state, keyed by alert key. Bounded by the caller. */
  async readAcks(hashKeys) {
    if (hashKeys.length === 0) return [];
    const c = await this.connect();
    const pipeline = c.pipeline();
    for (const k of hashKeys) pipeline.hmget(k, 'acked_by', 'acked_at');
    const results = await pipeline.exec();
    return results.map(([err, value]) => {
      if (err) return null;
      if (!Array.isArray(value)) return null;
      return { acked_by: value[0], acked_at: value[1] };
    });
  }

  /** Drop the client so the next call re-resolves the master. */
  async reset() {
    if (this.client) {
      try { this.client.disconnect(); } catch { /* ignore */ }
    }
    this.client = null;
  }

  /** Publish a frame. Used only to broadcast an ack change. */
  async publish(channel, payload) {
    const c = await this.connect();
    return c.publish(channel, payload);
  }

  async quit() {
    for (const c of [this.client, this.subscriber]) {
      if (c) { try { await c.quit(); } catch { /* ignore */ } }
    }
  }
}
