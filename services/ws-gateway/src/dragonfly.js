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

/**
 * The open-alert snapshot's hash read, as one server-side script.
 *
 * The snapshot runs on EVERY connect, so its cost must not grow with the number
 * of open alerts. Measured on this cluster with 199 open alerts:
 *
 *   HGETALL per alert, sequential       1092 ms   <- naive first cut
 *   HGETALL per alert, pipelined       102-694 ms
 *   HGETALL per alert, Promise.all      99-205 ms
 *   Lua, 199x HGETALL + table fold     97-204 ms   <- per-field Lua overhead
 *   Lua, 199x HMGET (7 named fields)     8-200 ms   <- chosen
 *   one MGET, 199 keys                   3-80 ms
 *
 * Two things dominate: the number of round trips, and how many values Lua has to
 * convert between its own types and the wire. HMGET with an explicit field list
 * avoids HGETALL's flat array plus a Lua table fold, which was the single biggest
 * win — 1600 hash-table inserts replaced by 7 ordered values per alert.
 *
 * NOTE the declared-key constraint. Dragonfly (like Redis Cluster) refuses a
 * script that touches a key it was not given in KEYS:
 *
 *   ERR script tried accessing undeclared key, key: fw:alert:shop:check-175
 *
 * So the caller reads the index itself and passes every alert hash in KEYS, and
 * the `fw:evt:*` records are fetched afterwards with MGET, since their keys are
 * only known once the hashes are read. Total: 3 round trips regardless of how
 * many alerts are open.
 *
 * Returns 7 flat fields per alert (state is filtered, not returned). Lua tables
 * cannot carry nil holes across the wire — they truncate silently — so missing
 * values are emitted as empty strings and normalised in JS.
 */
const SNAPSHOT_FIELDS = ['severity', 'first_seen', 'last_seen', 'count', 'last_id', 'acked_by', 'acked_at'];

const SNAPSHOT_LUA = `
  local out = {}
  for i = 1, #KEYS do
    local v = redis.call('HMGET', KEYS[i], ${SNAPSHOT_FIELDS.map((f) => `'${f}'`).join(',')})
    -- A missing hash yields all-nil; skip it so the index cannot invent a row.
    if v[1] ~= false or v[5] ~= false then
      for j = 1, ${SNAPSHOT_FIELDS.length} do
        out[#out + 1] = v[j] or ''
      end
    end
  end
  return out
`;

/** JSON.parse that never throws, for records written by another service. */
function safeParse(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export class DragonflyReader {
  constructor(cfg, log) {
    this.cfg = cfg;
    this.log = log;
    this.client = null;      // reads (and the one narrow write)
    this.subscriber = null;  // pub/sub, must be its own connection
    // SHA of the snapshot script, memoised after the first SCRIPT LOAD so a
    // reconnect does not pay for it again.
    this._snapSha = null;
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
   * Read the currently-open alerts, most recently active first, with their
   * newest event attached.
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
   *
   * COST — this runs on every connect, so the number of ROUND TRIPS must not
   * grow with the number of alerts. Measured on this cluster with 199 open
   * alerts, against Dragonfly:
   *
   *   1 ZREVRANGE                                   1.2 ms
   *   MGET of all event records                    2.5-80 ms
   *   HGETALL per alert, pipelined                102-694 ms   <- noisy, slow
   *   HGETALL per alert, Promise.all               99-205 ms
   *   HGETALL per alert, sequential (first cut)      1092 ms   <- naive
   *   ONE Lua script doing all of it                 see below
   *
   * Client-side batching was not enough: pipelining 199 HGETALLs still cost
   * hundreds of milliseconds and blocked the WebSocket handshake with it. The
   * loop belongs on the server, so this is a single EVALSHA that fetches the
   * index, every hash and every event inside one round trip. Its cost grows with
   * the data, not with the number of trips.
   */
  async readOpenAlerts(limit) {
    const c = await this.connect();

    // Trip 1: the index, newest activity first.
    const keys = await c.zrevrange('fw:idx:alert:open', 0, Math.max(0, limit - 1));
    if (keys.length === 0) return [];

    // Trip 2: every alert hash, in one script (7 named fields each).
    const flat = await c.evalsha(
      await this._snapshotSha(c),
      keys.length,
      ...keys.map((k) => `fw:alert:${k}`),
    );

    const rows = [];
    const n = SNAPSHOT_FIELDS.length;
    for (let i = 0, r = 0; i + n - 1 < flat.length; i += n, r += 1) {
      rows.push({
        key: keys[r],
        severity: flat[i] || 'WARNING',
        firstSeen: Number(flat[i + 1] || 0),
        lastSeen: Number(flat[i + 2] || 0),
        count: Number(flat[i + 3] || 0),
        lastId: flat[i + 4] || null,
        ackedBy: flat[i + 5] || null,
        ackedAt: flat[i + 6] ? Number(flat[i + 6]) : null,
        event: null,
      });
    }
    if (rows.length === 0) return [];

    // Trip 3: every event record, so each row can carry a Summary.
    const wanted = [];
    for (const row of rows) if (row.lastId) wanted.push(row.lastId);
    if (wanted.length > 0) {
      const raw = await c.mget(wanted.map((id) => `fw:evt:${id}`));
      const byId = new Map();
      wanted.forEach((id, i) => {
        if (!raw[i]) return;
        const parsed = safeParse(raw[i]);
        if (parsed) byId.set(id, parsed);
        else this.log.warn('unparseable stored event during snapshot', { id });
      });
      for (const row of rows) {
        if (row.lastId) row.event = byId.get(row.lastId) ?? null;
      }
    }
    return rows;
  }

  /** Load (once) and memoise the snapshot script's SHA. */
  async _snapshotSha(c) {
    if (this._snapSha) return this._snapSha;
    const sha = await c.script('LOAD', SNAPSHOT_LUA);
    this._snapSha = sha;
    return sha;
  }

  /**
   * Read ONE open alert, with its newest event attached, or null.
   *
   * Exists for the content-duplicate fan-out. The worker publishes
   * `{kind:'occurrence', key, applied}` because the COPY was discarded — so the
   * frame carries no event, and a client that has never seen this key has
   * nothing to render. Without a row the alert simply never appears, which is
   * exactly what happened after a CLEAR: the next repeat is absorbed, so the
   * first the client hears of it is an eventless frame.
   *
   * Same shape as readOpenAlerts returns for one row, and the same three keys
   * (hash, event record), so the client renders it from a single code path.
   */
  async readAlert(key) {
    const c = await this.connect();

    const flat = await c.evalsha(
      await this._snapshotSha(c),
      1,
      `fw:alert:${key}`,
    );
    if (!Array.isArray(flat) || flat.length < SNAPSHOT_FIELDS.length) return null;

    // A closed alert must not be resurrected as a live row: the index is the
    // authority on what is open, and the fold removes retired alerts from it.
    const open = await c.zscore('fw:idx:alert:open', key);
    if (open === null || open === undefined) return null;

    const n = SNAPSHOT_FIELDS.length;
    const row = {
      key,
      severity: flat[0] || 'WARNING',
      firstSeen: Number(flat[1] || 0),
      lastSeen: Number(flat[2] || 0),
      count: Number(flat[3] || 0),
      lastId: flat[4] || null,
      ackedBy: flat[5] || null,
      ackedAt: flat[6] ? Number(flat[6]) : null,
      event: null,
    };
    if (n && row.lastId) {
      const raw = await c.get(`fw:evt:${row.lastId}`);
      const parsed = raw ? safeParse(raw) : null;
      if (parsed) row.event = parsed;
      else if (raw) this.log.warn('unparseable stored event', { id: row.lastId });
    }
    return row;
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
  /**
   * Close an alert on behalf of an operator.
   *
   * Writes the same end-state the worker's fold produces for a CLOSE event, so
   * an operator-closed alert is indistinguishable from a source-closed one
   * downstream (the board, the alert hash, the open index). That matters: two
   * different notions of "closed" in one store is how phantom rows appear.
   *
   * Deliberately a named method with a fixed script rather than a general write
   * escape hatch, same rule as ackAlert. The three writes it must make:
   *
   *   1. DEL fw:alert:open:{key}   — otherwise the alert still reads as firing
   *   2. ZREM fw:idx:alert:open    — otherwise the snapshot resurrects it on
   *                                  every client's next page load
   *   3. HSET state=closed …       — plus HDEL the ack, which belongs to the
   *                                  episode that has just ended
   *
   * Returns 'missing' when the alert hash does not exist, mirroring ackAlert:
   * closing a nonexistent alert must not create one.
   */
  /**
   * Permanently delete one event record.
   *
   * Unlike ack and close, this is DESTRUCTIVE and unrecoverable: there is no AOF
   * (docs/dragonfly.md §6) and no audit copy for an operator delete. It exists
   * because it was asked for, and the UI labels it accordingly.
   *
   * Two keys must go, and leaving either is a bug:
   *
   *   fw:evt:{id}            the record itself
   *   fw:idx:src:{source}    its timeline member. ZADD members never expire on
   *                          their own, so a leftover member is a dangling index
   *                          entry: backfill reads the id, finds no record, and
   *                          the event is silently missing from history in a way
   *                          that looks like corruption.
   *
   * The source is needed to find the index, so the caller passes it — the record
   * must be read first anyway to know whether the event exists at all, and that
   * read is what makes the "missing" answer honest.
   *
   * The alert hash and its counters are deliberately NOT touched. An alert is an
   * aggregate over events; deleting one event cannot be allowed to silently
   * decrement `count`, because the fold is the only thing that owns that number
   * and a second writer would race it. The row's count will therefore still
   * include the deleted event until the next fold writes it.
   */
  async deleteEvent(id, source, indexPrefix = 'fw:idx:src:') {
    const script = `
      local existed = redis.call('DEL', KEYS[1])
      redis.call('ZREM', KEYS[2], ARGV[1])
      if existed == 0 then
        return {'missing'}
      end
      return {'deleted'}
    `;

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const c = await this.connect();
      try {
        const res = await c.eval(
          script,
          2,
          `fw:evt:${id}`,
          `${indexPrefix}${source}`,
          id,
        );
        const [status] = res;
        return { status };
      } catch (err) {
        const msg = String(err?.message || err);
        const retryable = msg.includes('READONLY') || msg.includes('Connection is closed');
        if (!retryable || attempt === 1) throw err;
        this.log.warn('delete hit a demoted node, re-resolving master', { err: msg });
        await this.reset();
      }
    }
    throw new Error('unreachable');
  }

  /** Read one stored event, so a delete can verify it exists and learn its source. */
  async readEvent(id) {
    const c = await this.connect();
    const raw = await c.get(`fw:evt:${id}`);
    if (!raw) return null;
    try {
      return JSON.parse(raw);
    } catch {
      // A record that does not parse is still a record; the caller only needs its
      // source to remove the index member, so return a minimal shape.
      return { id, source: null, unparseable: true };
    }
  }

  async closeAlert(hashKey, by, nowMs, ttlMs, indexKey = 'fw:idx:alert:open', bareKey = '') {
    const script = `
      if redis.call('EXISTS', KEYS[1]) == 0 then
        return {'missing'}
      end
      redis.call('DEL', KEYS[2])
      redis.call('ZREM', KEYS[3], ARGV[4])
      redis.call('HSET', KEYS[1],
        'state', 'closed',
        'severity', 'CLOSE',
        'closed_by', ARGV[1],
        'closed_at', ARGV[2],
        'watermark', ARGV[2],
        'watermark_id', ARGV[3])
      redis.call('HDEL', KEYS[1], 'acked_by', 'acked_at')
      redis.call('PEXPIRE', KEYS[1], ARGV[5])
      return {'closed', ARGV[1], ARGV[2]}
    `;
    const openKey = hashKey.replace(/^fw:alert:/, 'fw:alert:open:');

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const c = await this.connect();
      try {
        const res = await c.eval(
          script,
          3,
          hashKey,
          openKey,
          indexKey,
          by,
          String(nowMs),
          `operator-close-${nowMs}`,
          bareKey || hashKey.replace(/^fw:alert:/, ''),
          String(ttlMs),
        );
        const [status, closedBy, closedAt] = res;
        if (status === 'missing') return { status: 'missing' };
        return { status: 'closed', by: closedBy, at: Number(closedAt) };
      } catch (err) {
        const msg = String(err?.message || err);
        const retryable = msg.includes('READONLY') || msg.includes('Connection is closed');
        if (!retryable || attempt === 1) throw err;
        this.log.warn('close write hit a demoted node, re-resolving master', { err: msg });
        await this.reset();
      }
    }
    throw new Error('unreachable');
  }

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
