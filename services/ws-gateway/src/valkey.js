/**
 * Sentinel-aware Valkey client for the gateway.
 *
 * The gateway's data path is READ-ONLY: it streams events to browsers and never
 * modifies the event log. There is deliberately no generic `set`, `del` or `eval`
 * wrapper, because the absence of a method is a stronger guarantee than a comment
 * (docs/websocket-server.md §5).
 *
 * The exceptions are operator actions issued from the UI — ack, close, journal note
 * and event delete. Each is a named method with a fixed command, not a general write
 * escape hatch, so this file still cannot be used to rewrite an event record
 * (docs/acknowledge.md §4).
 *
 * KEY SHAPE. One hash per CONDITION at `event:<32 hex>` (see
 * services/worker/src/keygen.js). The gateway receives the BARE key from the worker
 * over pub/sub and from the browser over the socket, and never derives it — deriving
 * it in two places is how the two would drift.
 */
import Redis from 'ioredis';

/**
 * The open-event snapshot's hash read, as one server-side script.
 *
 * The snapshot runs on EVERY connect, so its cost must not grow with the number of
 * open rows. Measured on the previous keyspace with 199 open alerts:
 *
 *   HGETALL per alert, sequential       1092 ms   <- naive first cut
 *   HGETALL per alert, pipelined       102-694 ms
 *   HGETALL per alert, Promise.all      99-205 ms
 *   Lua, 199x HGETALL + table fold     97-204 ms   <- per-field Lua overhead
 *   Lua, 199x HMGET (7 named fields)     8-200 ms   <- chosen
 *
 * NOTE the declared-key constraint. Valkey (like Redis Cluster) refuses a script
 * that touches a key it was not given in KEYS:
 *
 *   ERR Script attempted to access a non local key
 *
 * So the caller reads the index itself and passes every record hash in KEYS. The
 * record carries the payload too now, so there is no second MGET: two round trips
 * regardless of how many rows are open.
 *
 * Returns one flat field per name per key (see SNAPSHOT_FIELDS). Lua tables cannot
 * carry nil holes across the wire — they truncate silently — so missing values are
 * emitted as empty strings and normalised in JS.
 */
const SNAPSHOT_FIELDS = [
  'severity', 'displaySeverity', 'source', 'type', 'msg', 'payload',
  'brokerTs', 'lastTs', 'firstSeen', 'lastSeen', 'count', 'lastId',
  'state', 'closedAt', 'closedBy', 'ackedBy', 'ackedAt',
];

const SNAPSHOT_LUA = `
  local out = {}
  local n = ${SNAPSHOT_FIELDS.length}
  for i = 1, #KEYS do
    local v = redis.call('HMGET', KEYS[i], ${SNAPSHOT_FIELDS.map((f) => `'${f}'`).join(',')})
    -- A missing hash yields all-nil; skip it so the index cannot invent a row.
    if v[1] ~= false or v[5] ~= false then
      for j = 1, n do
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

/**
 * Journal list key for an event record.
 *
 * A separate LIST, deliberately not fields on `event:<key>`. The worker's fold owns
 * that hash and rewrites it atomically; a second writer HSET-ing journal entries into
 * it would race the fold and leave state that disagrees with itself — the same reason
 * `count` is off limits to operators.
 */
export function journalKey(bareKey) {
  return `journal:${bareKey}`;
}

export class ValkeyReader {
  constructor(cfg, log) {
    this.cfg = cfg;
    this.log = log;
    this.client = null;      // reads (and the narrow writes)
    this.subscriber = null;  // pub/sub, must be its own connection
    // SHA of the snapshot script, memoised after the first SCRIPT LOAD so a
    // reconnect does not pay for it again.
    this._snapSha = null;
    // In-flight connect(), shared by concurrent callers. See connect().
    this._connecting = null;
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

  /**
   * Connect on demand, with ONE attempt in flight at a time.
   *
   * Every call used to build a brand-new client, overwrite `this.client`, and then
   * `await connect()`. That is not safe when several callers arrive together, which
   * is exactly what happens at boot: the snapshot, the ack-state read and the tick
   * all fire at once. The second call replaced the client the first was still
   * connecting, so commands went out on a socket that had not reached `ready` yet and
   * were rejected with
   *
   *   Stream isn't writeable and enableOfflineQueue options is false
   *
   * which is what left the board empty on first load ("alert snapshot failed"). The
   * `lazyConnect` fix addressed issuing commands before connect() was CALLED; this
   * addresses two connect() calls racing each other.
   */
  async connect() {
    if (this.client && this.client.status === 'ready') return this.client;
    if (this._connecting) return this._connecting;

    this._connecting = (async () => {
      const master = await this.resolveMaster();
      this.log.info('resolved valkey master for reads', master);

      const opts = {
        host: master.host,
        port: master.port,
        password: this.cfg.valkeyPassword,
        // lazyConnect + explicit connect(). With enableOfflineQueue:false any command
        // sent before the socket is ready is rejected with
        //   "Stream isn't writeable and enableOfflineQueue options is false"
        // so issuing ping() right after the constructor was a race. The gateway lost
        // it at boot and logged "subscribe failed, retrying in 3s".
        lazyConnect: true,
        enableOfflineQueue: false,
        maxRetriesPerRequest: 2,
        retryStrategy: (times) => Math.min(times * 200, 3000),
      };
      const client = new Redis(opts);
      client.on('error', (e) => this.log.warn('read socket error', { err: String(e) }));
      await client.connect();
      await client.ping();
      // Only publish it once it is usable: a half-open client must never be handed to
      // another caller, or the failure mode above comes straight back.
      this.client = client;
      return client;
    })().finally(() => { this._connecting = null; });

    return this._connecting;
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
      password: this.cfg.valkeyPassword,
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

  /** Read bare keys in a time window from the per-device index. */
  async readIndex(source, min, max, limit) {
    const c = await this.connect();
    return c.zrangebyscore(`idx:source:${source}`, min, max, 'LIMIT', 0, limit);
  }

  /**
   * Read bare keys in a time window from the GLOBAL index, OLDEST first.
   *
   * One bounded range over `idx:event:all`, ascending by broker time — the order the
   * live tail renders. This is what lets `subscribe` backfill without a source
   * filter: before the global index existed there was no bounded way to answer
   * "everything since T" across all devices, so the backfill was skipped and the
   * tail was empty on every refresh.
   *
   * @returns {Promise<string[]>} bare keys, OLDEST first
   */
  async readIndexRange({ sinceMs = 0, untilMs = Date.now(), limit = 500 } = {}) {
    const c = await this.connect();
    return c.zrangebyscore(
      'idx:event:all',
      `(${Math.max(0, sinceMs)}`,
      String(untilMs),
      'LIMIT', 0, Math.max(1, limit),
    );
  }

  /**
   * Read records for a set of bare keys, in the order given.
   *
   * One pipeline of HMGETs rather than an MGET of JSON: the record is a hash, so
   * every field has to be named. Chunked because a 7-day window can exceed what one
   * pipeline should carry.
   *
   * A key whose hash expired between the index read and this call is SKIPPED, not
   * rendered as a blank row; the prune drops the index member on its next pass.
   */
  async readRecords(bareKeys) {
    if (bareKeys.length === 0) return [];
    const c = await this.connect();
    const out = [];
    const CHUNK = 256;
    for (let i = 0; i < bareKeys.length; i += CHUNK) {
      const slice = bareKeys.slice(i, i + CHUNK);
      const pipe = c.pipeline();
      for (const k of slice) pipe.hmget(`event:${k}`, ...SNAPSHOT_FIELDS);
      // eslint-disable-next-line no-await-in-loop
      const res = await pipe.exec();
      res.forEach((tuple, j) => {
        const [err, value] = tuple || [];
        if (err || !Array.isArray(value)) return;
        if (value.every((v) => v === null || v === '')) {
          this.log.debug('event index member has no record', { key: slice[j] });
          return;
        }
        out.push(this._row(slice[j], value));
      });
    }
    return out;
  }

  /** Backwards-compatible alias used by the subscribe path. */
  async readEvents(bareKeys) {
    return this.readRecords(bareKeys);
  }

  /** Normalise a flat HMGET result into a row the wire can carry. */
  _row(bareKey, flat) {
    const at = (name) => flat[SNAPSHOT_FIELDS.indexOf(name)] || '';
    const num = (name) => {
      const v = at(name);
      return v === '' ? null : Number(v);
    };
    const payload = safeParse(at('payload')) || {};
    return {
      id: bareKey,
      key: bareKey,
      source: at('source'),
      type: at('type'),
      severity: at('severity') || 'WARNING',
      displaySeverity: at('displaySeverity') || at('severity') || 'WARNING',
      summary: at('msg'),
      // Payload is kept whole so the inspector and the compression path work
      // unchanged; `msg`/`msgc` are what the UI reads from it.
      payload,
      brokerTs: num('brokerTs'),
      ts: at('lastTs') || null,
      firstOccurrence: num('firstSeen'),
      lastOccurrence: num('lastSeen'),
      count: num('count'),
      lastId: at('lastId') || null,
      ackedBy: at('ackedBy') || null,
      ackedAt: num('ackedAt'),
      alertState: at('state') || null,
      alertClosedAt: num('closedAt'),
      alertClosedBy: at('closedBy') || null,
    };
  }

  /**
   * Read the newest records across ALL devices.
   *
   * Backs the All Events tab. Newest first, filtered SERVER-SIDE by `sinceMs`, so
   * asking for the last hour costs strictly less work than the last 7 days rather
   * than the server building everything and the client discarding most of it.
   *
   * The `total` is a ZCOUNT over the same window. The UI needs it for its badge:
   * deriving the badge from the page made it report "rows I have fetched", which is
   * 0 until the tab is opened.
   */
  async readAllEvents({ sinceMs = 0, untilMs = Date.now(), limit = 500 } = {}) {
    const c = await this.connect();

    const lo = Math.max(0, sinceMs);
    const keys = await c.zrevrangebyscore(
      'idx:event:all',
      String(untilMs),
      `(${lo}`,
      'LIMIT', 0, Math.max(1, limit),
    );

    const total = await c.zcount('idx:event:all', `(${lo}`, String(untilMs));
    if (keys.length === 0) return { events: [], total };

    return { events: await this.readRecords(keys), total };
  }

  /**
   * Read the currently-open records, most recently active first.
   *
   * This is the snapshot a client needs on connect. A browser that reloads has no
   * memory of what is already firing, and replaying history returns EVENTS only —
   * the event stream cannot reconstruct which conditions are still open, because "an
   * event was stored" and "this condition is still firing" are different facts.
   * Without this the board is empty on every load until something new fires, which
   * looks exactly like a broken deployment.
   *
   * The index is maintained inside event-fold.lua, so it never disagrees with the
   * records. Reading it is bounded by `limit`, unlike `SCAN event:*`.
   *
   * COST: one ZREVRANGE plus one EVALSHA. The script reads every hash, so the cost
   * grows with the data but not with the number of round trips — which is what made
   * the WebSocket handshake slow when this was done client-side per row.
   */
  async readOpenAlerts(limit) {
    const c = await this.connect();

    const keys = await c.zrevrange('idx:event:open', 0, Math.max(0, limit - 1));
    if (keys.length === 0) return [];

    const flat = await this._evalSnapshot(c, keys);
    return this._rowsFromFlat(keys, flat);
  }

  /** Turn the flat script output back into rows, skipping vanished hashes. */
  _rowsFromFlat(keys, flat) {
    const n = SNAPSHOT_FIELDS.length;
    const rows = [];
    for (let i = 0, r = 0; i + n - 1 < flat.length; i += n, r += 1) {
      const slice = flat.slice(i, i + n);
      // A hash that vanished between the index read and the script yields a row of
      // empty strings; it must not become a blank row on screen.
      if (slice.every((v) => v === '' || v === null)) continue;
      rows.push(this._row(keys[r], slice));
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
   * Run the snapshot script, recovering from NOSCRIPT.
   *
   * THE BUG THIS FIXES. The script is loaded once and its SHA memoised forever,
   * but the server's script cache does NOT survive a restart, a failover to a
   * replica, or `SCRIPT FLUSH`. After any of those, every EVALSHA fails with
   *
   *   ReplyError: NOSCRIPT No matching script.
   *
   * and because `_snapSha` was still cached the client never reloaded it — so the
   * failure was PERMANENT until the pod happened to restart. It was not silent in
   * the logs, but it degraded exactly the read an operator needs during an
   * incident: `readOpenAlerts` is the board snapshot, so a browser that loaded
   * after the restart got an empty board while the cluster was still firing.
   * `readClosedAlerts` and `readAlert` failed the same way.
   *
   * Recovery is to drop the memoised SHA and reload once. Reloading is cheap and
   * idempotent, and doing it here means every caller gets the fix rather than each
   * one needing its own retry.
   */
  async _evalSnapshot(c, keys) {
    try {
      return await c.evalsha(
        await this._snapshotSha(c),
        keys.length,
        ...keys.map((k) => `event:${k}`),
      );
    } catch (err) {
      if (!String(err?.message || err).includes('NOSCRIPT')) throw err;
      this.log.warn('snapshot script missing from the server cache, reloading', {
        err: String(err?.message || err),
      });
      this._snapSha = null;
      return c.evalsha(
        await this._snapshotSha(c),
        keys.length,
        ...keys.map((k) => `event:${k}`),
      );
    }
  }

  /**
   * Read recently RETIRED records, newest closure first.
   *
   * Backs the Closed tab. The open index cannot answer this — retiring removes a key
   * from that index — so the worker's fold maintains a second zset scored by
   * `closedAt` (docs/event-lifecycle.md §5.2).
   *
   * `sinceMs` filters server-side via ZRANGEBYSCORE rather than in the client, so
   * asking for a shorter window costs strictly less work.
   */
  async readClosedAlerts({ sinceMs = 0, limit = 500 } = {}) {
    const c = await this.connect();

    const keys = await c.zrevrangebyscore(
      'idx:event:closed',
      '+inf',
      `(${Math.max(0, sinceMs)}`,
      'LIMIT', 0, Math.max(1, limit),
    );
    if (keys.length === 0) return [];

    const flat = await this._evalSnapshot(c, keys);
    return this._rowsFromFlat(keys, flat);
  }

  /**
   * Read ONE record, or null.
   *
   * Exists for the occurrence fan-out: the worker publishes `{kind:'occurrence', key}`
   * and the gateway enriches the frame with the row, so a client that has never seen
   * this key has something to render.
   */
  /**
   * Read ONE record, or null.
   *
   * Exists for the occurrence fan-out: the worker publishes `{kind:'occurrence',
   * key}` and the gateway enriches the frame with the row, so a client that has
   * never seen this key has something to render.
   *
   * Gated on `idx:event:open`. The index is the authority on what is open, so a
   * retired record must not be resurrected as a live row by an occurrence frame for
   * a stale key — that would put a closed row back on the board, and nothing would
   * remove it until the next snapshot.
   */
  async readAlert(bareKey) {
    const c = await this.connect();

    const flat = await c.hmget(`event:${bareKey}`, ...SNAPSHOT_FIELDS);
    if (!Array.isArray(flat) || flat.every((v) => v === null || v === '')) return null;

    const open = await c.zscore('idx:event:open', bareKey);
    if (open === null || open === undefined) return null;

    return this._row(bareKey, flat.map((v) => v ?? ''));
  }

  /**
   * Read one record regardless of its state.
   *
   * The delete path needs this: an operator may delete a record that is closed, and
   * `readAlert` would return null for it. Returning the record even when its hash is
   * unparseable is deliberate — the caller only needs it to exist.
   */
  async readEvent(bareKey) {
    const c = await this.connect();
    const flat = await c.hmget(`event:${bareKey}`, ...SNAPSHOT_FIELDS);
    if (!Array.isArray(flat) || flat.every((v) => v === null || v === '')) {
      return { key: bareKey, unparseable: true };
    }
    return this._row(bareKey, flat.map((v) => v ?? ''));
  }

  /**
   * Delete one event record permanently.
   *
   * Removes the hash and its member from EVERY index the record appears in, so no
   * tab can list a row that resolves to nothing.
   *
   * THE PER-DEVICE INDEX WAS MISSED, and it showed as a leak. `idx:source:<device>`
   * is written by the fold alongside the global timeline, but the delete script
   * only removed `idx:event:all`, `idx:event:open` and `idx:event:closed`. Every
   * deleted record therefore left an orphan member behind, and because the worker's
   * prune works on a per-source counter it can be a long time before that member is
   * trimmed — measured on the lab as two `idx:source:*` keys holding members whose
   * `event:<key>` hashes were gone (`ttl=-2`). A filtered tail query reads that
   * index, so the visible effect is a backfill that requests records which no longer
   * exist and silently drops them.
   *
   * The device is passed in rather than read from the hash, because the script
   * needs the index KEY up front and Valkey refuses a script that touches a key it
   * was not given in `KEYS`.
   *
   * The `count` on the alert rollup is deliberately NOT touched: it describes the
   * CONDITION, and an operator deleting one record is not asserting the condition
   * never happened. Deleting is per record; closing is per condition.
   */
  async deleteEvent(bareKey, source = '') {
    const script = `
      local existed = redis.call('DEL', KEYS[1])
      redis.call('ZREM', KEYS[2], ARGV[1])
      redis.call('ZREM', KEYS[3], ARGV[1])
      redis.call('ZREM', KEYS[4], ARGV[1])
      -- KEYS[5] is optional: an older caller (or a record with no known device)
      -- passes no source index, and the script must still work rather than error.
      if KEYS[5] then
        redis.call('ZREM', KEYS[5], ARGV[1])
      end
      if existed == 0 then
        return 'missing'
      end
      return 'deleted'
    `;

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const c = await this.connect();
      try {
        const keyList = [
          `event:${bareKey}`,
          'idx:event:all',
          'idx:event:open',
          'idx:event:closed',
        ];
        if (source) keyList.push(`idx:source:${source}`);
        const res = await c.eval(script, keyList.length, ...keyList, bareKey);
        return { status: String(res) };
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

  /**
   * Close a condition as an operator.
   *
   * Writes FOUR things, and all four matter — an earlier version wrote only the first
   * two, which made an operator close invisible everywhere:
   *
   *   1. the record: `state=closed`, severity CLOSE, who and when
   *   2. `idx:event:open` loses the member, so nothing renders it as firing
   *   3. `idx:event:closed` gains it, scored by `closedAt` — without it the Closed
   *      tab cannot enumerate the closure at all
   *   4. a synthetic CLEAR field set on the record, so the closure has an author and
   *      a reason in history rather than being an unexplained state flag
   *
   * Written server-side rather than synthesised by each client, so every client and
   * the audit trail see byte-identical history.
   */
  async closeAlert(bareKey, by, nowMs, ttlMs) {
    const script = `
      if redis.call('EXISTS', KEYS[1]) == 0 then
        return {'missing'}
      end

      -- Nothing to do if it is already closed: a second operator clicking Close must
      -- not overwrite the original author or timestamp.
      local state = redis.call('HGET', KEYS[1], 'state')
      if state == 'closed' then
        return {'already', redis.call('HGET', KEYS[1], 'closedBy') or '',
                redis.call('HGET', KEYS[1], 'closedAt') or '0'}
      end

      redis.call('ZREM', KEYS[2], ARGV[4])
      redis.call('ZADD', KEYS[3], ARGV[2], ARGV[4])
      redis.call('HSET', KEYS[1],
        'state', 'closed',
        'severity', 'CLOSE',
        'displaySeverity', 'CLEAR',
        'closedBy', ARGV[1],
        'closedAt', ARGV[2],
        'watermark', ARGV[2],
        'watermarkId', ARGV[3],
        'lastSeen', ARGV[2])
      redis.call('HDEL', KEYS[1], 'ackedBy', 'ackedAt')
      redis.call('PEXPIRE', KEYS[1], ARGV[5])
      return {'closed', ARGV[1], ARGV[2]}
    `;

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const c = await this.connect();
      try {
        const res = await c.eval(
          script,
          3,
          `event:${bareKey}`,
          'idx:event:open',
          'idx:event:closed',
          by,
          String(nowMs),
          `operator-close-${nowMs}`,
          bareKey,
          String(ttlMs),
        );
        const [status, closedBy, closedAt] = res;
        if (status === 'missing') return { status: 'missing' };
        if (status === 'already') {
          return { status: 'already', by: closedBy, at: Number(closedAt) };
        }
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

  async ackAlert(bareKey, by, nowMs, ttlMs) {
    const script = `
      if redis.call('EXISTS', KEYS[1]) == 0 then
        return {'missing'}
      end
      local prevBy = redis.call('HGET', KEYS[1], 'ackedBy')
      local prevAt = redis.call('HGET', KEYS[1], 'ackedAt')
      if prevAt and tonumber(prevAt) and tonumber(prevAt) > 0 then
        return {'already', prevBy or '', prevAt}
      end
      redis.call('HSET', KEYS[1], 'ackedBy', ARGV[1], 'ackedAt', ARGV[2])
      redis.call('PEXPIRE', KEYS[1], ARGV[3])
      return {'acked', ARGV[1], ARGV[2]}
    `;

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const c = await this.connect();
      try {
        const res = await c.eval(script, 1, `event:${bareKey}`, by, String(nowMs), String(ttlMs));
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

  /** Bulk read of ack state, keyed by bare key. Bounded by the caller. */
  async readAcks(bareKeys) {
    if (bareKeys.length === 0) return [];
    const c = await this.connect();
    const pipeline = c.pipeline();
    for (const k of bareKeys) pipeline.hmget(`event:${k}`, 'ackedBy', 'ackedAt');
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

  /**
   * Is the datastore reachable?
   *
   * Used by the DB-health probe, which runs on a timer and pushes a
   * `{"op":"db","up":…}` frame to every client when the reported state CHANGES.
   * It deliberately returns a boolean rather than throwing: a probe that throws
   * on an outage becomes an unhandled rejection every interval, and the caller
   * only needs to know up/down so it can apply its own failure threshold.
   *
   * THIS WAS MISSING after the Valkey rewrite — the whole class was rewritten and
   * `ping()` was dropped while `index.js` kept calling `reader.ping()`. The result
   * was `TypeError: reader.ping is not a function` every 10s in the gateway log,
   * and the UI's database chip could never turn red: the only signal that the
   * gateway's reads are failing was gone. It is a one-line method, which is
   * exactly why its absence was easy to miss — the process stayed healthy and
   * every other read worked.
   */
  async ping() {
    try {
      const c = await this.connect();
      await c.ping();
      return true;
    } catch {
      return false;
    }
  }

  /** Publish a frame. Used only to broadcast a state change. */
  async publish(channel, payload) {
    const c = await this.connect();
    return c.publish(channel, payload);
  }

  /**
   * Append one journal entry and return the stored entry.
   *
   * An APPEND, never a SET. Two operators annotating the same incident must both keep
   * their text; last-write-wins on a single field would erase the first note with no
   * trace that it ever existed — the lost update this codebase already had to fix for
   * `count` and for the ack.
   *
   * The list is trimmed on every append rather than on a timer: a timer cannot know
   * which incidents are active and would walk every key forever. The TTL is refreshed
   * on each append so a journal expires with the record it belongs to instead of
   * outliving it as an orphan list.
   */
  async appendJournal(bareKey, entry, ttlMs, maxEntries) {
    const key = journalKey(bareKey);
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const c = await this.connect();
      try {
        const pipe = c.multi()
          .rpush(key, JSON.stringify(entry))
          // Keep only the newest `maxEntries`. LTRIM takes inclusive start/stop, so
          // -maxEntries..-1 keeps the tail; a negative start larger than the list is
          // clamped by Valkey, which is what makes this safe on a short list.
          .ltrim(key, -maxEntries, -1)
          .lrange(key, -maxEntries, -1)
          .pexpire(key, ttlMs);
        const res = await pipe.exec();
        // ioredis returns [err, value] pairs; a per-command error must not be
        // silently treated as success, which is how the closed-list pipeline bug
        // produced an opaque failure.
        for (const [err] of res) {
          if (err) throw err;
        }
        const raw = res[2][1] || [];
        const stored = [];
        for (const line of raw) {
          try { stored.push(JSON.parse(line)); } catch { /* skip a corrupt entry */ }
        }
        return {
          entry,
          count: stored.length,
          // `truncated` means entries were dropped to stay in the cap. Only true when
          // the list is exactly at the cap, since that is the only observable
          // evidence from here.
          truncated: stored.length >= maxEntries,
        };
      } catch (err) {
        const msg = String(err?.message || err);
        const retryable = msg.includes('READONLY') || msg.includes('Connection is closed');
        if (!retryable || attempt === 1) throw err;
        this.log.warn('journal write hit a demoted node, re-resolving master', { err: msg });
        await this.reset();
      }
    }
    throw new Error('unreachable');
  }

  /**
   * Read the newest entries for a set of keys.
   *
   * One `LRANGE` per key in a single pipeline, bounded by the caller. Returns an entry
   * per requested key even when the journal is empty, so a client can tell "read it
   * and there is nothing" from "never answered" — the distinction the Closed tab
   * needed a `loadedAt` flag for.
   */
  async readJournals(keys, limit) {
    if (keys.length === 0) return [];
    const c = await this.connect();
    const pipe = c.pipeline();
    for (const k of keys) pipe.lrange(journalKey(k), -limit, -1);
    const results = await pipe.exec();
    return keys.map((key, i) => {
      const [err, raw] = results[i] || [];
      if (err || !Array.isArray(raw)) return { key, entries: [], truncated: false };
      const entries = [];
      for (const line of raw) {
        try { entries.push(JSON.parse(line)); } catch { /* skip a corrupt entry */ }
      }
      // Newest first: the wire order the table renders, so the client does not have
      // to reverse it and two views cannot disagree about the order.
      entries.reverse();
      return { key, entries, truncated: entries.length >= limit };
    });
  }

  async quit() {
    for (const c of [this.client, this.subscriber]) {
      if (c) { try { await c.quit(); } catch { /* ignore */ } }
    }
  }
}
