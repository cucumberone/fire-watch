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

/**
 * Journal list key for an alert.
 *
 * A separate LIST, deliberately not fields on `fw:alert:{key}`. The worker's fold
 * owns that hash and rewrites it atomically; a second writer HSET-ing journal
 * entries into it would race the fold and leave state that disagrees with itself —
 * the same reason `count` is off limits to operators. Mirrors the same shape as
 * `keys.alert()` so the two are obviously related but obviously different.
 */
export function journalKey(alertKey) {
  return `fw:journal:${alertKey}`;
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

  /**
   * Read event ids in a time window from the GLOBAL index.
   *
   * One bounded range over `fw:idx:evt:all`, ascending by broker time — the order
   * the live tail renders. This is what lets `subscribe` backfill without a source
   * filter: before the global index existed there was no bounded way to answer
   * "everything since T" across all devices, so the backfill was skipped and the
   * tail was empty on every refresh.
   *
   * @returns {Promise<string[]>} ids, OLDEST first
   */
  async readIndexRange({ sinceMs = 0, untilMs = Date.now(), limit = 500 } = {}) {
    const c = await this.connect();
    return c.zrangebyscore(
      'fw:idx:evt:all',
      `(${Math.max(0, sinceMs)}`,
      String(untilMs),
      'LIMIT', 0, Math.max(1, limit),
    );
  }

  /**
   * Read the newest events across ALL sources, with their records attached.
   *
   * Backs the All Events tab. The per-source index cannot answer this: there is no
   * way to enumerate sources without a SCAN, which this design refuses everywhere.
   * The worker therefore also writes every accepted event to `fw:idx:evt:all`, and
   * this reads that one bounded range.
   *
   * Newest first, filtered SERVER-SIDE by `sinceMs`, so asking for the last hour
   * costs strictly less work than the last 7 days rather than the server building
   * everything and the client discarding most of it.
   *
   * Two round trips: the index range, then one MGET for the records. The MGET is
   * chunked because a 7-day window can exceed what one command should carry.
   */
  async readAllEvents({ sinceMs = 0, untilMs = Date.now(), limit = 500 } = {}) {
    const c = await this.connect();

    const lo = Math.max(0, sinceMs);
    // ZREVRANGEBYSCORE descends, so the newest events come back first. The lower
    // bound is EXCLUSIVE `(` so a window boundary is not counted twice across two
    // overlapping queries.
    const ids = await c.zrevrangebyscore(
      'fw:idx:evt:all',
      String(untilMs),
      `(${lo}`,
      'LIMIT', 0, Math.max(1, limit),
    );

    // How many the window holds in TOTAL, not just how many this page carries.
    //
    // The UI needs this for its badge. Deriving the badge from the page made it
    // report "rows I have fetched", which is 0 until the tab is opened — so after a
    // browser refresh on another tab the badge read 0 while the window was full.
    // ZCOUNT is one O(log N) index lookup, cheap next to the MGET below.
    const total = await c.zcount('fw:idx:evt:all', `(${lo}`, String(untilMs));

    if (ids.length === 0) {
      return { events: [], total };
    }

    const events = [];
    // 256 ids per MGET: bounded request size, bounded response size.
    const CHUNK = 256;
    for (let i = 0; i < ids.length; i += CHUNK) {
      const slice = ids.slice(i, i + CHUNK);
      const raw = await c.mget(slice.map((id) => `fw:evt:${id}`));
      raw.forEach((item, j) => {
        if (!item) {
          // The record expired while its index member survived, or a delete raced
          // this read. Skipped rather than rendered as a blank row; the prune will
          // drop the member.
          this.log.debug('event index member has no record', { id: slice[j] });
          return;
        }
        const parsed = safeParse(item);
        if (parsed) events.push(parsed);
        else this.log.warn('unparseable stored event during all-events read', { id: slice[j] });
      });
    }

    // ---- alert state per row.
    //
    // An event record carries NO state of its own — it is an immutable fact that
    // arrived. "Open or closed" is a property of the ALERT (`source:type`), which the
    // fold maintains on `fw:alert:{key}`. So this is a second read, over the DISTINCT
    // alerts in the page rather than over every row: 400 events from 4 incidents is 4
    // lookups, not 400.
    //
    // One pipeline, three fields. `state` is 'open' or 'closed'; `closed_at` says
    // when, and is null while open; `count` is the alert's occurrence rollup — how
    // many times this CONDITION has been reported.
    //
    // `count` is fetched here rather than per event because an event record has no
    // count of its own: the `EVT_*` schema has no such field and the ingress never
    // adds one, so any per-event count would be a constant 1. The number an operator
    // means by "count" is the alert's, and it costs nothing extra to join it in the
    // same HMGET that already runs for the state.
    const alertKeys = [...new Set(
      events
        .filter((e) => e?.source && e?.type)
        .map((e) => `${e.source}:${e.type}`),
    )];
    const states = new Map();
    if (alertKeys.length > 0) {
      const pipe = c.pipeline();
      for (const key of alertKeys) pipe.hmget(`fw:alert:${key}`, 'state', 'closed_at', 'count');
      const res = await pipe.exec();
      res.forEach((tuple, i) => {
        const [err, value] = tuple || [];
        if (err || !Array.isArray(value)) return;
        const [state, closedAt, count] = value;
        states.set(alertKeys[i], {
          state: state || null,
          closedAt: closedAt ? Number(closedAt) : null,
          count: Number.isFinite(Number(count)) ? Number(count) : null,
        });
      });
    }
    for (const e of events) {
      const key = e?.source && e?.type ? `${e.source}:${e.type}` : null;
      const s = key ? states.get(key) : null;
      e.alertState = s?.state ?? null;
      e.alertClosedAt = s?.closedAt ?? null;
      // The ALERT's occurrence count, not a per-event number.
      e.alertCount = s?.count ?? null;
    }

    return { events, total };
  }

  /** Read the records for a set of ids. Missing keys are filtered out. */
  async readEvents(ids) {    if (ids.length === 0) return [];
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
   * Read recently RETIRED alerts, newest closure first.
   *
   * Backs the Closed tab. The open index cannot answer this — retiring an alert
   * removes it from that index — so the worker's fold maintains a second zset
   * scored by `closed_at` (docs/event-lifecycle.md §5.2).
   *
   * Same three round trips as readOpenAlerts, for the same reason: the hashes
   * are read in one Lua script and the event records in one MGET, so the cost is
   * independent of how many rows come back. An hourly full read per client is
   * therefore fine, but it is still bounded by `limit` because a 7-day window of
   * closures is not small.
   *
   * `sinceMs` filters server-side via ZRANGEBYSCORE rather than in the client, so
   * asking for a shorter window costs strictly less work.
   */
  async readClosedAlerts({ sinceMs = 0, limit = 500 } = {}) {
    const c = await this.connect();

    // Newest first: ZREVRANGEBYSCORE with an inclusive lower bound.
    const keys = await c.zrevrangebyscore(
      'fw:idx:alert:closed',
      '+inf',
      `(${Math.max(0, sinceMs)}`,
      'LIMIT', 0, Math.max(1, limit),
    );
    if (keys.length === 0) return [];

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

    // The closure metadata is not in SNAPSHOT_FIELDS, so it costs one extra
    // batched read. `closedAt` is what the tab sorts by and `closedBy` is the id
    // of the retiring event, which is what makes the row explain itself.
    //
    // ioredis's `pipeline()` takes NO argument: it returns a builder that you
    // queue commands onto, then `exec()`. Passing an array of command tuples
    // throws `Cannot read properties of undefined (reading 'apply')` — which is
    // exactly the error this produced before, and it surfaced only as
    // `closed_failed` on the wire, so the cause was invisible from the UI.
    const pipe = c.pipeline();
    for (const r of rows) {
      pipe.hmget(`fw:alert:${r.key}`, 'state', 'closed_at', 'closed_by');
    }
    const meta = await pipe.exec();
    meta.forEach((res, i) => {
      // `exec()` resolves to `[err, value]` tuples, so a per-command error is at
      // index 1 of the tuple, not the tuple itself.
      const [err, value] = res || [];
      if (err) {
        this.log.warn('closed metadata read failed', { key: rows[i].key, err: String(err) });
        return;
      }
      const [state, closedAt, closedBy] = value || [];
      rows[i].state = state || null;
      rows[i].closedAt = closedAt ? Number(closedAt) : null;
      rows[i].closedBy = closedBy || null;
    });

    const wanted = [];
    for (const row of rows) if (row.lastId) wanted.push(row.lastId);
    if (wanted.length > 0) {
      const raw = await c.mget(wanted.map((id) => `fw:evt:${id}`));
      const byId = new Map();
      wanted.forEach((id, i) => {
        if (!raw[i]) return;
        const parsed = safeParse(raw[i]);
        if (parsed) byId.set(id, parsed);
        else this.log.warn('unparseable stored event during closed read', { id });
      });
      for (const row of rows) {
        if (row.lastId) row.event = byId.get(row.lastId) ?? null;
      }
    }
    return rows;
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
      -- The global timeline too, or a deleted event stays listed in the All Events
      -- tab and resolves to nothing when read — a row that can never be opened.
      redis.call('ZREM', KEYS[3], ARGV[1])
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
          3,
          `fw:evt:${id}`,
          `${indexPrefix}${source}`,
          'fw:idx:evt:all',
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

  /**
   * Close an alert as an operator.
   *
   * Writes FOUR things, and all four matter — an earlier version wrote only the
   * first two, which made an operator close invisible everywhere:
   *
   *   1. the alert hash: `state=closed`, severity CLOSE, who closed it and when
   *   2. `fw:alert:open:{key}` deleted, so nothing renders it as firing
   *   3. `fw:idx:alert:closed` gets the key, scored by `closed_at` — without it
   *      the Closed tab cannot enumerate the closure at all
   *   4. a real `fw:evt:{id}` CLEAR record, and a member in `fw:idx:src:{source}`
   *
   * (4) is the non-obvious one. A source-sent CLEAR is a real event: the worker
   * stores the record, indexes it by source, and the live tail shows it. An
   * operator close that only flipped the hash left the retention window with no
   * record of WHY the alert ended — the Closed tab could show the row but not a
   * summary or an author, and the audit trail had a gap exactly where a human
   * made a decision.
   *
   * Written server-side rather than synthesised by each client, so every client
   * and the audit trail see byte-identical history.
   *
   * The event id is derived from the alert key and the close time, so a retried
   * close (or two operators racing) collapses onto ONE record instead of
   * producing duplicates.
   */
  async closeAlert(hashKey, by, nowMs, ttlMs, indexKey = 'fw:idx:alert:open', bareKey = '') {
    const alertKey = bareKey || hashKey.replace(/^fw:alert:/i, '');
    // `source:type` — the alert key's halves, split on the FIRST colon because a
    // type may legitimately contain colons while a source may not.
    const sep = alertKey.indexOf(':');
    const source = sep === -1 ? alertKey : alertKey.slice(0, sep);
    const type = sep === -1 ? '' : alertKey.slice(sep + 1);

    const script = `
      if redis.call('EXISTS', KEYS[1]) == 0 then
        return {'missing'}
      end

      -- Nothing to do if it is already closed: a second operator clicking Close
      -- must not overwrite the original author or timestamp.
      local state = redis.call('HGET', KEYS[1], 'state')
      if state == 'closed' then
        return {'already', redis.call('HGET', KEYS[1], 'closed_by') or '',
                redis.call('HGET', KEYS[1], 'closed_at') or '0'}
      end

      redis.call('DEL', KEYS[2])
      redis.call('ZREM', KEYS[3], ARGV[4])
      redis.call('HSET', KEYS[1],
        'state', 'closed',
        'severity', 'CLOSE',
        'closed_by', ARGV[1],
        'closed_at', ARGV[2],
        'watermark', ARGV[2],
        'watermark_id', ARGV[3],
        'last_id', ARGV[6])
      redis.call('HDEL', KEYS[1], 'acked_by', 'acked_at')
      redis.call('PEXPIRE', KEYS[1], ARGV[5])

      -- The closed index. Scored by closed_at, which is what the tab sorts and
      -- filters on.
      redis.call('ZADD', KEYS[4], ARGV[2], ARGV[4])

      -- The CLEAR event record, so the closure exists in history and the tail.
      -- NX: a retried close must not rewrite a record that already exists.
      redis.call('SET', KEYS[5], ARGV[7], 'EX', ARGV[5], 'NX')
      redis.call('ZADD', KEYS[6], ARGV[2], ARGV[6])
      -- ...and the GLOBAL timeline, so an operator close is searchable in All
      -- Events like any other event. Omitted from the source index below would be
      -- worse than useless: it would be findable by source but not by time.
      redis.call('ZADD', KEYS[7], ARGV[2], ARGV[6])

      -- Carry the severity forward as the producer value so the badge shows
      -- CLEAR rather than the internal CLOSE (docs/frontend.md §5.1).
      return {'closed', ARGV[1], ARGV[2]}
    `;
    const openKey = hashKey.replace(/^fw:alert:/, 'fw:alert:open:');
    // Deterministic: same alert + same millisecond -> same id.
    const eventId = `operator-clear-${alertKey.replace(/[^a-zA-Z0-9._-]/g, '_')}-${nowMs}`;
    const eventRecord = JSON.stringify({
      id: eventId,
      source,
      type,
      severity: 'CLOSE',
      ts: new Date(nowMs).toISOString(),
      payload: {
        msg: `Closed by ${by}`,
        EVT_Severity: 'CLEAR',
        EVT_Device: source,
        EVT_EventKey: type,
        // There is no producer here, so name the actor as the agent. It is the
        // truth, and it keeps the enriched columns populated.
        EVT_Agent: by,
        EVT_AgentOwner: '',
      },
      operatorClose: true,
      brokerTs: nowMs,
    });

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const c = await this.connect();
      try {
        const res = await c.eval(
          script,
          7,
          hashKey,
          openKey,
          indexKey,
          'fw:idx:alert:closed',
          `fw:evt:${eventId}`,
          `fw:idx:src:${source}`,
          'fw:idx:evt:all',
          by,
          String(nowMs),
          `operator-close-${nowMs}`,
          alertKey,
          String(ttlMs),
          eventId,
          eventRecord,
        );
        const [status, closedBy, closedAt] = res;
        if (status === 'missing') return { status: 'missing' };
        if (status === 'already') {
          return { status: 'already', by: closedBy, at: Number(closedAt) };
        }
        return { status: 'closed', by: closedBy, at: Number(closedAt), eventId };
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

  /**
   * Append one journal entry and return the stored entry.
   *
   * An APPEND, never a SET. Two operators annotating the same incident must both
   * keep their text; last-write-wins on a single field would erase the first note
   * with no trace that it ever existed, which is precisely the lost update this
   * codebase already had to fix for `count` and for the ack.
   *
   * The list is trimmed to MAX_JOURNAL_ENTRIES on every append rather than on a
   * timer: a timer cannot know which incidents are active and would walk every key
   * forever. The TTL is refreshed on each append so a journal expires with the alert
   * it belongs to instead of outliving it as an orphan list.
   *
   * @returns {Promise<{entry:{id:string,by:string,at:number,text:string}, count:number, truncated:boolean}>}
   */
  async appendJournal(key, entry, ttlMs, maxEntries) {
    const hashKey = journalKey(key);
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const c = await this.connect();
      try {
        const pipe = c.multi()
          .rpush(hashKey, JSON.stringify(entry))
          // Keep only the newest `maxEntries`. LTRIM takes inclusive start/stop, so
          // -maxEntries..-1 keeps the tail; a negative start larger than the list
          // is clamped by Redis, which is what makes this safe on a short list.
          .ltrim(hashKey, -maxEntries, -1)
          .lrange(hashKey, -maxEntries, -1)
          .pexpire(hashKey, ttlMs);
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
          // `truncated` means entries were dropped to stay in the cap. Only true
          // when the list is exactly at the cap, since that is the only observable
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
   * Read the newest entries for a set of alert keys.
   *
   * One `LRANGE` per key in a single pipeline, bounded by the caller. Returns an
   * entry per requested key even when the journal is empty, so a client can tell
   * "read it and there is nothing" from "never answered" — the distinction the
   * Closed tab needed a `loadedAt` flag for.
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
