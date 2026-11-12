/**
 * Event persistence: the write half of the worker.
 *
 * Order is fixed by docs/workers.md §3 and is the whole point of the design:
 *
 *   record + index + fold (one atomic script)  ->  audit  ->  publish  ->  ack
 *
 * WHAT CHANGED WITH VALKEY
 *
 * The record used to be two keys: `fw:evt:{id}` (one per DELIVERY) and
 * `fw:alert:{source}:{type}` (the rollup). They are now ONE hash per CONDITION at
 * `event:<sha256(device + 0x1f + summary)[0:32]>` (services/worker/src/keygen.js).
 *
 * That collapses the two kinds of duplicate into one rule:
 *
 *   - a transport duplicate (the same MESSAGE again) keeps its broker timestamp and
 *     its id, so the fold's (ts, id) watermark returns 0 and nothing moves;
 *   - a content duplicate (the same CONDITION, new message id) maps to the SAME
 *     key, so it cannot create a second record — it bumps `count` and moves
 *     `lastSeen`.
 *
 * The old design needed a separate fingerprint set (`fw:fp:*`) to detect the second
 * case, because the record key was the message id. With a content-derived key that
 * set is redundant: the key IS the fingerprint. One fewer key per event and one
 * fewer round trip per write.
 *
 * Everything in the record therefore lives at one address: payload fields, state,
 * count, first/last occurrence, ack and closure info. The fold owns the update so
 * three workers racing on the same condition converge instead of clobbering.
 */
import { readFileSync, statSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { isGzip } from './event.js';
import { bareKeyFor, summaryOf } from './keygen.js';
import { keys } from './config.js';

/** Read the fold script. Reloaded periodically so a ConfigMap edit lands. */
export class FoldScript {
  constructor(path, log, reloadMs = 60000) {
    this.path = path;
    this.log = log;
    this.reloadMs = reloadMs;
    this.source = null;
    this.sha = null;
    this.loadedAt = 0;
  }

  _signature() {
    try {
      const st = statSync(this.path);
      return `${st.mtimeMs}:${st.size}`;
    } catch {
      return null;
    }
  }

  get() {
    const now = Date.now();
    if (this.source && now - this.loadedAt < this.reloadMs) return this.source;

    const sig = this._signature();
    if (sig === null) {
      if (!this.source) throw new Error(`event fold script not readable: ${this.path}`);
      return this.source;
    }
    if (sig !== this.sha) {
      this.source = readFileSync(this.path, 'utf8');
      this.sha = sig;
      this.log.info('loaded event fold script', { path: this.path, bytes: this.source.length });
    }
    this.loadedAt = now;
    return this.source;
  }
}

/**
 * The record fields the worker supplies, as flat field/value pairs for the fold.
 *
 * Flat pairs rather than a JSON blob because the fold applies them with one HSET in
 * the same atomic step as the counters. Handing the script JSON would mean decoding
 * it in Lua, which is both slower and a place for a schema mistake to hide.
 *
 * `payload` is kept as a JSON STRING holding the (possibly gzipped) payload object.
 * It is not exploded into the hash because payload keys are producer-defined and
 * unbounded — an `additionalProperties: true` schema means the field set is not
 * known ahead of time, and exploding it would let a producer mint arbitrary hash
 * fields.
 */
function recordFields({ evt, storedPayload, brokerTs }) {
  const pairs = [
    'source', String(evt.source ?? ''),
    'type', String(evt.type ?? ''),
    'msg', summaryOf(evt),
    // The producer's own severity label (MINOR, MAJOR, CLEAR, ...), kept so the UI
    // can show that a WARNING row was reported as MINOR.
    'displaySeverity', String(evt.payload?.EVT_Severity ?? evt.severity ?? ''),
    'brokerTs', String(brokerTs),
    'lastTs', String(evt.ts ?? ''),
    'payload', JSON.stringify(storedPayload ?? {}),
  ];
  return pairs;
}

/**
 * Fork EVERY delivery into the audit trail, before any dedup runs.
 *
 * This is deliberately NOT part of the fold.
 *
 * The fold is the dedup: it decides whether an arrival counts, retires, or is
 * dropped as stale. Writing inside it meant the trail could only describe the
 * arrivals the fold ACCEPTED (plus redeliveries it happened to recognise) — so the
 * one question an audit trail exists to answer, "what did the broker actually hand
 * us, and how many times", was still unanswerable.
 *
 * So the fork happens FIRST, unconditionally, and the fold runs afterwards and
 * knows nothing about it. Consequences, all intended:
 *
 *   - every accepted message produces exactly one row, including the ones the fold
 *     will reject as stale or redelivered;
 *   - a delivery is never overwritten by a later one, because the id is unique per
 *     DELIVERY (a UUID) rather than derived from the message id;
 *   - `applied` is NOT here. Before the fold there is no verdict to record. The tab
 *     shows the arrival; the verdict is a property of the condition and lives on
 *     the record (docs/history.md §2).
 *
 * Cost is one HSET + one ZADD + one PEXPIRE per delivery, pipelined — which is
 * exactly the storage doubling that was accepted when this was requested.
 *
 * @param {number} deliveryId  unique per delivery; becomes the index member
 */
export async function recordDelivery({
  valkey, evt, storedPayload, brokerTs, bareKey, deliveryId, cfg, log,
}) {
  const key = keys.history(deliveryId);
  const ttlMs = cfg.historyTtl * 1000;
  const fields = [
    'key', bareKey,
    'id', String(evt.id ?? ''),
    'source', String(evt.source ?? ''),
    'type', String(evt.type ?? ''),
    'severity', String(evt.severity ?? ''),
    'displaySeverity', String(evt.payload?.EVT_Severity ?? evt.severity ?? ''),
    'msg', summaryOf(evt),
    'brokerTs', String(brokerTs),
    'ts', String(evt.ts ?? ''),
    'payload', JSON.stringify(storedPayload ?? {}),
  ];

  const wrote = await valkey.write((c) => c
    .multi()
    .hset(key, ...fields)
    .pexpire(key, ttlMs)
    .zadd(keys.historyIndex, brokerTs, deliveryId)
    .exec());

  // A per-command error inside a MULTI is reported in the result tuples and does
  // NOT throw. Swallowing it would leave an index member with no hash behind it,
  // which reads as an empty row — so it is raised here, where the caller requeues.
  for (const [err] of wrote || []) {
    if (err) throw err;
  }

  if (log) log.debug('delivery recorded', { deliveryId, key: bareKey });
  return deliveryId;
}

/** A unique id for one delivery. Not derived from anything: see recordDelivery. */
export function newDeliveryId() {
  return `${Date.now().toString(36)}-${randomUUID()}`;
}

/**
 * Write one event: record + both timeline indexes + state fold, atomically.
 *
 * @returns {Promise<{applied:number, key:string, bareKey:string, eventKey:string}>}
 *   `applied` is the fold's verdict: 0 stale/duplicate, 1 occurrence, 3 closed.
 */
export async function persistEvent({ valkey, evt, storedPayload, brokerTs, cfg, fold, log }) {
  const bareKey = bareKeyFor(evt);
  const eventKey = keys.event(bareKey);
  const ttlMs = cfg.eventTtl * 1000;

  // ARGV layout must match k8s/base/20-event-fold-config.yaml exactly.
  const args = [
    String(brokerTs),       // ARGV[1] broker ts
    String(evt.id ?? ''),   // ARGV[2] arrival id -> lastId / closedBy
    String(evt.severity),   // ARGV[3] severity
    bareKey,                // ARGV[4] index member
    String(evt.source ?? ''), // ARGV[5] device
    String(ttlMs),          // ARGV[6] ttl in ms
    ...recordFields({ evt, storedPayload, brokerTs }),
  ];

  const applied = await valkey.write((c) =>
    c.eval(
      fold.get(),
      5,
      eventKey,
      keys.eventAllIndex,
      keys.eventOpenIndex,
      keys.eventClosedIndex,
      keys.sourceIndex(evt.source),
      ...args,
    ),
  );

  // Prune on the write path: a cron job cannot know which sources are active and
  // would scan dead keys forever (workers.md §4.1).
  await maybePruneIndex(valkey, evt.source, cfg, log);

  if (Number(applied) === 0) {
    log.debug('event dropped as stale or redelivered', { key: bareKey, id: evt.id });
  }

  return { applied: Number(applied), key: eventKey, bareKey, eventKey: bareKey };
}

/**
 * Trim every index to the retention window.
 *
 * ZADD members never expire on their own, so without this the working set becomes
 * unbounded regardless of TTL — which would break the capacity model in sizing.md
 * §2 outright. A missed prune is corrected by the next one.
 */
const pruneCounters = new Map();

export async function maybePruneIndex(valkey, source, cfg, log, force = false) {
  const n = (pruneCounters.get(source) || 0) + 1;
  if (!force && n < cfg.indexPruneEvery) {
    pruneCounters.set(source, n);
    return false;
  }
  pruneCounters.set(source, 0);

  const cutoff = Date.now() - cfg.eventTtl * 1000;
  try {
    const removed = await valkey.write((c) =>
      c.zremrangebyscore(keys.sourceIndex(source), '-inf', `(${cutoff}`),
    );
    if (removed > 0) log.debug('pruned source index members', { source, removed });

    // The closed and global indexes need the same cutoff. Both are single global
    // zsets, so they are pruned here rather than per source.
    //
    // Same cutoff as the record's PEXPIRE, so a tab cannot list a key whose hash has
    // already expired — an entry that renders as a blank row.
    const removedClosed = await valkey.write((c) =>
      c.zremrangebyscore(keys.eventClosedIndex, '-inf', `(${cutoff}`),
    );
    if (removedClosed > 0) log.debug('pruned closed index members', { removed: removedClosed });

    const removedAll = await valkey.write((c) =>
      c.zremrangebyscore(keys.eventAllIndex, '-inf', `(${cutoff}`),
    );
    if (removedAll > 0) log.debug('pruned global event index', { removed: removedAll });

    // The history index runs on its OWN, shorter window. Pruning it with the
    // record cutoff would keep three days of audit members alive for seven, which
    // is a million-member zset with no hashes behind it — every read would pay for
    // members that resolve to nothing.
    const histCutoff = Date.now() - cfg.historyTtl * 1000;
    const removedHistory = await valkey.write((c) =>
      c.zremrangebyscore(keys.historyIndex, '-inf', `(${histCutoff}`),
    );
    if (removedHistory > 0) log.debug('pruned history index', { removed: removedHistory });
    return true;
  } catch (err) {
    log.warn('index prune failed', { source, err: String(err) });
    return false;
  }
}

/**
 * Verify a stored value is in the shape we expect. Used by the health path so a
 * corrupt write pattern is caught early rather than at read time.
 */
export function recordLooksValid(buf) {
  return Buffer.isBuffer(buf) && buf.length > 0 && (buf[0] === 0x7b /* { */ || isGzip(buf));
}

/**
 * Find keys in a SOURCE index that are missing from the GLOBAL index.
 *
 * Pure, so the arithmetic is testable without a database. Both are membership lists
 * over the same records, written in the same atomic step — so any key present in one
 * and absent from the other is evidence of a gap, not of two different populations.
 *
 * @param {string[]} sourceKeys  members of idx:source:<device>
 * @param {Iterable<string>} globalKeys  members of idx:event:all
 * @returns {string[]} keys that need to be added to the global index
 */
export function missingFromGlobal(sourceKeys, globalKeys) {
  const have = globalKeys instanceof Set ? globalKeys : new Set(globalKeys || []);
  const out = [];
  const seen = new Set();
  for (const key of sourceKeys || []) {
    if (typeof key !== 'string' || key === '') continue;
    if (have.has(key) || seen.has(key)) continue;
    seen.add(key);
    out.push(key);
  }
  return out;
}

/**
 * Reconcile `idx:event:all` against the per-source indexes.
 *
 * WHY THIS RUNS AT STARTUP. The All events tab reads ONE zset. Records written
 * before that zset existed were stored but not indexed, so the tab could never list
 * them — measured on the lab as 3 index members for 7 stored records, and the
 * symptom was "All events is empty" with rows sitting in the live tail. A query over
 * an incomplete index is not a query bug; it is missing data, and nothing in the
 * read path can detect it.
 *
 * WHY ALL SOURCES. The global index is shared, so a gap is not per-source, but there
 * is no way to enumerate its intended members without reading the sources that were
 * supposed to write them. SCAN over `idx:source:*` is bounded by the number of
 * DEVICES (hundreds), not by the number of events (millions).
 *
 * WHY IT IS BOUNDED AND OPTIONAL. On a large history this is a lot of reads at boot
 * and it competes with consuming. It is skipped entirely when the gap is already
 * zero, which is the normal case after the first run, and it is capped so a
 * pathological history cannot stall startup indefinitely.
 *
 * @returns {Promise<{scanned:number, missing:number, added:number, skipped:number, capped:boolean}>}
 */
export async function reconcileEventIndex(valkey, cfg, log, opts = {}) {
  const maxToAdd = Number.isInteger(opts.maxToAdd) ? opts.maxToAdd : 50000;

  // ---- 1. the global index, as a Set.
  //
  // Read whole rather than per-key: ZSCORE per candidate would be one round trip per
  // event. At 1 000/min over 7 days that set is ~10 M members, which is why this is
  // capped and startup-only.
  const globalKeys = new Set(await valkey.write((c) => c.zrange(keys.eventAllIndex, 0, -1)));
  log.info('index reconcile: global index loaded', { members: globalKeys.size });

  // ---- 2. every source index, compared against it.
  //
  // SCAN, never KEYS. KEYS walks the whole keyspace in one blocking call, and this
  // database holds millions of records — it would stall every reader on the node,
  // including the gateway serving the board.
  const sourceKeys = [];
  {
    let cursor = '0';
    do {
      // eslint-disable-next-line no-await-in-loop
      const [next, batch] = await valkey.write((c) => c.scan(cursor, 'MATCH', 'idx:source:*', 'COUNT', 500));
      cursor = next;
      if (Array.isArray(batch)) sourceKeys.push(...batch);
    } while (cursor !== '0');
  }
  if (sourceKeys.length === 0) {
    return { scanned: 0, missing: 0, added: 0, skipped: 0, capped: false };
  }

  const candidates = [];
  let scanned = 0;
  let capped = false;
  for (const sourceKey of sourceKeys) {
    if (candidates.length >= maxToAdd) { capped = true; break; }
    // eslint-disable-next-line no-await-in-loop
    const members = await valkey.write((c) => c.zrange(sourceKey, 0, -1));
    scanned += members.length;
    for (const key of missingFromGlobal(members, globalKeys)) {
      if (candidates.length >= maxToAdd) { capped = true; break; }
      candidates.push(key);
    }
  }

  if (candidates.length === 0) {
    log.info('index reconcile: global index is complete', { scanned });
    return { scanned, missing: 0, added: 0, skipped: 0, capped: false };
  }
  log.warn('index reconcile: records missing from the global index', {
    scanned, missing: candidates.length, capped,
  });

  // ---- 3. read each candidate's hash for its brokerTs, then ZADD.
  //
  // The score must be the broker arrival time, not "now": scoring it now would put a
  // week-old event at the top of the All events list, which is worse than leaving it
  // out.
  let added = 0;
  let skipped = 0;
  const CHUNK = 500;
  for (let i = 0; i < candidates.length; i += CHUNK) {
    const slice = candidates.slice(i, i + CHUNK);
    const pipe = valkey.write((c) => {
      const p = c.pipeline();
      for (const key of slice) p.hmget(keys.event(key), 'brokerTs');
      return p.exec();
    });
    const rows = await pipe;
    const zadds = [];
    for (let j = 0; j < slice.length; j += 1) {
      const [err, value] = rows[j] || [];
      if (err || !Array.isArray(value)) { skipped += 1; continue; }
      const score = Number(value[0]);
      if (!Number.isFinite(score) || score <= 0) { skipped += 1; continue; }
      zadds.push([score, slice[j]]);
    }
    if (zadds.length > 0) {
      await valkey.write((c) => {
        for (const [score, key] of zadds) c.zadd(keys.eventAllIndex, score, key);
        return null;
      });
      added += zadds.length;
      for (const [, key] of zadds) globalKeys.add(key);
    }
  }

  log.info('index reconcile: done', { scanned, added, skipped, capped });
  return { scanned, missing: candidates.length, added, skipped, capped };
}
