/**
 * Event persistence: the write half of the worker.
 *
 * Order is fixed by docs/workers.md §3 and is the whole point of the design:
 *
 *   record  ->  index  ->  alert fold  ->  (audit)  ->  (publish)  ->  ack
 *
 * The fold runs in a Lua script so it is atomic across the three workers racing
 * on the same alert key (docs/event-lifecycle.md §5.2).
 */
import { readFileSync, statSync } from 'node:fs';
import { isGzip } from './event.js';
import { keys, alertKey } from './config.js';

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
      if (!this.source) throw new Error(`alert fold script not readable: ${this.path}`);
      return this.source;
    }
    if (sig !== this.sha) {
      this.source = readFileSync(this.path, 'utf8');
      this.sha = sig;
      this.log.info('loaded alert fold script', { path: this.path, bytes: this.source.length });
    }
    this.loadedAt = now;
    return this.source;
  }
}

/**
 * Write one event. Returns the fold verdict so the caller can publish it.
 *
 * `evt` is the validated envelope; `storedPayload` is the compressed payload.
 */
export async function persistEvent({ dfl, evt, storedPayload, brokerTs, cfg, fold, log }) {
  const key = alertKey(evt);
  const ttlSec = cfg.eventTtl;

  // Serialize once. The record keeps the envelope plain and only the payload
  // fields compressed (payload-and-compression.md §4). brokerTs is stored so a
  // client that loads this record later can show when the event actually
  // arrived, not just the producer's own `ts`.
  const record = JSON.stringify({ ...evt, payload: storedPayload, brokerTs });

  // -------------------------------------------------- 1. record (duplicate gate)
  //
  // NX makes the SET itself the dedup test. A duplicate is identified by EVENT
  // ID, not by "it looks like the last one": two events with the same id are the
  // same event, whether they arrived as a RabbitMQ redelivery or as two POSTs
  // from an over-eager producer.
  //
  // The previous design relied only on the fold's (timestamp, id) watermark,
  // which cannot catch this case — every broker delivery carries a fresh
  // timestamp, so a re-sent id always looked newer and was counted again. That
  // is why a duplicate storm inflated `count` instead of being dropped.
  //
  // Returns null when the key already existed: the caller must NOT fold, count,
  // publish or re-index it.
  const created = await dfl.write((c) => c.set(keys.event(evt.id), record, 'EX', ttlSec, 'NX'));
  if (created === null) {
    return { applied: 0, duplicate: true, alertKey: key };
  }

  // ----------------------------------------------------------------- 2. index
  // Score is the BROKER timestamp, not the producer ts: a redelivered event
  // must keep its original position in the timeline (workers.md §4).
  await dfl.write((c) => c.zadd(keys.index(evt.source), brokerTs, evt.id));

  // ------------------------------------------------------------ 3. alert fold
  // The fold also maintains the open-alert index (KEYS[3]) so the index cannot
  // drift from the state it indexes; see the comment block in alert-fold.lua.
  // ARGV[5] is the bare alert key, which is what the index stores.
  const applied = await dfl.write((c) =>
    c.eval(
      fold.get(),
      3,
      keys.alert(key),
      keys.alertOpen(key),
      keys.alertOpenIndex,
      String(brokerTs),
      evt.id,
      evt.severity,
      String(cfg.alertDedupWindow),
      key,
    ),
  );

  // ----------------------------------------------------------- 4. index prune
  // ZADD members never expire on their own, so the index is trimmed on the
  // write path (workers.md §4.1). Cheap and self-correcting.
  await maybePruneIndex(dfl, evt.source, cfg, log);

  return { applied: Number(applied), alertKey: key };
}

/**
 * Trim the per-source index to the retention window.
 *
 * Runs every FW_INDEX_PRUNE_EVERY writes per source, not on a timer: a cron job
 * cannot know which sources are active and would scan dead keys forever.
 */
const pruneCounters = new Map();

export async function maybePruneIndex(dfl, source, cfg, log, force = false) {
  const n = (pruneCounters.get(source) || 0) + 1;
  if (!force && n < cfg.indexPruneEvery) {
    pruneCounters.set(source, n);
    return false;
  }
  pruneCounters.set(source, 0);

  const cutoff = Date.now() - cfg.eventTtl * 1000;
  try {
    const removed = await dfl.write((c) =>
      c.zremrangebyscore(keys.index(source), '-inf', `(${cutoff}`),
    );
    if (removed > 0) log.debug('pruned index members', { source, removed });
    return true;
  } catch (err) {
    // A missed prune is corrected by the next one.
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
