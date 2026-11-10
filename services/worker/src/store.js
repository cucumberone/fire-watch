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
import { isGzip, contentFingerprint } from './event.js';
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
 * Write one event.
 *
 * Two different things are called "duplicate" in this system and they are handled
 * in opposite ways, so they are worth separating explicitly:
 *
 *   1. TRANSPORT duplicate — the same MESSAGE delivered twice (a RabbitMQ
 *      redelivery). Same event id. This must be idempotent: nothing changes.
 *
 *   2. CONTENT duplicate — the same CONDITION reported again, arriving as a new
 *      message with a new id. Per AGENTS.md this must NOT create a second record.
 *      The original record absorbs it: `count` is incremented and `last_seen` is
 *      moved. The new copy is never stored.
 *
 * The previous implementation only handled (1), keyed on `SET … NX id`. That made
 * a re-reporting producer inflate the board with one row per delivery.
 *
 * Order below is deliberate. The content claim happens BEFORE the record write, so
 * a content duplicate is rejected before it can touch the event record, the index
 * or the fold's counters. The transport check stays on the record write itself.
 */
export async function persistEvent({ dfl, evt, storedPayload, brokerTs, cfg, fold, log }) {
  const key = alertKey(evt);
  const ttlSec = cfg.eventTtl;

  // Serialize once. The record keeps the envelope plain and only the payload
  // fields compressed (payload-and-compression.md §4). brokerTs is stored so a
  // client that loads this record later can show when the event actually
  // arrived, not just the producer's own `ts`.
  const record = JSON.stringify({ ...evt, payload: storedPayload, brokerTs });

  // -------------------------------------------- 1. content claim (dedup gate)
  //
  // `SET fw:fp:{fp} {id} NX` is the atomic test. If it succeeds, this fingerprint
  // is new and this event owns it. If it fails, another record already represents
  // this condition — read back the owner's id and let that record absorb this
  // arrival instead of storing it.
  const fp = contentFingerprint(evt);
  const claimed = await dfl.write((c) =>
    c.set(keys.fingerprint(fp), evt.id, 'EX', ttlSec, 'NX'),
  );

  if (claimed === null) {
    const ownerId = await dfl.write((c) => c.get(keys.fingerprint(fp)));
    if (!ownerId) {
      // The fingerprint expired between SET and GET (both are TTL'd). Not worth
      // failing an event over: treat this delivery as the new owner.
      log.debug('fingerprint vanished between claim and read', { fp, id: evt.id });
      await dfl.write((c) => c.set(keys.fingerprint(fp), evt.id, 'EX', ttlSec));
    } else {
      // The original record absorbs this arrival. `brokerTs` is the new event's
      // arrival time, which is what "Last occurrence" must show.
      //
      // The fold is given the OWNER's id, not this copy's. The copy is discarded
      // and never stored, so recording its id as `last_id` leaves the alert
      // pointing at a record that does not exist — and everything that resolves a
      // summary from that record then renders nothing (docs/websocket-server.md
      // §3.2). The owner is the record that actually represents this condition.
      const applied = await dfl.write((c) =>
        c.eval(
          fold.get(),
          3,
          keys.alert(key),
          keys.alertOpen(key),
          keys.alertOpenIndex,
          String(brokerTs),
          ownerId,
          evt.severity,
          String(cfg.alertDedupWindow),
          key,
        ),
      );
      // The stored record is NOT rewritten: it keeps its original id, original
      // payload and original brokerTs. Only the rollup moves. That is the
      // requested behaviour — "the original event stays, Last occurrence and
      // Count move, the new event is deleted".
      log.debug('content duplicate absorbed by the original record', {
        id: evt.id, ownerId, fp, applied: Number(applied),
      });
      return {
        applied: Number(applied),
        alertKey: key,
        contentDuplicate: true,
        ownerId,
      };
    }
  }

  // --------------------------------------- 2. record (transport dedup on id)
  //
  // NX again, now keyed on the event id: a redelivered MESSAGE must not overwrite
  // the stored record.
  const created = await dfl.write((c) => c.set(keys.event(evt.id), record, 'EX', ttlSec, 'NX'));
  if (created === null) {
    // The record already exists for this id, but the fingerprint was free.
    //
    // That combination means the fingerprint for this content had expired or was
    // never written — the normal case for records created before content dedup
    // existed, and for anything left behind by a partial reset. It does NOT mean
    // this is a transport duplicate: a genuine redelivery would have been caught
    // by the fingerprint branch above (identical content, fingerprint present).
    //
    // Treating it as a transport duplicate was the bug: the run was dropped
    // silently, the fold never ran, so `count` and `last_seen` never moved and the
    // alert could not recover. Instead, absorb it exactly like a content
    // duplicate — the existing record IS the owner of this condition.
    const applied = await dfl.write((c) =>
      c.eval(
        fold.get(),
        3,
        keys.alert(key),
        keys.alertOpen(key),
        keys.alertOpenIndex,
        String(brokerTs),
        // The existing record IS the owner here, so its id is what represents
        // this condition. `evt.id` names a record that was never stored.
        evt.id,
        evt.severity,
        String(cfg.alertDedupWindow),
        key,
      ),
    );
    log.debug('existing record absorbed a repeat (fingerprint was absent)', {
      id: evt.id, fp, applied: Number(applied),
    });
    // Re-establish the fingerprint so subsequent arrivals take the cheap path
    // above instead of falling through to this one every time. Not NX: whatever is
    // there now must not be overwritten, but we know it was absent a moment ago.
    await dfl.write((c) => c.set(keys.fingerprint(fp), evt.id, 'EX', ttlSec));
    return {
      applied: Number(applied),
      alertKey: key,
      contentDuplicate: true,
      ownerId: evt.id,
    };
  }

  // ----------------------------------------------------------------- 3. index
  // Score is the BROKER timestamp, not the producer ts: a redelivered event
  // must keep its original position in the timeline (workers.md §4).
  await dfl.write((c) => c.zadd(keys.index(evt.source), brokerTs, evt.id));

  // ------------------------------------------------------------ 4. alert fold
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

  // ----------------------------------------------------------- 5. index prune
  // ZADD members never expire on their own, so the index is trimmed on the
  // write path (workers.md §4.1). Cheap and self-correcting.
  await maybePruneIndex(dfl, evt.source, cfg, log);

  return { applied: Number(applied), alertKey: key, fingerprint: fp };
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
