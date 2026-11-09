/**
 * Subscription matching and de-duplication.
 *
 * Pure functions only — this is the part worth testing without sockets.
 *
 * Specified by docs/websocket-server.md §3.
 */

/**
 * Does an event match a subscription?
 *
 * An empty `sources` or `types` list means "no filter on that dimension", which
 * is what a UI that just wants everything sends.
 */
export function matchesSubscription(sub, evt) {
  const sources = sub.sources || [];
  const types = sub.types || [];
  const sourceOk = sources.length === 0 || sources.includes(evt.source);
  const typeOk = types.length === 0 || types.includes(evt.type);
  return sourceOk && typeOk;
}

/**
 * Validate a subscribe frame. Returns { ok: true, sub } or { ok: false, code, message }.
 *
 * Rejects anything that would let a client make the pod do unbounded work.
 */
export function validateSubscribe(frame, { maxSubscriptions, existing }) {
  if (!frame || frame.op !== 'subscribe') {
    return { ok: false, code: 'bad_op', message: 'first frame must be {"op":"subscribe"}' };
  }
  if (existing >= maxSubscriptions) {
    return { ok: false, code: 'too_many_subscriptions', message: `limit is ${maxSubscriptions}` };
  }

  const norm = (v) => {
    if (v === undefined || v === null) return [];
    if (!Array.isArray(v)) return null;
    return v.filter((x) => typeof x === 'string' && x.length > 0 && x.length <= 256);
  };

  const sources = norm(frame.sources);
  const types = norm(frame.types);
  if (sources === null || types === null) {
    return { ok: false, code: 'bad_filter', message: 'sources and types must be string arrays' };
  }

  let since = null;
  if (frame.since !== undefined && frame.since !== null) {
    const t = Date.parse(frame.since);
    if (Number.isNaN(t)) {
      return { ok: false, code: 'bad_since', message: 'since must be an ISO timestamp' };
    }
    since = t;
  }

  return { ok: true, sub: { sources, types, since } };
}

/**
 * Bounded LRU of recently seen event ids.
 *
 * Used to drop duplicate `fw:ch:events` frames. The worker's `SET` is idempotent
 * so history is safe, but a duplicate frame would double-render in the UI
 * (workers.md §4). Deliberately in-process: the gateway has no write access.
 */
export class RecentIds {
  constructor(limit) {
    this.limit = limit;
    this.set = new Set();
    this.queue = [];
  }

  /** Returns true if this id was NOT seen before (i.e. it is new). */
  add(id) {
    if (!id) return true;
    if (this.set.has(id)) return false;
    this.set.add(id);
    this.queue.push(id);
    if (this.queue.length > this.limit) {
      const evicted = this.queue.shift();
      this.set.delete(evicted);
    }
    return true;
  }

  get size() {
    return this.set.size;
  }
}

/**
 * Build the backfill query plan for a subscription.
 *
 * A missing `since` means live-only: never an unbounded scan, which would stall
 * the pod (websocket-server.md §3).
 */
export function backfillPlan(sub, nowMs, max) {
  if (!sub.since) return null;
  const from = sub.since;
  const to = nowMs;
  if (from >= to) return null;
  return { min: from, max: to, limit: max };
}

/** Render the wire frame for an event. */
export function eventFrame(event, applied) {
  return JSON.stringify({ op: 'event', event, applied });
}

export function alertFrame(key, applied) {
  return JSON.stringify({ op: 'alert', key, applied });
}
