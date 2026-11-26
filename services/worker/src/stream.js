/**
 * Ingest stream consumer: XREADGROUP in, XACK + XDEL out.
 *
 * Delivery rules (docs/workers.md §6):
 *   - an entry leaves the stream ONLY after the fold has written it: XACK and XDEL
 *     run in one MULTI, so it is never acked-but-kept or deleted-but-pending;
 *   - a transient failure leaves the entry in this consumer's pending list (PEL);
 *     it is re-read with ID `0` after `retryMs`, forever — there is no delivery
 *     limit, because giving up on a valid event is data loss;
 *   - an entry whose consumer died is taken over with XAUTOCLAIM once it has been
 *     idle for `claimIdleMs`;
 *   - only an entry that can NEVER succeed (not JSON, invalid envelope) is moved
 *     to the dead-letter stream, in the same MULTI that removes it.
 *
 * The stream is never trimmed with MAXLEN: a trim deletes unprocessed entries.
 * Its length is the backlog, nothing else.
 */

/** ioredis `[[id, [f, v, f, v, ...]], ...]` -> `[{ id, fields: {f: v} }]`. Null entries are skipped. */
export function parseEntries(raw) {
  const out = [];
  for (const item of raw || []) {
    if (!Array.isArray(item) || typeof item[0] !== 'string') continue;
    const [id, flat] = item;
    if (!Array.isArray(flat)) continue;
    const fields = {};
    for (let i = 0; i + 1 < flat.length; i += 2) fields[flat[i]] = flat[i + 1];
    out.push({ id, fields });
  }
  return out;
}

/** XINFO's flat `[k, v, k, v]` -> object. */
export function flatToObject(flat) {
  const out = {};
  for (let i = 0; i + 1 < (flat || []).length; i += 2) out[flat[i]] = flat[i + 1];
  return out;
}

/**
 * Consumers that can be removed from the group: nothing pending, idle longer
 * than `idleMs`, and not us. Worker pods come from a Deployment, so every
 * restart leaves a new consumer name behind; without this the group grows by
 * one consumer per rollout.
 */
export function staleConsumers(infos, self, idleMs) {
  return infos
    .filter((c) => c.name !== self && Number(c.pending) === 0 && Number(c.idle) > idleMs)
    .map((c) => c.name);
}

export class StreamConsumer {
  /**
   * @param {object} cfg
   * @param {import('./valkey.js').ValkeyClient} reader  dedicated: XREADGROUP BLOCK holds the connection
   * @param {import('./valkey.js').ValkeyClient} writer  shared write client for XACK/XDEL/DLQ
   */
  constructor(cfg, reader, writer, log) {
    this.cfg = cfg;
    this.reader = reader;
    this.writer = writer;
    this.log = log;
    this.ready = false;
  }

  /** Create the group (and the stream) if absent. From `0`, so nothing written before it is skipped. */
  async ensureGroup() {
    const { streamKey, streamGroup } = this.cfg;
    try {
      await this.writer.write((c) => c.xgroup('CREATE', streamKey, streamGroup, '0', 'MKSTREAM'));
      this.log.info('created consumer group', { stream: streamKey, group: streamGroup });
    } catch (err) {
      if (!String(err?.message || err).includes('BUSYGROUP')) throw err;
    }
    this.ready = true;
  }

  /**
   * `id = '>'` reads new entries (blocking); `id = '0'` re-reads this consumer's
   * own pending entries (non-blocking), which is how a failed entry is retried.
   */
  async read(id) {
    const { streamKey, streamGroup, consumerName, streamBatch, streamBlockMs } = this.cfg;
    const args = ['GROUP', streamGroup, consumerName, 'COUNT', streamBatch];
    if (id === '>') args.push('BLOCK', streamBlockMs);
    args.push('STREAMS', streamKey, id);
    let res;
    try {
      res = await this.reader.write((c) => c.xreadgroup(...args));
    } catch (err) {
      // The group vanishes if the stream key is lost (e.g. a FLUSHALL in the lab).
      if (String(err?.message || err).includes('NOGROUP')) {
        this.ready = false;
        await this.ensureGroup();
        return [];
      }
      throw err;
    }
    if (!res || !res[0]) return [];
    return parseEntries(res[0][1]);
  }

  /** Take over entries another (dead) consumer left pending. */
  async claim() {
    const { streamKey, streamGroup, consumerName, claimIdleMs, streamBatch } = this.cfg;
    const res = await this.reader.write((c) => c.xautoclaim(
      streamKey, streamGroup, consumerName, claimIdleMs, '0-0', 'COUNT', streamBatch,
    ));
    return parseEntries(res?.[1]);
  }

  /** Remove the entry: acknowledged and deleted atomically. */
  async ackDelete(ids) {
    if (ids.length === 0) return;
    const { streamKey, streamGroup } = this.cfg;
    const res = await this.writer.write((c) => c
      .multi()
      .xack(streamKey, streamGroup, ...ids)
      .xdel(streamKey, ...ids)
      .exec());
    for (const [err] of res || []) if (err) throw err;
  }

  /** Copy to the dead-letter stream and remove from the ingest stream, atomically. */
  async deadLetter(entry, reason) {
    const { streamKey, streamGroup, streamDlqKey } = this.cfg;
    const flat = Object.entries(entry.fields).flat();
    const res = await this.writer.write((c) => c
      .multi()
      .xadd(streamDlqKey, '*', ...flat, 'origId', entry.id, 'reason', String(reason).slice(0, 512))
      .xack(streamKey, streamGroup, entry.id)
      .xdel(streamKey, entry.id)
      .exec());
    for (const [err] of res || []) if (err) throw err;
  }

  /** Drop consumers left behind by old pods. Best effort. */
  async gcConsumers() {
    const { streamKey, streamGroup, consumerName, consumerGcIdleMs } = this.cfg;
    const raw = await this.writer.write((c) => c.xinfo('CONSUMERS', streamKey, streamGroup));
    const stale = staleConsumers((raw || []).map(flatToObject), consumerName, consumerGcIdleMs);
    for (const name of stale) {
      // eslint-disable-next-line no-await-in-loop
      await this.writer.write((c) => c.xgroup('DELCONSUMER', streamKey, streamGroup, name));
    }
    if (stale.length > 0) this.log.info('removed idle stream consumers', { count: stale.length });
  }

  isReady() {
    return this.ready;
  }
}
