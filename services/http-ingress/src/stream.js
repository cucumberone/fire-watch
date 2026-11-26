/**
 * Append a batch to the ingest stream and wait for replication.
 *
 * The batch is one MULTI/EXEC, so a request is all-or-nothing: either every
 * event in it is in the stream or none is, and nginx's retry cannot leave half a
 * batch behind.
 *
 * `202` claims the event is durable. Two things make that true:
 *
 *   1. The master accepts the XADD only when `min-replicas-to-write` is satisfied;
 *      otherwise it answers NOREPLICAS and this throws -> 503.
 *   2. `WAIT <waitReplicas> <timeout>` blocks until that many replicas have
 *      acknowledged the write. Without it a master crash in the replication gap
 *      loses an event that was already answered 202 (docs/http-ingress.md §3).
 */
export class StreamWriter {
  constructor(cfg, valkey, log) {
    this.cfg = cfg;
    this.valkey = valkey;
    this.log = log;
  }

  /** @param {Array<string[]>} entries  field/value arrays, one per event */
  async appendBatch(entries) {
    const { streamKey, waitReplicas, confirmTimeoutMs } = this.cfg;
    const ids = await this.valkey.write(async (c) => {
      const multi = c.multi();
      for (const fields of entries) multi.xadd(streamKey, '*', ...fields);
      const res = await multi.exec();
      // A per-command error inside MULTI is reported in the tuples, not thrown.
      for (const [err] of res || []) if (err) throw err;
      if (waitReplicas > 0) {
        const acked = Number(await c.wait(waitReplicas, confirmTimeoutMs));
        if (acked < waitReplicas) {
          const err = new Error(`only ${acked}/${waitReplicas} replicas acknowledged within ${confirmTimeoutMs} ms`);
          err.code = 'NOT_REPLICATED';
          throw err;
        }
      }
      return res.map(([, id]) => id);
    });
    return ids;
  }
}
