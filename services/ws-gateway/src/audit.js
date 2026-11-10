/**
 * Best-effort audit replication for operator journal entries.
 *
 * WHY THIS EXISTS. A journal note lives in Dragonfly, which has no AOF. A snapshot
 * restore — or a Dragonfly loss between snapshots — therefore takes every note with
 * it. Events are not in that position: they pass through RabbitMQ and are copied to
 * `q.audit.events` before the ack, so the event log is recoverable. A note that was
 * only ever written to Dragonfly is not.
 *
 * So each entry is ALSO published to the audit exchange, exactly as durable as the
 * event audit copy: a persistent, broker-confirmed message on the same exchange the
 * worker already writes to.
 *
 * WHY IT IS BEST-EFFORT. The journal write has already succeeded by the time we
 * publish. Failing the append because the broker is unreachable would turn "your
 * note may not survive a database loss" into "you cannot write a note" — the wrong
 * trade for an annotation, and it would make the UI report a failure for a write
 * that did happen. Instead the failure is logged loudly and the note stays in
 * Dragonfly, which is where the UI reads it from anyway.
 *
 * The connection is lazy and re-established on demand: an operator journal is a
 * low-frequency write, so holding a channel open from process start buys nothing and
 * costs a reconnect path that never gets exercised.
 */
export class AuditPublisher {
  constructor(cfg, log) {
    this.cfg = cfg;
    this.log = log;
    this.conn = null;
    this.channel = null;
    this.connecting = null;
    this.enabled = Boolean(
      cfg.amqpHost && cfg.amqpUser && cfg.amqpPass
      && cfg.amqpAuditExchange && cfg.amqpAuditKey,
    );
    // Imported lazily so a harness without the dependency still runs the rest of
    // the gateway. `amqplib` is loaded on first use, not at module scope.
    this.loadModule = null;
  }

  /** True when notes are actually being replicated. Surfaced at startup and in /metrics. */
  isEnabled() {
    return this.enabled;
  }

  async connect() {
    if (this.channel) return this.channel;
    if (this.connecting) return this.connecting;
    this.connecting = (async () => {
      try {
        const amqp = this.loadModule || (await import('amqplib'));
        this.loadModule = amqp;
        // Credentials go in the connect OPTIONS, not interpolated into a URL: a
        // password containing `@` or `/` would silently produce a different host.
        this.conn = await amqp.connect({
          protocol: 'amqp',
          hostname: this.cfg.amqpHost,
          port: this.cfg.amqpPort,
          username: this.cfg.amqpUser,
          password: this.cfg.amqpPass,
        });
        this.conn.on('error', (err) => {
          // A closed connection must not be reused: the next publish reconnects.
          this.log.warn('audit connection error', { err: String(err) });
          this.conn = null;
          this.channel = null;
        });
        this.conn.on('close', () => {
          this.conn = null;
          this.channel = null;
        });
        this.channel = await this.conn.createConfirmChannel();
        return this.channel;
      } finally {
        this.connecting = null;
      }
    })();
    return this.connecting;
  }

  /**
   * Publish one journal entry and wait for the broker confirm.
   *
   * Returns true when the broker accepted it. Never throws: see the note above about
   * turning a replication failure into a failed append.
   *
   * @param {{key:string, entry:object}} record
   */
  async publishJournal(record) {
    if (!this.enabled) return false;
    try {
      const channel = await this.connect();
      const body = Buffer.from(JSON.stringify({
        kind: 'journal',
        key: record.key,
        entry: record.entry,
      }), 'utf8');
      await new Promise((resolve, reject) => {
        channel.publish(
          this.cfg.amqpAuditExchange,
          this.cfg.amqpAuditKey,
          body,
          {
            persistent: true,
            contentType: 'application/json',
            deliveryMode: 2,
            // Deterministic per entry, so a redelivery after a reconnect is the
            // same message rather than a second note. The entry id is minted
            // server-side and is unique per append.
            messageId: `journal:${record.key}:${record.entry.id}`,
          },
          (err) => (err ? reject(err) : resolve()),
        );
      });
      return true;
    } catch (err) {
      this.log.warn('journal audit publish failed — the note is stored but not durable', {
        key: record.key, id: record.entry.id, err: String(err),
      });
      // Drop the channel so the next attempt reconnects instead of reusing a dead one.
      this.channel = null;
      return false;
    }
  }

  async close() {
    try { if (this.channel) await this.channel.close(); } catch { /* ignore */ }
    try { if (this.conn) await this.conn.close(); } catch { /* ignore */ }
    this.channel = null;
    this.conn = null;
  }
}
