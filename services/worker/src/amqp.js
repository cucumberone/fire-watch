/**
 * AMQP consumer.
 *
 * Connection lifecycle rules that matter (docs/workers.md §6):
 *   - manual ack only, after the Dragonfly write AND the audit confirm
 *   - nack(requeue=false) only for validation failures
 *   - nack(requeue=true) for every transient condition, forever: the queue
 *     deliberately has no x-delivery-limit (fault-tolerance.md §3.1)
 */
import amqp from 'amqplib';

export class AmqpConsumer {
  constructor(cfg, log) {
    this.cfg = cfg;
    this.log = log;
    this.conn = null;
    this.channel = null;
    this.closing = false;
  }

  url() {
    const { amqpUser, amqpPass, amqpHost, amqpPort } = this.cfg;
    return `amqp://${encodeURIComponent(amqpUser)}:${encodeURIComponent(amqpPass)}@${amqpHost}:${amqpPort}`;
  }

  async connect() {
    const conn = await amqp.connect(this.url(), { heartbeat: 30 });
    conn.on('error', (err) => this.log.error('amqp connection error', { err: String(err) }));
    conn.on('close', () => {
      this.log.warn('amqp connection closed', { closing: this.closing });
      this.conn = null;
      this.channel = null;
    });
    const channel = await conn.createConfirmChannel();
    channel.on('error', (err) => this.log.error('amqp channel error', { err: String(err) }));
    channel.on('close', () => this.log.warn('amqp channel closed'));

    await channel.prefetch(this.cfg.prefetch);
    this.conn = conn;
    this.channel = channel;
    this.log.info('amqp connected', { queue: this.cfg.amqpQueue, prefetch: this.cfg.prefetch });
    return channel;
  }

  /**
   * Consume with a handler. The handler returns one of:
   *   'ack'      -> basic.ack
   *   'requeue'  -> basic.nack(requeue=true)   transient, retry forever
   *   'dlq'      -> basic.nack(requeue=false)  validation failure only
   *
   * Errors thrown by the handler are treated as 'requeue', because an
   * unexpected error is more likely transient than permanent, and the opposite
   * assumption silently discards data.
   */
  async consume(handler) {
    const channel = this.channel || (await this.connect());

    await channel.consume(
      this.cfg.amqpQueue,
      async (msg) => {
        if (msg === null) {
          this.log.info('consumer cancelled by broker');
          return;
        }
        let outcome;
        try {
          outcome = await handler(msg);
        } catch (err) {
          this.log.error('handler threw, requeueing', { err: String(err), stack: err?.stack });
          outcome = 'requeue';
        }

        try {
          if (outcome === 'ack') {
            channel.ack(msg);
          } else if (outcome === 'dlq') {
            channel.nack(msg, false, false);
          } else {
            channel.nack(msg, false, true);
          }
        } catch (err) {
          // The channel died mid-ack. The message will be redelivered; that is
          // at-least-once behaviour and is safe by design.
          this.log.warn('ack/nack failed', { err: String(err) });
        }
      },
      { noAck: false },
    );

    this.log.info('consuming', { queue: this.cfg.amqpQueue });
  }

  /**
   * Publish the plain audit copy and wait for the broker confirm.
   *
   * The WAL is deliberately uncompressed and deliberately separate from the
   * ingest message, so a Dragonfly loss can be recovered without depending on
   * the application's codec (payload-and-compression.md §3.1).
   */
  async publishAudit(buffer) {
    const channel = this.channel || (await this.connect());
    return new Promise((resolve, reject) => {
      channel.publish(
        this.cfg.amqpAuditExchange,
        this.cfg.amqpAuditKey,
        buffer,
        {
          persistent: true,
          contentType: 'application/json',
          deliveryMode: 2,
          messageId: undefined,
        },
        (err) => (err ? reject(err) : resolve()),
      );
    });
  }

  /** True when the channel is usable. Used by the readiness probe. */
  isReady() {
    return Boolean(this.conn && this.channel);
  }

  async close() {
    this.closing = true;
    try {
      if (this.channel) await this.channel.close();
    } catch { /* ignore */ }
    try {
      if (this.conn) await this.conn.close();
    } catch { /* ignore */ }
  }
}
