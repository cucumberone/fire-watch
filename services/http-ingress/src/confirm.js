/**
 * Channel pool with publisher confirms.
 *
 * Why a pool: on an AMQP channel, `waitForConfirms` acknowledges the whole
 * channel, so concurrent requests on one channel serialise. A small pool lets
 * concurrent HTTP requests overlap without unbounded channel growth.
 *
 * Why confirms at all: returning 202 to the external nginx claims the event is
 * accepted. Without a confirm that claim is false — the connection can die after
 * publish and before the broker persists. Same rule producers follow
 * (docs/rabbitmq.md §3).
 */
import amqp from 'amqplib';

/** Resolve when the channel's write buffer drains, or reject on close. */
function waitForDrain(channel) {
  return new Promise((resolve, reject) => {
    const onDrain = () => { channel.off('close', onClose); resolve(); };
    const onClose = () => { channel.off('drain', onDrain); reject(new Error('channel closed while waiting for drain')); };
    channel.once('drain', onDrain);
    channel.once('close', onClose);
  });
}

/** Race a promise against a timer. amqplib's waitForConfirms has no timeout. */
function withTimeout(promise, ms, message) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(message)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

class ChannelSlot {
  constructor(channel, log) {
    this.channel = channel;
    this.log = log;
    this.waiters = [];
    this.busy = false;
  }

  get available() {
    return !this.busy && this.channel !== null;
  }
}

export class ConfirmPool {
  constructor(cfg, log) {
    this.cfg = cfg;
    this.log = log;
    this.conn = null;
    this.slots = [];
    this.closing = false;
  }

  url() {
    const { amqpUser, amqpPass, amqpHost, amqpPort } = this.cfg;
    return `amqp://${encodeURIComponent(amqpUser)}:${encodeURIComponent(amqpPass)}@${amqpHost}:${amqpPort}`;
  }

  async connect() {
    if (this.conn) return;
    const conn = await amqp.connect(this.url(), { heartbeat: 30 });
    conn.on('error', (err) => this.log.error('amqp connection error', { err: String(err) }));
    conn.on('close', () => {
      this.log.warn('amqp connection closed', { closing: this.closing });
      this.conn = null;
      this.slots = [];
    });
    this.conn = conn;

    for (let i = 0; i < this.cfg.channels; i += 1) {
      const ch = await conn.createConfirmChannel();
      ch.on('error', (err) => this.log.error('channel error', { index: i, err: String(err) }));
      ch.on('close', () => this.log.warn('channel closed', { index: i }));
      // Assert on every channel so a channel that opens before the definitions
      // are loaded still has a target. Idempotent: the topology already exists.
      await ch.assertExchange(this.cfg.exchange, 'direct', { durable: true });
      this.slots.push(new ChannelSlot(ch, this.log));
    }
    this.log.info('confirm pool ready', { channels: this.slots.length, exchange: this.cfg.exchange });
  }

  /** Wait for a free slot. Returns null after the timeout. */
  async acquire(timeoutMs = 5000) {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const slot = this.slots.find((s) => s.available);
      if (slot) {
        slot.busy = true;
        return slot;
      }
      if (Date.now() > deadline) return null;
      await new Promise((r) => setTimeout(r, 5));
    }
  }

  release(slot) {
    slot.busy = false;
  }

  /**
   * Publish a batch and wait for the broker confirm.
   *
   * Returns the number of messages confirmed. Throws on timeout, which the HTTP
   * layer turns into a 503 — never a 202, because nothing was durably accepted.
   *
   * Two amqplib details worth knowing:
   *   - `publish` returns false when the channel's write buffer is full. The
   *     correct response is to wait for the 'drain' event; ignoring it buffers
   *     unboundedly in this process and turns a broker stall into an OOM.
   *   - `waitForConfirms` takes no timeout argument. Bounding it requires
   *     racing against a timer, or a stalled broker would hang the request
   *     until nginx gives up.
   */
  async publishBatch(events, buildProps, confirmTimeoutMs) {
    await this.connect();
    const slot = await this.acquire();
    if (!slot) {
      const err = new Error('all confirm channels busy');
      err.code = 'BUSY';
      throw err;
    }

    const ch = slot.channel;
    try {
      for (const evt of events) {
        const body = Buffer.from(JSON.stringify(evt), 'utf8');
        const props = buildProps(evt);
        const writable = ch.publish(this.cfg.exchange, this.cfg.routingKey, body, props);
        if (!writable) {
          await waitForDrain(ch);
        }
      }
      await withTimeout(ch.waitForConfirms(), confirmTimeoutMs, 'publish confirm timed out');
      return events.length;
    } finally {
      this.release(slot);
    }
  }

  isReady() {
    return Boolean(this.conn) && this.slots.some((s) => s.channel !== null);
  }

  async close() {
    this.closing = true;
    for (const s of this.slots) {
      try { await s.channel.close(); } catch { /* ignore */ }
    }
    this.slots = [];
    try { if (this.conn) await this.conn.close(); } catch { /* ignore */ }
    this.conn = null;
  }
}
