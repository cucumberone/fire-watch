/**
 * End-to-end test of the confirm path against a REAL RabbitMQ.
 *
 * Skipped automatically when no broker is reachable, so it is safe in CI
 * without one, and it is the only test here that exercises the actual
 * publish-confirm behaviour rather than our own logic.
 *
 * Run against a broker:
 *   AMQP_HOST=localhost AMQP_USER=guest AMQP_PASS=guest \
 *     node --test src/confirm.e2e.test.js
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import amqp from 'amqplib';
import { ConfirmPool } from './confirm.js';
import { messageProperties } from './normalise.js';

const cfg = {
  amqpHost: process.env.AMQP_HOST || 'localhost',
  amqpPort: Number(process.env.AMQP_PORT || 5672),
  amqpUser: process.env.AMQP_USER || 'guest',
  amqpPass: process.env.AMQP_PASS || 'guest',
  exchange: 'fw-e2e-test-exchange',
  routingKey: 'e2e',
  channels: 2,
};

const log = { debug() {}, info() {}, warn() {}, error() {} };

async function brokerAvailable() {
  const url = `amqp://${cfg.amqpUser}:${cfg.amqpPass}@${cfg.amqpHost}:${cfg.amqpPort}`;
  try {
    const conn = await amqp.connect(url);
    await conn.close();
    return true;
  } catch {
    return false;
  }
}

const available = await brokerAvailable();
const skip = available ? false : 'no RabbitMQ reachable; skipping e2e';

test('publishBatch returns only after the broker confirms', { skip }, async () => {
  const conn = await amqp.connect(`amqp://${cfg.amqpUser}:${cfg.amqpPass}@${cfg.amqpHost}:${cfg.amqpPort}`);
  const ch = await conn.createChannel();
  await ch.assertExchange(cfg.exchange, 'direct', { durable: true });
  const q = await ch.assertQueue('', { exclusive: true });
  await ch.bindQueue(q.queue, cfg.exchange, cfg.routingKey);

  const pool = new ConfirmPool(cfg, log);
  try {
    const events = [
      { id: 'a', source: 's', type: 't', severity: 'CRITICAL', ts: 'now', payload: { msg: 'one' } },
      { id: 'b', source: 's', type: 't', severity: 'CLOSE', ts: 'now', payload: { msg: 'two' } },
    ];
    const confirmed = await pool.publishBatch(events, (e) => messageProperties(e), 5000);
    assert.equal(confirmed, 2);

    // If the confirm resolved, the messages must already be queueable, so a
    // get with a short timeout should return both.
    const seen = [];
    for (let i = 0; i < 2; i += 1) {
      const msg = await ch.get(q.queue, { noAck: true });
      if (msg) seen.push(JSON.parse(msg.content.toString('utf8')).id);
    }
    assert.deepEqual(seen.sort(), ['a', 'b']);
  } finally {
    await pool.close();
    await ch.close();
    await conn.close();
  }
});

test('a confirm timeout rejects rather than silently succeeding', { skip }, async () => {
  const pool = new ConfirmPool(cfg, log);
  try {
    // 1ms is shorter than any real round trip, so this must reject.
    await assert.rejects(
      () => pool.publishBatch(
        [{ id: 'x', source: 's', type: 't', severity: 'WARNING', ts: 'now', payload: {} }],
        (e) => messageProperties(e),
        1,
      ),
      /timed out/,
    );
  } finally {
    await pool.close();
  }
});
