/**
 * Tests for HTTP → event normalisation. No broker.
 *   node --test src/
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import {
  extractEvents,
  normaliseEvent,
  normaliseBatch,
  messageProperties,
  BadRequest,
} from './normalise.js';

const opts = { maxBatch: 10, now: 1762699200000, randomId: () => 'gen-id' };

// ------------------------------------------------------------------- shapes
test('a single event object becomes a one-element batch', () => {
  assert.equal(extractEvents({ id: 'a' }).length, 1);
});

test('a bare array is taken as the batch', () => {
  assert.equal(extractEvents([{ id: 'a' }, { id: 'b' }]).length, 2);
});

test('a wrapped events array is unwrapped', () => {
  assert.equal(extractEvents({ events: [{ id: 'a' }] }).length, 1);
});

test('a non-object body is rejected', () => {
  assert.throws(() => extractEvents('string'), BadRequest);
  assert.throws(() => extractEvents(null), BadRequest);
  assert.throws(() => extractEvents(42), BadRequest);
});

// --------------------------------------------------------------- normalising
test('a complete event passes through unchanged', () => {
  const raw = {
    id: 'abc',
    source: 'sensor-a',
    type: 'temp',
    severity: 'CRITICAL',
    ts: '2026-11-09T00:00:00.000Z',
    payload: { msg: 'hot', msgc: 'detail' },
  };
  assert.deepEqual(normaliseEvent(raw, opts), raw);
});

test('a missing id is generated rather than rejected', () => {
  const e = normaliseEvent({ source: 'a' }, opts);
  assert.equal(e.id, 'gen-id');
});

test('missing source and type get defaults', () => {
  const e = normaliseEvent({ id: 'x' }, opts);
  assert.equal(e.source, 'http-ingress');
  assert.equal(e.type, 'http.event');
});

test('a missing severity defaults to WARNING, not CRITICAL', () => {
  // Defaulting to a high severity would make every unknown shipper look like an
  // outage; WARNING is visible without being an alarm.
  assert.equal(normaliseEvent({ id: 'x' }, opts).severity, 'WARNING');
});

test('a missing ts is filled with now, not left empty', () => {
  const e = normaliseEvent({ id: 'x' }, opts);
  assert.equal(e.ts, new Date(opts.now).toISOString());
});

test('bare msg/msgc fields are folded into payload', () => {
  const e = normaliseEvent({ id: 'x', msg: 'short', msgc: 'long detail' }, opts);
  assert.deepEqual(e.payload, { msg: 'short', msgc: 'long detail' });
});

test('an explicit payload wins over bare msg fields', () => {
  const e = normaliseEvent({ id: 'x', msg: 'ignored', payload: { a: 1 } }, opts);
  assert.deepEqual(e.payload, { a: 1 });
});

test('no payload yields an empty object, never undefined', () => {
  assert.deepEqual(normaliseEvent({ id: 'x' }, opts).payload, {});
});

test('an invalid severity is rejected with the allowed list', () => {
  assert.throws(() => normaliseEvent({ id: 'x', severity: 'ERROR' }, opts), /severity must be one of/);
});

test('lowercase severity is rejected: the contract is exact', () => {
  assert.throws(() => normaliseEvent({ id: 'x', severity: 'critical' }, opts), BadRequest);
});

test('an array payload is rejected', () => {
  assert.throws(() => normaliseEvent({ id: 'x', payload: [1, 2] }, opts), BadRequest);
});

test('an oversized id is rejected', () => {
  assert.throws(() => normaliseEvent({ id: 'z'.repeat(200) }, opts), /id must be/);
});

test('an oversized source is rejected', () => {
  assert.throws(() => normaliseEvent({ source: 's'.repeat(300) }, opts), /source must be/);
});

test('a nested event object is rejected', () => {
  assert.throws(() => normaliseEvent([{ id: 'x' }], opts), BadRequest);
});

// -------------------------------------------------------------------- batches
test('an empty batch is rejected', () => {
  assert.throws(() => normaliseBatch([], opts), /no events/);
});

test('an oversized batch is rejected as 413', () => {
  try {
    normaliseBatch(Array.from({ length: 11 }, (_, i) => ({ id: `e${i}` })), opts);
    assert.fail('should have thrown');
  } catch (err) {
    assert.equal(err.status, 413);
  }
});

test('the failing index is named in the error, so a producer can fix it', () => {
  assert.throws(
    () => normaliseBatch([{ id: 'ok' }, { id: 'bad', severity: 'NOPE' }], opts),
    /event\[1\]/,
  );
});

test('a valid batch is all-or-nothing', () => {
  const out = normaliseBatch([{ id: 'a' }, { id: 'b' }], opts);
  assert.equal(out.length, 2);
  assert.deepEqual(out.map((e) => e.id), ['a', 'b']);
});

// ------------------------------------------------------------------ properties
test('message properties mark the message persistent', () => {
  // deliveryMode 2 is what makes a durable quorum queue keep it across restart.
  const p = messageProperties({ id: 'x', type: 't', source: 's', severity: 'CRITICAL' }, 'corr-1');
  assert.equal(p.deliveryMode, 2);
  assert.equal(p.persistent, true);
  assert.equal(p.contentType, 'application/json');
  assert.equal(p.messageId, 'x');
  assert.equal(p.correlationId, 'corr-1');
  assert.equal(p.appId, 'http-ingress');
});

test('source and severity are exposed as headers for routing and debugging', () => {
  const p = messageProperties({ id: 'x', type: 't', source: 'sensor-a', severity: 'FATAL' });
  assert.equal(p.headers['x-event-source'], 'sensor-a');
  assert.equal(p.headers['x-event-severity'], 'FATAL');
  assert.equal(p.headers['x-ingress-source'], 'http');
});
