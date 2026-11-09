/**
 * Unit tests for the leader elector. No cluster needed: the Kubernetes client is
 * injected as a stub, so these pin the WIRE FORMAT rather than the API calls.
 *
 * Regression covered: Lease spec.acquireTime / spec.renewTime are MicroTime and
 * must carry exactly six fractional digits. Date#toISOString() emits three, and
 * the API server answered every lease write with
 *
 *   400 Lease in version "v1" cannot be handled as a Lease:
 *       parsing time "...58.666Z" as "2006-01-02T15:04:05.000000Z07:00":
 *       cannot parse ".666Z" as ".000000"
 *
 * which meant leadership was never acquired. The tests below fail if anyone
 * reintroduces a plain toISOString() into a lease body.
 *
 *   node --test services/sentinel-bridge/src/
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { LeaderElector } from './leader.js';

/** Matches YYYY-MM-DDTHH:MM:SS.ffffffZ — MicroTime, not milliseconds. */
const MICRO_TIME = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/;

const silentLog = {
  info() {},
  warn() {},
  debug() {},
  error() {},
};

/**
 * Stub CoordinationV1Api. `lease` is the object returned by reads; `fail`, when
 * given, is thrown by the mutating call so error-handling paths can be tested.
 *
 * Reads return the ENVELOPE shape the real 0.22.x client produces —
 * `{ response, body }` — because returning the lease bare made this stub hide a
 * real defect: the elector consumed the envelope, saw no `spec`, and replaced
 * the Lease with a body that had no metadata.
 */
function makeElector({ lease = null, fail = null, reads = [] } = {}) {
  const calls = { create: [], replace: [], read: [] };
  const api = {
    async readNamespacedLease(name, namespace) {
      calls.read.push({ name, namespace });
      if (!reads.length) {
        if (!lease) {
          const err = new Error('not found');
          err.response = { statusCode: 404 };
          throw err;
        }
        return { response: { statusCode: 200 }, body: structuredClone(lease) };
      }
      const next = reads.shift();
      if (next === undefined) {
        const err = new Error('not found');
        err.response = { statusCode: 404 };
        throw err;
      }
      return { response: { statusCode: 200 }, body: structuredClone(next) };
    },
    async createNamespacedLease(namespace, body) {
      calls.create.push({ namespace, body });
      if (fail) throw fail;
      return body;
    },
    async replaceNamespacedLease(name, namespace, body) {
      calls.replace.push({ name, namespace, body });
      if (fail) throw fail;
      return body;
    },
  };
  const kc = { makeApiClient: () => api };
  const elector = new LeaderElector({
    kc,
    namespace: 'fire-watch-lab',
    leaseName: 'sentinel-bridge-leader',
    identity: 'sentinel-bridge-abc',
    leaseDurationMs: 15000,
    log: silentLog,
  });
  return { elector, calls };
}

const now = Date.now();
const fresh = new Date(now).toISOString();
const expired = new Date(now - 60000).toISOString();

test('acquire sends MicroTime, not millisecond RFC3339', async () => {
  const { elector, calls } = makeElector({ lease: null });

  assert.equal(await elector.tryAcquireOrRenew(), true);

  assert.equal(calls.create.length, 1);
  const spec = calls.create[0].body.spec;
  // The exact bug: '.666Z' is rejected, '.666000Z' is accepted.
  assert.match(spec.acquireTime, MICRO_TIME);
  assert.match(spec.renewTime, MICRO_TIME);
  assert.equal(spec.holderIdentity, 'sentinel-bridge-abc');
  assert.equal(spec.leaseDurationSeconds, 15);
});

test('renew keeps the renewal stamp in MicroTime', async () => {
  const lease = {
    spec: {
      holderIdentity: 'sentinel-bridge-abc',
      leaseDurationSeconds: 15,
      renewTime: fresh,
    },
  };
  const { elector, calls } = makeElector({ lease });

  assert.equal(await elector.tryAcquireOrRenew(), true);
  assert.equal(calls.replace.length, 1);
  assert.match(calls.replace[0].body.spec.renewTime, MICRO_TIME);
  // Identity must be preserved, never reassigned on a plain renewal.
  assert.equal(calls.replace[0].body.spec.holderIdentity, 'sentinel-bridge-abc');
});

test('takeover of an expired lease also uses MicroTime', async () => {
  const lease = {
    spec: {
      holderIdentity: 'sentinel-bridge-old',
      leaseDurationSeconds: 15,
      renewTime: expired,
      acquireTime: expired,
      leaseTransitions: 2,
    },
  };
  const { elector, calls } = makeElector({ lease });

  assert.equal(await elector.tryAcquireOrRenew(), true);
  const spec = calls.replace[0].body.spec;
  assert.match(spec.renewTime, MICRO_TIME);
  assert.match(spec.acquireTime, MICRO_TIME);
  assert.equal(spec.holderIdentity, 'sentinel-bridge-abc');
  assert.equal(spec.leaseTransitions, 3);
});

test('a held, unexpired lease leaves us standby', async () => {
  const lease = {
    spec: {
      holderIdentity: 'sentinel-bridge-other',
      leaseDurationSeconds: 15,
      renewTime: fresh,
    },
  };
  const { elector, calls } = makeElector({ lease });

  assert.equal(await elector.tryAcquireOrRenew(), false);
  assert.equal(calls.create.length, 0);
  assert.equal(calls.replace.length, 0);
  assert.equal(elector.isLeader, false);
});

test('a 409 on create is a lost race, not an error', async () => {
  const conflict = new Error('conflict');
  conflict.response = { statusCode: 409 };
  const { elector } = makeElector({ lease: null, fail: conflict });

  assert.equal(await elector.tryAcquireOrRenew(), false);
  assert.equal(elector.isLeader, false);
});

test('a 400 on create is rethrown so it cannot hide', async () => {
  // The timestamp defect surfaced as a 400 logged at debug level. Genuine faults
  // must reach the caller.
  const bad = new Error('BadRequest');
  bad.response = { statusCode: 400 };
  bad.body = 'Lease in version "v1" cannot be handled as a Lease';
  const { elector } = makeElector({ lease: null, fail: bad });

  await assert.rejects(() => elector.tryAcquireOrRenew(), /BadRequest/);
  assert.equal(elector.isLeader, false);
});

test('a 400 on takeover is rethrown as well', async () => {
  const bad = new Error('BadRequest');
  bad.response = { statusCode: 400 };
  const lease = {
    spec: {
      holderIdentity: 'sentinel-bridge-old',
      leaseDurationSeconds: 15,
      renewTime: expired,
    },
  };
  const { elector } = makeElector({ lease, fail: bad });

  await assert.rejects(() => elector.tryAcquireOrRenew(), /BadRequest/);
  assert.equal(elector.isLeader, false);
});

test('release backdates renewTime so standbys take over at once', async () => {
  const lease = {
    spec: {
      holderIdentity: 'sentinel-bridge-abc',
      leaseDurationSeconds: 15,
      renewTime: fresh,
    },
  };
  const { elector, calls } = makeElector({ lease, reads: [lease, lease] });

  await elector.tryAcquireOrRenew();          // becomes leader
  assert.equal(elector.isLeader, true);

  await elector.release();                    // backdates and drops leadership
  assert.equal(elector.isLeader, false);

  const replaced = calls.replace.at(-1).body.spec.renewTime;
  assert.match(replaced, MICRO_TIME);
  assert.ok(new Date(replaced).getTime() < now - 15000);
});

test('release is a no-op for a standby', async () => {
  const { elector, calls } = makeElector({ lease: null });
  await elector.release();
  assert.equal(calls.replace.length, 0);
  assert.equal(elector.isLeader, false);
});
