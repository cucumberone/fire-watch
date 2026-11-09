/**
 * Unit tests for the Kubernetes I/O layer. No cluster needed: the CoreV1Api is
 * stubbed, so these pin the CALLING CONVENTION rather than the API behaviour.
 *
 * Regression covered: @kubernetes/client-node 0.22.x takes POSITIONAL arguments
 * and resolves to an envelope `{ response, body }`. The code originally used the
 * v1.x object form and read `.items` off the envelope, so it silently saw zero
 * pods and the bridge logged "no dragonfly pods found" on a healthy cluster while
 * holding the leader lease. Nothing threw; the wrong answer simply looked like an
 * empty cluster. These tests fail if either half of that returns.
 *
 *   node --test services/sentinel-bridge/src/
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { listPods, patchEndpoints, setWritableLabel } from './kube.js';

const silentLog = {
  info() {},
  warn() {},
  debug() {},
  error() {},
};

function pod(name, ip = '10.0.0.1') {
  return {
    metadata: { name, labels: { 'app.kubernetes.io/name': 'dragonfly' } },
    spec: { nodeName: 'node1' },
    status: {
      podIP: ip,
      conditions: [{ type: 'Ready', status: 'True' }],
    },
  };
}

test('listPods passes the selector positionally, not as an object', async () => {
  const seen = [];
  const core = {
    async listNamespacedPod(...args) {
      seen.push(args);
      // The real client resolves to this envelope shape.
      return { response: { statusCode: 200 }, body: { items: [pod('dragonfly-0')] } };
    },
  };

  const items = await listPods(core, 'fire-watch-lab', 'app.kubernetes.io/name=dragonfly');

  assert.equal(seen.length, 1);
  const args = seen[0];
  // If this ever becomes a single object again, the object-form regression is back.
  assert.equal(typeof args[0], 'string', 'namespace must be positional arg 0');
  assert.equal(args[0], 'fire-watch-lab');
  // labelSelector is the 6th positional parameter (index 5).
  assert.equal(args[5], 'app.kubernetes.io/name=dragonfly');

  // And the envelope must have been unwrapped.
  assert.equal(items.length, 1);
  assert.equal(items[0].metadata.name, 'dragonfly-0');
});

test('listPods returns ordinal order', async () => {
  const core = {
    async listNamespacedPod() {
      return {
        response: { statusCode: 200 },
        body: {
          items: [pod('dragonfly-2'), pod('dragonfly-0'), pod('dragonfly-1')],
        },
      };
    },
  };
  const items = await listPods(core, 'ns', 'sel');
  assert.deepEqual(items.map((p) => p.metadata.name), [
    'dragonfly-0',
    'dragonfly-1',
    'dragonfly-2',
  ]);
});

test('listPods tolerates a bare body with no items', async () => {
  // Belt and braces: a future client that resolves straight to the resource must
  // not produce a crash on an empty namespace.
  const core = { async listNamespacedPod() { return { body: {} }; } };
  assert.deepEqual(await listPods(core, 'ns', 'sel'), []);
});

test('patchEndpoints uses the positional replace signature', async () => {
  const calls = [];
  const core = {
    async replaceNamespacedEndpoints(...args) {
      calls.push(args);
      return { response: { statusCode: 200 }, body: {} };
    },
  };

  const pods = new Map([['dragonfly-0', pod('dragonfly-0', '10.0.0.7')]]);
  await patchEndpoints(core, 'fire-watch-lab', 'dragonfly', ['dragonfly-0'], pods, { dragonflyPort: 6379 }, silentLog);

  assert.equal(calls.length, 1);
  const [name, namespace, body] = calls[0];
  assert.equal(name, 'dragonfly');
  assert.equal(namespace, 'fire-watch-lab');
  assert.equal(body.subsets[0].addresses[0].ip, '10.0.0.7');
  assert.equal(body.subsets[0].ports[0].port, 6379);
});

test('an empty endpoint list is published as no subsets (write guard)', async () => {
  const calls = [];
  const core = {
    async replaceNamespacedEndpoints(...args) {
      calls.push(args);
      return { response: { statusCode: 200 }, body: {} };
    },
  };
  await patchEndpoints(core, 'ns', 'dragonfly', [], new Map(), { dragonflyPort: 6379 }, silentLog);
  assert.deepEqual(calls[0][2].subsets, []);
});

test('endpoints === null leaves the Service untouched', async () => {
  let called = false;
  const core = {
    async replaceNamespacedEndpoints() { called = true; },
  };
  await patchEndpoints(core, 'ns', 'dragonfly', null, new Map(), { dragonflyPort: 6379 }, silentLog);
  assert.equal(called, false, 'a null plan must not rewrite Endpoints');
});

test('a 404 on replace creates the Endpoints positionally', async () => {
  const created = [];
  const notFound = new Error('not found');
  notFound.response = { statusCode: 404 };
  const core = {
    async replaceNamespacedEndpoints() { throw notFound; },
    async createNamespacedEndpoints(...args) {
      created.push(args);
      return { response: { statusCode: 201 }, body: {} };
    },
  };

  const pods = new Map([['dragonfly-0', pod('dragonfly-0', '10.0.0.9')]]);
  await patchEndpoints(core, 'fire-watch-lab', 'dragonfly', ['dragonfly-0'], pods, { dragonflyPort: 6379 }, silentLog);

  assert.equal(created.length, 1);
  assert.equal(created[0][0], 'fire-watch-lab', 'namespace is positional arg 0');
  assert.equal(created[0][1].kind, 'Endpoints');
});

test('setWritableLabel uses the positional patch signature', async () => {
  const calls = [];
  const core = {
    async patchNamespacedPod(...args) {
      calls.push(args);
      return { response: { statusCode: 200 }, body: {} };
    },
  };

  await setWritableLabel(core, 'fire-watch-lab', 'dragonfly-0', true, 'fire-watch.io/writable', silentLog);

  assert.equal(calls.length, 1);
  const [name, namespace, body] = calls[0];
  assert.equal(name, 'dragonfly-0');
  assert.equal(namespace, 'fire-watch-lab');
  assert.equal(body.metadata.labels['fire-watch.io/writable'], 'true');
});
