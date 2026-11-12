/**
 * Unit tests for the pure planner. No cluster needed.
 *   node --test services/sentinel-bridge/src/
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { computePlan, resolvePodName } from './plan.js';

const cfg = { minReplicas: 2 };

function pod(name, extra = {}) {
  return {
    name,
    ready: true,
    role: 'slave',
    connectedSlaves: 0,
    replicasOf: { host: 'valkey-0.valkey-headless.fire-watch.svc.cluster.local', port: 6379 },
    ...extra,
  };
}

test('resolvePodName maps DNS name to pod', () => {
  const names = ['valkey-0', 'valkey-1'];
  assert.equal(resolvePodName('valkey-1.valkey-headless.ns.svc', names), 'valkey-1');
  assert.equal(resolvePodName('10.0.0.5', names), null);
  assert.equal(resolvePodName(null, names), null);
});

test('steady state produces no actions', () => {
  const sentinel = {
    master: { host: 'valkey-0.valkey-headless.ns.svc', port: 6379 },
    masterFlags: ['master'],
  };
  const pods = [
    pod('valkey-0', { role: 'master', connectedSlaves: 2, replicasOf: null }),
    pod('valkey-1'),
    pod('valkey-2'),
  ];
  const plan = computePlan({ sentinel, pods, cfg });
  assert.deepEqual(plan.actions, []);
  assert.deepEqual(plan.endpoints, ['valkey-0']);
});

test('no sentinel answer leaves everything untouched', () => {
  const sentinel = { master: null, masterFlags: [] };
  const pods = [pod('valkey-0', { role: 'master', connectedSlaves: 2, replicasOf: null })];
  const plan = computePlan({ sentinel, pods, cfg });
  assert.deepEqual(plan.actions, []);
  assert.equal(plan.endpoints, null, 'must not touch endpoints without a sentinel answer');
});

test('promoted pod still reporting slave keeps the endpoint withheld', () => {
  const sentinel = {
    master: { host: 'valkey-2.valkey-headless.ns.svc', port: 6379 },
    masterFlags: ['master'],
  };
  const pods = [
    pod('valkey-0', { role: 'slave' }),
    pod('valkey-1', { role: 'slave' }),
    pod('valkey-2', { role: 'slave', connectedSlaves: 0 }),
  ];
  const plan = computePlan({ sentinel, pods, cfg });
  assert.deepEqual(plan.endpoints, [], 'no writes until the promotion is visible');
});

test('write guard withholds the endpoint when replicas are short', () => {
  const sentinel = {
    master: { host: 'valkey-0.valkey-headless.ns.svc', port: 6379 },
    masterFlags: ['master'],
  };
  const pods = [
    pod('valkey-0', { role: 'master', connectedSlaves: 1, replicasOf: null }),
    pod('valkey-1', { role: 'unknown' }),
    pod('valkey-2', { role: 'unknown' }),
  ];
  const plan = computePlan({ sentinel, pods, cfg });
  assert.deepEqual(plan.endpoints, []);
  assert.ok(plan.notes.some((n) => n.includes('write guard')));
});

test('split brain: a stray master is demoted to the sentinel-designated master', () => {
  const sentinel = {
    master: { host: 'valkey-1.valkey-headless.ns.svc', port: 6379 },
    masterFlags: ['master'],
  };
  const pods = [
    pod('valkey-0', { role: 'master', connectedSlaves: 2, replicasOf: null }),
    pod('valkey-1', { role: 'master', connectedSlaves: 2, replicasOf: null }),
    pod('valkey-2', { role: 'slave' }),
  ];
  const plan = computePlan({ sentinel, pods, cfg });
  const demote = plan.actions.find((a) => a.pod === 'valkey-0');
  assert.ok(demote, 'stray master must be demoted');
  assert.equal(demote.type, 'replicaof');
  assert.equal(demote.master.host, sentinel.master.host);
});

test('replica following the wrong master is corrected', () => {
  const sentinel = {
    master: { host: 'valkey-1.valkey-headless.ns.svc', port: 6379 },
    masterFlags: ['master'],
  };
  const pods = [
    pod('valkey-0', { role: 'slave', replicasOf: { host: 'valkey-1.x', port: 6379 } }),
    pod('valkey-1', { role: 'master', connectedSlaves: 1, replicasOf: null }),
    pod('valkey-2', { role: 'slave', replicasOf: { host: 'valkey-1.x', port: 6379 } }),
  ];
  const plan = computePlan({ sentinel, pods, cfg });
  assert.deepEqual(plan.actions, [], 'all replicas already follow the designated master');
});

test('designated master that is a replica is promoted with REPLICAOF NO ONE', () => {
  const sentinel = {
    master: { host: 'valkey-2.valkey-headless.ns.svc', port: 6379 },
    masterFlags: ['master'],
  };
  const pods = [
    pod('valkey-0', { role: 'slave' }),
    pod('valkey-1', { role: 'slave' }),
    pod('valkey-2', { role: 'slave', replicasOf: { host: 'valkey-0.x', port: 6379 } }),
  ];
  const plan = computePlan({ sentinel, pods, cfg });
  const act = plan.actions.find((a) => a.pod === 'valkey-2');
  assert.ok(act);
  assert.equal(act.type, 'replicaof-no-one');
});

test('minReplicas 0 disables the write guard (lab mode)', () => {
  const sentinel = {
    master: { host: 'valkey-0.valkey-headless.ns.svc', port: 6379 },
    masterFlags: ['master'],
  };
  const pods = [pod('valkey-0', { role: 'master', connectedSlaves: 0, replicasOf: null })];
  const plan = computePlan({ sentinel, pods, cfg: { minReplicas: 0 } });
  assert.deepEqual(plan.endpoints, ['valkey-0']);
});

test('unready master is not published', () => {
  const sentinel = {
    master: { host: 'valkey-0.valkey-headless.ns.svc', port: 6379 },
    masterFlags: ['master'],
  };
  const pods = [
    pod('valkey-0', { role: 'master', connectedSlaves: 2, replicasOf: null, ready: false }),
    pod('valkey-1'),
    pod('valkey-2'),
  ];
  const plan = computePlan({ sentinel, pods, cfg });
  assert.deepEqual(plan.endpoints, []);
});

test('a master address we cannot map to a pod leaves the endpoint untouched', () => {
  const sentinel = { master: { host: '10.9.9.9', port: 6379 }, masterFlags: ['master'] };
  const pods = [pod('valkey-0', { role: 'master', connectedSlaves: 2, replicasOf: null })];
  const plan = computePlan({ sentinel, pods, cfg });
  // An IP we cannot map to a pod means we do not know which pod Sentinel means.
  // Guessing would risk publishing a replica, so do nothing this cycle.
  assert.equal(plan.endpoints, null);
  assert.ok(plan.notes.some((n) => n.includes('no sentinel master address')));
});

// ---------------------------------------------------------------------------
// Regression: ROLE on a replica reports its master by IP, not by DNS name.
//
// Before this was handled, a healthy cluster looked like "every replica follows
// the wrong master" and the bridge re-issued REPLICAOF on every 5s cycle,
// forever. Caught by running computePlan with realistic ROLE output.
// ---------------------------------------------------------------------------

test('steady state with replicas reporting their master as an IP issues no actions', () => {
  const sentinel = {
    master: { host: 'valkey-0.valkey-headless.fire-watch.svc', port: 6379 },
    masterFlags: ['master'],
  };
  const pods = [
    pod('valkey-0', { podIP: '10.1.0.10', role: 'master', connectedSlaves: 2, replicasOf: null }),
    pod('valkey-1', { podIP: '10.1.1.11', role: 'slave', replicasOf: { host: '10.1.0.10', port: 6379 } }),
    pod('valkey-2', { podIP: '10.1.2.12', role: 'slave', replicasOf: { host: '10.1.0.10', port: 6379 } }),
  ];
  const plan = computePlan({ sentinel, pods, cfg });
  assert.deepEqual(plan.actions, [], 'IP-based replica master must resolve to the master pod');
  assert.deepEqual(plan.endpoints, ['valkey-0']);
});

test('replica following a wrong master reported by IP is still corrected', () => {
  const sentinel = {
    master: { host: 'valkey-2.valkey-headless.fire-watch.svc', port: 6379 },
    masterFlags: ['master'],
  };
  const pods = [
    pod('valkey-0', { podIP: '10.1.0.10', role: 'slave', replicasOf: { host: '10.1.2.12', port: 6379 } }),
    pod('valkey-1', { podIP: '10.1.1.11', role: 'slave', replicasOf: { host: '10.1.2.12', port: 6379 } }),
    pod('valkey-2', { podIP: '10.1.2.12', role: 'master', connectedSlaves: 2, replicasOf: null }),
  ];
  const plan = computePlan({ sentinel, pods, cfg });
  assert.deepEqual(plan.actions, []);
  assert.deepEqual(plan.endpoints, ['valkey-2']);
});

test('resolvePodName accepts a pod IP when a name-by-IP map is supplied', () => {
  const names = ['valkey-0', 'valkey-1'];
  const byIp = new Map([['10.1.0.10', 'valkey-0']]);
  assert.equal(resolvePodName('10.1.0.10', names, byIp), 'valkey-0');
  assert.equal(resolvePodName('10.9.9.9', names, byIp), null);
});
