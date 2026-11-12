/**
 * Pure reconcile logic. No I/O — everything here is a function of its inputs,
 * which is what makes it testable without a cluster.
 *
 * Specified by docs/redis-sentinel.md §4 and docs/valkey.md §3.
 */

/**
 * Map whatever identity we were given to a pod name.
 *
 * Two different things arrive here and they do NOT look the same:
 *   - Sentinel reports the master as a DNS name  -> "valkey-0.valkey-headless.ns.svc"
 *   - a replica's ROLE reports its master by IP  -> "10.1.2.3"
 *
 * So we resolve against both the pod names and a pod-name-by-IP map. Getting
 * this wrong makes every replica look like it follows the wrong master, and the
 * bridge then re-issues REPLICAOF on every cycle forever.
 *
 * @param {string} host   a pod DNS name, an IP, or a bare pod name
 * @param {string[]} podNames
 * @param {Map<string,string>} [podNameByIp]
 */
export function resolvePodName(host, podNames, podNameByIp) {
  if (!host) return null;
  const h = String(host).trim();
  if (!h) return null;

  // 1. exact IP match (what ROLE on a replica gives us)
  if (podNameByIp && podNameByIp.has(h)) return podNameByIp.get(h);

  // 2. DNS name or bare pod name
  const first = h.split('.')[0];
  if (podNames.includes(first)) return first;

  // 3. bare pod name that is not first in a larger string (defensive)
  if (podNames.includes(h)) return h;

  return null;
}

function isDown(flags) {
  return flags.includes('s_down') || flags.includes('odown');
}

/**
 * Decide what should happen this cycle.
 *
 * Inputs:
 *   sentinel     { master: {host, port}|null, masterFlags: string[] }
 *   pods         [{ name, podIP, ready, role, connectedSlaves, replicasOf: {host,port}|null }]
 *   cfg          { minReplicas }
 *
 * Sentinel is the authority on WHO the master should be. Pods are the authority
 * on what they currently are. Any disagreement is resolved in Sentinel's favour
 * — that is the entire point of having a quorum decide.
 */
export function computePlan({ sentinel, pods, cfg }) {
  const podNames = pods.map((p) => p.name);
  // ROLE on a replica reports its master by IP, so we need name-by-IP to
  // compare it against a pod name. Without this every replica looks wrong.
  const podNameByIp = new Map(
    pods.filter((p) => p.podIP).map((p) => [p.podIP, p.name]),
  );
  const actions = [];
  const notes = [];

  const masterPodName = sentinel && sentinel.master
    ? resolvePodName(sentinel.master.host, podNames, podNameByIp)
    : null;

  // ---------------------------------------------------------------- no quorum
  if (!sentinel || !sentinel.master || !masterPodName) {
    // We cannot determine a master. Do NOT touch roles and do NOT change the
    // endpoint: Sentinel may be mid-failover or unreachable, and guessing here
    // is how two writers appear.
    return {
      actions: [],
      endpoints: null,          // null = leave the existing Endpoints alone
      notes: ['no sentinel master address; leaving state untouched'],
      masterPodName: null,
    };
  }

  const master = pods.find((p) => p.name === masterPodName);
  const sentinelInFailover = isDown(sentinel.masterFlags || []);

  // ------------------------------------------------------- endpoint decision
  //
  // Publish the master endpoint only when all of these hold:
  //   - the master pod exists and is Ready
  //   - the master reports role:master  (if it still says slave, Sentinel has
  //     not finished promoting it and no client should write yet)
  //   - the write guard allows it (enough connected replicas)
  let endpoints;
  const guardOk = (m) => cfg.minReplicas === 0 || (m.connectedSlaves ?? 0) >= cfg.minReplicas;

  if (!master) {
    endpoints = [];
    notes.push(`master pod ${masterPodName} not found; clearing endpoints`);
  } else if (!master.ready) {
    endpoints = [];
    notes.push(`master pod ${masterPodName} not Ready; clearing endpoints`);
  } else if (master.role !== 'master') {
    endpoints = [];
    notes.push(
      `sentinel says ${masterPodName} is master but it reports role=${master.role};` +
        ' waiting for promotion before publishing the endpoint',
    );
  } else if (!guardOk(master)) {
    endpoints = [];
    notes.push(
      `write guard: ${masterPodName} has ${master.connectedSlaves ?? 0} replicas,` +
        ` need ${cfg.minReplicas}; withholding endpoint so workers stop writing`,
    );
  } else {
    endpoints = [masterPodName];
  }

  // ------------------------------------------------------------ role actions
  for (const pod of pods) {
    if (pod.name === masterPodName) {
      // A stray replicaof on the master would make it a replica of itself or of
      // a demoted pod. Only repair the case where it claims to follow something.
      if (pod.role === 'slave' || pod.replicasOf) {
        if (!sentinelInFailover) {
          actions.push({
            type: 'replicaof-no-one',
            pod: pod.name,
            reason: 'sentinel designates this pod as master but it is a replica',
          });
        } else {
          notes.push(`${pod.name} is a replica of an unreachable master; Sentinel is handling it`);
        }
      }
      continue;
    }

    // Not the designated master.
    if (pod.role === 'master') {
      actions.push({
        type: 'replicaof',
        pod: pod.name,
        master: sentinel.master,
        reason: 'pod is a master but Sentinel designates another pod — demoting (split-brain repair)',
      });
      continue;
    }

    // A replica. Fix it only if it follows the wrong master.
    if (pod.role === 'slave') {
      const follows = pod.replicasOf
        ? resolvePodName(pod.replicasOf.host, podNames, podNameByIp)
        : null;
      if (follows !== masterPodName) {
        actions.push({
          type: 'replicaof',
          pod: pod.name,
          master: sentinel.master,
          reason: follows
            ? `replicating from ${follows} instead of ${masterPodName}`
            : `replicating from an unknown host (${pod.replicasOf?.host ?? 'none'})` +
              ` instead of ${masterPodName}`,
        });
      }
    }
  }

  return { actions, endpoints, notes, masterPodName };
}
