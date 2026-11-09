/**
 * Kubernetes I/O: read pods, read their roles, patch Endpoints and labels.
 *
 * All calls are namespaced and use the pod's own ServiceAccount. The Role in
 * 32-sentinel-bridge.yaml grants exactly what is used here.
 */
import k8s from '@kubernetes/client-node';
import Redis from 'ioredis';

const { CoreV1Api, AppsV1Api, CoordinationV1Api } = k8s;

export function createKube() {
  const kc = new k8s.KubeConfig();
  kc.loadFromCluster();

  return {
    core: kc.makeApiClient(CoreV1Api),
    apps: kc.makeApiClient(AppsV1Api),
    coordination: kc.makeApiClient(CoordinationV1Api),
    kc,
  };
}

/** List the Dragonfly pods, in ordinal order. */
export async function listPods(core, namespace, labelSelector) {
  const res = await core.listNamespacedPod({
    namespace,
    labelSelector,
  });
  return (res.items || []).sort((a, b) => a.metadata.name.localeCompare(b.metadata.name));
}

/**
 * Read one pod's replication state directly from Dragonfly.
 *
 * The bridge talks to the pod IP because it is a control-plane component; the
 * rule that applications must not use pod IPs (docs/redis-sentinel.md §5) is
 * about workers and gateway, not about this controller.
 */
export async function readRole(pod, cfg, log) {
  const ip = pod.status?.podIP;
  const base = {
    name: pod.metadata.name,
    podIP: ip || null,
    ready: (pod.status?.conditions || []).some(
      (c) => c.type === 'Ready' && c.status === 'True',
    ),
  };
  if (!ip) return { ...base, role: 'unknown', connectedSlaves: 0, replicasOf: null };

  const client = new Redis({
    host: ip,
    port: cfg.dragonflyPort,
    password: cfg.dragonflyPassword,
    lazyConnect: true,
    connectTimeout: 2000,
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false,
    retryStrategy: () => null,
  });
  client.on('error', () => { /* handled by the try/catch below */ });

  try {
    await client.connect();
    // ROLE replies:
    //   master -> [ "master", <offset>, [ [ip, port, offset], ... ] ]
    //   slave  -> [ "slave", ip, port, state, offset ]
    const arr = await client.send_command('ROLE', []);
    if (!Array.isArray(arr)) {
      return { ...base, role: 'unknown', connectedSlaves: 0, replicasOf: null };
    }

    if (arr[0] === 'master') {
      const replicas = Array.isArray(arr[2]) ? arr[2] : [];
      return { ...base, role: 'master', connectedSlaves: replicas.length, replicasOf: null };
    }
    if (arr[0] === 'slave') {
      return {
        ...base,
        role: 'slave',
        connectedSlaves: 0,
        replicasOf: { host: String(arr[1]), port: Number(arr[2]) },
      };
    }
    return { ...base, role: 'unknown', connectedSlaves: 0, replicasOf: null };
  } catch (err) {
    log.debug('could not read ROLE', { pod: base.name, ip, err: String(err) });
    return { ...base, role: 'unknown', connectedSlaves: 0, replicasOf: null };
  } finally {
    try { client.disconnect(); } catch { /* ignore */ }
  }
}

/** Send REPLICAOF to a pod. Returns true on success. */
export async function applyReplicaOf(podIps, podName, target, cfg, log) {
  const ip = podIps.get(podName);
  if (!ip) {
    log.warn('cannot apply REPLICAOF: no pod IP', { pod: podName });
    return false;
  }
  const client = new Redis({
    host: ip,
    port: cfg.dragonflyPort,
    password: cfg.dragonflyPassword,
    lazyConnect: true,
    connectTimeout: 2000,
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false,
    retryStrategy: () => null,
  });
  client.on('error', () => { /* handled below */ });
  try {
    await client.connect();
    await client.send_command('REPLICAOF', [target.host, String(target.port)]);
    log.info('applied REPLICAOF', { pod: podName, host: target.host, port: target.port });
    return true;
  } catch (err) {
    log.error('REPLICAOF failed', { pod: podName, err: String(err) });
    return false;
  } finally {
    try { client.disconnect(); } catch { /* ignore */ }
  }
}

/** Promote a pod to master with REPLICAOF NO ONE. */
export async function applyReplicaOfNoOne(podIps, podName, cfg, log) {
  const ip = podIps.get(podName);
  if (!ip) return false;
  const client = new Redis({
    host: ip,
    port: cfg.dragonflyPort,
    password: cfg.dragonflyPassword,
    lazyConnect: true,
    connectTimeout: 2000,
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false,
    retryStrategy: () => null,
  });
  client.on('error', () => { /* handled below */ });
  try {
    await client.connect();
    await client.send_command('REPLICAOF', ['NO', 'ONE']);
    log.warn('applied REPLICAOF NO ONE', { pod: podName });
    return true;
  } catch (err) {
    log.error('REPLICAOF NO ONE failed', { pod: podName, err: String(err) });
    return false;
  } finally {
    try { client.disconnect(); } catch { /* ignore */ }
  }
}

/**
 * Replace the Endpoints subsets for the master-pointing Service.
 * `endpoints === null` means "leave untouched".
 * Must be called only by the leader.
 */
export async function patchEndpoints(core, namespace, name, endpoints, podByName, cfg, log) {
  if (endpoints === null) return;
  const addresses = endpoints
    .map((podName) => {
      const pod = podByName.get(podName);
      if (!pod?.status?.podIP) return null;
      return {
        ip: pod.status.podIP,
        nodeName: pod.spec?.nodeName,
        targetRef: { kind: 'Pod', name: podName, namespace },
      };
    })
    .filter(Boolean);

  const body = {
    subsets: addresses.length
      ? [
          {
            addresses,
            ports: [{ name: 'client', port: cfg.dragonflyPort, protocol: 'TCP' }],
          },
        ]
      : [],
  };

  try {
    await core.replaceNamespacedEndpoints({
      name,
      namespace,
      body,
    });
    log.info('patched endpoints', { service: name, masters: endpoints });
  } catch (err) {
    // 404 on first boot is expected: the Endpoints object is created by the
    // Service controller only after the Service exists. Create it ourselves.
    if (err?.response?.statusCode === 404 || err?.statusCode === 404) {
      const created = {
        apiVersion: 'v1',
        kind: 'Endpoints',
        metadata: { name, namespace, labels: { 'app.kubernetes.io/name': 'dragonfly' } },
        ...body,
      };
      try {
        await core.createNamespacedEndpoints({ namespace, body: created });
        log.info('created endpoints', { service: name, masters: endpoints });
      } catch (e2) {
        log.error('could not create endpoints', { service: name, err: String(e2) });
      }
      return;
    }
    log.error('could not patch endpoints', { service: name, err: String(err) });
  }
}

/** Reflect the write-guard verdict as a pod label, for humans and for alerts. */
export async function setWritableLabel(core, namespace, podName, writable, labelKey, log) {
  try {
    await core.patchNamespacedPod({
      name: podName,
      namespace,
      body: {
        metadata: {
          labels: { [labelKey]: String(writable) },
        },
      },
    });
  } catch (err) {
    log.debug('could not set writable label', { pod: podName, err: String(err) });
  }
}
