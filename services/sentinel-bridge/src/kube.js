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

/**
 * @kubernetes/client-node 0.22.x differs from the v1.x API in TWO ways, and
 * getting either one wrong fails silently rather than loudly:
 *
 *   1. Arguments are POSITIONAL, not a single object. Passing
 *      `{ namespace, labelSelector }` leaves every positional parameter
 *      undefined, so the selector is dropped and the call returns an unfiltered
 *      or empty result instead of erroring.
 *   2. The promise resolves to an envelope `{ response, body }`, not to the
 *      resource. Reading `.items` off the envelope yields `undefined`, which
 *      callers then treat as "zero results".
 *
 * Both bugs shipped here: `listPods` used the object form, so `.items` was
 * undefined and the bridge reported "no valkey pods found" on a healthy
 * 1-pod cluster while holding the leader lease.
 *
 * This helper exists so there is exactly one place where the shape is handled.
 */
function unwrap(res) {
  return res?.body ?? res;
}

/** List the Valkey pods, in ordinal order. */
export async function listPods(core, namespace, labelSelector) {
  // listNamespacedPod(namespace, pretty, allowWatchBookmarks, _continue,
  //                   fieldSelector, labelSelector, limit, resourceVersion, ...)
  // labelSelector really is the 6th positional argument.
  const res = await core.listNamespacedPod(
    namespace,
    undefined,          // pretty
    undefined,          // allowWatchBookmarks
    undefined,          // _continue
    undefined,          // fieldSelector
    labelSelector,      // labelSelector
  );
  const list = unwrap(res);
  const items = list?.items ?? [];
  return items.sort((a, b) => a.metadata.name.localeCompare(b.metadata.name));
}

/**
 * Read one pod's replication state directly from Valkey.
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
    port: cfg.valkeyPort,
    password: cfg.valkeyPassword,
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
    port: cfg.valkeyPort,
    password: cfg.valkeyPassword,
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
    port: cfg.valkeyPort,
    password: cfg.valkeyPassword,
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
 *
 * TWO DEFECTS FIXED HERE, both found on a cluster with the `valkey` Service.
 *
 * 1. `metadata.name` must be in the REPLACE body. A replace is a PUT to
 *    `/api/v1/namespaces/{ns}/endpoints/{name}`, and the API server validates the
 *    body's name against the URL: omitting it fails with
 *
 *      400 BadRequest: the name of the object (valkey based on URL) was
 *      undeterminable: name must be provided
 *
 *    The body used to carry only `subsets`, so `replaceNamespacedEndpoints`
 *    could NEVER succeed — the first call always threw.
 *
 * 2. That 400 is what made the bug invisible on `dragonfly`. The old code treated
 *    only a 404 as "the object does not exist, create it", so on a brand-new
 *    Service the create branch never ran and every cycle logged
 *    `could not patch endpoints` forever, leaving `Endpoints/valkey` absent
 *    entirely. It looked fine on Dragonfly only because an earlier controller
 *    had already created that Endpoints object, so the GET-then-replace path
 *    happened to have something to overwrite.
 *
 *    The fix is both halves: send the name, and create on 404 *or* on a
 *    400 that mentions an undeterminable name (belt and braces — a future client
 *    version may surface the same condition differently).
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
    apiVersion: 'v1',
    kind: 'Endpoints',
    // REQUIRED by the API server on a replace: the body name must match the URL.
    // Without it every replace is a 400 (see the note above).
    metadata: { name, namespace },
    subsets: addresses.length
      ? [
          {
            addresses,
            ports: [{ name: 'client', port: cfg.valkeyPort, protocol: 'TCP' }],
          },
        ]
      : [],
  };

  try {
    // replaceNamespacedEndpoints(name, namespace, body, pretty, dryRun, ...)
    await core.replaceNamespacedEndpoints(name, namespace, body);
    log.info('patched endpoints', { service: name, masters: endpoints });
  } catch (err) {
    const status = err?.response?.statusCode ?? err?.statusCode;
    // The Endpoints object does not exist yet: it is created by the Service
    // controller, and only once the Service exists. Two shapes mean that here —
    // 404, and the 400 "name was undeterminable" that this client returns when
    // the object is absent.
    const missing = status === 404 || status === 400;
    if (missing) {
      const created = {
        apiVersion: 'v1',
        kind: 'Endpoints',
        metadata: {
          name,
          namespace,
          labels: { 'app.kubernetes.io/name': 'valkey' },
        },
        subsets: body.subsets,
      };
      try {
        // createNamespacedEndpoints(namespace, body, pretty, dryRun, ...)
        await core.createNamespacedEndpoints(namespace, created);
        log.info('created endpoints', { service: name, masters: endpoints });
      } catch (e2) {
        // 409 = another cycle or the Service controller created it between our
        // replace and our create. That is success, not a fault.
        const s2 = e2?.response?.statusCode ?? e2?.statusCode;
        if (s2 === 409) {
          log.debug('endpoints appeared concurrently', { service: name });
          return;
        }
        log.error('could not create endpoints', {
          service: name, status: s2, err: String(e2),
        });
      }
      return;
    }
    log.error('could not patch endpoints', {
      service: name, status, err: String(err),
    });
  }
}

/**
 * Reflect the write-guard verdict as a pod label, for humans and for alerts.
 *
 * THE `Content-Type` HEADER IS REQUIRED. This client does not set one for a patch,
 * and the API server answers a bare JSON body with
 *
 *   415 Unsupported Media Type
 *
 * The call then fails every cycle, `setWritableLabel` logs it at `debug` (by
 * design — a label is cosmetic), and the label is simply never applied. The
 * visible symptom is subtle rather than loud: `kubectl get pods -L
 * fire-watch.io/writable` shows an empty column, and the
 * `ValkeyServiceEndpointEmpty` alert — which watches
 * `kube_pod_labels{label_fire_watch_io_writable="false"}` — can never fire,
 * because that label value never exists in the first place.
 *
 * `merge-patch+json`, not `strategic-merge-patch+json`: a label map is merged
 * key-by-key by a merge patch, and a strategic merge on a pod would also let this
 * controller rewrite list fields it has no business touching.
 */
export async function setWritableLabel(core, namespace, podName, writable, labelKey, log) {
  try {
    // patchNamespacedPod(name, namespace, body, pretty, dryRun, fieldManager,
    //                   fieldValidation, force, options)
    await core.patchNamespacedPod(
      podName,
      namespace,
      { metadata: { labels: { [labelKey]: String(writable) } } },
      '', undefined, undefined, undefined, undefined,
      { headers: { 'Content-Type': 'application/merge-patch+json' } },
    );
  } catch (err) {
    // Debug, not error: the label is for humans and alerts, and losing it must
    // never stop the reconcile that actually moves traffic. The 415 above was
    // invisible for exactly this reason, which is why the test suite now pins the
    // header rather than trusting the call.
    log.debug('could not set writable label', { pod: podName, err: String(err) });
  }
}
