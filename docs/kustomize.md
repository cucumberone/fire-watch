# Kubernetes Layout: Kustomize + Overlays

`k8s/` is a **Kustomize** tree, not a flat directory of YAML. One base describes the production-shaped deployment, overlays adjust it per environment.

**Status: implemented and rendering.** `kubectl kustomize k8s/base` produces 49 objects, `k8s/overlays/ghcr` produces the same 49 with GHCR image references, and the lab produces 31 via `k8s/overlays/lab/openshift` and 29 via `k8s/overlays/lab/kubernetes` (the two Routes are deleted in the latter). `k8s/overlays/lab/common` also renders 31 on its own. The values below are verified against the rendered output, not aspirational.

```
k8s/
├── base/                          # the plan of record (3 replicas, H/A, full sizing)
│   ├── kustomization.yaml
│   ├── 01-pdb.yaml
│   ├── 10-rabbitmq-rbac.yaml
│   ├── 11-rabbitmq-config.yaml
│   ├── 13-rabbitmq-services.yaml
│   ├── 14-rabbitmq-statefulset.yaml
│   ├── 20-dragonfly-config.yaml
│   ├── 20-alert-fold-config.yaml
│   ├── 22-dragonfly-services.yaml
│   ├── 23-dragonfly-statefulset.yaml
│   ├── 30-sentinel-config.yaml
│   ├── 31-sentinel-statefulset.yaml
│   ├── 32-sentinel-bridge.yaml
│   ├── 40-workers.yaml
│   ├── 41-ws-gateway.yaml
│   └── 42-frontend.yaml
└── overlays/
    ├── lab/
    │   ├── common/                # lab base, platform-neutral (replicas, sizes, flags)
    │   │   ├── kustomization.yaml
    │   │   ├── replicas.yaml
    │   │   ├── resources.yaml
    │   │   ├── storage.yaml
    │   │   ├── delete-pdb.yaml
    │   │   ├── delete-anti-affinity.yaml
    │   │   ├── delete-hpa.yaml
    │   │   ├── dragonfly-lab.yaml
    │   │   ├── sentinel-lab.yaml
    │   │   └── bridge-lab.yaml
    │   ├── openshift/             # Routes, lab hostnames
    │   │   ├── kustomization.yaml
    │   │   ├── routes.yaml
    │   │   └── frontend-config.yaml
    │   └── kubernetes/            # Routes deleted, NodePort 30300/30800
    │       ├── kustomization.yaml
    │       ├── delete-routes.yaml
    │       ├── nodeports.yaml
    │       └── frontend-config.yaml
    └── prod/                      # (later) digest pinning, Route hosts, quota bumps
        └── kustomization.yaml
```

## 1. Why Kustomize and not Helm

| | Kustomize | Helm |
|---|---|---|
| Templating | none — plain YAML patched | Go templates + values |
| Readability of the artifact | the committed files *are* the manifests | the rendered output must be generated to be read |
| Where environment differences live | one small overlay per environment | values files, often sprawling |
| Native to `kubectl`/OpenShift | yes (`kubectl apply -k`) | needs Helm on the operator's machine |

The deployment has exactly two environments and a fixed component list. Kustomize's overlay model fits that precisely; Helm's templating would add a variable layer with no payoff.

## 2. Base

The base is **the plan in this repo, unchanged**: 3 replicas per component, required anti-affinity, PDBs, 48Gi Dragonfly limits, 128 GiB nodes assumed. It is the artifact the fault-tolerance audit ([fault-tolerance.md](fault-tolerance.md)) is written against.

```yaml
# k8s/base/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: fire-watch
resources:
  - 00-namespace.yaml
  - 01-pdb.yaml
  - 10-rabbitmq-rbac.yaml
  - 11-rabbitmq-config.yaml
  - 13-rabbitmq-services.yaml
  - 14-rabbitmq-statefulset.yaml
  - 20-dragonfly-config.yaml
  - 20-alert-fold-config.yaml
  - 22-dragonfly-services.yaml
  - 23-dragonfly-statefulset.yaml
  - 30-sentinel-config.yaml
  - 31-sentinel-statefulset.yaml
  - 32-sentinel-bridge.yaml
  - 33-redis-exporter.yaml
  - 34-http-ingress.yaml
  - 40-workers.yaml
  - 41-ws-gateway.yaml
  - 42-frontend.yaml
  - 60-monitoring.yaml
```

Rules the base follows that overlays must **not** silently break:

- required pod anti-affinity on all 7 workloads
- a PDB for every workload
- pinned image digests
- explicit `resources` on every container
- probes on every container
- `runAsNonRoot`, `allowPrivilegeEscalation: false`

## 3. The `lab` overlay

Purpose: run the whole system on one small machine, to exercise the event pipeline end to end. It is **explicitly not fault tolerant** and that is the point — it must be cheap, fast to start and easy to reset.

### 3.0 Two flavours, one shared base

The lab runs on **either OpenShift or plain Kubernetes**. Production is always OpenShift; the plain-Kubernetes flavour exists so the datastore half can be exercised on hardware that is already on the desk, not as a second production path.

```
overlays/
├── ghcr/              # same production shape, images from GHCR
│   └── kustomization.yaml   (image rewrite + imagePullSecrets)
└── lab/
    ├── common/            # identical on both platforms
    │   ├── kustomization.yaml   (this IS the lab base, not an overlay)
    │   ├── replicas.yaml, resources.yaml, storage.yaml
    │   ├── delete-pdb.yaml, delete-anti-affinity.yaml, delete-hpa.yaml
    │   └── dragonfly-lab.yaml, sentinel-lab.yaml, bridge-lab.yaml
    ├── openshift/         # Route-based, like production
    │   ├── kustomization.yaml
    │   ├── routes.yaml           (lab hostnames)
    │   └── frontend-config.yaml  (wss://ws.fire-watch-lab.apps.lab.local)
    └── kubernetes/        # no Routes, NodePort instead
        ├── kustomization.yaml
        ├── delete-routes.yaml    ($patch: delete on both Routes)
        ├── nodeports.yaml        (30300 ws-gateway, 30800 frontend)
        └── frontend-config.yaml  (host resolved from the page)
```

```bash
kubectl apply -k k8s/overlays/lab/openshift     # CRC / OpenShift Local / real OCP
kubectl apply -k k8s/overlays/lab/kubernetes    # MicroK8s / kind / k3s
kubectl apply -k k8s/overlays/ghcr              # production shape, GHCR images
```

`overlays/ghcr` is orthogonal to the lab: it changes where the five service images come from and nothing else. See [images.md](images.md) §6.

| | `lab/openshift` | `lab/kubernetes` |
|---|---|---|
| Objects rendered | 31 | 29 |
| `Route` objects | 2 | **0** (deleted at render time) |
| `ws-gateway` Service | ClusterIP | **NodePort 30300** |
| `frontend` Service | ClusterIP | **NodePort 30800** |
| Frontend `wsUrl` | `wss://ws.fire-watch-lab.apps.lab.local` | `ws://<page-host>:30300` |
| Everything else | identical | identical |

**`common/` is a base, not an overlay.** Both flavours list it under `resources`, which is what `base` in `kustomize.base` means. That matters because it makes `common` renderable on its own (`kubectl kustomize k8s/overlays/lab/common` → 30 objects) and it is the only structure where the two flavours cannot drift apart.

**Deleting a CRD object works.** `kubernetes/delete-routes.yaml` uses `$patch: delete` on `route.openshift.io/v1` objects that the *local* kustomize has no schema for. Verified with kustomize 5.x shipped in `kubectl`: the objects are removed at render time and never reach the API server. Without this, a plain cluster fails with `no matches for kind "Route"`.

**NodePorts are fixed** (30300/30800) rather than auto-assigned, so the frontend config can be a constant and you do not have to look up a port after every apply. Nothing but these two Services is exposed: RabbitMQ, Dragonfly and Sentinel stay ClusterIP-only on both flavours, exactly as in production.

**The plain-Kubernetes frontend config sets `resolveWsHostFromPage: true`.** A NodePort is reachable on any node's IP, so hard-coding one would be wrong on a second machine and stale after a DHCP change. The frontend substitutes `location.hostname` and keeps the port. This is a small requirement on the frontend code — it is written down here because it is the one behavioural difference the overlay imposes on application code.

### 3.1 What it changes

| Aspect | Base | lab |
|---|---|---|
| Replicas | 3 each (bridge 2) | **1 each** |
| Anti-affinity | required | **removed** (1 replica, no spread to enforce; required would still schedule but blocks nothing) |
| PDBs | all 8 workloads | **deleted** — a PDB with 1 replica and `minAvailable: 2` makes the pod impossible to evict |
| Dragonfly `maxmemory` | 36gb | **512mb** |
| Dragonfly mem limit/request | 48Gi / 8Gi | **1Gi / 256Mi** |
| Dragonfly PV | 64Gi | **2Gi** |
| RabbitMQ PV | 96Gi | **2Gi** |
| Sentinel PV | 1Gi | **256Mi** |
| Snapshot schedule | hourly | **every 15 min** (`*/15 * * * *`), so the restart path is exercised quickly |
| CPU/memory per pod | full | **tiny** (see §3.3) |
| HPA | 3..12 gateway | **removed**, fixed 1 |
| NetworkPolicies | absent (deliberate, shared namespace) | **absent** — nothing to remove |
| Routes | present | **kept**, hostnames from the overlay |
| Image digests | pinned | **kept**, same images |

### 3.2 Why each change is safe to make

| Change | Consequence |
|---|---|
| 1 replica | No quorum. RabbitMQ has 1 member of a quorum queue, so a restart means brief publish refusal; Dragonfly has no replica, so no failover; Sentinel has quorum 1. **All three degrade to "a single pod that can only restart", which is exactly what a lab wants.** |
| Sentinel quorum 1 | Failover logic cannot fire (there is nothing to promote). The `sentinel-bridge` still applies the initial `REPLICAOF`-less state and keeps the Service selector correct. |
| Removing PDBs | Otherwise `kubectl drain` and even some rollouts hang forever on a 1-replica deployment with `minAvailable: 2`. |
| Removing anti-affinity | With 1 replica there is nothing to spread. Keeping `required` would be harmless but misleading. |
| `maxmemory=512mb` + 1Gi limit | At 512 MB the lab holds roughly 200 000 events — plenty to demo, small enough for a laptop. Expect eviction to be **disabled** the same way as base (`noeviction`), so it fails loudly. |
| 15-minute snapshots | Makes the restore path testable in minutes instead of hours. |

### 3.3 Lab resource profile

Measured from `kubectl kustomize k8s/overlays/lab/common`, not estimated:

```text
1 replica per workload
requests:  310m CPU, 656Mi memory   (0.64 GiB)
limits:   1650m CPU, 2208Mi memory  (2.16 GiB)
storage:      2Gi + 2Gi + 256Mi    (4.25 GiB)
```

Fits any single-node cluster or a 2 vCPU / 4 GiB VM.

| Component | Req CPU | Lim CPU | Req Mem | Lim Mem | PV |
|---|---|---|---|---|---|
| RabbitMQ | 50m | 300m | 128Mi | 384Mi | 2Gi |
| Dragonfly (server) | 100m | 500m | 256Mi | 1Gi | 2Gi |
| Dragonfly (pruner) | 5m | 50m | 8Mi | 32Mi | — |
| Sentinel (server) | 10m | 50m | 32Mi | 64Mi | 256Mi |
| Sentinel (init) | 5m | 50m | 8Mi | 32Mi | — |
| Workers | 50m | 200m | 64Mi | 192Mi | — |
| WS gateway | 50m | 200m | 64Mi | 192Mi | — |
| Frontend | 10m | 50m | 16Mi | 32Mi | — |
| sentinel-bridge | 10m | 100m | 32Mi | 96Mi | — |
| http-ingress | 20m | 150m | 48Mi | 160Mi | — |
| **Total** | **310m** | **1650m** | **656Mi** | **2.16Gi** | **4.25Gi** |

### 3.4 The lab kustomization

```yaml
# k8s/overlays/lab/common/kustomization.yaml — the shared lab base
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: fire-watch-lab
resources:
  - ../../../base
patches:
  - path: replicas.yaml
  - path: resources.yaml
  - path: storage.yaml
  - path: delete-pdb.yaml
  - path: delete-anti-affinity.yaml
  - path: delete-hpa.yaml
  - path: dragonfly-lab.yaml
  - path: sentinel-lab.yaml
  - path: bridge-lab.yaml
```

```yaml
# k8s/overlays/lab/kubernetes/kustomization.yaml — plain-Kubernetes flavour
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: fire-watch-lab
resources:
  - ../common
patches:
  - path: delete-routes.yaml     # $patch: delete on both Routes
  - path: nodeports.yaml         # ws-gateway 30300, frontend 30800
  - path: frontend-config.yaml
```

The OpenShift flavour is the same file shape with `routes.yaml` and its own
`frontend-config.yaml` instead of the three Kubernetes patches.

`replicas.yaml` — one patch covering every workload (Kustomize has no cross-resource replicas field that reaches a StatefulSet and a Deployment in one entry):

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: worker }
spec: { replicas: 1 }
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: ws-gateway }
spec: { replicas: 1 }
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: frontend }
spec: { replicas: 1 }
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: sentinel-bridge }
spec: { replicas: 1 }
---
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: rabbitmq }
spec: { replicas: 1 }
---
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: dragonfly }
spec: { replicas: 1 }
---
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: sentinel }
spec: { replicas: 1 }
```

`delete-pdb.yaml` — delete, not shrink:

```yaml
$patch: delete
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: { name: rabbitmq }
---
$patch: delete
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: { name: dragonfly }
# ... one entry per PDB in the base
```

`dragonfly-lab.yaml` patches the Dragonfly **ConfigMap**, not an args list:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: dragonfly }
spec:
  template:
    spec:
      containers:
        - name: dragonfly
          args:
            - --logtostderr
            - --dir=/data
            - --dbfilename=firewatch-{timestamp}
            - --df_snapshot_format=true
            - "--snapshot_cron=*/15 * * * *"
            - --maxmemory=512mb
            - --maxmemory-policy=noeviction
            - --proactor_threads=1
            - --replica_delete_expired=false
            - $(DRAGONFLY_AUTH_ARGS)
            - --port=6379
            - --admin_port=9222
```

Note the list is replaced wholesale — Kustomize replaces `args` rather than merging lists. That is why the lab patch repeats every argument. **A dropped `--maxmemory-policy=noeviction` here would silently reintroduce defect D9** ([fault-tolerance.md](fault-tolerance.md) §3.9), so the overlay is reviewed against the base list.

### 3.5 Applying it

```bash
# ---- OpenShift (CRC, OpenShift Local, or a real OCP cluster)
kubectl kustomize k8s/overlays/lab/openshift | less
kubectl apply   -k k8s/overlays/lab/openshift

# ---- plain Kubernetes (MicroK8s, kind, k3s)
kubectl kustomize k8s/overlays/lab/kubernetes | less
kubectl apply   -k k8s/overlays/lab/kubernetes
```

Tear down. Note that `delete -k` does **not** remove the StatefulSet PVCs — that
is deliberate, so a re-apply reuses the data:

```bash
kubectl delete -k k8s/overlays/lab/openshift      # or .../kubernetes
kubectl -n fire-watch-lab delete pvc -l app.kubernetes.io/name=rabbitmq
kubectl -n fire-watch-lab delete pvc -l app.kubernetes.io/name=dragonfly
kubectl -n fire-watch-lab delete pvc -l app.kubernetes.io/name=sentinel
```

`kubectl apply -k` is built into `kubectl` and `oc`; there is no separate install
step.

On the plain-Kubernetes flavour the UI is at `http://<node-ip>:30800` and the
gateway at `ws://<node-ip>:30300`. There is no Route, so nothing resolves by
name — use the node address.

### 3.6 What the lab cannot test

State this before anyone trusts a lab result:

| Not testable in lab | Why |
|---|---|
| Quorum queue behaviour | 1 member — no quorum to lose |
| Dragonfly failover / promotion | no replica |
| Sentinel failover | quorum 1, nothing to promote |
| Write guard tripping | it needs `connected_slaves < 2` with 2 expected; with 1 replica the guard must be configured to expect 0 replicas or disabled |
| Split-brain scenarios | no peers to partition from |
| Node loss | single node |
| Memory/snapshot sizing | 512mb vs 36gb |
| Eviction pressure | working set is far below `maxmemory` |

**Consequence:** the bridge needs a lab-specific setting, `FW_WRITE_GUARD_MIN_REPLICAS=0`, or it will withhold the endpoint permanently in the lab. A single Dragonfly has no replica, so its replica count is always 0, which is below the base's expectation of 2 — the Service would have zero endpoints forever. This is the single most likely "why is everything read-only in my lab" trap, so the overlay sets it explicitly in `bridge-lab.yaml`:

```yaml
- name: FW_WRITE_GUARD_MIN_REPLICAS
  value: "0"
```

## 4. Scaling the lab up later

The lab overlay is a stepping stone, not a dead end. To get closer to production on a slightly bigger machine, add a second overlay that composes rather than replaces:

```yaml
# k8s/overlays/lab-ha/kustomization.yaml (a future third flavour-agnostic overlay)
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: fire-watch-lab
resources: [../lab]
patches:
  - path: replicas-3.yaml     # back to 3, keep the small resources
  - path: restore-pdb.yaml    # re-add PDBs
  - path: restore-affinity.yaml
```

That gives a cheap 3-replica cluster for rehearsal — small enough for a homelab, real enough to exercise quorum and failover. Tracked as [roadmap.md](roadmap.md) §3.11.

## 5. Rules for overlay authors

1. **Overlays never change the design.** They change replicas, resources, storage sizes, schedules and hostnames. Anything that alters a durability or safety invariant belongs in the base and must be reviewed there.
2. **A dropped argument is a dropped invariant.** Because Kustomize replaces lists, every `args` patch must be written out in full and diffed against the base.
3. **Every overlay must render.** `kubectl kustomize <overlay>` is part of review; a broken overlay is not a documentation gap, it is a broken deliverable.
4. **Deleting resources is explicit.** PDBs are removed with `$patch: delete`, never by omitting the file from `resources` — omitting it would also silently drop it from every future overlay.
5. **Lab namespaces are distinct.** `fire-watch-lab` vs `fire-watch`, so a lab apply can never touch production objects.
6. **`imagePullPolicy` lives in the base, not in an overlay.** All five GHCR Deployments set `Always`, because the overlays use the mutable tag `latest` and `IfNotPresent` would silently keep running a stale cached image after a rebuild ([images.md](images.md) §7.1). Overlays change `newTag`; they must not weaken the pull policy while the tag stays mutable. Once an environment pins an immutable tag, `IfNotPresent` becomes correct — that change belongs in the base with the rest of the invariant, deliberately.
