# Kubernetes Layout: Kustomize + Overlays

`k8s/` is a **Kustomize** tree, not a flat directory of YAML. One base describes the production-shaped deployment, overlays adjust it per environment.

```
k8s/
├── base/                       # the plan of record (3 replicas, H/A, full sizing)
│   ├── kustomization.yaml
│   ├── 00-namespace.yaml
│   ├── 01-pdb.yaml
│   ├── 10-rabbitmq-rbac.yaml
│   ├── 11-rabbitmq-config.yaml
│   ├── 12-rabbitmq-secret.yaml.example
│   ├── 13-rabbitmq-services.yaml
│   ├── 14-rabbitmq-statefulset.yaml
│   ├── 20-dragonfly-config.yaml
│   ├── 20-alert-fold-config.yaml
│   ├── 21-dragonfly-secret.yaml.example
│   ├── 22-dragonfly-services.yaml
│   ├── 23-dragonfly-statefulset.yaml
│   ├── 30-sentinel-config.yaml
│   ├── 31-sentinel-statefulset.yaml
│   ├── 32-sentinel-bridge.yaml
│   ├── 40-workers.yaml
│   ├── 41-ws-gateway.yaml
│   ├── 42-frontend.yaml
│   ├── 50-no-networkpolicies.md     # decision note only, no objects ([networking.md](networking.md) §5)
│   └── 60-monitoring.yaml
└── overlays/
    ├── lab/                    # 1 replica, tiny pods, no H/A — for the home lab
    │   ├── kustomization.yaml
    │   ├── replicas.yaml
    │   ├── resources.yaml
    │   ├── storage.yaml
    │   └── no-pdb.yaml         # delete PDBs: they block the single-pod lab
    └── prod/                   # (later) digest pinning, Route hosts, quota bumps
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

### 3.1 What it changes

| Aspect | Base | lab |
|---|---|---|
| Replicas | 3 each (bridge 2) | **1 each** |
| Anti-affinity | required | **removed** (1 replica, no spread to enforce; required would still schedule but blocks nothing) |
| PDBs | all 7 workloads | **deleted** — a PDB with 1 replica and `minAvailable: 2` makes the pod impossible to evict |
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

Total across all pods: **≈ 1 CPU requested, ≈ 2.5 GiB requested**, ≈ 6 GiB of storage. Fits any single-node cluster or a 2 vCPU / 4 GiB VM.

| Component | Req CPU | Lim CPU | Req Mem | Lim Mem | PV |
|---|---|---|---|---|---|
| RabbitMQ | 50m | 300m | 128Mi | 384Mi | 2Gi |
| Dragonfly | 100m | 500m | 256Mi | 1Gi | 2Gi |
| Sentinel | 10m | 50m | 32Mi | 64Mi | 256Mi |
| Workers | 50m | 200m | 64Mi | 192Mi | — |
| WS gateway | 50m | 200m | 64Mi | 192Mi | — |
| Frontend | 10m | 50m | 16Mi | 32Mi | — |

### 3.4 The lab kustomization

```yaml
# k8s/overlays/lab/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: fire-watch-lab

resources:
  - ../../base

patches:
  - path: replicas.yaml
  - path: resources.yaml
  - path: storage.yaml
  - path: no-pdb.yaml
  - path: dragonfly-lab-args.yaml
  - path: ingress-hosts.yaml

# routes need distinct hostnames so the lab never collides with anything else
replacements:
  - source: { kind: ConfigMap, name: lab-hosts, fieldPath: data.wsHost }
    targets:
      - select: { kind: Route, name: ws-gateway }
        fieldPaths: [spec.host]
```

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

`no-pdb.yaml` — delete, not shrink:

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

`dragonfly-lab-args.yaml` patches only the two arguments that must differ:

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
# render first — always look at what will be applied
kubectl kustomize k8s/overlays/lab | less

# apply
kubectl apply -k k8s/overlays/lab

# tear down
kubectl delete -k k8s/overlays/lab
```

`kubectl apply -k` is built into kubectl and `oc`; there is no separate install step.

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

**Consequence:** the write-guard sidecar needs a lab-specific setting, `FW_WRITE_GUARD_MIN_REPLICAS=0`, or it will trip permanently in the lab (a master with 1 configured replica has `connected_slaves:0`, which is below the base's expectation of 2). This is the single most likely "why is everything read-only in my lab" trap, so it belongs in the overlay explicitly:

```yaml
# env patch for the write-guard sidecar
- name: FW_WRITE_GUARD_MIN_REPLICAS
  value: "0"
```

## 4. Scaling the lab up later

The lab overlay is a stepping stone, not a dead end. To get closer to production on a slightly bigger machine, add a second overlay that composes rather than replaces:

```yaml
# k8s/overlays/lab-ha/kustomization.yaml
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
