# Fault Tolerance & Split-Brain Audit

Verification of every component against R3 (**100% H/A, 3 replicas on different nodes, survives eviction/deletion**) and R4 (**handles split brain**).

**Verdict up front: the design is *not* 100% fault tolerant as previously written.** Seven defects were found. One of them silently loses data. All are fixable in configuration, not architecture, and the fixes are applied in the sections below.

## 1. Verdict summary

| Component | 3 replicas | Different nodes | Survives 1 pod loss | Survives 1 node loss | Split brain | Verdict |
|---|---|---|---|---|---|---|
| RabbitMQ | ✓ | ✓ required | ✓ | ✓ 2/3 quorate | ✓ quorum queues | **PASS** after §3.1 |
| DragonflyDB | ✓ | ✓ required | ✓ | ✓ (master failover) | ⚠ routing gate only | **PARTIAL** — §4 |
| Redis Sentinel | ✓ | ✓ required | ✓ | ✓ 2/3 quorum | ✗ **broken by `emptyDir`** | **FAIL** → fixed §3.2 |
| Workers | ✓ | ✓ required | ✓ | ✓ | n/a stateless | **PASS** after §3.3 |
| WS gateway | ✓ | ✗ **preferred only** | ✓ | ✗ possible total outage | n/a stateless | **FAIL** → fixed §3.4 |
| Frontend | ✓ | ✗ **preferred only** | ✓ | ✗ possible total outage | n/a stateless | **FAIL** → fixed §3.4 |
| sentinel-bridge | ✗ **1 replica** | ✗ | — | — | — | **FAIL** → fixed §3.5 |
| replay-gap Job | n/a | n/a | n/a | n/a | n/a | PASS (manual, idempotent) |

## 2. Defects found

| # | Defect | Severity | Impact |
|---|---|---|---|
| D1 | `x-delivery-limit: 10` on `q.event.ingest` combined with `nack(requeue=true)` | **CRITICAL** | **Silent data loss.** A Dragonfly outage longer than 10 redeliveries dead-letters events that were never written. Directly violates R1. |
| D2 | Sentinel uses `emptyDir` for `sentinel.conf` | **CRITICAL** | Restarted sentinels lose `config epoch`, `known-sentinels` and `known-replicas`. Two restarted sentinels can authorise a failover with a stale epoch → **the exact split-brain the quorum was meant to prevent**. |
| D3 | WS gateway and frontend use *preferred* anti-affinity | **HIGH** | All 3 pods can land on one node. Losing that node takes the component to **zero replicas** — not 100% H/A. |
| D4 | `sentinel-bridge` is 1 replica | **HIGH** | Single point of failure for the `dragonfly` Service selector and `REPLICAOF` application. If it dies, no client can find the master after the next failover. |
| D5 | No `--requirepass` on Dragonfly, yet Sentinel and clients use `auth-pass` | **HIGH** | Sentinel cannot authenticate → it cannot monitor the master, so **failover never happens**. Or, if auth is skipped, Dragonfly is unauthenticated. |
| D6 | `fw:write_guard:state` is read by workers but written by **nothing** | **HIGH** | The write-guard publishes to `fw:ch:alerts` and sets a pod label, but never sets the key workers check. The guard is therefore inert. |
| D7 | No `PodDisruptionBudget` for workers, gateway, frontend, sentinel, bridge | **MEDIUM** | `kubectl drain` can evict all replicas of a component simultaneously. R3 explicitly requires surviving eviction. |

Two additional correctness issues:

| # | Issue | Impact |
|---|---|---|
| D8 | `cluster_partition_handling` never set (RabbitMQ default `ignore`) | On a 3-node cluster the recommended value is `pause_minority`. Absent it, a partitioned node keeps serving clients with an incomplete view. |
| D9 | `maxmemory-policy` never set for Dragonfly | If it is not `noeviction`, hitting `maxmemory` **silently evicts live events** and quietly breaks the 7-day retention promise. |

## 3. Fixes

### 3.1 D1 — remove the delivery limit from the ingest queue

```json
// definitions.json — q.event.ingest
{
  "x-queue-type": "quorum",
  "x-dead-letter-exchange": "fx.events.dlx",
  "x-dead-letter-routing-key": "event.ingest"
}
```

`x-delivery-limit` is **removed**. Rationale:

- The only legitimate reason to dead-letter an ingest message is a **schema violation**, and that is an explicit `nack(requeue=false)` from the validator — it does not need a broker-side counter.
- Every other requeue is *transient* (Dragonfly down, master failover, audit queue full). A delivery limit converts those into permanent loss.
- A poison message that crashes the worker is now prevented by the watermark and idempotency design ([event-lifecycle.md](event-lifecycle.md) §5), not by a counter.
- Bound the risk instead with `x-max-length-bytes` on the DLQ and an alert on `q.event.ingest.dlq > 0`.

**Rule for the implementation:** `nack(requeue=false)` is only allowed for validation failures. Every other failure path must `nack(requeue=true)`.

### 3.2 D2 — Sentinel needs a persistent volume

```yaml
volumeClaimTemplates:
  - metadata: { name: sentinel-data }
    spec:
      accessModes: [ReadWriteOnce]
      resources: { requests: { storage: 1Gi } }
```

Mount `sentinel-data` at `/data` and point `dir /data`. The ConfigMap is copied to `/data/sentinel.conf` by an init container **only if absent**, so Sentinel's own rewrite persists across restarts.

Why this matters for split brain: Sentinel uses a monotonically increasing **config epoch** to reject stale failover votes. The epoch lives in `sentinel.conf`. With `emptyDir`, a restarted sentinel advertises epoch 0 and can vote for a second failover while the first is still being propagated — two masters. Persisting the epoch is what makes 2-of-3 safe across restarts; it is not optional.

The prior claim that `emptyDir` is fine because "a lost sentinel just relearns the topology" was **wrong** — relearning is fine, losing the epoch is not.

### 3.3 D7 — PodDisruptionBudgets for every component

| Component | PDB |
|---|---|
| RabbitMQ | `minAvailable: 2` |
| Dragonfly | `minAvailable: 2` |
| Sentinel | `minAvailable: 2` |
| Workers | `minAvailable: 1` |
| WS gateway | `minAvailable: 2` |
| Frontend | `minAvailable: 2` |
| sentinel-bridge | `minAvailable: 1` |

`minAvailable: 1` for workers is deliberate: workers are a throughput resource, not a quorum, and pinning them at 2 would block legitimate single-pod maintenance. For the gateway and frontend, 2 is the right floor because the service must stay reachable during a drain.

### 3.4 D3 — required anti-affinity for stateless components

```yaml
affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels: { app.kubernetes.io/name: ws-gateway }
        topologyKey: kubernetes.io/hostname
```

The prior reasoning — "preferred, because required spread would leave pods Pending on a 2-node cluster" — is not applicable: **the target is 3 nodes** ([sizing.md](sizing.md) §3.2). With 3 nodes and 3 replicas, required spread always schedules. Preferred anti-affinity only buys tolerance for a mis-sized cluster, at the cost of the actual requirement.

Consequence to accept: after a node loss the third pod stays `Pending` (no second copy can share a node). That is the same behaviour as RabbitMQ and Dragonfly, and it is the correct one — a component that has silently collapsed onto one node is not H/A.

### 3.5 D4 — sentinel-bridge with 2 replicas + leader election

```yaml
kind: Deployment
spec:
  replicas: 2
  template:
    spec:
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution: [...]
```

Both replicas run; only the lease holder acts (`coordination.k8s.io/v1` `Lease`, the same mechanism the kube scheduler uses). A non-leader stays warm and takes over within one lease duration (15s).

Alternative rejected: 1 replica with a `restartPolicy: Always` Deployment. A restart takes seconds to a minute, and during that window **no component can determine the master**, so writes stop even if Dragonfly is perfectly healthy. That is not 100% H/A.

### 3.6 D5 — Dragonfly authentication

```yaml
args:
  - --requirepass=$(DRAGONFLY_PASSWORD)
```

`auth-pass` in `sentinel.conf` and the client password in workers/gateway must match. Without it Sentinel's `PING` is rejected and the master is considered permanently down — a failover loop that never converges.

The password comes from `Secret/fire-watch-dragonfly`, injected via `valueFrom`, never inlined.

### 3.7 D6 — fix the write-guard contract

The guard cannot write to Dragonfly (that is the very component it is judging), so the state must not live there. Corrected design:

| Signal | Written by | Read by | Mechanism |
|---|---|---|---|
| `fire-watch.io/writable` label | write-guard sidecar (local pod only) | `sentinel-bridge` | pod patch, no Dragonfly dependency |
| `fw_dragonfly_write_guard_tripped` | write-guard | Prometheus | sidecar `/metrics` |
| Service endpoint removal | `sentinel-bridge` | all clients | reacts to the label |

Workers then rely on the **Service** rather than on a key:

```text
if the dragonfly Service has no ready endpoint -> do not write; nack(requeue=true)
```

This removes the broken `fw:write_guard:state` key entirely, and removes the circularity of a degraded Dragonfly being asked to report its own degradation.

### 3.8 D8 — RabbitMQ partition handling

```ini
# rabbitmq.conf
cluster_partition_handling = pause_minority
```

With quorum queues, `pause_minority` is the correct setting for a 3-node cluster: a node that cannot see a majority of its peers stops serving, instead of answering clients from a stale view. The minority side pauses; the majority side keeps a working quorum and clients reconnect to it.

`autoheal` is wrong here — it can restart the minority side and discard its state, which is fine for classic mirrored queues but pointless with quorum queues and disruptive to connections.

### 3.9 D9 — Dragonfly eviction policy

```yaml
args:
  - --maxmemory-policy=noeviction
```

Eviction must be an **incident**, not a silent behaviour. With `noeviction`, writes fail loudly when `maxmemory` is reached, the worker requeues, and the backlog becomes visible — exactly the behaviour the audit WAL is designed to recover from ([persistence-and-durability.md](persistence-and-durability.md) §4). Any other policy quietly deletes events and breaks retention without any signal.

## 4. The one gap that cannot be closed by configuration

**Dragonfly split brain: the write guard is a routing gate, not a storage gate.**

| Scenario | Behaviour |
|---|---|
| Master partitioned from both replicas, Sentinel quorum on the majority side | Guard trips in 30s, master removed from the `dragonfly` Service, new master elected. **All routed writes are safe.** |
| A client that ignores the Service and writes to the old master's pod IP | **The write succeeds and is later discarded on rejoin.** |

Layer 3 (Sentinel) prevents two masters from being *authoritative*, and layer 2 prevents clients from *routing* to the stale one. What is left is a direct-IP write, which is not routed through anything the design controls.

Why it cannot be closed with manifests: Dragonfly has no `min-replicas-to-write` equivalent ([dragonfly.md](dragonfly.md) §3). There is no server-side switch that makes a replica-starved master reject writes.

Three ways to close it, in order of cost:

| Option | Result | Cost |
|---|---|---|
| **Enforce the rule in code and review** — workers and gateway are forbidden from using pod IPs; the only remaining control is configuration discipline | The only realistic writer is a human with `kubectl exec` | zero, and it is the current design |
| Add NetworkPolicies denying worker→`dragonfly-N` traffic | closes the gap at the network layer | **not available** — the deployment target is a shared namespace ([networking.md](networking.md) §5.1) |
| Run Dragonfly in Valkey/Redis with `min-replicas-to-write 2` | True storage-level gate | contradicts the Dragonfly requirement |
| Accept it | Documented residual risk | none |

The design takes the first option. **NetworkPolicy was previously listed as a second control and is no longer deployed**, so the gap is now narrower in mitigation than it was in the earlier revision. It is **not** equivalent to a storage-level gate and this audit does not claim otherwise.

### 4.1 Also verify: is layer 3 actually sufficient?

Sentinel with quorum 2 and a persisted epoch (§3.2) gives:

| Partition | Sentinel outcome | Write outcome |
|---|---|---|
| 1 sentinel isolated | quorum 2 remains, no failover | unaffected |
| Master + 1 sentinel isolated on the same node | quorum 2 on the majority side, failover | routed to new master |
| 2 nodes lost | quorum lost (1 of 3), **no failover** | writes stop until a second node returns |
| Whole cluster restart | epoch persisted, no spurious failover | resumes with the same master |

The 2-node-loss row is important and correct: **the cluster is not available with 2 of 3 nodes down.** No quorum system is. "100% H/A" here means surviving any *single* node loss, which it does.

## 5. What is genuinely 100% after the fixes

| Requirement | Mechanism | Verified by |
|---|---|---|
| 3 replicas per component | StatefulSet/Deployment `replicas: 3` (bridge: 2) | `kubectl get pods` |
| Pods on different nodes | **required** anti-affinity, all components | `get pods -o wide` |
| Survives pod deletion | controllers recreate; quorums stay at 2/3 | [validation.md](validation.md) §1 |
| Survives eviction/drain | PDBs on 7 components | §3.3 |
| Survives node loss | 2/3 quorums + reschedule on 128 GiB nodes | [validation.md](validation.md) §3 |
| RabbitMQ split brain | quorum queues + `pause_minority` | §3.8 |
| Sentinel split brain | quorum 2 + **persisted config epoch** | §3.2 |
| Dragonfly split brain | 3 layers, one residual gap | §4 |
| No data loss under retry | no delivery limit + ack-last + WAL | §3.1 |

## 6. Residual risks (accepted, not hidden)

| Risk | Mitigation | Residual |
|---|---|---|
| Direct-pod-IP write to a minority master (§4) | application configuration + code review only; NetworkPolicy is **not** deployed ([networking.md](networking.md) §5.2) | possible for any client that ignores the Service |
| Namespace is shared, no traffic isolation | ClusterIP-only Services, two Routes, no pod-IP usage in code | lateral traffic from other tenants is unrestricted |
| 2 simultaneous node losses | none — no quorum system survives it | **cluster unavailable** |
| Dragonfly eviction at `maxmemory` | `noeviction` makes it loud, not silent | writes blocked until TTL frees memory |
| Sentinel epoch reset if the PVC is also lost | 1Gi PV, treated as critical data | needs a manual epoch check on restore |
| `sentinel-bridge` lease split during a partition | only one lease holder acts; both watch the same label | ≤15s of ambiguous selector |

## 7. Verification commands

```bash
# 1. every component has the required replica count
kubectl -n fire-watch get deploy,sts -o custom-columns='KIND:.kind,NAME:.metadata.name,WANT:.spec.replicas,READY:.status.readyReplicas'

# 2. required anti-affinity really spread the pods (no two same-name pods on one node)
kubectl -n fire-watch get pods -o wide --no-headers \
  | awk '{print $1, $7}' | sed -E 's/-[a-z0-9]+-[a-z0-9]+ / /' | sort | uniq -d
# expect: no output

# 3. PDBs exist and are honoured
kubectl -n fire-watch get pdb

# 4. no delivery limit on the ingest queue (D1)
kubectl -n fire-watch exec rabbitmq-0 -- rabbitmqctl list_queues name arguments | grep q.event.ingest
# expect: no x-delivery-limit

# 5. Sentinel epochs agree and are non-zero (D2)
for i in 0 1 2; do kubectl -n fire-watch exec sentinel-$i -- \
  redis-cli -p 26379 SENTINEL CKQUORUM fire-watch-dragonfly; done

# 6. Sentinel can authenticate (D5)
kubectl -n fire-watch exec sentinel-0 -- redis-cli -p 26379 \
  SENTINEL MASTER fire-watch-dragonfly | grep -E 'flags|num-slaves'
# expect: flags master, num-slaves 2   (not "s_down")

# 7. partition handling and eviction policy are what we think
kubectl -n fire-watch exec rabbitmq-0 -- rabbitmqctl environment | grep partition
kubectl -n fire-watch exec dragonfly-0 -- redis-cli -p 6379 CONFIG GET maxmemory-policy

# 8. exactly one bridge holds the lease (D4)
kubectl -n fire-watch get lease -o wide

# 9. the master-pointing Service has exactly one endpoint
kubectl -n fire-watch get endpoints dragonfly -o wide
```

Drill coverage is in [validation.md](validation.md) §1–§6; this document supplies the acceptance criteria those drills assert.
