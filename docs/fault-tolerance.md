# Fault Tolerance & Split-Brain Audit

Verification of every component against R3 (**100% H/A, 3 replicas on different nodes, survives eviction/deletion**) and R4 (**handles split brain**).

**Verdict up front: the design was *not* 100% fault tolerant as first written.** Nine defects were found. One of them silently lost data. All were fixable in configuration, not architecture, and the fixes are applied in the sections below. Since then the message broker was replaced by a Valkey stream ([roadmap.md](roadmap.md) §4); the defects that were broker-specific are restated for the stream (D1, D8).

## 1. Verdict summary

| Component | 3 replicas | Different nodes | Survives 1 pod loss | Survives 1 node loss | Split brain | Verdict |
|---|---|---|---|---|---|---|
| http-ingress | ✓ | ✓ required | ✓ nginx retries | ✓ | n/a stateless; refuses with 503 when Valkey refuses | **PASS** |
| Ingest stream (`fw:stream:ingest`) | lives in Valkey | — | ✓ PEL + `XAUTOCLAIM` | ✓ replicated | ✓ inherits Valkey's gates | **PASS** after §3.1, §3.8 |
| Valkey | ✓ | ✓ required | ✓ (writes continue with 1 replica) | ✓ (master failover) | ✓ engine + `WAIT` + routing gate | **PASS** after §4 (residual window stated) |
| Redis Sentinel | ✓ | ✓ required | ✓ | ✓ 2/3 quorum | ✗ **broken by `emptyDir`** | **FAIL** → fixed §3.2 |
| Workers | ✓ | ✓ required | ✓ | ✓ | n/a stateless | **PASS** after §3.3 |
| WS gateway | ✓ | ✗ **preferred only** | ✓ | ✗ possible total outage | n/a stateless | **FAIL** → fixed §3.4 |
| Frontend | ✓ | ✗ **preferred only** | ✓ | ✗ possible total outage | n/a stateless | **FAIL** → fixed §3.4 |
| sentinel-bridge | ✗ **1 replica** | ✗ | — | — | — | **FAIL** → fixed §3.5 |

## 2. Defects found

| # | Defect | Severity | Impact |
|---|---|---|---|
| D1 | A delivery limit on ingest retries (first `x-delivery-limit: 10` on the former broker queue) | **CRITICAL** | **Silent data loss.** A Valkey outage longer than N retries dead-letters events that were never written. Directly violates R1. |
| D2 | Sentinel uses `emptyDir` for `sentinel.conf` | **CRITICAL** | Restarted sentinels lose `config epoch`, `known-sentinels` and `known-replicas`. Two restarted sentinels can authorise a failover with a stale epoch → **the exact split-brain the quorum was meant to prevent**. |
| D3 | WS gateway and frontend use *preferred* anti-affinity | **HIGH** | All 3 pods can land on one node. Losing that node takes the component to **zero replicas** — not 100% H/A. |
| D4 | `sentinel-bridge` is 1 replica | **HIGH** | Single point of failure for the `valkey` Service selector and `REPLICAOF` application. If it dies, no client can find the master after the next failover. |
| D5 | No `--requirepass` on Valkey, yet Sentinel and clients use `auth-pass` | **HIGH** | Sentinel cannot authenticate → it cannot monitor the master, so **failover never happens**. Or, if auth is skipped, Valkey is unauthenticated. |
| D6 | `fw:write_guard:state` is read by workers but written by **nothing** | **HIGH** | The write-guard publishes to `fw:ch:alerts` and sets a pod label, but never sets the key workers check. The guard is therefore inert. Fixed by moving the gate to `Endpoints/valkey` (§3.7), and now reinforced by the engine's `min-replicas-to-write` |
| D7 | No `PodDisruptionBudget` for workers, gateway, frontend, sentinel, bridge | **MEDIUM** | `kubectl drain` can evict all replicas of a component simultaneously. R3 explicitly requires surviving eviction. |

Two additional correctness issues:

| # | Issue | Impact |
|---|---|---|
| D8 | Ingest entries can be orphaned or dropped by the transport (first: the former broker's partition handling left at its default; now: a stream consumer that dies with entries pending, or a `MAXLEN` trim) | Pending entries of a dead worker would never be processed; a trim deletes unprocessed entries. |
| D9 | `maxmemory-policy` never set for Valkey | If it is not `noeviction`, hitting `maxmemory` **silently evicts live events — and ingest stream entries** — and quietly breaks the 7-day retention promise. |

## 3. Fixes

### 3.1 D1 — no delivery limit, anywhere

The worker retries a failed entry from its pending list **forever** ([workers.md](workers.md) §6.2). There is no counter on `XPENDING`'s delivery count and nothing that moves an entry to `fw:stream:dlq` because it was tried too often. Rationale:

- The only legitimate reason to dead-letter an ingest entry is that it can **never** succeed (unparseable `evt`, invalid envelope, malformed ID), and that is an explicit decision in the handler — it does not need a counter.
- Every other failure is *transient* (Valkey down, master failover, `NOREPLICAS`). A delivery limit converts those into permanent loss.
- A poison entry that would loop is prevented by the watermark and idempotency design ([event-lifecycle.md](event-lifecycle.md) §5): a redelivery returns `applied 0` and is removed.
- An unexpected exception is classed as `retry`, not `dlq`.
- Bound the risk instead with the `IngestStreamPendingStuck` and `IngestDeadLetteredEvents` alerts ([monitoring.md](monitoring.md)).

**Rule for the implementation:** `dlq` only for entries that can never succeed. Every other failure path leaves the entry pending.

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
| Valkey | `minAvailable: 2` |
| Sentinel | `minAvailable: 2` |
| http-ingress | `minAvailable: 2` |
| Workers | `minAvailable: 1` |
| WS gateway | `minAvailable: 2` |
| Frontend | `minAvailable: 2` |
| sentinel-bridge | `minAvailable: 1` |

`minAvailable: 1` for workers is deliberate: workers are a throughput resource, not a quorum, and pinning them at 2 would block legitimate single-pod maintenance. For the gateway, ingress and frontend, 2 is the right floor because the service must stay reachable during a drain. Valkey at 2 keeps a drain from ever taking the master below one replica, i.e. below `min-replicas-to-write 1`.

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

Consequence to accept: after a node loss the third pod stays `Pending` (no second copy can share a node). That is the same behaviour as Valkey and Sentinel, and it is the correct one — a component that has silently collapsed onto one node is not H/A.

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

Alternative rejected: 1 replica with a `restartPolicy: Always` Deployment. A restart takes seconds to a minute, and during that window **no component can determine the master**, so writes stop even if Valkey is perfectly healthy. That is not 100% H/A.

### 3.6 D5 — Valkey authentication

```yaml
args:
  - --requirepass=$(VALKEY_PASSWORD)
```

`auth-pass` in `sentinel.conf` and the client password in http-ingress/workers/gateway must match. Without it Sentinel's `PING` is rejected and the master is considered permanently down — a failover loop that never converges.

The password comes from `Secret/fire-watch-valkey`, injected via `valueFrom`, never inlined.

### 3.7 D6 — fix the write-guard contract

The guard cannot write to Valkey (that is the very component it is judging), so the state must not live there. Corrected design:

| Signal | Written by | Read by | Mechanism |
|---|---|---|---|
| `Endpoints/valkey` (zero addresses) | `sentinel-bridge` | ingress, workers, gateway | cluster state, no Valkey dependency |
| `fire-watch.io/writable` label | `sentinel-bridge` | humans, alerts | pod patch |

The bridge evaluates the guard itself: it already reads `ROLE` on every pod each cycle, and the replica count is right there in the reply. It trips when the master has fewer than `FW_WRITE_GUARD_MIN_REPLICAS` (= **1**, must equal `min-replicas-to-write`) connected replicas. The separate per-pod `valkey-write-guard` sidecar was **removed** — it duplicated that polling, could only express a verdict as a label for someone else to interpret, and needed its own lab override. Fewer moving parts, same guarantee.

Writers then rely on the **Service** rather than on a key: no ready endpoint → the write fails → ingress answers 503, the worker leaves the entry pending.

This removes the broken `fw:write_guard:state` key entirely, and removes the
circularity of a degraded Valkey being asked to report its own degradation.

**Valkey adds an independent signal**: `min-replicas-to-write 1` makes the
datastore itself refuse the write when the master sees no replica. It is not a
replacement for the endpoint gate — an engine flag cannot stop stale reads — but it is
the one control that survives a client that ignores the Service (§4).

### 3.8 D8 — nothing pending is orphaned, nothing is trimmed

- Every worker runs `XAUTOCLAIM fw:stream:ingest workers <pod> 60000 0-0 COUNT 32` every 30 s, so entries pending on a dead, evicted or wedged consumer for more than 60 s are taken over ([workers.md](workers.md) §6.3).
- A fresh process does one `XREADGROUP ... 0` pass, so a pod that restarts under the same name finishes its own pending entries.
- The consumer group and its PEL are part of the stream key: they are replicated and in every pod's AOF, so a Valkey failover or restart keeps them.
- The stream is **never** trimmed with `MAXLEN`/`MINID`; entries leave only through `XACK`+`XDEL` after the fold write (one `MULTI`).
- Stale consumers (0 pending, idle > 1 h) are removed with `XGROUP DELCONSUMER`; a consumer that owns entries is never removed.

The broker-era fix (`pause_minority` partition handling) is obsolete with the broker gone. The stream's split-brain behaviour is Valkey's (§4).

### 3.9 D9 — Valkey eviction policy

```ini
maxmemory-policy noeviction
```

Eviction must be an **incident**, not a silent behaviour. With `noeviction`, writes fail loudly when `maxmemory` is reached: the ingress answers 503 (nginx buffers), the worker leaves entries pending, and the backlog becomes visible. Any other policy quietly deletes records — or ingest stream entries that were already answered 202 — and breaks retention without any signal.

## 4. Split brain: what the engine closes, and what `min-replicas-to-write 1` accepts

Valkey's write gate has three layers: the engine (`min-replicas-to-write 1`), the writers (`WAIT 1` before answering), and routing (`sentinel-bridge` withdrawing the `valkey` endpoint). Sentinel needs 2 of 3 sentinels to agree before it fails over.

| Scenario | Behaviour |
|---|---|
| One replica lost | Master still sees 1 replica: writes continue, `WAIT 1` is satisfied by the other replica. `ValkeyReplicaDown` warns. |
| Master partitioned from **both** replicas, Sentinel quorum on the majority side | Master sees 0 replicas → **engine refuses** every write (`NOREPLICAS`); guard trips, master removed from the `valkey` Service; new master elected. **No write is accepted on the minority side.** |
| Master partitioned with **one** replica on its side, the other replica + 2 sentinels on the other side | The majority sentinels promote the third pod. The old master still sees 1 replica, so the engine **accepts** writes from clients that have not re-resolved, until (a) its replica's lag exceeds `min-replicas-max-lag 10` — it cannot, the replica is on its side — or (b) the bridge withdraws its endpoint / clients re-resolve via Sentinel, or (c) it is demoted when the partition heals. Writes it accepted in that window are **discarded** on demotion. |
| A client that ignores the Service and **reads** from the old master's pod IP | **The read succeeds and may be stale.** Reads are not gated by `min-replicas-to-write` |

### 4.1 The accepted window, and what bounds it

The third row is the honest cost of `min-replicas-to-write 1`. What bounds it:

- **`WAIT 1`.** A producer only answers success after a replica acknowledged. On the old master's side, the replica that acknowledges is the one that will be resynced from the new master — so those events are still lost on demotion. `WAIT 1` therefore does **not** close this window; it closes the simpler "master dies before replicating" case.
- **sentinel-bridge endpoint withholding.** The bridge follows Sentinel's view of the master. Once the majority sentinels have promoted the third pod, the bridge points `Endpoints/valkey` at it, so every client that connects through the Service — or re-resolves through Sentinel — writes to the new master. The window is the time until ingress/workers/gateway re-resolve (they do on any connection error, `READONLY`, or reconnect).
- **nginx retry on 503.** Ingress requests that fail during the switch are retried and land on the new master.
- **Worker redelivery.** A worker whose fold write went to the old master did not `XACK` on the new master, so the entry (which the new master has, if it was 202'd before the partition) is still pending there and is processed again.

With the earlier setting of 2 this window did not exist — an old master with one replica would have refused — but losing **either** replica stopped all ingest. The decision was to keep ingest running through a single Valkey pod loss and accept this bounded partition window ([roadmap.md](roadmap.md) §4).

```ini
# k8s/base/20-valkey-config.yaml
min-replicas-to-write 1
min-replicas-max-lag 10
```

The lab sets `min-replicas-to-write`, the bridge's `FW_WRITE_GUARD_MIN_REPLICAS` and `FW_WAIT_REPLICAS` to `0`, because one pod has no replica and the guard would refuse every write forever ([kustomize.md](kustomize.md) §3.4).

### 4.2 Why the routing gate is still needed

`min-replicas-to-write` only stops WRITES. The `Endpoints/valkey` gate in
`sentinel-bridge` also stops stale READS, steers clients to the new master in the §4.1
window, and makes the condition visible as a pod label and an empty Service — which
is what the `ValkeyServiceEndpointGateClosed` alert keys on ([workers.md](workers.md) §5).

### 4.3 Is the Sentinel layer sufficient?

Sentinel with quorum 2 and a persisted epoch (§3.2) gives:

| Partition | Sentinel outcome | Write outcome |
|---|---|---|
| 1 sentinel isolated | quorum 2 remains, no failover | unaffected |
| Master + 1 sentinel isolated on the same node | quorum 2 on the majority side, failover | old master sees 0 replicas → refuses; routed to new master |
| 2 nodes lost | quorum lost (1 of 3), **no failover** | if the master survived, it sees 0 replicas → refuses; writes stop until a second node returns |
| Whole cluster restart | epoch persisted, no spurious failover | resumes with the same master |

The 2-node-loss row is important and correct: **the cluster is not available with 2 of 3 nodes down.** No quorum system is. "100% H/A" here means surviving any *single* node loss, which it does.

## 5. What is genuinely 100% after the fixes

| Requirement | Mechanism | Verified by |
|---|---|---|
| 3 replicas per component | StatefulSet/Deployment `replicas: 3` (bridge: 2) | `kubectl get pods` |
| Pods on different nodes | **required** anti-affinity, all components | `get pods -o wide` |
| Survives pod deletion | controllers recreate; Sentinel quorum stays at 2/3; pending entries are claimed | [validation.md](validation.md) §1 |
| Survives eviction/drain | PDBs on every component | §3.3 |
| Survives node loss | 2/3 quorum, writes continue with 1 replica, reschedule on 128 GiB nodes | [validation.md](validation.md) §3 |
| Ingest transport split brain | the stream is a Valkey key — same gates as the records | §4 |
| Sentinel split brain | quorum 2 + **persisted config epoch** | §3.2 |
| Valkey split brain | engine + `WAIT 1` + routing; isolated master refuses; one bounded window accepted | §4 |
| No data loss under retry | no delivery limit + remove-last + `XAUTOCLAIM` + no trim | §3.1, §3.8 |

## 6. Residual risks (accepted, not hidden)

| Risk | Mitigation | Residual |
|---|---|---|
| Old master keeps one replica during a partition and accepts writes (§4.1) | bridge endpoint withholding, client re-resolve, nginx retry, worker redelivery | writes from un-re-resolved clients in the window are discarded on demotion |
| Direct-pod-IP write to a minority master | application configuration + code review only; NetworkPolicy is **not** deployed ([networking.md](networking.md) §5.2) | refused if the master sees 0 replicas; accepted in the §4.1 case |
| All three Valkey PVCs lost | none in-system since the audit copies were dropped ([persistence-and-durability.md](persistence-and-durability.md) §4) | data loss; needs a volume backup |
| Namespace is shared, no traffic isolation | ClusterIP-only Services, two Routes, no pod-IP usage in code | lateral traffic from other tenants is unrestricted |
| 2 simultaneous node losses | none — no quorum system survives it | **cluster unavailable** |
| Valkey eviction at `maxmemory` | `noeviction` makes it loud, not silent | writes blocked (ingress 503) until TTL frees memory |
| Sentinel epoch reset if the PVC is also lost | 1Gi PV, treated as critical data | needs a manual epoch check on restore |
| `sentinel-bridge` lease split during a partition | only one lease holder acts; both watch the same label | ≤15s of ambiguous selector |

## 7. Verification commands

```bash
vk() { kubectl -n fire-watch exec valkey-0 -c valkey -- sh -c "valkey-cli -a \"\$VALKEY_PASSWORD\" --no-auth-warning $*"; }

# 1. every component has the required replica count
kubectl -n fire-watch get deploy,sts -o custom-columns='KIND:.kind,NAME:.metadata.name,WANT:.spec.replicas,READY:.status.readyReplicas'

# 2. required anti-affinity really spread the pods (no two same-name pods on one node)
kubectl -n fire-watch get pods -o wide --no-headers \
  | awk '{print $1, $7}' | sed -E 's/-[a-z0-9]+-[a-z0-9]+ / /' | sort | uniq -d
# expect: no output

# 3. PDBs exist and are honoured
kubectl -n fire-watch get pdb

# 4. the ingest group exists, nothing is stuck pending (D1, D8)
vk XINFO GROUPS fw:stream:ingest      # pending stays small and falls
vk XLEN fw:stream:dlq                 # expect 0

# 5. Sentinel epochs agree and are non-zero (D2)
for i in 0 1 2; do kubectl -n fire-watch exec sentinel-$i -- \
  valkey-cli -p 26379 SENTINEL CKQUORUM fire-watch-valkey; done

# 6. Sentinel can authenticate (D5)
kubectl -n fire-watch exec sentinel-0 -- valkey-cli -p 26379 \
  SENTINEL MASTER fire-watch-valkey | grep -E 'flags|num-slaves'
# expect: flags master, num-slaves 2   (not "s_down")

# 7. eviction policy and write gate are what we think (D9, §4)
vk CONFIG GET maxmemory-policy        # noeviction
vk CONFIG GET min-replicas-to-write   # 1

# 8. exactly one bridge holds the lease (D4)
kubectl -n fire-watch get lease -o wide

# 9. the master-pointing Service has exactly one endpoint
kubectl -n fire-watch get endpoints valkey -o wide
```

Drill coverage is in [validation.md](validation.md) §1–§6; this document supplies the acceptance criteria those drills assert.
