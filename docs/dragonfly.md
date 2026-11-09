# DragonflyDB

Primary datastore. 1 master + 2 replicas, manifests only, no Dragonfly operator.

> **Blocking finding — read [persistence-and-durability.md](persistence-and-durability.md) §3 first.**
> DragonflyDB has **no AOF implementation**. Upstream is explicit: *"Currently, Dragonfly does not support AOF."* The `AGENTS.md` requirement "Dragonfly shall use AOF feature" cannot be met as written. Only forkless point-in-time snapshots exist. Durability therefore has to come from RabbitMQ.

## 1. Topology

| Item | Value |
|---|---|
| Image | `docker.dragonflydb.io/dragonflydb/dragonfly:<verified-tag>@sha256:<digest>` — resolve the tag and pin the digest before committing |
| Kind | `StatefulSet`, 3 replicas, `podManagementPolicy: OrderedReady` |
| Service (headless) | `dragonfly-headless` — `clusterIP: None`, gives stable per-pod DNS |
| Service (client) | `dragonfly` — ClusterIP, **selector managed by Sentinel**, see [redis-sentinel.md](redis-sentinel.md) |
| Ports | 6379 client + replication, 9222 admin + Prometheus |
| Data | `data` volumeClaimTemplate, **64Gi** (2 generations of a ~29 GiB working set, [sizing.md](sizing.md) §3.5) |
| Roles at bootstrap | `dragonfly-0` master, `-1` and `-2` replicas |

`OrderedReady` is required: replicas must not attempt replication before `dragonfly-0` exists.

## 2. Replication

Primary-replica, wire-compatible with Redis (`REPLICAOF`, `ROLE`, `INFO replication`).

- Roles are assigned **at runtime**, not baked into the manifest. A restarted pod must not hard-code a master that may since have been demoted.
- The Sentinel controller issues `REPLICAOF <master-host> 6379` on replicas and `REPLICAOF NO ONE` on promotion. Rationale in [redis-sentinel.md](redis-sentinel.md) §4.
- `--replica_partial_sync` defaults to `true`: a short blip does not force a full resync. If resyncs are frequent, raise `--shard_repl_backlog_time_ms` (default `5000`) or `--shard_repl_backlog_max_bytes`.
- `--replica_delete_expired=false` stops replicas from diverging on TTL expiry (the default `true` deletes on the replica's own read path).
- `--replica_priority` (default `100`) is published in `INFO`; Sentinel ranks failover candidates by it. Give the preferred failover target a lower value.

## 3. Split-brain protection

Requirement: "always find 2 replicas before accepting writes". Dragonfly ships **no** `min-replicas-to-write` equivalent (that flag belongs to Redis/Valkey). Split brain is enforced in three layers.

### Layer 1 — mode

Keep `--cluster_mode` at its default `emulated`. Multi-shard cluster mode is a data plane only: no control plane, no automatic failover, slot ownership pushed via `DFLYCLUSTER CONFIG` by an external manager. Wrong tool for 3 fully-replicated nodes.

### Layer 2 — write guard (master side)

Sidecar `dragonfly-write-guard`, one per Dragonfly pod, polls local `INFO REPLICATION`:

```
role:master
connected_slaves:N
slave0:ip=...,state=stable_sync,lag=0
```

Logic:

- If `role:master` and `connected_slaves < 2` continuously for `FW_WRITE_GUARD_GRACE` (default 30s) → mark the master **degraded**.
- On degraded: set pod label `fire-watch.io/writable=false`, emit `fw_dragonfly_write_guard_tripped=1`, publish to `fw:ch:alerts`. `sentinel-bridge` then removes that pod from the `dragonfly` Service, and workers see an empty endpoint list and stop writing ([workers.md](workers.md) §5).
- Recover when `connected_slaves >= 2` for `FW_WRITE_GUARD_RECOVER` (default 60s).

The guard never writes to Dragonfly. It cannot: the state it would write lives in the very database it is judging, so it persists the state as a **pod label** instead. The earlier design read `fw:write_guard:state` from Dragonfly, which nothing ever wrote, leaving the guard inert ([fault-tolerance.md](fault-tolerance.md) §3.7).

Accepted limitation, stated plainly: this is a **routing** gate, not a storage-engine gate. A client that bypasses the Service and talks to a pod IP can still write. Workers and gateways are therefore forbidden from using pod IPs — enforced by review, since K8s cannot enforce it.

### Layer 3 — failover guard (Sentinel side)

`quorum = 2` with majority promotion. A partitioned-off master cannot be promoted while the side holding 2 of 3 sentinels elects a new master; the old master is demoted via `REPLICAOF` on rejoin and discards its divergent tail. Full walkthrough in [redis-sentinel.md](redis-sentinel.md) §6.

## 4. Persistence

### 4.1 What exists upstream

| Mechanism | Supported | Notes |
|---|---|---|
| AOF | **No** | Not implemented. Documented as such upstream. |
| RDB (`.rdb`) | Yes | `--df_snapshot_format=false` |
| Dragonfly snapshot (`.dfs`) | Yes, default | Forkless, sharded; `--df_snapshot_format=true` |
| `snapshot_cron` | Yes (>= 1.7.1) | cron-driven automatic snapshots |
| `SAVE` / `BGSAVE` | Yes | manual trigger |
| Save on shutdown | Yes | active whenever `--dbfilename` is non-empty |
| S3 / MinIO destination | Yes, preview | `--dir=s3://bucket/prefix` |

`save_schedule` is deprecated; `snapshot_cron` replaces it.

### 4.2 Configured behaviour

```yaml
args:
  - --logtostderr
  - --dir=/data
  - --dbfilename=firewatch-{timestamp}
  - --df_snapshot_format=true
  - "--snapshot_cron=0 * * * *"
  - --maxmemory=36gb
  - --maxmemory-policy=noeviction
  - --proactor_threads=4
  - --replica_delete_expired=false
  - $(DRAGONFLY_AUTH_ARGS)      # --requirepass=<from Secret>
  - --port=6379
  - --admin_port=9222
```

Two settings here are correctness requirements, not tuning:

- **`--requirepass` is mandatory.** `sentinel auth-pass` and the worker/gateway clients all authenticate. Without it Sentinel's `PING` is rejected, the master is permanently considered down, and failover loops without ever converging. The password is injected from `Secret/fire-watch-dragonfly`, never inlined ([fault-tolerance.md](fault-tolerance.md) §3.6).
- **`--maxmemory-policy=noeviction` is mandatory.** Any other policy silently deletes live events when `maxmemory` is reached, breaking the 7-day retention promise with no signal. With `noeviction`, writes fail loudly, the worker requeues, and the backlog becomes visible — exactly what the audit WAL exists to recover ([fault-tolerance.md](fault-tolerance.md) §3.9).

The cron expression must be **quoted** in YAML; unquoted, `0 * * * *` is parsed as separate list items or a comment.

`{timestamp}` makes each hourly snapshot a distinct, lexicographically sortable file. `maxmemory=36gb` against a 48Gi pod limit holds the 7-day working set (10.08M events at 2 048 B ≈ 24.5 GiB) plus headroom for one weekly 1-hour storm at 500/s (+4.7 GiB); derivation in [sizing.md](sizing.md) §3.4.

**`maxmemory` must never be set below the working set.** Eviction would silently break the 7-day retention promise; the audit WAL is a recovery net, not a licence to evict.

### 4.3 "Refresh the snapshot hourly so restart is fast"

This part **is** achievable: `snapshot_cron` writes a complete fresh snapshot every hour, so restart never replays a growing history. The original intent (append log + periodic compaction) is exactly AOF+rewrite, which does not exist here. Restart-speed goal: met. AOF durability goal: not met — see §4.4.

### 4.4 RPO consequence

| Design | Loss window |
|---|---|
| Snapshots only | up to 1 hour → **violates R1** |
| RabbitMQ as WAL ([persistence-and-durability.md](persistence-and-durability.md) §4) | 0 for events acked by RabbitMQ, within a 24h window |

Note the two different promises: 7-day *retention* is a RAM property, 24-hour *recoverability* is a WAL property. Details in [persistence-and-durability.md](persistence-and-durability.md) §4.4.

## 5. Anti-affinity, PDB, probes

```yaml
affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels: { app.kubernetes.io/name: dragonfly }
        topologyKey: kubernetes.io/hostname
```

- `PodDisruptionBudget: minAvailable: 2`.
- No HPA. A 4th replica adds no write durability and creates Sentinel ambiguity about the best promotion candidate.

| Probe | Command | Notes |
|---|---|---|
| startup | `redis-cli -p 6379 ping` every 5s, `failureThreshold: 60` | full-sync of a ~30 GiB dataset takes well under a minute; 60 is generous |
| readiness | `ping` **and**, on replicas, `INFO replication` showing `master_link_status:up` | a replica mid-full-sync must not count as healthy |
| liveness | `ping`, `initialDelaySeconds: 60`, `failureThreshold: 6` | never assert `ROLE` — a legitimate failover would restart the pod |

## 6. Storage and pruning

- PV **64Gi** `ReadWriteOnce`, `volumeMode: Filesystem` (local NVMe preferred).
- Each sharded `.dfs` set is ≈ the working set with storm headroom (≈29 GiB). Retain **2** generations → ~58 GiB, leaving ~6 GiB of headroom on a 64Gi volume. Derivation in [sizing.md](sizing.md) §3.5.
- Hourly snapshot write ≈ 29 GiB / 3 600 s ≈ **8.2 MiB/s sustained** — comfortable for a network PV, trivial for NVMe.
- Retain **2** generations only. More buys nothing because `q.audit.events` now covers the **full 7-day window** ([sizing.md](sizing.md) §3.6), so a corrupt snapshot is recoverable from the WAL.
- Sidecar `snapshot-pruner` keeps the newest 2 `firewatch-*.dfs*` sets, runs every 10 min, and never deletes the current hour's set.
- OpenShift runs the pod with a random UID: mount with `fsGroup` and make `/data` group-writable, or snapshot writes fail. Verify on a real pod before freezing the manifest.
- Pod sizing: request 500m CPU / 8Gi, limit 4 CPU / 48Gi. **Node RAM must be ≥ 128 GiB so that a survivor can host two Dragonfly pods** after a node loss (`2 × 48Gi + ~7Gi`); with smaller nodes an evicted Dragonfly pod stays Pending ([sizing.md](sizing.md) §3.3).
- Compression is a 3x lever if 128 GiB nodes are unacceptable: gzip the value in the worker and the working set drops to ≈9.6 GiB ([sizing.md](sizing.md) §2.4).

## 7. Monitoring

Metrics on `--admin_port=9222` (`/metrics`).

| Signal | Alert condition |
|---|---|
| `dragonfly_connected_replicas` | `< 2` for 2 min |
| `dragonfly_connected_replica_lag_records` | `> 10000` for 5 min |
| `dragonfly_evicted_keys_total` | **`> 0` — page immediately**, retention is already broken ([sizing.md](sizing.md) §3.3) |
| container RSS / limit (from cAdvisor, `container_memory_working_set_bytes`) | `> 0.90` — risk of OOMKill during a snapshot |
| newest `.dfs` age (from pruner sidecar) | `> 90 min` |
| snapshot load errors on restart | `> 0` |
| `fw_dragonfly_write_guard_tripped` | `== 1` for 1 min |

Deliberately **not** alerting on `used_memory / maxmemory`: `maxmemory` is sized to the working set by design, so that ratio sits near 1.0 permanently and would be pure noise. Eviction, not utilisation, is the failure signal here.

## 8. Verification checklist

```bash
kubectl -n fire-watch exec dragonfly-0 -- redis-cli -p 6379 INFO REPLICATION
# role:master, connected_slaves:2, both state=stable_sync

kubectl -n fire-watch exec dragonfly-1 -- redis-cli -p 6379 INFO REPLICATION
# role:slave, master_link_status:up

kubectl -n fire-watch exec dragonfly-0 -- redis-cli -p 6379 SET probe 1
kubectl -n fire-watch exec dragonfly-2 -- redis-cli -p 6379 GET probe      # -> 1

kubectl -n fire-watch exec dragonfly-0 -- redis-cli -p 6379 DBSIZE         # ~10.08M at 1000/min × 7d
kubectl -n fire-watch exec dragonfly-0 -- redis-cli -p 6379 MEMORY USAGE fw:evt:<id>   # ≈ 2 266 B model
kubectl -n fire-watch exec dragonfly-0 -- ls -la /data                     # <= 2 snapshot sets
kubectl -n fire-watch get pods -l app.kubernetes.io/name=dragonfly -L fire-watch.io/writable
```

Scenario rehearsal in [validation.md](validation.md).
