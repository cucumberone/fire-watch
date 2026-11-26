# Monitoring

`k8s/base/60-monitoring.yaml` — 2 ServiceMonitors (the two exporters) and 4 PrometheusRules (`fire-watch-stream`, `fire-watch-valkey`, `fire-watch-sentinel`, `fire-watch-workloads`). Requires the
OpenShift monitoring stack or `kube-prometheus-stack` (for the
`monitoring.coreos.com/v1` CRDs) plus `kube-state-metrics` and cAdvisor.

The lab overlays **delete** all of it — a lab has neither the CRDs nor a Prometheus,
so the objects would be rejected or inert ([kustomize.md](kustomize.md) §3).

---

## 1. Two exporters, because Valkey has no HTTP endpoint

Neither Sentinel nor Valkey exposes HTTP. Both speak RESP — Sentinel on 26379, Valkey
on 6379 — so an exporter has to translate. `k8s/base/33-redis-exporter.yaml` runs
**two** `oliver006/redis_exporter` Deployments, and the reason is not redundancy:

| Component | Metrics source | Port | Answers |
|---|---|---|---|
| **redis-exporter** | `redis_exporter --is-sentinel` against `sentinel-0` | 9121 `/metrics` | quorum health, master flapping (`redis_sentinel_*`) |
| **valkey-exporter** | `redis_exporter` against the `valkey` Service | 9121 `/metrics` | memory, eviction, replica count, AOF (`redis_*`), and the ingest/DLQ streams (`redis_stream_*`) |
| Workloads | kube-state-metrics + cAdvisor | n/a | readiness, restarts, memory limits |

**Why one is not enough.** A Sentinel-mode scrape never issues `INFO` against the data
node, so it produces no `used_memory`, no `evicted_keys` and no `connected_slaves`.
Rules built on those would have been permanently silent — the same class of defect as
the metric names in §2 that did not exist at all. A direct-mode scrape, conversely,
knows nothing about quorum.

`valkey-exporter` targets `redis://valkey:6379`, i.e. the ClusterIP Service whose
Endpoints `sentinel-bridge` keeps pointed at the CURRENT MASTER. That is what makes
every `redis_*` rule describe the master rather than an arbitrary replica.

`valkey-exporter` sets `REDIS_EXPORTER_CHECK_SINGLE_STREAMS="db0=fw:stream:ingest,db0=fw:stream:dlq"`,
which adds `redis_stream_length` and, per consumer group,
`redis_stream_group_messages_pending` for exactly those two keys. Single-stream mode
is used instead of a key pattern so the exporter never `SCAN`s the keyspace.

---

## 2. Metric names that were wrong

The Dragonfly-era rules named two metrics that Dragonfly does not emit, so those
alerts would have been dead forever. The Valkey rules avoid the same mistake by using
names `redis_exporter` is documented to produce — but the lesson stands:

| Claimed (Dragonfly) | Reality | Valkey-era rule uses |
|---|---|---|
| `dragonfly_connected_replicas` | not a metric; the lag metric is emitted once per replica | `redis_connected_slaves`, straight from `INFO replication` |
| `dragonfly_snapshot_load_num_errors` | not a metric | `redis_aof_last_write_status`, `redis_aof_last_bgrewrite_status` |

A wrong metric name does not fail loudly — it produces an alert that never fires. That
is discovered during an outage, which is the worst possible time. §5 exists to check
the names against a running exporter before trusting any rule.

---

## 3. The alert set

### Ingest stream (`fire-watch-stream`)

| Alert | Condition | Severity |
|---|---|---|
| `IngestStreamBacklogGrowing` | `redis_stream_length{stream="fw:stream:ingest"} > 0` and not falling for 15m | warning |
| `IngestStreamPendingStuck` | `redis_stream_group_messages_pending{stream="fw:stream:ingest",group="workers"} > 1000` for 10m | warning |
| `IngestDeadLetteredEvents` | `redis_stream_length{stream="fw:stream:dlq"} > 0` for 5m | warning |

The stream is never trimmed, so its length **is** the backlog: a stream that stays
non-empty and does not shrink means workers are down, not ready, or refused by Valkey.
A large pending count means entries were delivered but not acknowledged — workers are
failing writes and retrying, or a dead worker's entries have not been claimed yet
([workers.md](workers.md) §6). A non-empty DLQ means producers are sending entries
that can never succeed; inspect with `XRANGE fw:stream:dlq - +`.
### Valkey

| Alert | Condition | Severity |
|---|---|---|
| **`ValkeyWriteGateClosed`** | `redis_connected_slaves < 1` for 2m | **critical** |
| `ValkeyReplicaDown` | `redis_connected_slaves < 2` for 5m — one replica gone, writes still succeed | warning |
| `ValkeyReplicaLag` | master/replica offset gap > 100 MB for 5m | warning |
| **`ValkeyEvictingKeys`** | `increase(redis_evicted_keys_total[5m]) > 0` | **critical, no delay** |
| `ValkeyOutOfMemory` | `used / max > 0.98` for 5m — writes are being refused | critical |
| `ValkeyAofWriteFailing` | `redis_aof_last_write_status == 0` for 5m | critical |
| `ValkeyAofRewriteFailing` | `redis_aof_last_bgrewrite_status == 0` for 1h | warning |
| `ValkeyMemoryNearLimit` | working set / pod limit > 0.90 for 10m | warning |
| `ValkeyServiceEndpointEmpty` | pod label `writable=false` for 5m | critical |

Two of these deserve a note:

**`ValkeyWriteGateClosed` is the engine telling you it is refusing writes.** With
`min-replicas-to-write 1` and no replica connected, the master returns `NOREPLICAS`:
http-ingress answers 503 (nginx buffers and retries) and workers leave entries
pending. That is the intended behaviour, not a failure. The alert exists so nobody
spends that time debugging the ingress or the worker ([valkey.md](valkey.md) §5).

**`ValkeyReplicaDown` is only a warning.** With one replica left, writes and `WAIT 1`
still succeed; redundancy is reduced until the pod returns.

**`ValkeyEvictingKeys` should be impossible.** `maxmemory-policy noeviction` is
mandatory ([fault-tolerance.md](fault-tolerance.md) §3.9), so a non-zero eviction
counter means the flag was overridden or a different instance is being scraped — and
records have already been deleted silently. It pages on the first increment, with no
delay.

### Sentinel

| Alert | Condition | Severity |
|---|---|---|
| `SentinelMasterNotOk` | master status != 1 for 2m | critical |
| `SentinelQuorumAtRisk` | fewer than 2 sentinels reporting for 5m | critical |
| `SentinelFailoverFlapping` | > 2 role changes per hour | critical |
| `SentinelMasterCountWrong` | series count != 3 for 5m | warning |

### Workloads

| Alert | Condition | Severity |
|---|---|---|
| `WorkersNotReady` | `ready_replicas < 1` for 5m | critical |
| `PodCrashLooping` | > 3 restarts per 15m window | warning |
| `PodPending` | Pending for 15m — usually a Valkey that cannot reschedule | warning |

---

## 4. What is deliberately not alerted on

| Signal | Why not |
|---|---|
| `used_memory / maxmemory` as a *warning* threshold | `maxmemory` is sized to the working set on purpose, so this sits near 1.0 permanently. Pure noise. The useful signal is the REFUSAL (`ValkeyOutOfMemory`) and eviction |
| AOF file age via mtime | requires a custom exporter; `redis_aof_last_bgrewrite_status` covers the same failure |
| Per-consumer pending counts | the group total (`IngestStreamPendingStuck`) is the incident; one worker with pending entries is normal mid-batch |
| Record counts per condition | `ZCARD idx:event:all` is a capacity question ([sizing.md](sizing.md) §6), not an incident |

---

## 5. Verify before trusting it

Metric names from exporters are version-dependent. Check both exporter-backed families
against the running pods, and check that the names the rules reference actually exist:

```bash
# Sentinel family
kubectl -n fire-watch exec deploy/redis-exporter -- \
  wget -qO- localhost:9121/metrics | grep -E '^redis_sentinel'

# Valkey data family — confirm every name used in 60-monitoring.yaml
kubectl -n fire-watch exec deploy/valkey-exporter -- \
  wget -qO- localhost:9121/metrics \
  | grep -E 'redis_connected_slaves|redis_evicted_keys_total|redis_aof_last_write_status|redis_memory_max_bytes|redis_stream_length|redis_stream_group_messages_pending'

# Prometheus picked up the rules
kubectl -n fire-watch get prometheusrule
kubectl -n fire-watch get servicemonitor

# the write gate's current state, which the alerts read second-hand
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 -a "$VALKEY_PASSWORD" \
  CONFIG GET min-replicas-to-write
```
