# Monitoring

`k8s/base/60-monitoring.yaml` — 3 ServiceMonitors and 4 PrometheusRules. Requires the
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
| RabbitMQ | `rabbitmq_prometheus` plugin | 15672 `/metrics` | queue depth, alarms, cluster membership |
| **redis-exporter** | `redis_exporter --is-sentinel` against `sentinel-0` | 9121 `/metrics` | quorum health, master flapping (`redis_sentinel_*`) |
| **valkey-exporter** | `redis_exporter` against the `valkey` Service | 9121 `/metrics` | memory, eviction, replica count, AOF (`redis_*`) |
| Workloads | kube-state-metrics + cAdvisor | n/a | readiness, restarts, memory limits |

**Why one is not enough.** A Sentinel-mode scrape never issues `INFO` against the data
node, so it produces no `used_memory`, no `evicted_keys` and no `connected_slaves`.
Rules built on those would have been permanently silent — the same class of defect as
the metric names in §2 that did not exist at all. A direct-mode scrape, conversely,
knows nothing about quorum.

`valkey-exporter` targets `redis://valkey:6379`, i.e. the ClusterIP Service whose
Endpoints `sentinel-bridge` keeps pointed at the CURRENT MASTER. That is what makes
every `redis_*` rule describe the master rather than an arbitrary replica.

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

### RabbitMQ

| Alert | Condition | Severity |
|---|---|---|
| `RabbitMQQueueBacklogGrowing` | ready messages non-decreasing for 15m | warning |
| `RabbitMQMemoryAlarm` | `rabbitmq_alarms_memory_used_watermark == 1` | critical |
| `RabbitMQDiskAlarm` | `rabbitmq_alarms_free_disk_space_watermark == 1` | critical |
| `RabbitMQUnackedWithNoConsumers` | unacked > 0 **and** consumers == 0 for 5m | critical |
| `RabbitMQClusterIncomplete` | fewer than 3 `rabbitmq_identity_info` targets | critical |
| `RabbitMQDeadLetteredEvents` | `q.event.ingest.dlq` non-empty for 5m | warning |
| `RabbitMQAuditQueueFilling` | audit queue > 70 % of its 64 GiB cap for 30m | warning |

### Valkey

| Alert | Condition | Severity |
|---|---|---|
| **`ValkeyWriteGateClosed`** | `redis_connected_slaves < 2` for 2m | **critical** |
| `ValkeyReplicaDown` | same signal held for 5m — names the lagging side | warning |
| `ValkeyReplicaLag` | master/replica offset gap > 100 MB for 5m | warning |
| **`ValkeyEvictingKeys`** | `increase(redis_evicted_keys_total[5m]) > 0` | **critical, no delay** |
| `ValkeyOutOfMemory` | `used / max > 0.98` for 5m — writes are being refused | critical |
| `ValkeyAofWriteFailing` | `redis_aof_last_write_status == 0` for 5m | critical |
| `ValkeyAofRewriteFailing` | `redis_aof_last_bgrewrite_status == 0` for 1h | warning |
| `ValkeyMemoryNearLimit` | working set / pod limit > 0.90 for 10m | warning |
| `ValkeyServiceEndpointEmpty` | pod label `writable=false` for 5m | critical |

Two of these deserve a note:

**`ValkeyWriteGateClosed` is the engine telling you it is refusing writes.** With
`min-replicas-to-write 2`, the master returns `NOREPLICAS` and the worker correctly
`nack`s. Events accumulate in RabbitMQ — which is the intended behaviour, not a
failure. The alert exists so nobody spends that time debugging the worker
([valkey.md](valkey.md) §5).

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
| Per-pod restart counts on RabbitMQ | restart-loops are already caught by `PodCrashLooping`, and RabbitMQ restarts legitimately on rolling updates |
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
  | grep -E 'redis_connected_slaves|redis_evicted_keys_total|redis_aof_last_write_status|redis_memory_max_bytes'

# Prometheus picked up the rules
kubectl -n fire-watch get prometheusrule
kubectl -n fire-watch get servicemonitor

# the write gate's current state, which the alerts read second-hand
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 -a "$VALKEY_PASSWORD" \
  CONFIG GET min-replicas-to-write
```
