# Monitoring

`k8s/base/60-monitoring.yaml` — 3 ServiceMonitors and 4 PrometheusRules containing **21 alerts**. Requires the OpenShift monitoring stack or `kube-prometheus-stack` (for the `monitoring.coreos.com/v1` CRDs) plus `kube-state-metrics` and cAdvisor.

The lab overlays **delete** all of it — a lab has neither the CRDs nor a Prometheus, so the objects would be rejected or inert ([kustomize.md](kustomize.md) §3).

## 1. Why a redis-exporter exists

Three of the four components expose Prometheus metrics natively. Sentinel does not — it speaks RESP on 26379 and nothing else. `k8s/base/33-redis-exporter.yaml` runs `oliver006/redis_exporter` in Sentinel mode to translate.

| Component | Metrics source | Port |
|---|---|---|
| RabbitMQ | `rabbitmq_prometheus` plugin | 15672 `/metrics` |
| Dragonfly | native | **9222** `/metrics` |
| Sentinel | `redis-exporter` | 9121 `/metrics` |
| Workloads | kube-state-metrics + cAdvisor | n/a |

Dragonfly also serves `/metrics` on 6379, but that is HTTP over the RESP port and scrapers fail with `Received HTTP/0.9 when not allowed`. Scrape the admin port.

## 2. Metric names that were wrong

Two names in the original design documents **do not exist in Dragonfly**, so the alerts built on them would have been silently dead forever:

| Claimed | Reality | Corrected to |
|---|---|---|
| `dragonfly_connected_replicas` | not a metric | `count(dragonfly_connected_replica_lag_records)` — the lag metric is emitted once per connected replica, so counting its series gives the replica count |
| `dragonfly_snapshot_load_num_errors` | not a metric | `dragonfly_failed_restores_total` |

Both were verified against `src/server/metrics.cc` in the Dragonfly source. This is exactly the kind of error that only surfaces when the alert *doesn't* fire during an outage, which is the worst possible time to discover it.

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
| `RabbitMQAuditQueueFilling` | audit queue > 70% of its 64GiB cap for 30m | warning |

### Dragonfly

| Alert | Condition | Severity |
|---|---|---|
| `DragonflyReplicasMissing` | connected replicas < 2 for 2m | warning |
| `DragonflyReplicaLag` | lag records > 10 000 for 5m | warning |
| **`DragonflyEvictingKeys`** | `increase(dragonfly_evicted_keys_total[5m]) > 0` | **critical, no delay** |
| `DragonflyMemoryNearLimit` | working set / limit > 0.90 for 10m | warning |
| `DragonflyRestoreFailures` | `increase(dragonfly_failed_restores_total[1h]) > 0` | critical |
| `DragonflySnapshotStuck` | `save_in_progress == 1` for 30m | warning |
| `DragonflyWriteGateClosed` | pod label `writable=false` for 5m | critical |

`DragonflyEvictingKeys` is the most important alert in the set. `maxmemory-policy=noeviction` is mandatory ([fault-tolerance.md](fault-tolerance.md) §3.9), so eviction should be *impossible*. If this ever fires, either the flag was overridden or you are scraping a different instance — and events have already been silently deleted.

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
| `PodPending` | Pending for 15m — usually a Dragonfly that cannot reschedule | warning |

## 4. What is deliberately not alerted on

| Signal | Why not |
|---|---|
| `used_memory / maxmemory` on Dragonfly | `maxmemory` is sized to the working set on purpose, so this sits near 1.0 permanently. Pure noise. Eviction is the real signal. |
| Dragonfly snapshot age via file mtime | requires a custom exporter. `dragonfly_save_in_progress` + `dragonfly_failed_restores_total` cover the same failures without one. |
| Per-pod restart counts on RabbitMQ | restart-loops are already caught by `PodCrashLooping`; RabbitMQ restarts legitimately on rolling updates |

## 5. Verify before trusting it

Metric names from exporters are version-dependent. Check the two exporter-backed families against the running pods:

```bash
# Sentinel exporter metric names
kubectl -n fire-watch exec deploy/redis-exporter -- \
  wget -qO- localhost:9121/metrics | grep -E '^redis_sentinel'

# Dragonfly: confirm the two corrected names are present
kubectl -n fire-watch exec dragonfly-0 -- \
  wget -qO- localhost:9222/metrics | grep -E 'failed_restores_total|replica_lag_records|evicted_keys_total'

# Prometheus picked up the rules
kubectl -n fire-watch get prometheusrule
kubectl -n fire-watch get servicemonitor
```

If a `redis_sentinel_*` rule reports no data, the exporter version has different names — adjust the rule, not the exporter.
