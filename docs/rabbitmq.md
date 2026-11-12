# RabbitMQ

3-node RabbitMQ cluster, quorum queues, pods forced onto distinct nodes.

## 1. Image and topology

| Item | Value |
|---|---|
| Image | `docker.io/library/rabbitmq:4.1-management` (pin a digest in git) |
| Kind | `StatefulSet`, 3 replicas, `podManagementPolicy: Parallel` |
| Service (headless) | `rabbitmq-headless` — `clusterIP: None`, publishes not-ready addresses |
| Service (client) | `rabbitmq` — ClusterIP, **created separately**, not the StatefulSet service |
| Ports | 5672 AMQP, 15672 management, 4369 epmd, 25672 Erlang distribution |
| Data | `data` volumeClaimTemplate, 96Gi (see §6) |
| Nodename | `rabbit@$(POD_NAME).rabbitmq-headless.fire-watch.svc.cluster.local` (`RABBITMQ_USE_LONGNAME=true`) |

Long node names are mandatory. Short names (`rabbit@rabbitmq-0`) break after a pod reschedule because K8s DNS resolution is the only stable identity.

## 2. Cluster formation

Pods must **not** use DNS-based peer discovery in OpenShift: `/etc/hosts` is rewritten by the container runtime and reverse lookups break. Use the Kubernetes peer-discovery plugin.

```hcl
# enabled_plugins ConfigMap
[rabbitmq_management,rabbitmq_peer_discovery_k8s,rabbitmq_prometheus].
```

```ini
# rabbitmq.conf
cluster_formation.peer_discovery_backend   = k8s
cluster_formation.k8s.host                 = kubernetes.default.svc.cluster.local
cluster_formation.k8s.address_type         = hostname
cluster_formation.k8s.service_name         = rabbitmq-headless
cluster_formation.k8s.namespace            = fire-watch
cluster_formation.node_cleanup.only_log_warning = true
cluster_name                               = fire-watch
queue_master_locator                       = min-masters
load_definitions                            = /etc/rabbitmq/definitions.json

# Broker-assigned arrival timestamp — the cluster's ordering authority
message_interceptors.incoming.set_header_timestamp.overwrite = true
```

Notes:

- RabbitMQ 4.1 K8s plugin: **only the pod with the lowest ordinal may seed** a cluster. `rabbitmq-0` bootstraps; `-1` and `-2` join forever until it is reachable. This removes the parallel-formation race entirely, so no startup jitter is needed.
- `node_cleanup.only_log_warning = true` is required with persistent volumes. Forceful cleanup would permanently eject a temporarily unreachable node whose PVC still holds the quorum queue journals.
- RBAC: a `ServiceAccount` + `Role` granting `get/list/watch` on `pods` and `endpoints` in the namespace, bound via `RoleBinding`, referenced from the StatefulSet `serviceAccountName`.

## 3. Required configuration for no-data-loss

```ini
# rabbitmq.conf
default_queue_type                   = quorum
quorum_queue.property_equivalence.relaxed_checks_on_redeclaration = false
cluster_partition_handling           = pause_minority
consumer_timeout                     = 900000      # 15 min, do not lower
confirm_timeout                      = 15
channel_max                          = 2047
```

- `default_queue_type = quorum` guarantees any queue declared without `x-queue-type` is a quorum queue. Classic queues are non-replicated and cannot satisfy R1.
- `cluster_partition_handling = pause_minority` is required for split-brain safety on 3 nodes: a node that cannot reach a majority of peers **stops serving** rather than answering from a stale view ([fault-tolerance.md](fault-tolerance.md) §3.8). The default `ignore` would let an isolated node keep accepting publishes that can never reach a quorum queue.
- **No `x-delivery-limit` on `q.event.ingest`.** A delivery limit combined with `nack(requeue=true)` dead-letters events that were never written — silent data loss during any Valkey outage longer than 10 retries ([fault-tolerance.md](fault-tolerance.md) §3.1).
- Quorum queues need **2 of 3 members online** to accept publishes. Publishing is refused while only 1 node is up — this is the intended behaviour, not a bug.
- Publishers **must** use publisher confirms. A publish without a confirm can be silently dropped when the connection dies. Worker/gateway code must not `basic_publish` without `confirm.select`.

### 3.1 Broker timestamps are the ordering authority

```ini
message_interceptors.incoming.set_header_timestamp.overwrite = true
```

- Sets the AMQP `timestamp` property (seconds) **and** the `timestamp_in_ms` header (milliseconds) from the broker node clock as the message enters RabbitMQ.
- Built into RabbitMQ 3.12+; it replaces the archived `rabbitmq_message_timestamp` plugin (which only supported ≤ 3.11). Never enable both — two interceptors on `basic.publish` break channel creation for every AMQP 0-9-1 connection.
- `overwrite = true` is deliberate: **producers must not be able to influence ordering.** With `false`, a producer clock skew or a forged value could make an old CLOSE appear newer than a FATAL and retire an active alarm.
- Requeue and DLQ moves preserve properties and headers, so a redelivered message keeps its original timestamp — this is what makes the alert fold safe under retry.
- A republish does **not** preserve anything: it is a new publish and gets a new timestamp. Any code that moves messages between queues must copy `timestamp_in_ms` into `x-original-ts` and the worker must prefer `x-original-ts` ([event-lifecycle.md](event-lifecycle.md) §6).
- Cost: interceptors touch every published message. At 16.7 msg/s sustained and 500/s peak this is irrelevant; on a high-throughput broker it would be measurable.

The full severity/close ordering model is in [event-lifecycle.md](event-lifecycle.md).

## 4. Anti-affinity and disruption

```yaml
affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels: { app.kubernetes.io/name: rabbitmq }
        topologyKey: kubernetes.io/hostname
```

- **Required** (not preferred): best-effort affinity would let two pods land on one node and silently reduce fault tolerance to 1 node loss.
- `PodDisruptionBudget: minAvailable: 2` so node drains never take the quorum below 2.
- No HPA: replica count is fixed at 3 by design.
- `terminationGracePeriodSeconds: 120` and `preStop: rabbitmqctl stop_app` (or just `quit`) so that the node leaves cleanly instead of being SIGKILLed mid-journal-write.

## 5. Readiness and liveness

| Probe | Command | Meaning |
|---|---|---|
| startup | `rabbitmq-diagnostics -q check_running` | process up, `periodSeconds: 10`, `failureThreshold: 30` |
| readiness | `rabbitmq-diagnostics -q check_running -q check_local_alarms` | do not send traffic to a node with a memory/disk alarm |
| liveness | `rabbitmq-diagnostics -q ping` | deliberately shallow; a deep check would kill nodes during a benign partition and cause a restart loop |

Do **not** use `check_port_connectivity` or a cluster-wide check for liveness.

## 6. Sizing

| Aspect | Value | Rationale |
|---|---|---|
| Requests | 250m CPU, 512Mi RAM | quorum queues hold journals in memory when possible |
| Limits | 1 CPU, 1Gi RAM | `vm_memory_high_watermark.relative = 0.7` → 700Mi usable |
| `disk_free_limit` | `8GB` absolute | below this publishing blocks; alarms must fire well before the 96Gi PV fills |
| PV | **96Gi** `ReadWriteOnce` | ≈8Gi ingest journal + 64Gi audit stream + 8Gi DLQ + headroom. Derivation in [sizing.md](sizing.md) §3.6 |
| `q.event.ingest` message TTL | none | the queue is the source of truth; a slow consumer must build backlog, never lose messages |

`x-max-length-bytes` is set on the **DLQ and the audit queue only**; `q.event.ingest` stays unbounded so a backlog is never silently discarded.

### 6.1 Audit queue (durability, not delivery)

`q.audit.events` is the write-ahead log that makes R1 achievable without Valkey AOF.

```yaml
# in definitions.json
q.audit.events:
  arguments:
    x-queue-type: quorum
    x-message-ttl: 604800000              # 7 days — matches Valkey retention
    x-max-length-bytes: 68719476736        # 64GiB
    x-overflow: reject-publish              # never drop the newest events silently
```

- Written by the worker **before** it acks the ingest message ([persistence-and-durability.md](persistence-and-durability.md) §4.1).
- No steady-state consumers. A `replay-gap` Job drains it non-destructively after a Valkey restore.
- Because the sustained volume is only ≈2.75 GiB/day (1 000/min × 2 048 B), the WAL can cover the **same 7 days as Valkey** — ≈19.3 GiB total. That means a total PVC loss is recoverable for the full retention window. At the 64GiB cap there is **3.3x slack**.
- `x-message-ttl: 7d` must match `FW_EVENT_TTL`. If the Valkey TTL is ever shortened, shorten this too, or the queue holds more history than the cache can serve.
- `x-max-length-bytes` is the hard backstop: a sustained burst that exceeds 3x the design volume trips `reject-publish` and surfaces as ingest backpressure rather than silent audit loss.
- `x-overflow: reject-publish` is deliberate: when the audit queue is full the audit publish fails, the ingest message is requeued, and the backlog becomes visible instead of silent data loss.
- Monitoring: `rabbitmq_quorum_queue_messages_ready{q.queue="q.audit.events"}` on a slow upward trend means the audit window is filling faster than it is being trimmed. Alert at 70% of `x-max-length-bytes` and on `> 6 days` of queued messages when the design window is 7 days.

## 7. Definitions

Ship `definitions.json` in a ConfigMap and load it at boot. It declares:

- exchange `fx.events` (direct, durable)
- exchange `fx.events.dlx` (fanout, durable)
- exchange `fx.audit` (direct, durable) — audit/WAL path
- queue `q.event.ingest` — `x-queue-type: quorum`, `x-dead-letter-exchange: fx.events.dlx`, `x-dead-letter-routing-key: event.ingest` — **no `x-delivery-limit`** ([fault-tolerance.md](fault-tolerance.md) §3.1)
- queue `q.event.ingest.dlq` — `x-queue-type: quorum`, `x-max-length-bytes: 5368709120`
- queue `q.audit.events` — quorum, `x-message-ttl: 604800000`, `x-max-length-bytes: 68719476736`, `x-overflow: reject-publish`
- bindings: `q.event.ingest` ← `fx.events`/`event.ingest`; `q.audit.events` ← `fx.audit`/`event.audit`
- one vhost `/` and no users (credentials come from a Secret, injected via `RABBITMQ_DEFAULT_USER`/`RABBITMQ_DEFAULT_PASS` on first boot only)

Definitions contain no secrets; credentials live in `Secret/fire-watch-rabbitmq`, excluded from git via `.gitignore`.

## 8. Monitoring

`rabbitmq_prometheus` on 15672 `/metrics`. Alert on:

- `rabbitmq_quorum_queue_messages_ready` growing monotonically for 15 min
- `rabbitmq_alarms_memory_used_watermark == 1`
- `rabbitmq_alarms_free_disk_space_watermark == 1`
- `rabbitmq_queue_messages_unacked` pinned high with 0 consumers (consumer died silently)
- fewer than 3 `rabbitmq_identity_info` targets scraped

## 9. Verification checklist

```bash
kubectl -n fire-watch exec rabbitmq-0 -- rabbitmq-diagnostics cluster_status
# expect: running_nodes = all 3, partitions = []
kubectl -n fire-watch exec rabbitmq-0 -- rabbitmqctl list_queues name type members
# expect: q.event.ingest quorum 3
kubectl -n fire-watch exec rabbitmq-0 -- rabbitmq-queues check_if_node_is_quorum_critical
```

Failure scenarios to rehearse, see [validation.md](validation.md):

1. `kubectl delete pod rabbitmq-1` → publish continues, queue reports 2/3.
2. `kubectl delete pod rabbitmq-1 rabbitmq-2` → publish refused with `PRECONDITION_FAILED - quorum queue ... unavailable`; no acknowledged message lost.
3. Cordon a node and drain → PDB blocks until the pod is rescheduled elsewhere.
