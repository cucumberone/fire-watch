# Roadmap

Implementation order, open decisions, and known deviations from the spec.

## 1. Implementation phases

### Phase 0 — decisions (blocks everything)

| # | Decision | Owner action |
|---|---|---|
| 0.1 | Durability rests on Valkey alone: AOF on every pod, replication, `min-replicas-to-write 1`, writers `WAIT 1` — no copy outside Valkey | read [persistence-and-durability.md](persistence-and-durability.md) §3–4 and sign off the total-PVC-loss gap (or provide volume backups) |
| 0.2 | Measure the real event size distribution | [sizing.md](sizing.md) §2.2 — baseline uses the 2 048 B **floor**, not an average; every doubling doubles the working set |
| 0.3 | Choose node size | [sizing.md](sizing.md) §3.3 — a survivor must host two 52Gi Valkey pods, so **128 GiB nodes** are needed to keep 3-replica redundancy through a node loss |
| 0.4 | Confirm Sentinel is the role manager | [redis-sentinel.md](redis-sentinel.md) |
| 0.5 | Confirm image tags/digests for Valkey and redis_exporter | pin before any manifest is committed |

### Phase 1 — datastore and ingest queue

1. `fire-watch-valkey` Secret (password) + ConfigMap (`valkey.conf`)
2. `valkey-headless` Service
3. `valkey` StatefulSet with AOF on every pod, the 32 MB-rotation sidecar, PDB
4. `sentinel` ConfigMap + StatefulSet + `sentinel-headless` Service
5. `sentinel-bridge` Deployment + RBAC (patch Service, exec into pods) — **implemented**, source in [services/sentinel-bridge](../services/sentinel-bridge/)
6. Verify: master + 2 stable_sync replicas, sentinels agree, Service points at the master, `min-replicas-to-write 1` on every pod

### Phase 2 — workloads

1. Producer contract doc ([event-schema.md](event-schema.md)) + `severity` enum + `msg`/`msgc` fields
2. `http-ingress` Deployment + NodePort: validate → `XADD fw:stream:ingest` (one `MULTI` per batch) → `WAIT 1` → 202 ([http-ingress.md](http-ingress.md))
3. `event-fold.lua` — the watermark guard, reviewed line by line ([event-lifecycle.md](event-lifecycle.md) §5.2)
4. `worker` Deployment: `XREADGROUP → validate → compress msgc → hist: fork → fold → publish → XACK+XDEL`, PEL retry, `XAUTOCLAIM`, DLQ stream ([workers.md](workers.md))
5. `ws-gateway` Deployment + HPA + Route, forwarding `applied`, `op: alert` and compressed `msgc` untouched; operator close via `XADD` + `WAIT`
6. `frontend` Deployment + Route, with `DecompressionStream` decoding and an undecodable fallback
7. Verify: event round trip producer → UI, idempotency checks in [workers.md](workers.md) §10, and the close-ordering drills in [validation.md](validation.md) §6

### Phase 3 — hardening

1. NetworkPolicies ([networking.md](networking.md) §5) — deferred while the namespace is shared
2. ServiceMonitors / alert rules incl. `fire-watch-stream` ([monitoring.md](monitoring.md), [valkey.md](valkey.md) §7, [redis-sentinel.md](redis-sentinel.md) §7) — **done**
3. Volume backups for the Valkey PVCs (the one gap [persistence-and-durability.md](persistence-and-durability.md) §4 leaves)
4. Run every scenario in [validation.md](validation.md)

## 2. Manifest inventory — Kustomize tree

`k8s/` is a Kustomize tree: a **base** describing the plan of record, and **overlays** that adjust it per environment. Layout, rationale and the lab overlay are specified in [kustomize.md](kustomize.md).

| Path | Contents |
|---|---|
| `base/01-pdb.yaml` | PodDisruptionBudgets for all 7 workloads ([fault-tolerance.md](fault-tolerance.md) §3.3) |
| `base/20-valkey-config.yaml` | `valkey.conf` (AOF, `min-replicas-to-write 1`, `noeviction`) + entrypoint + AOF-rotation script |
| `base/20-event-fold-config.yaml` | `event-fold.lua` mounted into the worker |
| `base/22-valkey-services.yaml` | headless + master-pointer Service |
| `base/23-valkey-statefulset.yaml` | StatefulSet + `aof-rewriter` sidecar |
| `base/30-sentinel-config.yaml` | `sentinel.conf` template |
| `base/31-sentinel-statefulset.yaml` | StatefulSet + headless Service + **persistent 1Gi volume** for the config epoch |
| `base/32-sentinel-bridge.yaml` | Deployment, RBAC, ServiceAccount; `FW_WRITE_GUARD_MIN_REPLICAS=1` |
| `base/33-redis-exporter.yaml` | TWO Deployments + Services — one Sentinel-mode, one against the master (with `REDIS_EXPORTER_CHECK_SINGLE_STREAMS` for the ingest and DLQ streams). Neither Sentinel nor Valkey has an HTTP endpoint |
| `base/34-http-ingress.yaml` | Deployment + NodePort 30900 — external nginx → `XADD fw:stream:ingest` + `WAIT` ([http-ingress.md](http-ingress.md)) |
| `base/40-workers.yaml` | Deployment — stream consumer group `workers` |
| `base/41-ws-gateway.yaml` | Deployment, Service, HPA, Route |
| `base/42-frontend.yaml` | Deployment, ConfigMap, Service, Route |
| `base/60-monitoring.yaml` | 2 ServiceMonitors + 4 PrometheusRules (`fire-watch-stream`, `-valkey`, `-sentinel`, `-workloads`) |
| `base/kustomization.yaml` | resource list + namespace + common labels |
| `overlays/lab/common/` | **lab base**: 1 replica each, tiny pods, no PDBs, no HPA, no monitoring, 512mb Valkey, `min-replicas-to-write 0`, bridge guard 0, `stream-lab.yaml` (`FW_WAIT_REPLICAS=0` on http-ingress and ws-gateway) — shared by both flavours ([kustomize.md](kustomize.md) §3) |
| `overlays/lab/openshift/` | Route-based lab (24 objects) |
| `overlays/lab/kubernetes/` | NodePort lab, Routes deleted (22 objects) |
| `overlays/ghcr/` | production shape, images from GHCR ([images.md](images.md) §6) |
| `overlays/prod/` | (later) digest pinning, Route hostnames, quota bumps |

**Deliberately absent, with the reason:**

| Not a file | Why |
|---|---|
| `00-namespace.yaml` | the namespace already exists and is shared with other workloads — creating it, or a ResourceQuota/LimitRange in it, would affect things we do not own ([networking.md](networking.md) §5.1) |
| `*secret*.yaml` | credentials are created out of band, never committed: `fire-watch-valkey` and `fire-watch-ingress`, via `scripts/create-secrets.sh` or the deploy workflow ([images.md](images.md) §9) |
| `50-networkpolicies.yaml` | removed from scope while the namespace is shared ([networking.md](networking.md) §5) |
| `10`–`14-rabbitmq-*.yaml`, `overlays/lab/common/rabbitmq-lab.yaml` | **removed** — the message broker was replaced by the Valkey stream (§4) |

Rules for every manifest: pinned image digests, explicit `resources`, probes on every container, `securityContext` with `runAsNonRoot`, `allowPrivilegeEscalation: false`, `readOnlyRootFilesystem: true` where possible, pod anti-affinity, and no `:latest`.

Overlay rule: **overlays change replicas, resources, storage, schedules and hostnames — never a durability or safety invariant.** Those live in the base and are reviewed there. The lab's `min-replicas-to-write 0` / guard 0 / `FW_WAIT_REPLICAS=0` trio is the one documented exception, forced by having no replica.

## 3. Open decisions

| # | Question | Current proposal | Blocking |
|---|---|---|---|
| 3.1 | ~~Valkey durability without AOF~~ | **closed**: AOF with `appendfsync everysec` on every pod, replication, `min-replicas-to-write 1` and `WAIT 1`. Total PVC loss is not covered in-system (§4) | no |
| 3.2 | Storm headroom | Baseline assumes one 1-hour 500/s storm per week (+5.4 GiB). Daily storms need ≈66 GiB per pod or value compression ([sizing.md](sizing.md) §2.3–2.4) | no |
| 3.3 | Role management | Sentinel + `sentinel-bridge` | yes |
| 3.4 | Producer authentication | one shared `X-Ingest-Token` at http-ingress; per-producer tokens later | no |
| 3.5 | In-cluster TLS for RESP | deferred to a second phase | no |
| 3.6 | Multi-tenant isolation (sources per customer) | not modelled; all sources share one keyspace | no |
| 3.7 | Backfill reads from replicas | deferred until replica lag is monitored | no |
| 3.8 | Record key definition | **decided:** `event:<sha256(EVT_Device + 0x1f + EVT_Summary)[0:32]>` ([valkey.md](valkey.md) §2). Two identically-worded faults collapse into one row — accepted | no |
| 3.9 | Storm events aging out of alerts | alert state TTL is 7d, so an unclosed alert silently disappears with its last event; consider a longer TTL once retention is validated | no |
| 3.10 | `EVT_Summary` is part of the record key | Two reworded reports of one fault are two rows, and two identically-worded faults are one. Producers must therefore keep summaries stable — an operator-visible consequence of content identity ([valkey.md](valkey.md) §2) | no |
| 3.11 | Lab H/A overlay (`overlays/lab-ha`) | cheap 3-replica rehearsal of Sentinel quorum, failover and `XAUTOCLAIM`, small resources ([kustomize.md](kustomize.md) §4) | no |
| 3.12 | NetworkPolicies | **removed from scope** while the namespace is shared. Re-add once we own the namespace ([networking.md](networking.md) §5.4) | no |
| 3.13 | Kubernetes flavour | **decided:** production OpenShift only; the **lab** has two flavours (`lab/openshift` and `lab/kubernetes`) sharing `lab/common` ([kustomize.md](kustomize.md) §3.0) | no |
| 3.14 | Volume backups for the Valkey PVCs | needed to cover total PVC loss now that no copy exists outside Valkey ([persistence-and-durability.md](persistence-and-durability.md) §4) | no |

## 4. Known deviations from `AGENTS.md`, and decisions that changed the design

| Spec statement / earlier design | Reality | Resolution |
|---|---|---|
| "RabbitMQ: 3 replicas of the latest RabbitMQ server with quorum queues" | **Replaced.** RabbitMQ was removed; the ingest queue is the Valkey stream `fw:stream:ingest` with consumer group `workers` and dead-letter stream `fw:stream:dlq` | One stateful system instead of two: the stream inherits Valkey's replication, AOF, Sentinel failover and split-brain gates. http-ingress `XADD`s + `WAIT 1` before 202; workers `XREADGROUP`, retry from the PEL forever, `XAUTOCLAIM` dead consumers' entries, `XACK`+`XDEL` last ([architecture.md](architecture.md) §4–5, [workers.md](workers.md) §6) |
| Audit copies (event audit queue as WAL, journal audit exchange) | **Dropped** by decision | Durability rests on AOF on every Valkey pod + replication + `min-replicas-to-write 1` + `WAIT 1`. Cost: total loss of all three Valkey PVCs is no longer recoverable in-system ([persistence-and-durability.md](persistence-and-durability.md) §3.1, §4) |
| "Valkey: … always find 2 replicas to receive writes — `min-replicas-to-write 2`" | **Changed to `min-replicas-to-write 1`** | With 2, losing either replica stopped all writes; with 1, any one Valkey pod can be lost and ingest continues, while an isolated master still refuses. Accepted cost: a bounded partition window where an old master that kept one replica can accept writes that are discarded on demotion — bounded by `WAIT 1`, bridge endpoint withholding, client re-resolution and nginx retry ([fault-tolerance.md](fault-tolerance.md) §4.1). The lab runs 1 pod with `0` |
| "Valkey shall use AOF feature" | **met** — on every pod | `appendonly yes` + `appendfsync everysec`, with rotation at 32 MB of growth enforced by a sidecar because the engine cannot express that rule ([valkey.md](valkey.md) §4.2) |
| "Redis Sentinels are needed for the workers, I think" | confirmed: needed for workers, http-ingress, the gateway and the Service selector | [redis-sentinel.md](redis-sentinel.md) |
| "Valkey operator is off the table" | honoured — everything is raw manifests plus a small bridge controller | this document |
| "No data loss" | met for every single-component failure and full-cluster restart: a 202'd event is on two pods and in their AOFs. Not met for simultaneous loss of all three Valkey PVCs | [persistence-and-durability.md](persistence-and-durability.md) §3–4 |

## 5. Explicitly out of scope (this iteration)

- Multi-region / multi-cluster replication
- Kafka, RabbitMQ or any additional queue/WAL component
- Valkey multi-shard cluster mode
- Authentication/authorisation of browser clients (assumed handled at the Route or a future OIDC layer)
- Long-term archival beyond the Valkey TTL
