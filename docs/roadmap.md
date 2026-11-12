# Roadmap

Implementation order, open decisions, and known deviations from the spec.

## 1. Implementation phases

### Phase 0 — decisions (blocks everything)

| # | Decision | Owner action |
|---|---|---|
| 0.1 | Accept RabbitMQ-as-WAL instead of Valkey AOF (R1) | read [persistence-and-durability.md](persistence-and-durability.md) §3 and approve option 1 |
| 0.2 | Measure the real event size distribution | [sizing.md](sizing.md) §2.2 — baseline uses the 2 048 B **floor**, not an average; every doubling doubles the working set |
| 0.3 | Choose node size | [sizing.md](sizing.md) §3.3 — a survivor must host two 52Gi Valkey pods, so **128 GiB nodes** are needed to keep 3-replica redundancy through a node loss |
| 0.4 | Confirm Sentinel is the role manager | [redis-sentinel.md](redis-sentinel.md) |
| 0.5 | Confirm image tags/digests for RabbitMQ, Valkey, Redis | pin before any manifest is committed |

### Phase 1 — broker

1. Namespace, ResourceQuota, LimitRange
2. `rabbitmq` Secret + `definitions.json` ConfigMap + `enabled_plugins`
3. `rabbitmq-headless` and `rabbitmq` Services
4. RBAC for the K8s peer-discovery plugin
5. `rabbitmq` StatefulSet, PDB, anti-affinity, probes
6. Verify: cluster_status 3 nodes, `q.event.ingest` quorum 3, `q.audit.events` quorum 3 with the 24h TTL and `reject-publish` overflow

### Phase 2 — datastore

1. `valkey` Secret (password) + ConfigMap (`valkey.conf`)
2. `valkey-headless` Service
3. `valkey` StatefulSet with AOF, the hourly-rewrite sidecar, PDB
4. `sentinel` ConfigMap + StatefulSet + `sentinel-headless` Service
5. `sentinel-bridge` Deployment + RBAC (patch Service, exec into pods) — **implemented**, source in [services/sentinel-bridge](../services/sentinel-bridge/)
6. Verify: master + 2 stable_sync replicas, sentinels agree, Service points at the master

### Phase 3 — workloads

1. Producer contract doc + `fx.events` / `fx.events.dlx` definitions + `severity` enum + `msg`/`msgc` fields
2. `event-fold.lua` — the watermark guard, reviewed line by line ([event-lifecycle.md](event-lifecycle.md) §5.2)
3. `worker` Deployment (`validate → compress msgc → write → audit (plain) → publish → ack`)
4. `ws-gateway` Deployment + HPA + Route, forwarding `applied`, `op: alert` and compressed `msgc` untouched
5. `frontend` Deployment + Route, with `DecompressionStream` decoding and an undecodable fallback
6. Verify: event round trip producer → UI, idempotency checks in [workers.md](workers.md) §10, and the close-ordering drills in [validation.md](validation.md) §6

### Phase 4 — hardening

1. NetworkPolicies ([networking.md](networking.md) §5)
2. ServiceMonitor / alert rules ([rabbitmq.md](rabbitmq.md) §8, [valkey.md](valkey.md) §7, [redis-sentinel.md](redis-sentinel.md) §7)
3. `replay-gap` Job/CronJob ([persistence-and-durability.md](persistence-and-durability.md) §4)
4. Run every scenario in [validation.md](validation.md)

## 2. Manifest inventory — Kustomize tree

`k8s/` is a Kustomize tree: a **base** describing the plan of record, and **overlays** that adjust it per environment. Layout, rationale and the lab overlay are specified in [kustomize.md](kustomize.md).

| Path | Contents |
|---|---|
| `base/01-pdb.yaml` | PodDisruptionBudgets for all 7 workloads ([fault-tolerance.md](fault-tolerance.md) §3.3) |
| `base/10-rabbitmq-rbac.yaml` | ServiceAccount, Role, RoleBinding |
| `base/11-rabbitmq-config.yaml` | `enabled_plugins`, `rabbitmq.conf`, `definitions.json` ConfigMaps |
| `base/13-rabbitmq-services.yaml` | headless + client Services |
| `base/14-rabbitmq-statefulset.yaml` | StatefulSet |
| `base/20-valkey-config.yaml` | `valkey.conf` + entrypoint + hourly AOF-rewrite script |
| `base/20-event-fold-config.yaml` | `event-fold.lua` mounted into the worker |
| `base/22-valkey-services.yaml` | headless + master-pointer Service |
| `base/23-valkey-statefulset.yaml` | StatefulSet + `aof-rewriter` sidecar |
| `base/30-sentinel-config.yaml` | `sentinel.conf` template |
| `base/31-sentinel-statefulset.yaml` | StatefulSet + headless Service + **persistent 1Gi volume** for the config epoch |
| `base/32-sentinel-bridge.yaml` | Deployment, RBAC, ServiceAccount |
| `base/33-redis-exporter.yaml` | TWO Deployments + Services — one Sentinel-mode, one against the master. Neither Sentinel nor Valkey has an HTTP endpoint |
| `base/34-http-ingress.yaml` | Deployment + NodePort 30900 — external nginx → AMQP ([http-ingress.md](http-ingress.md)) |
| `base/40-workers.yaml` | Deployment |
| `base/41-ws-gateway.yaml` | Deployment, Service, HPA, Route |
| `base/42-frontend.yaml` | Deployment, ConfigMap, Service, Route |
| `base/kustomization.yaml` | resource list + namespace + common labels |
| `base/60-monitoring.yaml` | 3 ServiceMonitors + 4 PrometheusRules |
| `overlays/lab/common/` | **lab base**: 1 replica each, tiny pods, no PDBs, no HPA, 512mb Valkey — shared by both flavours ([kustomize.md](kustomize.md) §3) |
| `overlays/lab/openshift/` | Route-based lab (27 objects) |
| `overlays/lab/kubernetes/` | NodePort lab, Routes deleted (25 objects) |
| `overlays/prod/` | (later) digest pinning, Route hostnames, quota bumps |

**Deliberately absent, with the reason:**

| Not a file | Why |
|---|---|
| `00-namespace.yaml` | the namespace already exists and is shared with other workloads — creating it, or a ResourceQuota/LimitRange in it, would affect things we do not own ([networking.md](networking.md) §5.1) |
| `*secret*.yaml` | credentials are created out of band, never committed. Commands are in `k8s/base/kustomization.yaml` |
| `50-networkpolicies.yaml` | removed from scope while the namespace is shared ([networking.md](networking.md) §5) |
| `60-monitoring.yaml` | **done** — ServiceMonitors, PrometheusRules, redis-exporter ([monitoring.md](monitoring.md)) |

Rules for every manifest: pinned image digests, explicit `resources`, probes on every container, `securityContext` with `runAsNonRoot`, `allowPrivilegeEscalation: false`, `readOnlyRootFilesystem: true` where possible, pod anti-affinity, and no `:latest`.

Overlay rule: **overlays change replicas, resources, storage, schedules and hostnames — never a durability or safety invariant.** Those live in the base and are reviewed there.

## 3. Open decisions

| # | Question | Current proposal | Blocking |
|---|---|---|---|
| 3.1 | ~~Valkey durability without AOF~~ | **closed by Valkey**: AOF with `appendfsync everysec` gives a ≤1 s RPO, and the RabbitMQ audit stream still covers a total PV loss | no |
| 3.2 | Storm headroom | Baseline assumes one 1-hour 500/s storm per week (+5.4 GiB). Daily storms need ≈66 GiB per pod or value compression ([sizing.md](sizing.md) §2.3–2.4) | no |
| 3.3 | Role management | Sentinel + `sentinel-bridge` | yes |
| 3.4 | Producer authentication | per-producer AMQP users, vhost `/`, permissions limited to `fx.events` | no |
| 3.5 | In-cluster TLS for AMQP/RESP | deferred to a second phase | no |
| 3.6 | Multi-tenant isolation (sources per customer) | not modelled; all sources share one keyspace | no |
| 3.7 | Backfill reads from replicas | deferred until replica lag is monitored | no |
| 3.8 | Record key definition | **decided:** `event:<sha256(EVT_Device + 0x1f + EVT_Summary)[0:32]>` ([valkey.md](valkey.md) §2). Two identically-worded faults collapse into one row — accepted | no |
| 3.9 | Storm events aging out of alerts | alert state TTL is 7d, so an unclosed alert silently disappears with its last event; consider a longer TTL once retention is validated | no |
| 3.10 | `EVT_Summary` is part of the record key | Two reworded reports of one fault are two rows, and two identically-worded faults are one. Producers must therefore keep summaries stable — an operator-visible consequence of content identity ([valkey.md](valkey.md) §2) | no |
| 3.11 | Lab H/A overlay (`overlays/lab-ha`) | cheap 3-replica rehearsal of quorum and failover, small resources ([kustomize.md](kustomize.md) §4) | no |
| 3.12 | NetworkPolicies | **removed from scope** while the namespace is shared. Re-add once we own the namespace ([networking.md](networking.md) §5.4) | no |
| 3.13 | Kubernetes flavour | **decided:** production OpenShift only; the **lab** has two flavours (`lab/openshift` and `lab/kubernetes`) sharing `lab/common` ([kustomize.md](kustomize.md) §3.0) | no |

## 4. Known deviations from `AGENTS.md`

| Spec statement | Reality | Resolution |
|---|---|---|
| "Valkey shall use AOF feature" | **now met** — Valkey implements AOF, unlike the Dragonfly it replaced | `appendonly yes` + `appendfsync everysec`, with an hourly `BGREWRITEAOF` sidecar for restart speed ([valkey.md](valkey.md) §4) |
| "always find 2 replicas to receive writes" | **now met in the engine** — `min-replicas-to-write 2` | the flag refuses writes on the master itself; `sentinel-bridge` still empties `Endpoints/valkey` so stale READS are blocked too ([valkey.md](valkey.md) §5) |
| "Redis Sentinels are needed for the workers, I think" | confirmed: needed, and also needed for the gateway and Service selector | [redis-sentinel.md](redis-sentinel.md) |
| "Valkey operator is off the table" | honoured — everything is raw manifests plus a small bridge controller | this document |
| "No data loss" | AOF bounds the window to ≤1 s; a total PV loss is still recovered from the audit stream | [persistence-and-durability.md](persistence-and-durability.md) §3 |

## 5. Explicitly out of scope (this iteration)

- Multi-region / multi-cluster replication
- Kafka or any additional WAL component
- Valkey multi-shard cluster mode
- Authentication/authorisation of browser clients (assumed handled at the Route or a future OIDC layer)
- Long-term archival beyond the Valkey TTL and the audit queue window
