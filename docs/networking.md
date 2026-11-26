# Networking

Services, Routes, DNS, and every port used by `fire-watch`.

## 1. Service inventory

| Service | Type | Selector | Ports | Purpose |
|---|---|---|---|---|
| `valkey-headless` | ClusterIP None | `app.kubernetes.io/name=valkey` | 6379 | per-pod DNS for `REPLICAOF` |
| `valkey` | ClusterIP, **no selector** | Endpoints object maintained by `sentinel-bridge` | 6379 | the only address applications use — records **and** the ingest stream `fw:stream:ingest` |
| `sentinel-headless` | ClusterIP None | `app.kubernetes.io/name=sentinel` | 26379 | Sentinel inter-node gossip |
| `ws-gateway` | ClusterIP | `app.kubernetes.io/name=ws-gateway` | 8080 | HTTP + WebSocket |
| `frontend` | ClusterIP | `app.kubernetes.io/name=frontend` | 8080 | static assets |
| `http-ingress` | **NodePort 30900** | `app.kubernetes.io/name=http-ingress` | 8080 | HTTP from an external nginx ([http-ingress.md](http-ingress.md) §4) |

**Never** create a Service that load-balances across all Valkey pods. Writes must reach the master only.

## 2. Routes (OpenShift)

| Route | Target | TLS | Annotations |
|---|---|---|---|
| `ws-gateway` | `ws-gateway:8080` | edge | `haproxy.router.openshift.io/timeout: 1h`, `disable_cookies: "true"` |
| `frontend` | `frontend:8080` | edge | none |

No Ingress objects anywhere (R2). No `hostNetwork`, no `hostPort`: the router reaches services through the normal pod network.

## 3. DNS names

| Name | Resolves to | Used by |
|---|---|---|
| `valkey.fire-watch.svc.cluster.local` | ClusterIP → current master only | fallback for non-Sentinel-aware clients |
| `valkey-N.valkey-headless.fire-watch.svc.cluster.local` | pod IP | Sentinel, `sentinel-bridge` |
| `sentinel-N.sentinel-headless.fire-watch.svc.cluster.local` | pod IP | `sentinel-bridge`, Sentinel-aware clients |

Rule: application code (http-ingress, workers, ws-gateway) may reference `valkey` and the sentinel headless names. It must never reference `valkey-N.*` — that is control-plane territory.

## 4. Ports

| Port | Process | Exposure |
|---|---|---|
| 6379 | Valkey client + replication (records and the ingest stream) | in-cluster |
| 26379 | Redis Sentinel | in-cluster |
| 8080 | gateway / frontend HTTP | via Route |
| 8080 / 30900 | http-ingress | NodePort 30900, from the external nginx |
| 9121 | exporters `/metrics` | in-cluster, scraped by Prometheus |

There is **no Valkey metrics port**. Valkey has no HTTP endpoint at all; metrics are read over RESP by `valkey-exporter` ([monitoring.md](monitoring.md) §1).

Nothing is exposed outside the cluster except the two Routes and the ingress NodePort. There is no message broker, so there are no broker ports.

## 5. NetworkPolicies — deliberately not used

**This deployment ships no NetworkPolicy objects.** The decision is explicit, not an oversight.

### 5.1 Why

The plan of record is to deploy into a **namespace that already exists** and is shared with other workloads. NetworkPolicies are **additive and allow-list only** — there are no deny rules and no prioritisation. That makes them the wrong tool to install into a namespace you do not own:

| Risk of adding them here | Consequence |
|---|---|
| A `podSelector: {}` default-deny applies to **every pod in the namespace**, including ones belonging to other teams | their traffic is silently cut off the moment the policy is applied |
| No rule can exclude a foreign workload except by listing it as an allow | maintaining someone else's traffic rules in our manifests |
| Policies are unioned, never overridden | we cannot express "deny for us, but leave everyone else alone" |
| Removing them later is not a rollback for the other tenants | they were only working because of allow-rules we wrote |

A single `default-deny-ingress` in a shared namespace can take down unrelated applications. That is a far greater risk than the network-level hardening it would buy.

### 5.2 What this costs us

Two things the policies were doing must be stated plainly, because their absence weakens a claim made elsewhere:

| Was enforced by NetworkPolicy | Now |
|---|---|
| Only the master is reachable on 6379 from our own pods | **Applies by convention only.** Ingress, workers and gateway use the `valkey` Service / Sentinel; nothing prevents a direct pod-IP connection |
| The residual direct-pod-IP WRITE gap ([fault-tolerance.md](fault-tolerance.md) §4) | **Narrowed by the engine:** `min-replicas-to-write 1` refuses the write over a pod IP when the master sees no replica (the partition window in [fault-tolerance.md](fault-tolerance.md) §4.1 excepted). Direct-pod-IP READS of a stale master remain possible |

The Valkey split-brain protection is therefore **routing-based plus engine-based, not
network-enforced**: the Service endpoint gate lives in `sentinel-bridge`, and
`min-replicas-to-write 1` refuses writes in the datastore itself
([valkey.md](valkey.md) §5). That engine flag is the part that closes the direct
pod-IP gap for WRITES — a client that bypasses the Service still cannot write to an
isolated master. Reads from a pod IP remain possible and remain a convention
([fault-tolerance.md](fault-tolerance.md) §4).

What still holds without any policy: Sentinel quorum, the write guard, the drain of a degraded master out of the Service, and remove-last (`XACK`+`XDEL` after the write) durability. Those are application-layer and unaffected.

### 5.3 How the network is constrained instead

| Control | Effect |
|---|---|
| Services are `ClusterIP` except the http-ingress NodePort | nothing is reachable from outside the cluster except the two Routes and NodePort 30900 |
| Two Routes only (`ws-gateway`, `frontend`) | no datastore port is exposed |
| One fixed NodePort (30900, http-ingress, token-authenticated), no LoadBalancer service, no `hostNetwork` | no other per-node port leaks |
| Applications use `valkey` Service / Sentinel, never pod IPs | enforced by review ([redis-sentinel.md](redis-sentinel.md) §5) |

### 5.4 If a dedicated namespace becomes available

The policies are a small, well-understood addition **once we own the namespace**. The intended set, for the day that happens:

| Policy | Selects | Allows from | Ports |
|---|---|---|---|
| `default-deny-ingress` | `podSelector: {}` | nothing | — |
| `allow-valkey-peers` | `valkey` | `valkey`, `sentinel` | 6379 |
| `allow-valkey-clients` | `valkey` | `http-ingress`, `worker`, `ws-gateway`, `sentinel-bridge` | 6379 |
| `allow-valkey-metrics` | `valkey` | `valkey-exporter` | 6379 |
| `allow-sentinel` | `sentinel` | `sentinel`, `sentinel-bridge`, `http-ingress`, `worker`, `ws-gateway`, `redis-exporter` | 26379 |
| `allow-ingress-nodeport` | `http-ingress` | the external nginx address range | 8080 |
| `allow-gateway-ingress` | `ws-gateway` | `openshift-ingress` | 8080 |
| `allow-frontend-ingress` | `frontend` | `openshift-ingress` | 8080 |

Prerequisites before anyone applies that set: we own the namespace, every pod in it is ours, and the egress default-deny (if added) allows port 53 to `openshift-dns` first — otherwise DNS breaks and it looks like a total outage.

Tracked as [roadmap.md](roadmap.md) §3.12.

### 5.5 Reference: what a NetworkPolicy scopes

Kept because the question comes up every time this is revisited. **It selects pods** — not Services, not Routes, not the namespace as an enforceable unit.

| Object | Controlled by NetworkPolicy? | Why |
|---|---|---|
| **Pods** | **yes** — the only thing `podSelector` selects | enforcement happens at the pod's network interface |
| **Services** (ClusterIP) | **no** | policy is evaluated on the *pod* IP after the Service has already DNAT'd the connection |
| **Routes / Ingress** | **no** | policy sees the ingress-router pod's traffic, not the Route object |
| **Namespace** | only as a boundary | `metadata.namespace` says where the object lives; `namespaceSelector` says whose pods may talk to it |
| **Nodes / hostNetwork pods** | effectively no | node-level traffic usually bypasses pod policy — CNI-dependent |

| Property | Behaviour |
|---|---|
| Direction | `Ingress`, `Egress`, or both, per policy |
| Model | **allow-list only — there are no deny rules** |
| Isolation | a pod becomes isolated in a direction as soon as **any** policy selects it for that direction; that direction is then default-deny except for explicit allows |
| Composition | multiple policies selecting the same pod are **unioned**, never overridden |
| Order | irrelevant — Kubernetes policies have no priority or ordering |
| CNI support | required; OVN-Kubernetes and OpenShift SDN implement it, some CNIs silently ignore the objects |

## 6. Load-balancing behaviour that matters

| Layer | Behaviour | Consequence |
|---|---|---|
| Route → gateway | round-robin, no stickiness (cookies disabled) | each WebSocket is pinned to one pod for its lifetime by TCP, which is all that is needed |
| Route → frontend | round-robin | stateless assets, no problem |
| Service `valkey` | ClusterIP over a single Endpoint | effectively a stable pointer to the master |
| NodePort `http-ingress` | kube-proxy spreads across ready pods | a pod whose Valkey `PING` fails is not ready and gets no traffic |
| Headless services | DNS A records per pod | used only by control-plane components |

## 7. TLS

| Hop | TLS |
|---|---|
| browser → Route | edge termination, cert managed by the platform or cert-manager |
| Route → pod | plaintext HTTP/WS |
| pod → pod (RESP) | plaintext inside the cluster network |
| pod → pod (management) | plaintext |

If in-cluster TLS is required later, Valkey supports `--tls` with `--tls_replication`; add it as a second phase and update this file.

## 8. Verification checklist

```bash
kubectl -n fire-watch get svc,endpoints,route
kubectl -n fire-watch get endpoints valkey -o wide         # exactly 1 address (the master)
kubectl -n fire-watch get endpoints ws-gateway -o wide        # 3 addresses
kubectl -n fire-watch get networkpolicy                       # expect: No resources found

# confirm no datastore port is exposed outside the cluster: only http-ingress may be a NodePort
kubectl -n fire-watch get svc -o wide | grep -E 'NodePort|LoadBalancer'   # expect: http-ingress only
kubectl -n fire-watch get route
```
