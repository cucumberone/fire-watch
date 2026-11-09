# Networking

Services, Routes, DNS, and every port used by `fire-watch`.

## 1. Service inventory

| Service | Type | Selector | Ports | Purpose |
|---|---|---|---|---|
| `rabbitmq-headless` | ClusterIP None | `app.kubernetes.io/name=rabbitmq` | 5672, 15672, 4369, 25672 | StatefulSet identity + peer discovery |
| `rabbitmq` | ClusterIP | same | 5672, 15672 | client + management |
| `dragonfly-headless` | ClusterIP None | `app.kubernetes.io/name=dragonfly` | 6379, 9222 | per-pod DNS for `REPLICAOF` |
| `dragonfly` | ClusterIP, **no selector** | Endpoints object maintained by `sentinel-bridge` | 6379 | the only address applications use |
| `sentinel-headless` | ClusterIP None | `app.kubernetes.io/name=sentinel` | 26379 | Sentinel inter-node gossip |
| `ws-gateway` | ClusterIP | `app.kubernetes.io/name=ws-gateway` | 8080 | HTTP + WebSocket |
| `frontend` | ClusterIP | `app.kubernetes.io/name=frontend` | 8080 | static assets |
| `http-ingress` | **NodePort 30900** | `app.kubernetes.io/name=http-ingress` | 8080 | HTTP from an external nginx ([http-ingress.md](http-ingress.md) §4) |

**Never** create a Service that load-balances across all Dragonfly pods. Writes must reach the master only.

## 2. Routes (OpenShift)

| Route | Target | TLS | Annotations |
|---|---|---|---|
| `ws-gateway` | `ws-gateway:8080` | edge | `haproxy.router.openshift.io/timeout: 1h`, `disable_cookies: "true"` |
| `frontend` | `frontend:8080` | edge | none |

No Ingress objects anywhere (R2). No `hostNetwork`, no `hostPort`: the router reaches services through the normal pod network.

## 3. DNS names

| Name | Resolves to | Used by |
|---|---|---|
| `rabbitmq.fire-watch.svc.cluster.local` | ClusterIP → 3 pods | workers, producers |
| `rabbitmq-0.rabbitmq-headless.fire-watch.svc.cluster.local` | pod IP | RabbitMQ nodes (peer discovery) |
| `dragonfly.fire-watch.svc.cluster.local` | ClusterIP → current master only | fallback for non-Sentinel-aware clients |
| `dragonfly-N.dragonfly-headless.fire-watch.svc.cluster.local` | pod IP | Sentinel, `sentinel-bridge` |
| `sentinel-N.sentinel-headless.fire-watch.svc.cluster.local` | pod IP | `sentinel-bridge`, Sentinel-aware clients |

Rule: application code may reference `rabbitmq`, `dragonfly`, and the sentinel headless names. It must never reference `dragonfly-N.*` — that is control-plane territory.

## 4. Ports

| Port | Process | Exposure |
|---|---|---|
| 5672 | RabbitMQ AMQP | in-cluster |
| 15672 | RabbitMQ management + Prometheus metrics | in-cluster; optional Route for ops, off by default |
| 4369 | epmd | in-cluster |
| 25672 | Erlang distribution | in-cluster |
| 6379 | Dragonfly client + replication | in-cluster |
| 9222 | Dragonfly admin + Prometheus metrics | in-cluster |
| 26379 | Redis Sentinel | in-cluster |
| 8080 | gateway / frontend HTTP | via Route |

| Nothing is exposed to the internet except the two Routes and the ingress NodePort. |

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
| Only the master is reachable on 6379 from our own pods | **Applies by convention only.** Workers and gateway use the `dragonfly` Service; nothing prevents a direct pod-IP connection |
| The residual direct-pod-IP write gap ([fault-tolerance.md](fault-tolerance.md) §4) | **Widened.** The only remaining control is application configuration and code review |

The Dragonfly split-brain protection is therefore **routing-based, not network-enforced**. It was already partly so; it is now entirely so. [fault-tolerance.md](fault-tolerance.md) §4 and §6 are updated to reflect that, rather than continuing to credit a control that is not deployed.

What still holds without any policy: Sentinel quorum, the write guard, the drain of a degraded master out of the Service, and ack-last durability. Those are application-layer and unaffected.

### 5.3 How the network is constrained instead

| Control | Effect |
|---|---|
| Services are `ClusterIP` only | nothing is reachable from outside the cluster except the two Routes |
| Two Routes only (`ws-gateway`, `frontend`) | no broker or datastore port is exposed |
| No NodePort, no LoadBalancer service, no `hostNetwork` | no per-node port leaks |
| Applications use `dragonfly` Service / Sentinel, never pod IPs | enforced by review ([redis-sentinel.md](redis-sentinel.md) §5) |

### 5.4 If a dedicated namespace becomes available

The policies are a small, well-understood addition **once we own the namespace**. The intended set, for the day that happens:

| Policy | Selects | Allows from | Ports |
|---|---|---|---|
| `default-deny-ingress` | `podSelector: {}` | nothing | — |
| `allow-rmq-peers` | `rabbitmq` | `rabbitmq` | 4369, 25672, 5672 |
| `allow-rmq-clients` | `rabbitmq` | `worker`, producers | 5672 |
| `allow-dragonfly-peers` | `dragonfly` | `dragonfly`, `sentinel` | 6379 |
| `allow-dragonfly-clients` | `dragonfly` | `worker`, `ws-gateway`, `sentinel-bridge` | 6379 |
| `allow-dragonfly-metrics` | `dragonfly` | monitoring namespace | 9222 |
| `allow-sentinel` | `sentinel` | `sentinel`, `sentinel-bridge`, `worker`, `ws-gateway` | 26379 |
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
| Service `rabbitmq` | ClusterIP, TCP | AMQP connection sticks to one node; a reconnect may land elsewhere, which is fine for quorum queues |
| Service `dragonfly` | ClusterIP over a single Endpoint | effectively a stable pointer to the master |
| Headless services | DNS A records per pod | used only by control-plane components |

## 7. TLS

| Hop | TLS |
|---|---|
| browser → Route | edge termination, cert managed by the platform or cert-manager |
| Route → pod | plaintext HTTP/WS |
| pod → pod (AMQP, RESP) | plaintext inside the cluster network policy boundary |
| pod → pod (management) | plaintext |

If in-cluster TLS is required later, RabbitMQ supports AMQP over TLS and Dragonfly supports `--tls` with `--tls_replication`; add it as a second phase and update this file.

## 8. Verification checklist

```bash
kubectl -n fire-watch get svc,endpoints,route
kubectl -n fire-watch get endpoints dragonfly -o wide         # exactly 1 address (the master)
kubectl -n fire-watch get endpoints ws-gateway -o wide        # 3 addresses
kubectl -n fire-watch get networkpolicy                       # expect: No resources found

# confirm no broker/datastore port is exposed outside the cluster
kubectl -n fire-watch get svc -o wide | grep -E 'NodePort|LoadBalancer' || echo 'none — ClusterIP only'
kubectl -n fire-watch get route
```
