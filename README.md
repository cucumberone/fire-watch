# Fire-Watch Event Server

Event ingestion and visualization for OpenShift. 1 000 events/min sustained, 500 evt/s storm peaks, 7-day retention, no data loss.

## Diagram

**[docs/fire-watch.drawio](docs/fire-watch.drawio)** — all pods and components on one canvas.

Open it with:

- the [diagrams.net editor](https://app.diagrams.net/) (File → Open From → Device), or
- the VS Code `hediet.vscode-drawio` extension (click the file), or
- the draw.io desktop app.

The canvas shows the `fire-watch` namespace boundary, all 19 planned pods, the two Routes, the external producer and browser, and every data path with its encoding — RabbitMQ quorum queues, the `q.audit.events` write-ahead log, worker fan-out, Dragonfly replication, Sentinel quorum, and the gateway→browser WebSocket leg.

## Components

| Component | Replicas | Purpose |
|---|---|---|
| http-ingress | 3 | external nginx (HTTP) → AMQP, 202 only after a confirm |
| RabbitMQ | 3 | quorum queues, ingest + audit WAL |
| Workers | 3 | consume, compress, write, ack last |
| DragonflyDB | 3 | 1 master + 2 replicas, snapshots, no operator |
| Redis Sentinel | 3 | master discovery and failover |
| sentinel-bridge | 2 | applies `REPLICAOF`, patches the Service selector (lease-elected) |
| WebSocket gateway | 3..12 | stateless fan-out, HPA |
| Frontend | 3 | static React app |

## Documentation

- [Architecture](docs/architecture.md) — components, data flow, failure matrix
- [Fault Tolerance Audit](docs/fault-tolerance.md) — H/A and split-brain verification per component
- [Event Lifecycle](docs/event-lifecycle.md) — severities, close semantics, broker-timestamp ordering
- [Payload & Compression](docs/payload-and-compression.md) — `msg`/`msgc`, gzip at the worker
- [RabbitMQ](docs/rabbitmq.md) — quorum cluster, peer discovery, audit queue
- [DragonflyDB](docs/dragonfly.md) — replication, write guard, snapshots
- [Redis Sentinel](docs/redis-sentinel.md) — failover, split-brain walkthrough
- [Persistence & Durability](docs/persistence-and-durability.md) — how "no data loss" is met
- [Workers](docs/workers.md) — ack-last ordering, idempotency, coalescing
- [WebSocket Server](docs/websocket-server.md) — subscription protocol, HPA
- [Frontend](docs/frontend.md) — static app, Routes, CSP
- [Networking](docs/networking.md) — Services, Routes, ports, network exposure
- [HTTP Ingress](docs/http-ingress.md) — external nginx → AMQP with publisher confirms
- [Monitoring](docs/monitoring.md) — 21 alerts, redis-exporter for Sentinel
- [Kustomize](docs/kustomize.md) — base + overlays, `lab` single-replica overlay
- [Sizing](docs/sizing.md) — capacity model
- [Validation](docs/validation.md) — failure drills and acceptance gate
- [Roadmap](docs/roadmap.md) — phases, manifest inventory, open decisions

## Deploy

`k8s/` is a Kustomize tree. The base is the production shape (OpenShift). The lab has two flavours — pick the one that matches your lab cluster, both render the same deployment apart from networking.

```bash
kubectl kustomize k8s/base | less                     # review production shape
kubectl apply -k k8s/base                             # OpenShift, production

kubectl apply -k k8s/overlays/lab/openshift          # OpenShift lab (CRC / OCP)
kubectl apply -k k8s/overlays/lab/kubernetes         # MicroK8s / kind / k3s lab

kubectl delete -k k8s/overlays/lab/kubernetes        # then delete the PVCs too
```

See [docs/kustomize.md](docs/kustomize.md) §3 for what the lab changes, how the two flavours differ, and what the lab **cannot** test.

## Status

**Manifests implemented.** `k8s/base` renders 44 objects. The lab renders 27 (OpenShift flavour) or 25 (Kubernetes flavour) — 1 replica each, no PDBs, no HPA, no monitoring. All verified with `kubectl kustomize`.

Still needed before a real deploy:

| Item | Why |
|---|---|
| Build images: `worker`, `ws-gateway`, `frontend` | manifests reference `image-registry.openshift-image-registry.svc:5000/fire-watch/*` |
| Build `sentinel-bridge` | source is in [services/sentinel-bridge](services/sentinel-bridge/) — needs only a container build, not a rewrite |
| Pin real image digests | base uses tags; production requires digests |
| Create the two Secrets | `fire-watch-rabbitmq{username,password}`, `fire-watch-dragonfly{password}` — see `k8s/base/kustomization.yaml` |
| Confirm node size | 48Gi Dragonfly limit needs 128 GiB nodes to reschedule ([docs/sizing.md](docs/sizing.md) §3.3) |

### `sentinel-bridge`

Implemented — [services/sentinel-bridge](services/sentinel-bridge/). 11 planner unit tests pass (`node --test src/plan.test.js`). It needs a container build, not a rewrite.
