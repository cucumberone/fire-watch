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
- [Kustomize](docs/kustomize.md) — base + overlays, `lab` single-replica overlay
- [Sizing](docs/sizing.md) — capacity model
- [Validation](docs/validation.md) — failure drills and acceptance gate
- [Roadmap](docs/roadmap.md) — phases, manifest inventory, open decisions

## Deploy

`k8s/` is a Kustomize tree. The base is the production shape; the `lab` overlay is 1 replica per component with small pods.

```bash
kubectl kustomize k8s/overlays/lab | less    # render and review
kubectl apply -k k8s/overlays/lab            # homelab / testing
kubectl apply -k k8s/base                    # production shape
kubectl delete -k k8s/overlays/lab
```

See [docs/kustomize.md](docs/kustomize.md) for what the lab overlay changes and what it **cannot** test.

## Status

Design phase. `k8s/` is empty; the manifest inventory is in [docs/roadmap.md](docs/roadmap.md) §2 and gated on the decisions in §3.
