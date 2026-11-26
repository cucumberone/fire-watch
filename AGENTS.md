# Fire-Watch Event Server

This repo sets up an event server.

## Tech Stack

- **No data loss.** Dataloss is not accepted. Unless operator deletes an event.
- **OpenShift** as the target platform.
- **100% H/A** for every component, and it must handle split brain for every component.
  - 3 replicas on different nodes.
  - Must handle eviction / deletion of pods etc.
- **Valkey operator is off the table.** Make it work with manifests.
- **WebSocket node deployment** acting as server for web clients. Can scale with HPA or whatever it is called.
- **React frontend** talking to the node websocket server for visualization.

## Deployment

- Create manifests that fit OpenShift (i.e. **routes instead of ingress**) in the `k8s` directory.
- **No RabbitMQ.** The HTTP ingress writes straight to a Valkey stream (`XADD fw:stream:ingest`, then `WAIT 1`) and answers 202 only after that; workers read it with a consumer group.
- **Valkey:** 3 pods (1 master + 2 replicas) on different nodes, **AOF on every pod**. `min-replicas-to-write 1`, enforced by the engine — one Valkey pod can be lost and writes continue, an isolated master refuses writes. Sentinel quorum is 2. The lab runs **1 pod** with it set to `0`.
- **One record per condition.** The Valkey key is derived from `EVT_Device` + `EVT_Summary` (`event:<sha256[0:32]>`), so a duplicate is dropped by updating `lastSeen` / `count` on the existing hash instead of storing a second record.
- **Workers:** 3 replicas reading `fw:stream:ingest` with `XREADGROUP` (group `workers`), running the same validate → compress → history → fold logic, and removing an entry with `XACK` + `XDEL` (one MULTI) only after Valkey confirmed the write. Failed entries stay pending and are retried; a dead worker's entries are taken over with `XAUTOCLAIM`; unprocessable ones go to `fw:stream:dlq`.
- **Probes:** if you see the need for readiness probes, create them and handle them.
- **Valkey persistence:** use the AOF feature, but refresh that file with a new one from what's left in Valkey once it reaches 32 MB. This is to make restart and reread fast. Implemented as `appendonly yes` + `appendfsync everysec`, with rotation at 32 MB of AOF growth enforced by the `aof-rewriter` sidecar (the engine has no "N bytes of growth" setting, and `auto-aof-rewrite-percentage 0` disables its own trigger rather than relaxing it).
- **Redis Sentinels:** needed for the workers, I think.
- **Sizing:** 1 000 events/min sustained (500/s storm peaks), 7-day retention — see [docs/sizing.md](docs/sizing.md), which shows this needs ~52 GiB limit per Valkey pod.

## Plan

- Refine this spec and put the different components in their own `.md` files in the `docs` directory, then just link them in `AGENTS.md`.
- Always update the relevant files under `docs` whenever changes are made.

## Documentation

- [Architecture](docs/architecture.md) — components, data flow, message contract, failure matrix
- [Event Schema](docs/event-schema.md) — the producer contract (`EVT_*`), severity mapping, validation rules
- [Fault Tolerance Audit](docs/fault-tolerance.md) — per-component H/A and split-brain verification, 9 defects found and fixed
- [Event Lifecycle](docs/event-lifecycle.md) — severities, close semantics, stream-entry-ID ordering, content-keyed dedup, redelivery safety
- [Acknowledgement](docs/acknowledge.md) — operator ack, the one gateway write, cleared on re-fire
- [Operator Journal](docs/journal.md) — right-click notes per incident, append-only, signed
- [Payload & Compression](docs/payload-and-compression.md) — `msg` / `msgc`, gzip at the worker, `DecompressionStream` in the browser
- [Valkey](docs/valkey.md) — master + 2 replicas, `min-replicas-to-write 1`, AOF on every pod, the ingest stream, the `event:<key>` record model, the Lua fold, sizing
- [Redis Sentinel](docs/redis-sentinel.md) — failover, `sentinel-bridge`, split-brain walkthrough
- [Persistence & Durability](docs/persistence-and-durability.md) — how "no data loss" is met (**read §3**)
- [Configuration](docs/configuration.md) — the left nav and its two pages: Data to columns (global mapping of `EVT_*` keys to column names, `config:columns`, locked core columns) and Ingest schema (`input-schema`, operator-added keys, the identifier hash that becomes the record key)
- [History](docs/history.md) — the per-delivery audit trail: `hist:` + `idx:history`, 3-day retention, why it is written inside the fold, `op: history`, the History tab
- [Workers](docs/workers.md) — consumer group, write then XACK+XDEL, retry, XAUTOCLAIM, DLQ stream, idempotency
- [WebSocket Server](docs/websocket-server.md) — stateless gateway, subscription protocol, windowed event queries, HPA
- [Frontend](docs/frontend.md) — static React + MUI app, two Routes, CSP
- [Networking](docs/networking.md) — Services, Routes, ports, why NetworkPolicies are omitted
- [HTTP Ingress](docs/http-ingress.md) — external nginx → Valkey stream, the 202 contract, `XADD` + `WAIT`
- [Monitoring](docs/monitoring.md) — the alert set, the two exporters (Sentinel and master), and the metric names that were wrong before
- [Kustomize](docs/kustomize.md) — base + overlays, the `lab` single-replica overlay, the `ghcr` image overlay
- [Images](docs/images.md) — GitHub Actions build/push to GHCR on a path-filtered `push` to `main` (plus manual dispatch), tag derivation, pull secret, pinning, weekly version purge, repo variables vs secrets for deploy
- [Sizing](docs/sizing.md) — capacity model and resource totals
- [Validation](docs/validation.md) — failure drills and acceptance gate
- [Roadmap](docs/roadmap.md) — phases, manifest inventory, open decisions, deviations

## Guidelines

- **commits** Never commit or push until I say so.
- **Security:** Always add any sensitve files like keys secrets etc into .gitignore

## Agent Directives & Behavior

### Output Style & Brevity Rules

- **Language** Always return the prompt in English.

- **Direct Answers First:** Never begin a response with a conversational preamble, acknowledgment, or filler phrases (e.g., "Sure, I can help with that", "Based on your request", "Here is"). Start immediately with the answer or tool output summary.

- **Maximum Conciseness:** Eliminate all redundant text, conversational wrap-ups, and unrequested code summaries. Provide only the requested code, fix, or data.

- **Strict Conversational Ban:** Do not feign human emotions, offer pleasantries, or apologize unless a critical execution error occurs. Keep the tone clinical, objective, and developer-centric.

- **Code-First Hierarchy:** When answering code queries, output the copy-pasteable snippet immediately. Place brief bulleted explanations *below* the code block, never above it.

- **No Conceptual Lectures:** Do not explain the history, theory, or architectural concepts of a technology unless explicitly asked to define or explain it.

