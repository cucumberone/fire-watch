# WebSocket Server

Node.js gateway that streams events to browser clients over WebSocket. Stateless and horizontally scalable.

## 1. Topology

| Item | Value |
|---|---|
| Kind | `Deployment` + `HorizontalPodAutoscaler` (3..12) |
| Image | `image-registry.openshift-image-registry.svc:5000/fire-watch/ws-gateway:<sha>` |
| Runtime | Node.js 22 LTS, `ws` or `uWebSockets.js` |
| Port | 8080 HTTP + WS |
| Exposure | OpenShift `Route` with `haproxy.router.openshift.io/timeout: 1h` |
| State | none; clients are re-established after a pod disappears |
| Anti-affinity | **required** against `app.kubernetes.io/name=ws-gateway` on `kubernetes.io/hostname` |
| PDB | `minAvailable: 2` |

Anti-affinity is **required**, not preferred. The earlier reasoning ("preferred, because required spread would leave pods Pending on a 2-node cluster") does not apply: the target is 3 nodes ([sizing.md](sizing.md) §3.2), so required spread always schedules. Preferred affinity would allow all three pods to land on one node, and losing that node would take the gateway to **zero replicas** — the opposite of the H/A requirement ([fault-tolerance.md](fault-tolerance.md) §3.4).

Accepted consequence: after a node loss the third pod stays `Pending`, because no node may hold two copies. Same behaviour as RabbitMQ and Dragonfly, and the correct one.

## 2. Why stateless works

Each gateway pod simply subscribes to Dragonfly pub/sub and fans out to its own sockets. No pod needs to know about any other pod:

```text
on client connect:
  subscribe to fw:ch:events (+ optional filter)
  backfill: ZRANGEBYSCORE fw:idx:src:{source} (now - since, now) then MGET fw:evt:{id}
  stream: forward every fw:ch:events message matching the subscription
```

Any pod can serve any client, so a Route round-robins and an HPA can add pods freely. No sticky sessions required.

## 3. Subscription protocol

Client → server (JSON, first frame after connect):

```json
{ "op": "subscribe", "sources": ["sensor-a"], "types": ["telemetry.temperature"], "since": "2026-11-09T11:55:00Z" }
```

Server → client:

```json
{ "op": "event", "event": { "id": "...", "source": "sensor-a", "type": "telemetry.temperature", "severity": "CRITICAL", "ts": "...", "payload": {} }, "applied": 1 }
{ "op": "alert", "key": "sensor-a:telemetry.temperature", "state": "open", "severity": "CRITICAL", "first_seen": 1762699200000, "last_seen": 1762699260000, "count": 3 }
{ "op": "alert", "key": "sensor-a:telemetry.temperature", "state": "closed", "closed_at": 1762699900000 }
{ "op": "hello", "server": "ws-gateway-5f7c", "backfill": 42 }
{ "op": "error", "code": "bad_filter", "message": "unknown source" }
```

Rules:

- `since` bounds the backfill. A missing `since` means live-only — never an unbounded scan, which would stall the pod. Backfill ranges use the **broker timestamp** index (`fw:idx:src:*`), so they stay correct even when producer clocks are skewed.
- `applied` is the alert fold's return code from the worker ([event-lifecycle.md](event-lifecycle.md) §5.2): `0` stale, `1` new occurrence, `2` coalesced, `3` closed. A client must not treat `2` as a new alert entry (it only moves `last_seen`) and must not treat `0` as anything at all. Ignoring the code produces a close/reopen flicker on stale CLOSEs and a count that increments 1 000×/min ([event-lifecycle.md](event-lifecycle.md) §9).
- `op: alert` frames carry the **coalesced** alert row, not the raw event: `count` is occurrences separated by more than `FW_ALERT_DEDUP_WINDOW`, and `last_seen` moves on every applied repeat. Alert frames are emitted from `fw:ch:alerts`; a client that missed one recovers by re-reading `fw:alert:*` on reconnect rather than by replaying events.
- `payload.msgc` may arrive **gzip-compressed** and base64-encoded. The gateway passes it through untouched — it must not decompress, because that adds per-message CPU to every pod for no benefit ([payload-and-compression.md](payload-and-compression.md) §5.1).
- Raw events are still delivered on `op: event` for the event log. The UI must not compute alert state from them.
- Backfill is capped (`FW_BACKFILL_MAX`, default 2000 records); when exceeded, reply with `"truncated": true` and let the client narrow its window.
- Heartbeat: server sends `{"op":"ping"}` every 20s; the client must reply `{"op":"pong"}` within 10s or the socket is closed. Required because an OpenShift router will not report a half-open TCP connection.

## 4. Resource guardrails per connection

| Limit | Default | Reason |
|---|---|---|
| Max frames/sec from a client | 10 | prevents a chatty client from consuming the pod |
| Max subscriptions per connection | 20 | bounds the matcher cost |
| Max outbound queue per connection | 1000 messages | drop with `error: slow_consumer` instead of unbounded growth |
| Idle timeout | 5 min without pong | frees sockets from vanished clients |
| Max connections per pod | 5000 | see sizing; HPA scales on this |

Backpressure policy is explicit: a slow consumer is disconnected, not buffered. Buffering turns one slow browser into a memory incident that takes down every client on the pod.

## 5. Dragonfly access

| Aspect | Decision |
|---|---|
| Master discovery | Sentinel-aware client ([redis-sentinel.md](redis-sentinel.md) §5) |
| Pub/sub | `SUBSCRIBE fw:ch:events` on the **master**, since Dragonfly propagates pub/sub from the primary |
| Backfill reads | `ZRANGEBYSCORE` + `MGET` against the master by default |
| Read scaling (later) | point backfill reads at a replica Service once replica lag is monitored; not needed at current scale |
| Write access | **exactly one method**: `ackAlert` ([acknowledge.md](acknowledge.md) §4) |

There is no general write surface. `dragonfly.js` has no `set`, `del` or generic `eval`, so the gateway cannot modify events, indexes or alert state beyond the two ack fields. The absence of a method is a stronger guarantee than a comment.

Because no event write is possible, de-duplication is an in-process LRU of recent event ids (`FW_DEDUPE_LRU`, default 10 000), not a Dragonfly `SET`.

### 5.1 Acknowledgement writes

An operator ack is the one client-initiated mutation, and it is deliberately narrow:

| Guard | Reason |
|---|---|
| fixed Lua, `HSET` of `acked_by` / `acked_at` only | `severity`, `count`, `last_seen` and the watermark belong to the worker's fold; writing them here would race it |
| `EXISTS` check first | an ack-only hash would render as a phantom row |
| first-ack-wins | two clicks cannot fight, and a second click cannot silently re-attribute |
| `PEXPIRE` extends the existing TTL | never invents retention |
| READONLY → re-resolve master once, then fail | a demoted node must not report a successful ack |

Broadcast is a single `fw:ch:alerts` frame, which every pod already receives, so a click reaches browsers on other pods with no pod-to-pod coordination.

### 5.2 Compression transparency

The gateway is a **byte pipe** for payload fields: `payload.msgc` may be gzip, and the gateway forwards it base64-encoded without decoding.

| Why not decompress here | Reason |
|---|---|
| CPU cost × N pods | every gateway pod would decompress every message, per connected source filter |
| memory churn | inflated strings for every subscriber, then discarded |
| pointless | the browser can decompress natively with `DecompressionStream` |

The gateway *does* need the plain envelope (`id`, `source`, `type`, `severity`, broker ts), which is never compressed — that is what makes filtering and subscription matching possible without touching the payload ([payload-and-compression.md](payload-and-compression.md) §4).

## 6. OpenShift Route

```yaml
apiVersion: route.openshift.io/v1
kind: Route
metadata:
  name: ws-gateway
  annotations:
    haproxy.router.openshift.io/timeout: 1h
    haproxy.router.openshift.io/disable_cookies: "true"   # no stickiness needed
spec:
  to: { kind: Service, name: ws-gateway }
  port: { targetPort: 8080 }
  tls: { termination: edge, insecureEdgeTerminationPolicy: Redirect }
  wildcardPolicy: None
```

- Routes, never Ingress (R2).
- The router's default 30s timeout kills WebSockets; the annotation is mandatory.
- Edge TLS terminates at the router; in-cluster traffic stays plaintext unless the service mesh requires otherwise.

## 7. Probes

| Probe | Endpoint | Notes |
|---|---|---|
| startup | `GET /healthz` | 200 once the Dragonfly subscriber is established |
| readiness | `GET /healthz` | false when the subscriber is disconnected — a pod that cannot subscribe should not receive new sockets |
| liveness | `GET /livez` | process-only |

`terminationGracePeriodSeconds: 60` plus a preStop that sends `{"op":"shutdown"}` to all sockets and closes them, so a rolling update does not look like a crash to every browser. Clients must implement reconnect with jittered exponential backoff.

## 8. Sizing and HPA

| Aspect | Value | Rationale |
|---|---|---|
| Requests | 200m CPU, 256Mi RAM | mostly idle per connection |
| Limits | 1 CPU, 768Mi RAM | fan-out to 5000 sockets at ~1 KB payload |
| HPA metric | custom: `fw_ws_connections` avg > 3000, or CPU > 70% | scale before saturation, with a 300s stabilization window to avoid flapping |
| HPA bounds | min 3, max 12 | min 3 satisfies R3 |

Publish `fw_ws_connections{instance}` and `fw_ws_msgs_out_total` from the gateway for the HPA and for alerting.

## 9. Verification checklist

```bash
kubectl -n fire-watch get route ws-gateway -o jsonpath='{.spec.host}{"\n"}'

# connect and subscribe (websocat or wscat)
websocat -t "wss://$(kubectl -n fire-watch get route ws-gateway -o jsonpath='{.spec.host}')" \
  <<< '{"op":"subscribe","sources":["probe"],"since":"2026-11-09T00:00:00Z"}'

# publish an event and confirm it arrives on the socket
kubectl -n fire-watch exec rabbitmq-0 -- rabbitmqadmin publish exchange=fx.events routing_key=event.ingest \
  payload='{"id":"t2","source":"probe","type":"test","ts":"2026-11-09T00:10:00Z","payload":{}}'

# scale-out check
kubectl -n fire-watch scale deploy/ws-gateway --replicas=6
kubectl -n fire-watch get endpoints ws-gateway    # 6 addresses, Route load-balances across all
```
