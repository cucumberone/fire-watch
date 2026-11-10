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
{ "op": "snapshot", "alerts": [ { "key": "sensor-a:telemetry.temperature", "severity": "CRITICAL", "firstSeen": 1762699200000, "lastSeen": 1762699260000, "count": 3, "lastId": "abc", "ackedBy": null, "ackedAt": null, "event": { } } ] }
{ "op": "alert", "key": "sensor-a:telemetry.temperature", "state": "open", "severity": "CRITICAL", "first_seen": 1762699200000, "last_seen": 1762699260000, "count": 3 }
{ "op": "alert", "key": "sensor-a:telemetry.temperature", "state": "closed", "closed_at": 1762699900000 }
{ "op": "hello", "server": "ws-gateway-5f7c", "backfill": 42 }
{ "op": "error", "code": "bad_filter", "message": "unknown source" }
```

Rules:

- `since` bounds the backfill. A missing `since` means live-only — never an unbounded scan, which would stall the pod. Backfill ranges use the **broker timestamp** index (`fw:idx:src:*`), so they stay correct even when producer clocks are skewed.
- `applied` is the alert fold's return code from the worker ([event-lifecycle.md](event-lifecycle.md) §5.2): `0` dropped (duplicate or stale), `1` applied, `3` closed. `2` is a retired alias for `1`. A duplicate never reaches the client at all — the worker's `SET … NX` gate drops it — so treating `0` as an applied event would double-count, and treating it as a close would flicker the row.
- `op: alert` frames carry an alert row, not the raw event. `count` counts every applied event; a redelivered event is dropped by the worker's `SET … NX` gate before it can affect the count ([event-lifecycle.md](event-lifecycle.md) §5.4). Alert frames are emitted from `fw:ch:alerts`; a client that missed one recovers via the snapshot on reconnect rather than by replaying events.
- `payload.msgc` may arrive **gzip-compressed** and base64-encoded. The gateway passes it through untouched — it must not decompress, because that adds per-message CPU to every pod for no benefit ([payload-and-compression.md](payload-and-compression.md) §5.1).
- Raw events are still delivered on `op: event` for the event log. The UI must not compute alert state from them.
- Backfill is capped (`FW_BACKFILL_MAX`, default 2000 records); when exceeded, reply with `"truncated": true` and let the client narrow its window.
- Heartbeat: server sends `{"op":"ping"}` every 20s; the client must reply `{"op":"pong"}` within 10s or the socket is closed. Required because an OpenShift router will not report a half-open TCP connection.

### 3.0 Client → server ops

Four ops a client may send after `subscribe`:

| Op | Frame | Effect |
|---|---|---|
| `ack` | `{"op":"ack","key":"source:type"}` | claim the episode; first ack wins ([acknowledge.md](acknowledge.md) §4) |
| `close` | `{"op":"close","key":"source:type"}` | retire the alert as an operator ([acknowledge.md](acknowledge.md) §8.1) |
| `alerts` | `{"op":"alerts","keys":[...]}` | re-read ack state for the displayed rows |
| `resync` | `{"op":"resync"}` | re-send the open-alert snapshot (§3.3) |
| `closed` | `{"op":"closed"}` | the last 7 days of retired alerts (§3.5) |
| `allEvents` | `{"op":"allEvents","range":"24h"}` | every event in a time window, searchable (§3.6) |

`close` is confirmed with `{"op":"alert","key":…,"applied":3}` — the same frame a
source-sent CLOSE produces — so no client needs a second code path to retire a
row. Both `ack` and `close` answer `no_such_alert` rather than creating a hash: an
ack-only or close-only hash would render as a phantom row with no severity and no
count.

Bulk actions are **not** a batched op. The client sends one frame per key, which
keeps partial failure trivial to reason about and avoids inventing a batch result
shape the UI would have to render.

### 3.1 `op: snapshot` — why replaying events is not enough

The first version of this protocol had no snapshot, and the board was **empty on
every page load** while the cluster held 200 open alerts. That is not a frontend
bug; it is a missing read path:

- Backfill replays **events**. Events only become alert rows as they arrive, so on
  a quiet cluster nothing arrives and nothing renders.
- The event stream cannot reconstruct open state anyway: "an event was stored" and
  "this alert is still firing" are different observations, and a client that
  connects between two events has no way to learn about the other 199 alerts.

So on every `subscribe` the gateway sends `op: snapshot` **before** `hello`,
carrying every open alert the subscription matches, most recently active first,
each with its newest event attached so the row has a Summary and the inspector has
a full record.

### 3.2 `op: alert` carrying a row

`op: alert` is normally `{key, applied}` and nothing else. The event that caused it
arrives on its own `op: event` frame, so attaching the record here would duplicate
the payload on the hot path.

There is one case where that is not enough: a **content duplicate**. The worker
discards the copy and publishes `{kind:'occurrence', key, applied}` with no event
([event-lifecycle.md](event-lifecycle.md) §5.4), so a client that has never seen
this alert key receives a frame it cannot render a row from. The alert is genuinely
open in Dragonfly, but the board shows nothing until the next snapshot — which is
why a repeat after a CLEAR appeared only after a browser refresh.

So for an `occurrence` the gateway reads the alert hash **once** and attaches it:

```json
{"op":"alert","key":"src:type","applied":1,
 "row":{"severity":"WARNING","firstSeen":…,"lastSeen":…,"count":3,
        "event":{ …newest record, or null if it expired… }}}
```

- cost is one `HMGET` plus one `GET` of the newest event record, and only on an
  absorbed duplicate — the common path is untouched
- `readAlert` returns `null` when the key is not in `fw:idx:alert:open`, so a
  retired alert can never be resurrected as a live row
- if the read fails the bare frame is still sent: a client that already has the row
  moves its count, and one without it stays as it was rather than inventing a row

The client treats `row` as authoritative for a key it has not seen (it carries the
fold's real count), and ignores it for a key it already has so the visible count
cannot jump around ([frontend.md](frontend.md) §5.3).

**`row.count` already includes the occurrence that triggered the frame** — the
fold increments before the gateway reads the hash. The client must therefore
*use* it, not add to it. Adding one on top double-counted: a CLEAR followed by a
re-send showed `Count 2`, because the new episode had reset to 1 and the client
made it 2. When no row is present (an older gateway), the client falls back to
incrementing its own copy.

`handleAlertMessage` is therefore async, and its rejection is handled at the
subscriber — an unhandled rejection inside an event emitter would take the process
down and drop every client on the pod.

### 3.3 `op: resync` — repairing a board that missed a frame

The snapshot is only sent on `subscribe`, which happens once per connection. That
left a real failure mode, observed in the lab:

```
T0  client renders row X                        X is open
T1  worker retires X, publishes the fan-out
T2  that frame never reaches this client        (missed pong -> socket closed,
                                                 dropped pub/sub message across a
                                                 reconnect, throttled background
                                                 tab, laptop sleep)
T3  the client shows X forever
```

The symptom is a row that disappears on a browser refresh but not on its own —
which is exactly what a refresh fixes: it re-subscribes and receives a fresh
snapshot. Nothing is wrong in Dragonfly; the client simply never learned.

`{"op":"resync"}` re-sends the snapshot on the connection's **existing**
subscription. It is not "send `subscribe` again", for two reasons:

- every `subscribe` appends to `conn.subscriptions`, and those are capped by
  `FW_MAX_SUBSCRIPTIONS` (default 20). A client that resynced on a timer would
  exhaust the budget and then be unable to subscribe at all.
- `subscribe` also re-runs the bounded backfill, re-sending events the client
  already has.

No subscription is required. A client with none gets the unfiltered set, which is
what an unsubscribed client would have received anyway; erroring instead would
make the op unsafe to fire blindly from a timer.

**An empty snapshot is always sent**, never suppressed. The client replaces its
alert map with the snapshot, so "nothing is firing" is a real answer that has to
reach the client. An earlier version only sent the frame when `alerts.length > 0`,
and that single condition is what made stale rows possible: a resync at the moment
everything had closed would have returned nothing and left the old rows untouched.

The client sends `resync`:

- every `resyncSeconds` (runtime config, default 60),
- immediately on reconnect, since the disconnect window is when frames go missing,
- when the tab becomes visible again, because a backgrounded tab has its timers
  throttled and an operator returning to it is the common case.

`resyncSeconds: 0` disables the timer; the reconnect and visibility triggers stay.

Cost is the same as a connect snapshot: three round trips regardless of alert
count (§3.4), so a one-minute interval per client is negligible.

#### Deploy skew: a newer page against an older gateway

The frontend bundle and the ws-gateway image are **separate artifacts**, so a page
can be newer than the gateway serving it. When that happens the gateway answers

```json
{"op":"error","code":"bad_op","message":"unknown op resync"}
```

and the client must stop asking, because no retry can succeed and the fix is a
redeploy. Unhandled, this error reappears on every interval and every reconnect —
`bad_op: unknown op resync` every 60 seconds reads as a frontend bug and hides the
real cause.

So the client watches for that specific rejection, stops sending `resync` for the
lifetime of the page, and replaces the raw protocol error with a notice saying the
gateway needs updating. The match is deliberately narrow — `code === 'bad_op'`
**and** the message being exactly `unknown op resync` — so that a genuine
malformed-frame bug elsewhere still surfaces instead of silently disarming the
auto-refresh — see `isUnsupportedOpError` in `services/frontend/src/lib.js`.

Recovery is a page reload once the gateway has been updated; nothing is persisted,
so the next load tries `resync` again.

Two consequences worth stating:

- The snapshot is **unconditional** — it ignores `since`. "What is open right now"
  is not a time-windowed question, and gating it on `since` would have reproduced
  the empty board for any client that sends a recent timestamp.
- The client **replaces** its alert map with the snapshot rather than merging.
  The snapshot is the complete open set, so a row it omits is genuinely closed;
  merging would resurrect rows retired while the socket was down.

**`fw:idx:alert:open`.** Enumerating open alerts needs an index, and
`SCAN fw:alert:open:*` is precisely the unbounded scan §3 forbids. The index is a
zset (member = alert key, score = last activity) maintained **inside
`alert-fold.lua`**, not from application code. That placement is the point: the
close path and the open path are already atomic there, so the index can never
drift from `fw:alert:open:*`. A separate `ZADD` after the `EVAL` would leave a
phantom row on every operator's board whenever a worker died between the two
calls — and a phantom alert that nobody can clear is worse than a missing one.

The read is bounded by `FW_ALERT_SNAPSHOT_MAX` (default 2000), and the fold script
guards both index writes behind `if has_index`, so a worker running newer code
against an older cached script (or the reverse, during a rolling update) degrades
to "no index" instead of failing every event.

### 3.4 Snapshot cost: three round trips, measured

The snapshot runs on **every** connect, so the number of round trips must not grow
with the number of open alerts. This was got wrong first and had to be measured —
with 199 open alerts on the lab cluster:

| Shape | Time |
|---|---|
| `HGETALL` per alert, sequential (naive) | **1092 ms** |
| `HGETALL` per alert, pipelined | 102–694 ms |
| `HGETALL` per alert, `Promise.all` | 99–205 ms |
| Lua, 199× `HGETALL` + table fold | 97–204 ms |
| Lua, 199× `HMGET` (7 named fields) | 8–200 ms |
| one `MGET` of 199 keys | 3–80 ms |

The naive shape blocks the WebSocket handshake for ~1 s at 199 alerts and ~11 s at
the 2000 cap — which reads as "the gateway is hanging", not as a slow snapshot.

The shipped path is exactly **three** round trips regardless of alert count:
`ZREVRANGE` the index, one `EVALSHA` returning 7 named fields per alert via
`HMGET`, then one `MGET` for the event records. `HMGET` beat `HGETALL` because
`HGETALL` returns a flat field/value array that Lua then has to fold into a table —
1600 hash-table inserts for 199 alerts, which was the single largest cost.

Two Dragonfly-specific constraints shaped this:

- **Scripts may only touch declared keys.** Dragonfly enforces Redis Cluster
  semantics and rejects dynamic key construction:
  `ERR script tried accessing undeclared key, key: fw:alert:shop:check-175`.
  So the index must be read by the caller and every alert hash passed in `KEYS`,
  and the `fw:evt:*` records must be fetched afterwards — their keys are not known
  until the hashes have been read.
- **Lua cannot return tables with nil holes**; they truncate silently. Missing
  values are emitted as `""` and normalised in JS.

**Residual latency is the gateway's CPU limit, not the read path.** Baseline `PING`
to Dragonfly measured p50 0.93 ms / p95 5.5 ms, but ~1 in 30 operations stalled for
~80 ms, giving the bimodal 8 ms / ~100 ms pattern above. The cause is CFS
throttling: the gateway runs with `limits.cpu: 200m` and `/sys/fs/cgroup/cpu.stat`
showed **111 s** of cumulative `throttled_usec`, climbing ~600 ms every 3 s, while
Dragonfly was throttled in only 2 % of periods. The snapshot is a burst of CPU work
(Node string handling for 199 rows), so it lands right at the quota. If handshake
latency matters, raise that limit before optimising the reads further.

### 3.5 `op: closed` — the retired alerts

The Closed tab needs what is no longer firing, and neither existing op can supply
it: a snapshot is the OPEN set, and backfill replays events without knowing which
alert each one left behind.

```json
{"op":"closed"}
{"op":"closed","limit":200,"sinceMs":1762000000000}
```

Neither field is required; an empty frame means the default 7-day window and
`limit` 500. Both are **clamped, not rejected** — a client asking for more than
exists wants as much as there is, and refusing would leave it with an empty table
for no reason. A non-numeric value IS rejected, because that is a bug rather than
an over-ask.

The answer is one frame:

```json
{"op":"closed","alerts":[ … ],"truncated":false,"sinceMs":…}
```

Each row is a snapshot row (`key`, `severity`, `firstSeen`, `lastSeen`, `count`,
`event`) plus the closure metadata the tab sorts and filters on: `closedAt`,
`closedBy` (the id of the retiring event) and `state`.

Read path: `ZREVRANGEBYSCORE fw:idx:alert:closed` for the keys, one Lua script for
every hash, one pipeline for the closure fields, one MGET for the event records.
Four round trips regardless of row count, because a 7-day window is not small and
per-row reads would make the tab take seconds to open.

`sinceMs` filters **server-side**, so a shorter window costs strictly less work
rather than the server building the full set and the client discarding most of it.

`truncated` is honest rather than silent: a full page means "there may be more",
which the UI shows as a notice. Returning the newest N and saying nothing would
make a busy week look smaller than it is.

### 3.6 `op: allEvents` — the raw log, windowed

```json
{"op":"allEvents","range":"24h"}
{"op":"allEvents","range":"1h","limit":500,"before":1762000000000}
```

Answers "what happened in the last N", regardless of correlation. Three things it
is **not**:

- not the live tail — that is a bounded in-memory buffer that dies on reload; this
  reads storage and reaches the full 7-day retention
- not `closed` — that lists alert rollups; this lists events, including those that
  never opened an alert (`applied: 0`)
- not `subscribe` — a subscription filters by source/type and streams; this is a
  one-shot query

| Field | Default | Notes |
|---|---|---|
| `range` | `24h` | one of `1h`, `6h`, `24h`, `7d`. Anything else falls back to `24h` rather than erroring, so a stray value cannot blank the table |
| `limit` | 1000 | clamped; a full page sets `truncated` |
| `before` | now | pages backwards without overlapping the page already held |

**The window is applied server-side** (`ZREVRANGEBYSCORE` on `fw:idx:evt:all`), so a
1-hour request reads strictly less than a 7-day one instead of the server building
everything and the client discarding most of it.

Read path: the index range, then ONE `MGET` for the records, chunked at 256 ids so
neither the request nor the response is unbounded. An index member whose record has
expired is skipped rather than rendered as a blank row; the prune drops the member.

**The global index exists because the per-source one cannot answer this.** There is
no way to enumerate sources without a SCAN, so the worker writes every accepted
event to `fw:idx:evt:all` in the same step as `fw:idx:src:{source}`. An operator
close writes its CLEAR there too, and an operator delete removes the member —
otherwise a deleted event would stay listed and resolve to nothing.

## 4. Resource guardrails per connection

| Limit | Default | Reason |
|---|---|---|
| Max frames/sec from a client | `FW_MAX_FRAMES_PER_SEC` = 50 | prevents a chatty client from consuming the pod |
| Max subscriptions per connection | `FW_MAX_SUBSCRIPTIONS` = 20 | bounds the matcher cost |
| Max outbound queue per connection | `FW_MAX_OUTBOUND_QUEUE` = 1000 messages | drop with `error: slow_consumer` instead of unbounded growth |
| Heartbeat | `FW_HEARTBEAT_MS` = 20000 | cadence of the liveness ping |
| Pong timeout | `FW_PONG_TIMEOUT_MS` = 10000 | how long a socket has to answer before it is terminated |
| Max connections per pod | `FW_MAX_CONNS` = 5000 | see sizing; HPA scales on this |

Backpressure policy is explicit: a slow consumer is disconnected, not buffered. Buffering turns one slow browser into a memory incident that takes down every client on the pod.

### 4.1 Liveness: ONE flag

The heartbeat is a single property per socket, `ws.__awaitingPong`:

| Who | What they do |
|---|---|
| the loop, every `FW_HEARTBEAT_MS` | if the flag is already set → `terminate()`; else set it and `ping()` |
| `ws.on('pong')` — the protocol-level reply | `ws.__awaitingPong = false` |
| the `{"op":"pong"}` JSON frame | `ws.__awaitingPong = false` |
| a `FW_PONG_TIMEOUT_MS` timer | if the flag is still set → `terminate()` |

**This was broken, and it is worth recording how.** The pong handler set
`conn.alive`, which nothing ever read, while the loop tested and set
`ws.__awaitingPong`, which nothing ever cleared. Every socket was therefore
terminated one `PONG_TIMEOUT` after its first heartbeat — measured at **14.97 s,
close code 1006** — and because the client resets its backoff counter on `onopen`,
it reconnected about 500 ms later and died again. Forever. From the UI that looked
like a status chip flickering `open → reconnecting → open`, with the data
resilient because every `onopen` re-subscribes and resyncs.

Two lessons are encoded as tests in `subscription.test.js`:

- a liveness property that is written and never read is indistinguishable from a
  working one, so the loop and both pong paths must be shown to use the **same**
  name
- **terminating a connection is logged at `warn`, not `debug`.** At the default
  `info` level the bug produced no log output whatsoever — which is why the log
  looked like a healthy idle gateway while every socket died every 15 seconds


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

### 5.0 Fan-out must not echo to its own publisher

`ack` and `close` are confirmed **directly** to the client that asked, and then
broadcast on `fw:ch:alerts` so other pods' clients learn about it. Pub/sub has no
sender filtering, so that broadcast comes back to the publishing pod — which
fanned it out **again**, delivering the same frame twice to the originator.

For `close` that meant one operator close arrived as two `applied: 3` frames and
the client counted the Closed badge twice: the tab said 2, the list showed one
event with `count` 2.

The broadcasts therefore carry `origin: POD_NAME`, and each handler runs
`shouldFanOut(parsed, POD_NAME)` (exported from `subscription.js` so it is testable
without a live cluster). An **untagged** frame is still delivered: a peer on an
older build sends no origin, and a duplicate is recoverable while a missed close
leaves a row nobody can clear.

The client is defensive as well — it counts a retirement once per key per session,
forgetting the key when it is seen open again, so a genuine second episode still
counts while a duplicated frame does not.

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
