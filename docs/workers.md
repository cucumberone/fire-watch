# Workers

Consume events from RabbitMQ, persist them to Dragonfly, acknowledge last.

## 1. Topology

| Item | Value |
|---|---|
| Kind | `Deployment`, 3 replicas |
| Image | `image-registry.openshift-image-registry.svc:5000/fire-watch/worker:<sha>` |
| Runtime | Node.js 22 LTS |
| Ports | none exposed; HTTP `/healthz` on 8080 for probes only |
| State | none — everything reconstructible from RabbitMQ + Dragonfly |
| Anti-affinity | required against `app.kubernetes.io/name=worker` on `kubernetes.io/hostname` |
| PDB | `minAvailable: 1` — workers are throughput, not quorum; pinning at 2 would block legitimate single-pod maintenance |

Workers are stateless, so HPA is safe here (unlike RabbitMQ/Dragonfly). Start with 3 fixed; scale on queue depth only after a measured need.

## 2. Responsibilities

1. Open an AMQP connection to `rabbitmq` Service, `confirm.select`, `basic.qos(prefetch)`.
2. Consume `q.event.ingest`.
3. Validate the envelope, including `severity` and the presence of a broker timestamp.
4. Compute `broker_ts_ms` = `x-original-ts` ?? `timestamp_in_ms` ?? `properties.timestamp * 1000` ([event-lifecycle.md](event-lifecycle.md) §3, §6).
5. **Compress** `payload.msgc` (and any payload field ≥ `FW_COMPRESS_MIN`) with gzip, leaving `msg` and the envelope plain ([payload-and-compression.md](payload-and-compression.md) §3).
6. Write the event record and index entry to the current Dragonfly master (resolved via Sentinel).
7. Apply the alert-state fold via the atomic Lua script ([event-lifecycle.md](event-lifecycle.md) §5.2) — this is the step that is watermark-guarded.
8. Publish a **plain** copy to `fx.audit` / `q.audit.events` with a publisher confirm — the WAL is never compressed (§3.2).
9. Publish to `fw:ch:events`, including the fold's `applied` flag, with the same compressed payload that went to Dragonfly.
10. `basic.ack` — only now.
11. On unrecoverable validation failure: `basic.nack(requeue=false)` → DLX → `q.event.ingest.dlq`.
12. On transient failure (Dragonfly unavailable, replica lag, audit queue full): `basic.nack(requeue=true)` with backoff, or drop the channel and let redelivery handle it.

## 3. Ordering of operations (critical)

```text
validate -> COMPRESS msgc -> WRITE dragonfly (record + index + alert fold) -> PUBLISH audit (plain) -> PUBLISH channel (compressed) -> ack
```

Why the write precedes the ack: acknowledging first is a data-loss window. If the process dies between `ack` and the writes, the message is gone from RabbitMQ with nothing in Dragonfly — a direct R1 violation.

Why compression is only applied to the Dragonfly/pub-sub copies: the RabbitMQ original is the recovery source ([§3.2](#32-two-copies-different-encodings)).

Why the audit publish precedes the ack: Dragonfly has no AOF, so an acked message that only lives in Dragonfly memory is lost if the master and both replicas die ([persistence-and-durability.md](persistence-and-durability.md) §3). The audit copy is what makes replay possible.

Why pub/sub is published only after the write: a browser must never see an event that is not yet durable. A subscriber that misses the pub/sub message recovers via the index (`fw:idx:src:{source}`) on reconnect.

### 3.1 Redelivery does not break closing semantics

A worker failure **after** the Dragonfly write but **before** the ack causes a redelivery. That is safe by construction, and it is the exact scenario the design was built for:

1. The redelivered message carries its **original** broker timestamp, so the alert fold sees an old timestamp and treats it as stale if anything newer has been applied ([event-lifecycle.md](event-lifecycle.md) §6).
2. The record and index writes are idempotent, so a replay produces identical state.
3. The fold is an atomic Lua script, so two workers racing on the same key converge on the highest timestamp.

Worked example — an old CLOSE must not retire a newer CRITICAL:

| Step | Action | Alert state |
|---|---|---|
| 1 | CRITICAL arrives, `ts=1000` | open, watermark 1000 |
| 2 | CLOSE arrives, `ts=1500`, Dragonfly write fails → `nack(requeue=true)` | unchanged, watermark 1000 |
| 3 | new CRITICAL arrives, `ts=2000` | open, watermark 2000 |
| 4 | CLOSE redelivered with `ts=1500` | fold returns `0` (stale) — **alert stays open** |

Without broker timestamps and the watermark this sequence would silently retire an active alarm. Full treatment in [event-lifecycle.md](event-lifecycle.md) §4–5.

### 3.2 Two copies, different encodings

The worker produces **two** representations of the same event and they must not be confused:

| Copy | Destination | Encoding | Why |
|---|---|---|---|
| WAL | `q.audit.events` | **plain** JSON | replay and export must not depend on an app codec ([payload-and-compression.md](payload-and-compression.md) §3.1) |
| Cache | `fw:evt:{id}` + `fw:ch:events` | compressed `msgc` | memory, snapshot and network savings |

Consequences to implement carefully:

- The audit publish happens **before** the ack but **after** compression is computed, reusing the original buffer — do not re-serialize the event twice.
- The compressed value is written to Dragonfly **before** the audit publish, so a crash in between leaves a cache entry with no WAL entry. That is the safe direction: the message is still unacked and will be redelivered, and the cache write is idempotent.
- A replay reads plain WAL records and re-runs compression, so replay output is identical to live output.

## 4. Idempotency

RabbitMQ is at-least-once: a message is redelivered whenever a worker dies before acking. Writes are therefore keyed by `id` and guarded by the broker timestamp:

```
SET fw:evt:{id} <json> EX 604800
ZADD fw:idx:src:{source} <broker-ts-ms> <id>
EVAL alert-fold.lua 2 fw:alert:{key} fw:alert:open:{key} <broker-ts-ms> <id> <severity> <FW_ALERT_DEDUP_WINDOW>
PUBLISH fw:ch:events <json>
```

- `SET` is naturally idempotent.
- `ZADD` on an existing member only updates the score, also idempotent.
- The alert fold is idempotent **by timestamp**: re-applying an event with a timestamp the watermark has already passed returns `0` and changes nothing ([event-lifecycle.md](event-lifecycle.md) §5).
- The fold also performs **occurrence coalescing**: repeats of the same `{key}` inside `FW_ALERT_DEDUP_WINDOW` update `last_seen` without incrementing `count`, so a permanently-firing sensor produces one alert row rather than 1 000/min ([event-lifecycle.md](event-lifecycle.md) §5.4).
- Note the index score is the **broker timestamp**, not the producer `ts`. A redelivered event therefore keeps its original position in the timeline instead of jumping to its (later) re-arrival time.

Fold return codes, forwarded as `applied`:

| Code | Meaning |
|---|---|
| `0` | stale — history only |
| `1` | new occurrence |
| `2` | coalesced into the current occurrence |
| `3` | alert closed |

Duplicate processing produces no duplicate state. The one non-idempotent step is `PUBLISH` (a duplicate would double-render in the UI). Mitigation: each gateway keeps an in-process LRU of the last 10 000 event ids and drops repeats. It is deliberately **not** a Dragonfly write — gateways have no write access ([websocket-server.md](websocket-server.md) §5).

### 4.1 Index pruning is mandatory

`fw:evt:{id}` expires on its own TTL, but a sorted-set member never does. Without pruning the index grows forever and the working set becomes unbounded regardless of TTL — which would break the capacity model in [sizing.md](sizing.md) §2 entirely.

Every `FW_INDEX_PRUNE_EVERY` writes per source (default `1000` — confirmed as the agreed cadence), the worker also issues:

```
ZREMRANGEBYSCORE fw:idx:src:{source} -inf (now - 604800000)
```

- Cost: at 16.7 writes/s sustained with 500 sources and a prune every 1 000 writes, this is **≈0.017 calls/s** fleet-wide — free. Even during a 500/s storm it stays ≈0.5 calls/s.
- It must be **piggybacked on the write path**, not a cron job: a cron job cannot know which sources are active and would scan dead keys.
- It is not idempotent-critical; a missed prune is corrected by the next one.
- Verify with `ZCARD fw:idx:src:<source> <= writes-in-7d` ([sizing.md](sizing.md) §6).

### 4.2 Two dedup mechanisms, not one

The word "dedup" covers two unrelated jobs. Both are implemented, in different ways, for different reasons.

| | Transport dedup | Semantic dedup |
|---|---|---|
| Problem | the same **message** arrives twice | the same **condition** is reported 1 000×/min |
| Cause | at-least-once delivery, crash before ack, replay | a sensor that stays faulted |
| Mechanism | idempotent `SET` / `ZADD` on `id`, plus the watermark | the fold's `count` / `last_seen` rollup with `FW_ALERT_DEDUP_WINDOW` |
| Guarantees | exactly one stored record per event id | one alert row per condition, `last_seen` moving, `count` counted in windows |
| Fails if missing | duplicate history rows | 10 million alert rows in a week, unusable UI |

Transport dedup is what §4 above describes. Semantic dedup is inside `alert-fold.lua` and is specified in [event-lifecycle.md](event-lifecycle.md) §5.4.

What each layer contributes to "only update last occurrence":

1. `SET fw:evt:{id}` — the record is written once; a redelivery overwrites identical bytes.
2. `ZADD` — the index member is not duplicated; only its score is set.
3. Watermark — a stale redelivery cannot move state backwards.
4. Occurrence window — a repeat updates `last_seen` and **not** `count`, which is exactly "only update last occurrence".
5. Gateway LRU — the browser is not shown the same `id` twice.

Verification for all five is in §10 and [validation.md](validation.md) §6.

## 5. Replica-sync guard (write path)

Per [dragonfly.md](dragonfly.md) §3, writes must not land on a master that has lost its replicas. Workers therefore check before writing:

```text
if the dragonfly Service has no ready endpoint -> do not write; nack(requeue=true)
```

The gate is the **Service**, not a key inside Dragonfly. A degraded Dragonfly cannot be asked to report its own degradation. `sentinel-bridge` reads every pod's `ROLE`, and when the designated master has fewer than `FW_WRITE_GUARD_MIN_REPLICAS` connected replicas it sets `Endpoints/dragonfly` to zero addresses and labels the pod `fire-watch.io/writable=false`. Workers observe the empty endpoint list and stop writing ([dragonfly.md](dragonfly.md) §3, [fault-tolerance.md](fault-tolerance.md) §3.7).

This also keeps the rule that the write target must come from the `dragonfly` Service / Sentinel — never from a pod IP (see [redis-sentinel.md](redis-sentinel.md) §5).

## 6. Connection and failure handling

| Failure | Handling |
|---|---|
| Dragonfly master changes | Sentinel-aware client reconnects to the new master; in-flight write retried once, then `nack(requeue=true)` |
| Dragonfly returns `READONLY` | This worker is pointed at a demoted node: refresh master from Sentinel, retry once, else `nack(requeue=true)` |
| RabbitMQ connection lost | Client auto-reconnects; unacked messages are redelivered (at-least-once) |
| Message fails schema validation | `nack(requeue=false)` → DLX → `q.event.ingest.dlq`, increment `fw_worker_dlq_total` |
| Dragonfly Service has no ready endpoint | The master is gone or the write guard tripped it: `nack(requeue=true)`, back off, retry forever |
| Pod is evicted mid-write | Unacked message redelivered; the partial write is overwritten idempotently |

**`nack(requeue=false)` is permitted for validation failures only.** Every transient condition must requeue. The queue deliberately carries **no `x-delivery-limit`**, so an event can be retried for the entire duration of a Dragonfly outage without ever being dead-lettered ([fault-tolerance.md](fault-tolerance.md) §3.1).

`prefetch`: default 32. Higher raises throughput and re-delivery blast radius on restart; keep it modest until throughput requires otherwise.

## 7. Probes

| Probe | Endpoint | Notes |
|---|---|---|
| startup | `GET /healthz` | returns 200 when the AMQP connection and Dragonfly client are both established |
| readiness | `GET /healthz` | must be **false** while the write guard is degraded — a worker that cannot write must not consume |
| liveness | `GET /livez` | process-only; never fails on dependency loss, or an outage restart-loops all workers simultaneously |

Readiness failing on a degraded write guard is what turns backpressure into a natural pause: the queue retains messages, nothing is lost.

## 8. Sizing

| Aspect | Value | Rationale |
|---|---|---|
| Requests | 100m CPU, 256Mi RAM | I/O-bound |
| Limits | 500m CPU, 512Mi RAM | JSON parse + two network round trips |
| Expected throughput | ~1 000–3 000 msg/s per pod at 2 KiB payload | 1 000/min sustained with 500/s storms is met by a single pod; 3 replicas exist for H/A, not capacity ([sizing.md](sizing.md) §1) |
| HPA (later) | 3..12 on `rabbitmq_quorum_queue_messages_ready` | scale on backlog, not CPU |

## 9. Configuration

| Key | Source | Example |
|---|---|---|
| `AMQP_URL` | `Secret/worker-amqp` | `amqp://fw:<pass>@rabbitmq:5672/%2f` |
| `SENTINELS` | ConfigMap | `sentinel-0.sentinel-headless:26379,sentinel-1...,sentinel-2...` |
| `DFLY_MASTER_NAME` | ConfigMap | `fire-watch-dragonfly` |
| `DRAGONFLY_PASSWORD` | `Secret/dragonfly` | injected via `sentinel auth-pass` + client options |
| `FW_EVENT_TTL` | ConfigMap | `604800` (7 days; retention rationale in [sizing.md](sizing.md) §1) |
| `FW_INDEX_PRUNE_EVERY` | ConfigMap | `1000` — writes per source between `ZREMRANGEBYSCORE` prunes ([architecture.md](architecture.md) §6.2) |
| `FW_ALERT_DEDUP_WINDOW` | ConfigMap | `300000` — ms; repeats inside this window coalesce into one occurrence ([event-lifecycle.md](event-lifecycle.md) §5.4) |
| `FW_COMPRESS_MIN` | ConfigMap | `1024` — gzip a payload field only if its UTF-8 length ≥ this ([payload-and-compression.md](payload-and-compression.md) §7) |
| `FW_COMPRESS_LEVEL` | ConfigMap | `6` — gzip level |
| `FW_PREFETCH` | ConfigMap | `32` |

`.gitignore` must exclude any `.env`, `*.pem`, `*.key`, `secrets/`.

## 10. Verification checklist

```bash
kubectl -n fire-watch logs deploy/worker -f
# expect: "connected amqp", "sentinel master resolved", "consuming q.event.ingest"

# publish a test event and confirm the round trip
kubectl -n fire-watch exec rabbitmq-0 -- rabbitmqadmin publish exchange=fx.events routing_key=event.ingest \
  payload='{"id":"t1","source":"probe","type":"test","severity":"CRITICAL","ts":"2026-11-09T00:00:00Z","payload":{}}'
kubectl -n fire-watch exec dragonfly-0 -- redis-cli -p 6379 GET fw:evt:t1
kubectl -n fire-watch exec dragonfly-0 -- redis-cli -p 6379 ZRANGE fw:idx:src:probe 0 -1
kubectl -n fire-watch exec dragonfly-0 -- redis-cli -p 6379 HGETALL fw:alert:probe:test

# idempotency: publish the same id twice, expect one record and one index entry
kubectl -n fire-watch exec dragonfly-0 -- redis-cli -p 6379 ZCARD fw:idx:src:probe   # -> 1

# close semantics: CLOSE closes, and a stale CLOSE is refused
kubectl -n fire-watch exec rabbitmq-0 -- rabbitmqadmin publish exchange=fx.events routing_key=event.ingest \
  payload='{"id":"t2","source":"probe","type":"test","severity":"CLOSE","ts":"2026-11-09T00:01:00Z","payload":{}}'
kubectl -n fire-watch exec dragonfly-0 -- redis-cli -p 6379 EXISTS fw:alert:open:probe:test     # -> 0

# kill a worker mid-flight and confirm no loss
kubectl -n fire-watch delete pod -l app.kubernetes.io/name=worker --field-selector status.phase=Running
# expect: queue drains again, no messages in q.event.ingest.dlq
```

The adversarial ordering case (redelivered CLOSE vs newer CRITICAL) is drill 6.3 in [validation.md](validation.md) §6 and cannot be verified by hand-publishing alone.
