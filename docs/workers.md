# Workers

Consume events from RabbitMQ, persist them to Valkey, acknowledge last.

## 1. Topology

| Item | Value |
|---|---|
| Kind | `Deployment`, 3 replicas |
| Image | `image-registry.openshift-image-registry.svc:5000/fire-watch/worker:<sha>` |
| Runtime | Node.js 22 LTS |
| Ports | none exposed; HTTP `/healthz` on 8080 for probes only |
| State | none — everything reconstructible from RabbitMQ + Valkey |
| Anti-affinity | required against `app.kubernetes.io/name=worker` on `kubernetes.io/hostname` |
| PDB | `minAvailable: 1` — workers are throughput, not quorum; pinning at 2 would block legitimate single-pod maintenance |

Workers are stateless, so HPA is safe here (unlike RabbitMQ/Valkey). Start with 3 fixed; scale on queue depth only after a measured need.

## 2. Responsibilities

1. Open an AMQP connection to `rabbitmq` Service, `confirm.select`, `basic.qos(prefetch)`.
2. Consume `q.event.ingest`.
3. Validate the envelope, including `severity` and the presence of a broker timestamp.
4. Compute `broker_ts_ms` = `x-original-ts` ?? `timestamp_in_ms` ?? `properties.timestamp * 1000` ([event-lifecycle.md](event-lifecycle.md) §3, §6).
5. **Compress** `payload.msgc` (and any payload field ≥ `FW_COMPRESS_MIN`) with gzip, leaving `msg` and the envelope plain ([payload-and-compression.md](payload-and-compression.md) §3).
6. Write the event record and index entry to the current Valkey master (resolved via Sentinel).
7. Apply the alert-state fold via the atomic Lua script ([event-lifecycle.md](event-lifecycle.md) §5.2) — this is the step that is watermark-guarded.
8. Publish a **plain** copy to `fx.audit` / `q.audit.events` with a publisher confirm — the WAL is never compressed (§3.2).
9. Publish to `fw:ch:events`, including the fold's `applied` flag, with the same compressed payload that went to Valkey.
10. `basic.ack` — only now.
11. On unrecoverable validation failure: `basic.nack(requeue=false)` → DLX → `q.event.ingest.dlq`.
12. On transient failure (Valkey unavailable, replica lag, audit queue full): `basic.nack(requeue=true)` with backoff, or drop the channel and let redelivery handle it.

## 3. Ordering of operations (critical)

```text
validate -> COMPRESS msgc -> WRITE valkey (record + index + alert fold) -> PUBLISH audit (plain) -> PUBLISH channel (compressed) -> ack
```

Why the write precedes the ack: acknowledging first is a data-loss window. If the process dies between `ack` and the writes, the message is gone from RabbitMQ with nothing in Valkey — a direct R1 violation.

Why compression is only applied to the Valkey/pub-sub copies: the RabbitMQ original is the recovery source ([§3.2](#32-two-copies-different-encodings)).

Why the audit publish precedes the ack: Valkey has no AOF, so an acked message that only lives in Valkey memory is lost if the master and both replicas die ([persistence-and-durability.md](persistence-and-durability.md) §3). The audit copy is what makes replay possible.

Why pub/sub is published only after the write: a browser must never see an event that is not yet durable. A subscriber that misses the pub/sub message recovers via the index (`idx:source:<device>`) on reconnect.

### 3.1 Redelivery does not break closing semantics

A worker failure **after** the Valkey write but **before** the ack causes a redelivery. That is safe by construction, and it is the exact scenario the design was built for:

1. The record write is the fold itself. On a redelivery the `(ts, id)` watermark
   already
   exists, so the write returns `null` and the event is **dropped before anything
   else runs** — no fold, no index, no audit copy, no live frame. It is still
   ACKed, because a message that can never succeed must not loop forever.
2. The index write is idempotent (`ZADD` on a repeated member only moves its
   score), so a replay produces identical state.
3. The fold is an atomic Lua script, so two workers racing on the same key converge on the highest timestamp.

**Why identity is the event id, not the timestamp.** The fold's `(timestamp, id)`
watermark compares against the broker arrival time, so it catches a redelivery
(the same message keeps its original timestamp). It does *not* catch a producer
re-sending an id or two POSTs carrying the same id: each is a new message with a
new timestamp, so the repeat looks newer and would be counted every time. That is
what a duplicate storm actually looks like, and it is why the gate lives on the
event record rather than in the fold.

Worked example — an old CLOSE must not retire a newer CRITICAL:

| Step | Action | Alert state |
|---|---|---|
| 1 | CRITICAL arrives, `ts=1000` | open, watermark 1000 |
| 2 | CLOSE arrives, `ts=1500`, Valkey write fails → `nack(requeue=true)` | unchanged, watermark 1000 |
| 3 | new CRITICAL arrives, `ts=2000` | open, watermark 2000 |
| 4 | CLOSE redelivered with `ts=1500` | fold returns `0` (stale) — **alert stays open** |

Without broker timestamps and the watermark this sequence would silently retire an active alarm. Full treatment in [event-lifecycle.md](event-lifecycle.md) §4–5.

### 3.2 Two copies, different encodings

The worker produces **two** representations of the same event and they must not be confused:

| Copy | Destination | Encoding | Why |
|---|---|---|---|
| WAL | `q.audit.events` | **plain** JSON | replay and export must not depend on an app codec ([payload-and-compression.md](payload-and-compression.md) §3.1) |
| Cache | `event:<key>` + `fw:ch:events` | compressed `msgc` | memory, AOF and network savings |

Consequences to implement carefully:

- The audit publish happens **before** the ack but **after** compression is computed, reusing the original buffer — do not re-serialize the event twice.
- The compressed value is written to Valkey **before** the audit publish, so a crash in between leaves a cache entry with no WAL entry. That is the safe direction: the message is still unacked and will be redelivered, and the cache write is idempotent.
- A replay reads plain WAL records and re-runs compression, so replay output is identical to live output.

## 4. Idempotency

RabbitMQ is at-least-once: a message is redelivered whenever a worker dies before acking. Writes are therefore keyed by `id` and guarded by the broker timestamp:

```
EVAL event-fold.lua 5 event:<key> idx:event:all idx:event:open idx:event:closed idx:source:<device>
     <broker-ts-ms> <arrival-id> <severity> <bare-key> <device> <ttl-ms> <field> <value> …
PUBLISH fw:ch:events <json>
```

One call does the record, both timelines, both state indexes and the rollup. It is a
single `EVAL` precisely so that no interleaved writer can leave them disagreeing —
the reason the old design needed three separate writes was that the record key (the
message id) and the rollup key (the condition) were different objects.

- Every write is idempotent **by timestamp**: re-applying an event whose
  `(brokerTs, id)` the watermark has already passed returns `0` and changes nothing
  ([event-lifecycle.md](event-lifecycle.md) §5).
- `ZADD` on an existing member only sets its score, so an index cannot gain a
  duplicate member.
- There is **no coalescing window**. A repeat is counted, not suppressed: the
  duplicate problem is solved by the key, not by a clock
  ([event-lifecycle.md](event-lifecycle.md) §5.4.3).
- The index score is the **broker timestamp**, not the producer `ts`. A redelivered
  event therefore keeps its original position in the timeline.

Fold return codes, forwarded as `applied`:

| Code | Meaning |
|---|---|
| `0` | stale or redelivered — nothing changed |
| `1` | occurrence counted |
| `3` | record closed |

Duplicate processing produces no duplicate state. The one non-idempotent step is
`PUBLISH` (a duplicate would double-render in the UI). Mitigation: each gateway keeps
an in-process LRU of the last 10 000 `id@brokerTs` pairs and drops repeats. It is
deliberately **not** a Valkey write — gateways have no write access
([websocket-server.md](websocket-server.md) §5).

## 3.1 Repeated conditions: one record, the repeat folded in

A repeat of the same CONDITION hashes to the **same key** as the original
([valkey.md](valkey.md) §2), so there is no second record to write and nothing to
reconcile. What the fold does with the arrival is:

- `count` increments once per arrival;
- `lastSeen` moves to the newest arrival's broker timestamp;
- `lastId` records the newest arrival's id;
- `firstSeen` does **not** move — it marks when this episode began.

**Per-delivery history is NOT kept in Valkey.** It used to be, under a separate
`fw:evt:<id>` key per delivery (the old model, now gone), and that was reversed once to fix the All events tab.
The content key makes the reversal unnecessary: All events lists CONDITIONS, which is
what a row on the board is. The per-delivery log is `q.audit.events`
([persistence-and-durability.md](persistence-and-durability.md) §4) — that is where a
dedup investigation reads individual arrivals.

Cost, stated because it is real: the working set now scales with **conditions**, not
deliveries ([sizing.md](sizing.md) §1.1). A producer that repeats one fault forever
costs one record, however many times it reports.

### 4.1 Index pruning is mandatory
`event:<key>` expires on its own TTL, but a sorted-set member never does. Without
pruning the index grows forever and the working set becomes unbounded regardless of
TTL — which would break the capacity model in [sizing.md](sizing.md) §2 entirely.

### 4.2 Index repair at startup

The record write and both index writes happen in one pipelined step, so they cannot
normally diverge. They *can* diverge across a version boundary: records written before
`idx:event:all` existed are in storage and absent from the index, and nothing in the
read path can detect that. A query over an incomplete index is not a query bug — it is
missing data, and the symptom is "All events is empty" while the live tail has rows.

Measured on the lab: **3 index members for 7 stored records.** Four events existed and
were unlistable.

`reconcileEventIndex` runs at startup, before consuming:

1. load `idx:event:all` as a set
2. `SCAN idx:source:*` — never `KEYS`, which blocks the whole keyspace and would stall
   every reader on the node
3. for each source index, `missingFromGlobal` (a pure, tested function)
4. `MGET` each candidate for its `brokerTs` and `ZADD` it at that score

Score is the **record's** broker arrival time, never "now": scoring now would put a
week-old event at the top of the All events list, which is worse than leaving it out.
A record that has already expired, or carries no usable timestamp, is counted as
skipped rather than given an invented position.

| Property | Value |
|---|---|
| Runs before consuming | yes — so the tab is correct the moment the worker is up |
| Skipped when the gap is zero | yes — the normal case after the first run |
| Bounded | `FW_INDEX_RECONCILE_MAX` (50 000 default); reports `capped` |
| Fatal on failure | no — ingestion must not depend on it |
| Disabled by | `FW_INDEX_RECONCILE=0` |

A partial reconcile is still progress: the next restart compares the same two sets
again. `scripts/backfill-event-index.mjs` does the same work from outside the cluster,
for a history larger than the startup cap or to repair without a restart.


Every `FW_INDEX_PRUNE_EVERY` writes per source (default `1000` — confirmed as the agreed cadence), the worker also issues:

```
ZREMRANGEBYSCORE idx:source:<device> -inf (now - 604800000)
```

- Cost: at 16.7 writes/s sustained with 500 sources and a prune every 1 000 writes, this is **≈0.017 calls/s** fleet-wide — free. Even during a 500/s storm it stays ≈0.5 calls/s.
- It must be **piggybacked on the write path**, not a cron job: a cron job cannot know which sources are active and would scan dead keys.
- It is not idempotent-critical; a missed prune is corrected by the next one.
- Verify with `ZCARD idx:source:<source> <= writes-in-7d` ([sizing.md](sizing.md) §6).

### 4.2 Two dedup mechanisms, not one

The word "dedup" covers two unrelated jobs. Both are implemented, in different ways,
for different reasons.

| | Transport dedup | Content dedup |
|---|---|---|
| Problem | the same **message** arrives twice | the same **condition** is reported 1 000×/min |
| Cause | at-least-once delivery, crash before ack, replay | a sensor that stays faulted |
| Mechanism | the fold's `(brokerTs, id)` ordering guard | the CONTENT KEY: a repeat hashes to the same record |
| Guarantees | a redelivery changes nothing | one record per condition, `lastSeen` moving, `count` per arrival |
| Fails if missing | a repeat is counted twice | 10 million rows in a week, unusable UI |

Transport dedup is what §4 above describes. Content dedup is the key derivation
([valkey.md](valkey.md) §2) — it needs no runtime mechanism at all, which is the whole
point of keying on content.

What each layer contributes to "only update last occurrence":

1. The **content key** — a repeat of the same device+summary cannot address a second
   record.
2. The **watermark** — a stale redelivery cannot move state backwards, and a
   same-millisecond repeat with the same id is dropped.
3. `ZADD` — an index member is never duplicated; only its score is set.
4. The **fold** — `count` increments and `lastSeen` moves in ONE atomic step with the
   index writes, so no reader can see them disagree.
5. Gateway LRU on `id@brokerTs` — the browser is not shown the same delivery twice.

Verification for all five is in §10 and [validation.md](validation.md) §6.

## 5. Replica-sync guard (write path)

Per [valkey.md](valkey.md) §3, writes must not land on a master that has lost its replicas. Workers therefore check before writing:

```text
if the valkey Service has no ready endpoint -> do not write; nack(requeue=true)
```

The gate is the **Service**, not a key inside Valkey. A degraded Valkey cannot be asked to report its own degradation. `sentinel-bridge` reads every pod's `ROLE`, and when the designated master has fewer than `FW_WRITE_GUARD_MIN_REPLICAS` connected replicas it sets `Endpoints/valkey` to zero addresses and labels the pod `fire-watch.io/writable=false`. Workers observe the empty endpoint list and stop writing ([valkey.md](valkey.md) §3, [fault-tolerance.md](fault-tolerance.md) §3.7).

This also keeps the rule that the write target must come from the `valkey` Service / Sentinel — never from a pod IP (see [redis-sentinel.md](redis-sentinel.md) §5).

## 6. Connection and failure handling

| Failure | Handling |
|---|---|
| Valkey master changes | Sentinel-aware client reconnects to the new master; in-flight write retried once, then `nack(requeue=true)` |
| Valkey returns `READONLY` | This worker is pointed at a demoted node: refresh master from Sentinel, retry once, else `nack(requeue=true)` |
| RabbitMQ connection lost | Client auto-reconnects; unacked messages are redelivered (at-least-once) |
| Message fails schema validation | `nack(requeue=false)` → DLX → `q.event.ingest.dlq`, increment `fw_worker_dlq_total` |
| Valkey Service has no ready endpoint | The master is gone or the write guard tripped it: `nack(requeue=true)`, back off, retry forever |
| Valkey returns `NOREPLICAS` | `min-replicas-to-write` is refusing writes because the master cannot see two replicas. **Not** retried in-process — it is the intended refusal, not a routing fault — so the message is `nack`ed and RabbitMQ accumulates until the replicas return ([valkey.md](valkey.md) §5) |
| Pod is evicted mid-write | Unacked message redelivered; the partial write is overwritten idempotently |

**`nack(requeue=false)` is permitted for validation failures only.** Every transient condition must requeue. The queue deliberately carries **no `x-delivery-limit`**, so an event can be retried for the entire duration of a Valkey outage without ever being dead-lettered ([fault-tolerance.md](fault-tolerance.md) §3.1).

`prefetch`: default 32. Higher raises throughput and re-delivery blast radius on restart; keep it modest until throughput requires otherwise.

### 6.1 Valkey client: two bugs that made a healthy database look dead

The worker reported `valkey connection ready` and then failed every one of its
30 startup attempts with `valkey not reachable yet` — against a database that
answered `PING` fine. Two independent defects, both in `connect()`:

**`enableOfflineQueue: false` plus a non-lazy constructor is a lost race.** The
flag is needed so a write fails loudly instead of queueing forever, but it also
rejects *any* command issued before the socket is ready:

```
Error: Stream isn't writeable and enableOfflineQueue options is false
```

`new Redis(...)` starts dialling in the background, so the `ping()` on the next
line raced it and lost on every cold start. The fix is `lazyConnect: true` plus an
explicit `await client.connect()`, which resolves only once the connection is
authenticated and ready. The same bug was present in the ws-gateway read and
subscribe clients, where it surfaced as `subscribe failed, retrying in 3s` at boot.

**A rejected in-flight promise was never cleared.** `connect()` memoised its work
in `this._connecting` and cleared that field only on the success path. After a
single transient failure every later call returned the same rejected promise, so
the 30-attempt retry budget burned itself out against one error and then logged
`could not reach valkey through sentinel` — a wrong diagnosis that would send
an operator hunting a database outage. The `finally` block now clears the field on
failure too, so each retry genuinely reconnects.

Diagnostic signature: `resolved valkey master` and `valkey connection ready`
followed immediately by an unbroken run of `valkey not reachable yet`, with the
master resolving correctly throughout.

## 7. Probes

| Probe | Endpoint | Notes |
|---|---|---|
| startup | `GET /healthz` | returns 200 when the AMQP connection and Valkey client are both established |
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
| `VALKEY_MASTER_NAME` | ConfigMap | `fire-watch-valkey` |
| `VALKEY_PASSWORD` | `Secret/valkey` | injected via `sentinel auth-pass` + client options |
| `FW_EVENT_TTL` | ConfigMap | `604800` (7 days; retention rationale in [sizing.md](sizing.md) §1) |
| `FW_INDEX_PRUNE_EVERY` | ConfigMap | `1000` — writes per source between `ZREMRANGEBYSCORE` prunes ([architecture.md](architecture.md) §6.2) |
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
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 GET event:t1
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 ZRANGE idx:source:probe 0 -1
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 HGETALL event:probe:test

# idempotency: publish the same id twice, expect one record and one index entry
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 ZCARD idx:source:probe   # -> 1

# close semantics: CLOSE closes, and a stale CLOSE is refused
kubectl -n fire-watch exec rabbitmq-0 -- rabbitmqadmin publish exchange=fx.events routing_key=event.ingest \
  payload='{"id":"t2","source":"probe","type":"test","severity":"CLOSE","ts":"2026-11-09T00:01:00Z","payload":{}}'
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 EXISTS event:probe:test     # -> 0

# kill a worker mid-flight and confirm no loss
kubectl -n fire-watch delete pod -l app.kubernetes.io/name=worker --field-selector status.phase=Running
# expect: queue drains again, no messages in q.event.ingest.dlq
```

The adversarial ordering case (redelivered CLOSE vs newer CRITICAL) is drill 6.3 in [validation.md](validation.md) §6 and cannot be verified by hand-publishing alone.
