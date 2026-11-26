# Workers

Consume events from the Valkey ingest stream, persist them to Valkey, remove the entry last.

## 1. Topology

| Item | Value |
|---|---|
| Kind | `Deployment`, 3 replicas |
| Image | `image-registry.openshift-image-registry.svc:5000/fire-watch/worker:<sha>` |
| Runtime | Node.js 22 LTS |
| Ports | none exposed; HTTP `/healthz` on 8080 for probes only |
| State | none — everything reconstructible from Valkey (the ingest stream's pending list survives the pod) |
| Anti-affinity | required against `app.kubernetes.io/name=worker` on `kubernetes.io/hostname` |
| PDB | `minAvailable: 1` — workers are throughput, not quorum; pinning at 2 would block legitimate single-pod maintenance |

Workers are stateless, so HPA is safe here (unlike Valkey). Start with 3 fixed; scale on stream backlog only after a measured need.

Implemented in [services/worker](../services/worker/); the stream handling is [`src/stream.js`](../services/worker/src/stream.js) (`StreamConsumer`).

## 2. Responsibilities

1. Resolve the Valkey master through Sentinel; open **two** connections: a dedicated one for `XREADGROUP ... BLOCK` (it holds the socket for up to `FW_STREAM_BLOCK_MS`) and a shared one for the fold writes, `XACK`/`XDEL` and the DLQ.
2. Ensure the consumer group: `XGROUP CREATE fw:stream:ingest workers 0 MKSTREAM`. From `0`, so nothing appended before the group existed is skipped; `BUSYGROUP` is ignored; a later `NOGROUP` (stream key lost, e.g. a lab `FLUSHALL`) recreates it.
3. Read: `XREADGROUP GROUP workers <POD_NAME> COUNT 32 BLOCK 5000 STREAMS fw:stream:ingest >`. The entries of a batch are processed concurrently.
4. Parse the `evt` field and validate the envelope, including `severity`.
5. Compute `brokerTs` = the ms part of the entry ID (`<ms>-<seq>`, assigned by the master at `XADD`) — `entryTsMs()` in [`event.js`](../services/worker/src/event.js) ([event-lifecycle.md](event-lifecycle.md) §2).
6. **Compress** `payload.msgc` (and any payload field ≥ `FW_COMPRESS_MIN`) with gzip, leaving `msg` and the envelope plain ([payload-and-compression.md](payload-and-compression.md) §3).
7. Fork the delivery into the per-arrival trail (`hist:<deliveryId>` + `idx:history`, [history.md](history.md)).
8. Write the record, its indexes and the alert-state fold via the atomic Lua script ([event-lifecycle.md](event-lifecycle.md) §5.2) — this is the step that is watermark-guarded.
9. Publish to `fw:ch:events` (and `fw:ch:alerts` on a state change), including the fold's `applied` flag, with the same compressed payload that went to Valkey. Best effort.
10. `XACK` + `XDEL` in **one `MULTI`** — only now.
11. On a permanent failure (unparseable `evt`, invalid envelope, malformed ID): one `MULTI` of `XADD fw:stream:dlq * <original fields> origId <id> reason <text>` + `XACK` + `XDEL`.
12. On any transient failure: leave the entry in this consumer's pending list (PEL); it is re-read after `FW_RETRY_MS` (§6).

## 3. Ordering of operations (critical)

```text
XREADGROUP -> parse evt -> validate -> brokerTs = entry-ID ms -> COMPRESS msgc
           -> hist: fork -> fold write (record + index + state, ONE script)
           -> PUBLISH channel (compressed, best effort) -> XACK + XDEL (one MULTI)
```

Why the write precedes the `XACK`+`XDEL`: removing the entry first is a data-loss window. If the process dies between the removal and the write, the event is gone from the stream with nothing in Valkey — a direct R1 violation. Until `XACK` the entry sits in the PEL, where a retry or another worker's `XAUTOCLAIM` finds it.

Why `XACK` and `XDEL` are one `MULTI`: separately, a crash between them leaves an entry that is acknowledged but still in the stream (inflating the backlog forever) or deleted but still pending (claimable but unreadable). Together the entry is either pending or gone.

Why the `hist:` fork precedes the fold: the trail records every arrival, including the ones the fold rejects. It must also succeed before the `XACK` — a delivery that was processed but not recorded is a hole in the log ([history.md](history.md)).

Why pub/sub is published only after the write: a browser must never see an event that is not yet durable. A subscriber that misses the pub/sub message recovers via the index (`idx:source:<device>`) on reconnect. A failed publish is logged and **not** retried — the event is durable already.

Why there is no audit copy any more: the stream entry and the record live in the same replicated, AOF-backed Valkey, and the producer only got 202 after `WAIT 1`. A second copy in the same database adds no failure independence, and an external copy was dropped by decision ([persistence-and-durability.md](persistence-and-durability.md) §3).

### 3.1 Redelivery does not break closing semantics

A worker failure **after** the fold write but **before** the `XACK`+`XDEL` leaves the entry pending, and it is delivered again — to this worker after `FW_RETRY_MS`, or to another via `XAUTOCLAIM`. That is safe by construction, and it is the exact scenario the design was built for:

1. A redelivered entry keeps its **entry ID**, so the same `brokerTs`, and the same entry id. The fold's `(ts, entry id)` watermark already holds that pair, so it returns `applied 0` and **nothing moves** — no count, no index change, no live frame. The entry is still `XACK`+`XDEL`'d, because an entry that can never apply must not loop forever. The `hist:` fork runs again, so a redelivery is **two history rows** — deliberately ([history.md](history.md)).
2. The index write is idempotent (`ZADD` on a repeated member only moves its score), so a replay produces identical state.
3. The fold is an atomic Lua script, so two workers racing on the same key converge on the highest timestamp.

**What the watermark does not catch.** A producer re-sending an id, or nginx re-POSTing a batch after a 503 that had in fact been appended, creates a **new** entry with a new ID and therefore a newer `brokerTs`. That is a new arrival: it counts once more (`count++`, `lastSeen` moves) on the same condition key. It never creates a second record, because the key is derived from content ([valkey.md](valkey.md) §2).

Worked example — an old CLOSE must not retire a newer CRITICAL:

| Step | Action | Alert state |
|---|---|---|
| 1 | CRITICAL entry, `brokerTs=1000` | open, watermark 1000 |
| 2 | CLOSE entry, `brokerTs=1500`, Valkey write fails → stays pending | unchanged, watermark 1000 |
| 3 | new CRITICAL entry, `brokerTs=2000` (read by another worker) | open, watermark 2000 |
| 4 | CLOSE re-read from the PEL with `brokerTs=1500` | fold returns `0` (stale) — **alert stays open** |

Without stream-assigned timestamps and the watermark this sequence would silently retire an active alarm. Full treatment in [event-lifecycle.md](event-lifecycle.md) §4–5.

## 4. Idempotency

Stream consumption is at-least-once: an entry is delivered again whenever a worker dies (or fails) before its `XACK`. Writes are therefore keyed by content and guarded by the arrival timestamp:

```
EVAL event-fold.lua 5 event:<key> idx:event:all idx:event:open idx:event:closed idx:source:<device>
     <entry-ts-ms> <arrival-id> <severity> <bare-key> <device> <ttl-ms> <field> <value> …
PUBLISH fw:ch:events <json>
```

One call does the record, both timelines, both state indexes and the rollup. It is a
single `EVAL` precisely so that no interleaved writer can leave them disagreeing —
the reason the old design needed three separate writes was that the record key (the
message id) and the rollup key (the condition) were different objects.

- Every write is idempotent **by timestamp**: re-applying an event whose
  `(brokerTs, entry id)` the watermark has already passed returns `0` and changes nothing
  ([event-lifecycle.md](event-lifecycle.md) §5).
- `ZADD` on an existing member only sets its score, so an index cannot gain a
  duplicate member.
- There is **no coalescing window**. A repeat is counted, not suppressed: the
  duplicate problem is solved by the key, not by a clock
  ([event-lifecycle.md](event-lifecycle.md) §5.4.3).
- The index score is the **stream entry timestamp** (`brokerTs`), not the producer `ts`. A redelivered
  entry therefore keeps its original position in the timeline.

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
- `lastSeen` moves to the newest arrival's stream entry timestamp;
- `lastId` records the newest arrival's **stream entry id** (`<ms>-<seq>`), so it addresses the `hist:` row for that delivery. The producer's own message id is kept separately as `eventId`, because it is not unique per delivery;
- `firstSeen` does **not** move — it marks when this episode began.

**The record does not keep per-delivery history.** It used to, under a separate
`fw:evt:<id>` key per delivery (the old model, now gone), and that was reversed once to fix the All events tab.
The content key makes the reversal unnecessary: All events lists CONDITIONS, which is
what a row on the board is. The per-delivery log is the `hist:` trail
([history.md](history.md), 3-day retention) — that is where a dedup investigation
reads individual arrivals.

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

Score is the **record's** stream arrival time, never "now": scoring now would put a
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
| Problem | the same **stream entry** is delivered twice | the same **condition** is reported 1 000×/min |
| Cause | at-least-once delivery, crash before `XACK`, `XAUTOCLAIM` | a sensor that stays faulted |
| Mechanism | the fold's `(brokerTs, entry id)` ordering guard | the CONTENT KEY: a repeat hashes to the same record |
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

Per [valkey.md](valkey.md) §3, writes must not land on a master that has lost all of its replicas. Two layers enforce that without the worker checking anything itself:

- **Engine:** `min-replicas-to-write 1` — the master answers `NOREPLICAS` to every write (fold, `XACK`, `XDEL`) while it sees no replica acknowledging within `min-replicas-max-lag 10`.
- **Routing:** `sentinel-bridge` reads every pod's `ROLE`, and when the designated master has fewer than `FW_WRITE_GUARD_MIN_REPLICAS` (= 1, must equal `min-replicas-to-write`) connected replicas it sets `Endpoints/valkey` to zero addresses and labels the pod `fire-watch.io/writable=false` ([fault-tolerance.md](fault-tolerance.md) §3.7).

The gate is never a key inside Valkey — a degraded Valkey cannot be asked to report its own degradation. Either way the worker sees a failed write, marks the entry `retry`, and the entry stays pending.

This also keeps the rule that the write target must come from the `valkey` Service / Sentinel — never from a pod IP (see [redis-sentinel.md](redis-sentinel.md) §5).

## 6. Delivery, retry, claim and dead-letter

### 6.1 Outcomes per entry

| Outcome | When | Stream operations |
|---|---|---|
| `ack` | fold write done (any `applied`, including `0` for a redelivery) | `MULTI` `XACK` + `XDEL` `EXEC` |
| `retry` | **any** transient error: Valkey unreachable, `READONLY`, `NOREPLICAS`, failover in progress, fold script error, unexpected throw | none — the entry stays in this consumer's PEL |
| `dlq` | can never succeed: `evt` not JSON, envelope fails validation, entry ID carries no timestamp | `MULTI` `XADD fw:stream:dlq * <orig fields> origId <id> reason <text>` + `XACK` + `XDEL` `EXEC` |

An unexpected throw is classed as `retry`, not `dlq`: assuming "permanent" for an unknown error silently discards data. A failed dead-letter `MULTI` is itself retried.

### 6.2 Retry: the pending list, forever

After a batch with any `retry` outcome the loop re-reads its own PEL once `FW_RETRY_MS` (5 000) has passed:

```
XREADGROUP GROUP workers <pod> COUNT 32 STREAMS fw:stream:ingest 0
```

There is **no delivery limit**. An event is retried for the entire duration of a Valkey outage without ever being dead-lettered — giving up on a valid event is data loss ([fault-tolerance.md](fault-tolerance.md) §3.1). A fresh process also does one `0` pass at start, in case its pod name already owns pending entries.

### 6.3 Claim: entries left by a dead worker

Every `FW_CLAIM_INTERVAL_MS` (30 000) the worker runs

```
XAUTOCLAIM fw:stream:ingest workers <pod> 60000 0-0 COUNT 32
```

and takes over entries that have been pending on any consumer for more than `FW_CLAIM_IDLE_MS` (60 000) — a worker that was killed, evicted, or is wedged. `FW_CLAIM_IDLE_MS` must stay far above the time one entry takes to process, or two live workers would trade an entry back and forth (harmless — the watermark absorbs it — but wasteful).

Workers are a Deployment, so every restart leaves a new consumer name behind. In the same tick the worker removes consumers with **0 pending** and idle longer than `FW_CONSUMER_GC_IDLE_MS` (1 h) via `XGROUP DELCONSUMER`. A consumer that still owns entries is never deleted.

### 6.4 The stream is never trimmed

No `MAXLEN`, no `MINID`. A trim deletes entries regardless of whether they were processed, which is an accepted event lost. Entries leave the stream only through `XDEL` after the fold write (or the DLQ move), so `XLEN fw:stream:ingest` **is** the backlog — nothing else. `maxmemory-policy noeviction` keeps eviction from doing what a trim would ([valkey.md](valkey.md) §5).

`fw:stream:dlq` is never trimmed either; an operator inspects (`XRANGE fw:stream:dlq - +`) and deletes. `IngestDeadLetteredEvents` fires while it is non-empty ([monitoring.md](monitoring.md)).

### 6.5 Failure table

| Failure | Handling |
|---|---|
| Valkey master changes | Sentinel-aware client reconnects to the new master; the batch's writes fail → `retry`; the read loop resets its connection and re-reads |
| Valkey returns `READONLY` | This worker is pointed at a demoted node: refresh master from Sentinel; the entry stays pending and is retried |
| Valkey returns `NOREPLICAS` | `min-replicas-to-write 1` is refusing writes because the master sees no replica. The intended refusal: entries stay pending, the stream accumulates nothing new (ingress gets the same refusal and answers 503) until a replica returns |
| Valkey Service has no ready endpoint | The master is gone or the write guard tripped: `retry`, back off, retry forever |
| Entry fails parsing / validation | moved to `fw:stream:dlq` with `reason` |
| `XACK`/`XDEL` fails after the writes | entries stay pending and are redelivered; the watermark returns `applied 0` |
| Pod is evicted or killed mid-batch | its pending entries are `XAUTOCLAIM`ed by another worker after 60 s; the partial write is absorbed by the watermark |

`FW_STREAM_BATCH` (default 32) is the read size. Higher raises throughput and the number of entries a killed pod leaves pending; keep it modest until throughput requires otherwise.

### 6.6 Shutdown

On `SIGTERM` the worker stops reading, lets the in-flight batch finish its writes and `XACK`+`XDEL`, then exits; a hard exit fires at 20 s, inside `terminationGracePeriodSeconds`. The blocking read returns within `FW_STREAM_BLOCK_MS`. Anything still pending is `XAUTOCLAIM`ed by another worker — delayed, never lost.

### 6.7 Valkey client: two bugs that made a healthy database look dead

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
| startup | `GET /healthz` | 200 once the consumer group is ensured and the last Valkey write/ping succeeded |
| readiness | `GET /healthz` | `group ready && last write ok && not draining`; false while Valkey refuses writes — a worker that cannot write should not be counted as capacity |
| liveness | `GET /livez` | process-only; never fails on dependency loss, or an outage restart-loops all workers simultaneously |

`last write ok` is re-probed with a Valkey `PING` every 10 s, so readiness recovers without a restart. Readiness failing turns backpressure into a natural pause: entries stay in the stream, visibly (`XLEN`, `IngestStreamBacklogGrowing`), and nothing is lost.

## 8. Sizing

| Aspect | Value | Rationale |
|---|---|---|
| Requests | 100m CPU, 256Mi RAM | I/O-bound |
| Limits | 500m CPU, 512Mi RAM | JSON parse + a few network round trips |
| Expected throughput | ~1 000–3 000 msg/s per pod at 2 KiB payload | 1 000/min sustained with 500/s storms is met by a single pod; 3 replicas exist for H/A, not capacity ([sizing.md](sizing.md) §1) |
| HPA (later) | 3..12 on `redis_stream_length{stream="fw:stream:ingest"}` | scale on backlog, not CPU |

## 9. Configuration

| Key | Source | Default |
|---|---|---|
| `SENTINELS` | Deployment env | `sentinel-0.sentinel-headless:26379,sentinel-1...,sentinel-2...` |
| `VALKEY_MASTER_NAME` | Deployment env | `fire-watch-valkey` |
| `VALKEY_PASSWORD` | `Secret/fire-watch-valkey` | injected via `sentinel auth-pass` + client options |
| `POD_NAME` | downward API (`metadata.name`) | consumer name; falls back to the hostname |
| `FW_STREAM_KEY` | Deployment env | `fw:stream:ingest` |
| `FW_STREAM_GROUP` | Deployment env | `workers` |
| `FW_STREAM_DLQ_KEY` | Deployment env | `fw:stream:dlq` |
| `FW_STREAM_BATCH` | Deployment env | `32` — entries per `XREADGROUP` |
| `FW_STREAM_BLOCK_MS` | Deployment env | `5000` |
| `FW_RETRY_MS` | Deployment env | `5000` — delay before re-reading the PEL |
| `FW_CLAIM_IDLE_MS` | Deployment env | `60000` — pending this long → claimable |
| `FW_CLAIM_INTERVAL_MS` | Deployment env | `30000` |
| `FW_CONSUMER_GC_IDLE_MS` | Deployment env | `3600000` |
| `FW_EVENT_TTL` | Deployment env | `604800` (7 days; retention rationale in [sizing.md](sizing.md) §1) |
| `FW_HISTORY_TTL` | Deployment env | `259200` (3 days, [history.md](history.md)) |
| `FW_INDEX_PRUNE_EVERY` | Deployment env | `1000` — writes per source between `ZREMRANGEBYSCORE` prunes ([architecture.md](architecture.md) §6.2) |
| `FW_COMPRESS_MIN` | Deployment env | `1024` — gzip a payload field only if its UTF-8 length ≥ this ([payload-and-compression.md](payload-and-compression.md) §7) |
| `FW_COMPRESS_LEVEL` | Deployment env | `6` — gzip level |

`.gitignore` must exclude any `.env`, `*.pem`, `*.key`, `secrets/`.

## 10. Verification checklist

```bash
kubectl -n fire-watch logs deploy/worker -f
# expect: "worker starting" with stream/group/consumer, then no "write failed"

vk() { kubectl -n fire-watch exec valkey-0 -c valkey -- sh -c "valkey-cli -a \"\$VALKEY_PASSWORD\" --no-auth-warning $*"; }

# the group exists and every worker pod is a consumer
vk XINFO GROUPS fw:stream:ingest
vk XINFO CONSUMERS fw:stream:ingest workers

# post a test event through the ingress (http-ingress.md §9), then:
vk XLEN fw:stream:ingest                      # -> back to 0 once folded
vk XPENDING fw:stream:ingest workers          # -> 0 pending
vk ZRANGE idx:source:probe 0 -1               # -> the bare key
vk HGETALL event:<bare-key>                   # -> count, lastSeen, state

# kill a worker mid-flight and confirm no loss
kubectl -n fire-watch delete pod -l app.kubernetes.io/name=worker --field-selector status.phase=Running
# expect: XPENDING drains within ~60-90 s (XAUTOCLAIM), XLEN fw:stream:dlq stays 0
```

The adversarial ordering case (redelivered CLOSE vs newer CRITICAL) is drill 6.3 in [validation.md](validation.md) §6 and cannot be verified by hand-posting alone.
