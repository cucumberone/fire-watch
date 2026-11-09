# Event Lifecycle: Severities, Ordering and Close Semantics

How an event becomes an alert, how a close retires one, and why redelivery must never make an old CLOSE retire a newer CRITICAL.

## 1. Severity levels

| Severity | Opens an alert | Closes open alerts | Notes |
|---|---|---|---|
| `FATAL` | yes | no | highest; never self-clears |
| `CRITICAL` | yes | no | |
| `WARNING` | yes | no | |
| `HARMLESS` | no | **yes** | benign termination (e.g. sensor recovered normally) |
| `CLOSE` | no | **yes** | explicit operator/system close |

Rule quoted from the spec: **a `HARMLESS` or `CLOSE` always closes the others if they exist.**

Two consequences worth stating explicitly:

- `HARMLESS` and `CLOSE` are *not* severities in the same sense as the others — they are **retirement events**. They never create alert state of their own.
- All five still get stored as events in `fw:evt:{id}` for history. Only the alert-state fold treats them differently.

## 2. The ordering requirement

Closing is inherently a comparison: "was this CLOSE after the CRITICAL it is closing?" That makes event order a correctness property, not a display detail.

The cluster must answer that question with **one total order all three workers agree on**. Two candidate clocks exist and only one is acceptable:

| Clock source | Problem |
|---|---|
| Producer clock (`ts` in the payload) | Every producer host has its own clock. Skew of a few seconds between hosts reorders events. Unfixable without PTP/NTP discipline across all producers. |
| **RabbitMQ broker clock** | A single authority per message, assigned when RabbitMQ accepts the message. All workers read the same value. |

Therefore: **ordering uses the RabbitMQ-assigned arrival timestamp, never the producer timestamp.** The producer's `ts` is retained for display and for producer-side correlation only.

## 3. Getting a broker timestamp

AMQP 0-9-1 has a `timestamp` message property, but it is **seconds** granularity and producers may set it themselves (or leave it empty). For millisecond ordering with no producer trust, have RabbitMQ stamp on arrival:

```ini
# rabbitmq.conf — required
message_interceptors.incoming.set_header_timestamp.overwrite = true
```

This sets both:

- the `timestamp` property (POSIX **seconds**), and
- the `timestamp_in_ms` header (**milliseconds**)

The default for `overwrite` is `false`, which *preserves* a producer-supplied value. **Set it to `true`**: with `false`, a producer with clock skew — or a malicious one — can forge its position in the ordering, and an old CLOSE could be made to look newer than a FATAL. Overwriting removes that entire class of problem.

This is the built-in replacement for the deprecated `rabbitmq_message_timestamp` plugin. That plugin targeted RabbitMQ ≤ 3.11 and the repository is archived; on 3.12+ use the config above. Never enable both — two interceptors on `basic.publish` break channel creation for every AMQP 0-9-1 connection.

Ordering key used by the worker:

```
broker_ts_ms = headers.timestamp_in_ms    (fallback: properties.timestamp * 1000)
```

If neither is present the message is a schema violation → `nack(requeue=false)` → DLQ.

## 4. The hazard: a redelivered CLOSE can kill a newer CRITICAL

This is the scenario the design must survive, written out precisely.

```mermaid
sequenceDiagram
  participant P as Producer
  participant RMQ as RabbitMQ
  participant W as Worker
  participant DF as Dragonfly

  P->>RMQ: CRITICAL (t=1000)
  RMQ->>W: deliver CRITICAL (t=1000)
  W->>DF: SET alert state = CRITICAL @1000
  W->>RMQ: ack

  P->>RMQ: CLOSE (t=1500)
  RMQ->>W: deliver CLOSE (t=1500)
  W--xDF: WRITE FAILS (master failover / guard tripped)
  W->>RMQ: nack(requeue=true)  --> CLOSE goes back to the queue

  P->>RMQ: CRITICAL (t=2000)
  RMQ->>W: deliver CRITICAL (t=2000) to a *different* worker
  W->>DF: SET alert state = CRITICAL @2000
  W->>RMQ: ack

  RMQ->>W: redeliver CLOSE (t=1500)
  W->>DF: apply CLOSE??  <-- would wrongly close the t=2000 CRITICAL
```

The bug is applying the CLOSE by *arrival* order instead of by *broker timestamp*. An old CLOSE redelivered late would retire a newer FATAL/CRITICAL/WARNING — silently losing an active alarm.

Note the same hazard exists without any failure at all: with **3 consumers on one quorum queue, RabbitMQ does not guarantee delivery order**. Two events published 10 ms apart can be delivered to different workers in the opposite order. Broker timestamps are what make the fold order-independent.

## 5. The fix: monotonic watermark per alert key

Every event carries a **broker timestamp**. The alert state for a given key is only advanced by an event whose timestamp is strictly newer than the last applied one.

### 5.1 Keys

| Key | Type | Contents | TTL |
|---|---|---|---|
| `fw:alert:{key}` | hash | current state + occurrence rollup + watermark | 7d (rolling) |
| `fw:alert:open:{key}` | string | `"1"` while an alert is open | 7d |

`fw:alert:{key}` fields:

| Field | Meaning |
|---|---|
| `state` | `open` or `closed` |
| `severity` | severity of the last applied event |
| `first_seen` | broker ts of the first event in the **current open episode** |
| `last_seen` | broker ts of the **most recent** applied event — this is the "last occurrence" |
| `count` | occurrences in the current episode, after coalescing (§5.4) |
| `acked_by` | operator label who acknowledged this episode, cleared on a new episode ([acknowledge.md](acknowledge.md) §6) |
| `last_id` | event id of the most recent applied event |
| `closed_by`, `closed_at` | the CLOSE/HARMLESS that retired the episode |
| `watermark`, `watermark_id` | ordering guard (§5.2) |

`{key}` is the alert identity: by default `{source}:{type}`. Two events only interact if they share a key — a CLOSE for `sensor-a:temp` never affects `sensor-b:temp`.

### 5.2 The fold, as an atomic Lua script

Three workers can process different events for the same key concurrently, so the compare-and-set must be atomic. A `WATCH`/`MULTI` dance is not needed; a single `EVAL` is:

```lua
-- KEYS[1] = fw:alert:{key}          KEYS[2] = fw:alert:open:{key}
-- ARGV[1] = broker_ts_ms  ARGV[2] = event id  ARGV[3] = severity  ARGV[4] = dedup window ms
local wm_ts  = tonumber(redis.call('HGET', KEYS[1], 'watermark') or '0')
local wm_id  = redis.call('HGET', KEYS[1], 'watermark_id') or ''
local ts     = tonumber(ARGV[1])
local id     = ARGV[2]
local sev    = ARGV[3]
local window = tonumber(ARGV[4])

-- 1. ordering guard: (timestamp, id) lexicographic, see 5.3
if ts < wm_ts or (ts == wm_ts and id <= wm_id) then
  return 0                                        -- STALE
end

local is_open = redis.call('EXISTS', KEYS[2])

-- 2. retirement events
if sev == 'CLOSE' or sev == 'HARMLESS' then
  redis.call('DEL', KEYS[2])
  redis.call('HSET', KEYS[1], 'state','closed','closed_by',id,'closed_at',ts,
                              'watermark',ts,'watermark_id',id)
  redis.call('PEXPIRE', KEYS[1], 604800000)
  return 3                                        -- CLOSED
end

-- 3. occurrence rollup, see 5.4
local last  = tonumber(redis.call('HGET', KEYS[1], 'last_seen') or '0')
local count = tonumber(redis.call('HGET', KEYS[1], 'count') or '0')
local first = redis.call('HGET', KEYS[1], 'first_seen') or tostring(ts)
local fresh = 0

if is_open == 0 or last == 0 then
  count, first, fresh = 1, ts, 1                   -- new episode
elseif ts - last > window then
  count, fresh = count + 1, 1                      -- new occurrence, outside the window
end                                                -- else: coalesced, count unchanged

redis.call('SET', KEYS[2], '1', 'EX', 604800)
redis.call('HSET', KEYS[1],
  'state','open','severity',sev,'last_id',id,
  'first_seen',first,'last_seen',ts,'count',count,
  'watermark',ts,'watermark_id',id)
redis.call('PEXPIRE', KEYS[1], 604800000)
return fresh == 1 and 1 or 2                       -- 1 = new occurrence, 2 = coalesced
```

Return values — these become the `applied` field on the wire:

| Code | Meaning | UI action |
|---|---|---|
| `0` | stale, dropped from the fold | show as history only, change nothing |
| `1` | applied, **new occurrence** | update alert, bump `count`, set `last_seen` |
| `2` | applied, **coalesced into the current occurrence** | update `last_seen` only, do not bump `count` |
| `3` | applied, alert **closed** | remove the alert |

Because the script is atomic, two workers racing on the same key converge on the higher `(timestamp, id)` pair regardless of which one arrives first. **That is what makes the design safe under redelivery, failover and multi-consumer reordering simultaneously.**

### 5.3 Tie-break within the same millisecond

Two events can share a millisecond, and RabbitMQ gives no ordering guarantee between them across consumers. The fold needs a deterministic winner so all workers agree, which is why the watermark is the **pair** `(broker_ts_ms, event_id)` compared lexicographically (lines 6–9 above).

Without the tie-break, a CRITICAL and a CLOSE stamped in the same millisecond could resolve differently on different workers depending on delivery order — a race that appears only under load and is nearly impossible to reproduce afterwards.

Alternative considered and rejected: random UUIDv4 ids cannot provide a meaningful tie-break *order*, but they do provide a deterministic one, which is all that is required — the requirement is convergence, not semantic priority. If a same-millisecond CRITICAL and CLOSE must resolve in favour of the CRITICAL, add an explicit severity rank as the second sort key instead of the id.

### 5.4 Two different kinds of dedup — both needed

These are unrelated mechanisms and the design has both. Conflating them is the usual source of "why does this alert have count 4 000".

| | **Transport dedup** | **Semantic dedup (occurrence coalescing)** |
|---|---|---|
| Question | "is this the same *message* again?" | "is this the same *condition* still firing?" |
| Cause | at-least-once delivery, worker crash before ack, replay | a sensor reporting the same fault every second |
| Key | event `id` | alert `{key}` |
| Where | worker: idempotent `SET` + watermark | worker: the fold's `count` rollup |
| Result | the event is stored exactly once | the alert shows one row with a `last_seen` that keeps moving |

#### Transport dedup (already implemented)

- `SET fw:evt:{id}` — a redelivered event overwrites itself with identical bytes.
- `ZADD fw:idx:src:{s}` — a repeated member only updates its score.
- The watermark — a redelivered event older than the fold's watermark returns `0`.
- Gateway LRU — a repeated `PUBLISH` is dropped before it reaches the browser.

#### Semantic dedup: coalescing repeated occurrences

A sensor that stays broken does not send one event; it sends one per interval. Without coalescing, a 7-day retention window turns a single fault into ~600 000 alert rows and blows the working set.

The fold handles this with a time window, `FW_ALERT_DEDUP_WINDOW` (default **300 000 ms = 5 min**):

```text
if the alert is not open              -> start a new episode: count = 1, first_seen = ts
else if ts - last_seen >  window      -> a new occurrence:      count += 1
else                                  -> same occurrence:       count unchanged
in every applied case                 -> last_seen = ts, last_id = this event
```

So the record carries **both** numbers, which is what an operator actually needs:

| Field | Meaning | Example |
|---|---|---|
| `first_seen` | when this episode began | 12:00:00 — "firing since" |
| `last_seen` | when it was last confirmed | 14:37:12 — "last occurrence", keeps moving |
| `count` | distinct occurrences, not raw event count | 32 — "flapped 32 times in 2.5h" |

#### Why `count` is a windowed bucket, not a raw tally

The raw event count is already available and exact: `ZCARD` over the index time range. `count` deliberately counts **occurrences** (distinct bursts separated by more than the window) because:

- at 1 000 events/min a raw tally reaches 10 080 000 for one permanently broken sensor, which is meaningless to a human;
- a windowed count distinguishes "fired once and stayed" (count 1, `last_seen` moving) from "flapping every few minutes" (count 40) — a genuinely different operational situation;
- it is bounded by episode length ÷ window, so it never grows faster than ~12 per hour for a continuously firing alert.

Tuning: raise the window (e.g. 900 000 = 15 min) if the UI is noisy with high-frequency sources; lower it to 60 000 if occurrences are meaningfully distinct at one-per-minute resolution. Store the chosen value in the ConfigMap; changing it does **not** require recomputing historical alerts, because alert state is derived (§8).

#### Coalescing does not lose information

Every individual event is still stored in `fw:evt:{id}` and indexed in `fw:idx:src:*`, so a drill-down can show every raw report. Coalescing only affects the **summary row** the operator sees first.

#### Interaction with severities

| Situation | Behaviour |
|---|---|
| CRITICAL at t, CRITICAL at t+10s | same occurrence → `count` stays 1, `last_seen` moves |
| CRITICAL at t, WARNING at t+10s | same alert key, applied → severity becomes WARNING, `count` stays 1 (severity change is not a new occurrence) |
| WARNING at t, CRITICAL at t+10s | severity escalates to CRITICAL — visible because `severity` is overwritten by the last applied event |
| CRITICAL at t, CLOSE at t+1s | alert closed; the next CRITICAL starts a **new episode** with `count` reset to 1 and a new `first_seen` |
| CRITICAL at t, CLOSE at t+1s, CRITICAL at t+2s | new episode — this is correct, an explicit close ends the previous one |

Escalation history is intentionally not kept in the alert row: it is recoverable from the event stream via the index, which is the authoritative record.

### 5.5 What is *not* guarded

The event itself is always written, always indexed, always published to `fw:ch:events`, regardless of staleness. Only the **alert state fold** is watermark-guarded. History is complete and immutable; the derived state is order-independent.

## 6. Why redelivery is safe

RabbitMQ preserves message properties and headers across requeue. A redelivered CLOSE therefore arrives with its **original** `timestamp_in_ms`, not a fresh one:

| Scenario | Redelivered CLOSE timestamp | Fold result |
|---|---|---|
| CLOSE requeued, newer CRITICAL written meanwhile | old (e.g. 1500) | watermark is 2000 → **stale, dropped** ✓ |
| CLOSE requeued, nothing newer happened | old (e.g. 1500) | watermark 1000 → **applied** ✓ |
| CLOSE requeued twice | old, twice | first applies, second is stale → **idempotent** ✓ |
| Two workers process the same CLOSE concurrently | old, both | atomic script → applied once ✓ |

**Critical operational rule:** never *republish* a message to move it, because a republish is a new publish and gets a new timestamp. Use `nack(requeue=true)` (which returns the message to the same queue with properties intact) or, when moving between queues deliberately, preserve the timestamp by copying `x-original-ts`:

```text
if republishing (DLQ -> ingest):
  set headers['x-original-ts'] = headers['timestamp_in_ms']  # MUST be preserved
worker preference order for broker_ts_ms:
  headers['x-original-ts'] > headers['timestamp_in_ms'] > properties.timestamp*1000
```

The `replay-gap` job must apply the same preference, otherwise a replay would re-stamp history with "now" and could close newer alerts.

## 7. Replay safety

The same watermark protects replay from the audit queue:

- Replaying 7 days of history re-runs every fold. Each event either advances the watermark or is dropped as stale — so the final state after a full replay is **identical** to the state before the wipe, regardless of the order records come back.
- Replay from the audit queue is therefore order-insensitive by construction. The replay job does not need to sort, batch by key, or single-thread.
- The one thing replay must not do is invent timestamps (§6).

## 8. Alert state is derived, not authoritative

| Data | Authority | Recovery |
|---|---|---|
| Events (`fw:evt:*`, `fw:idx:*`) | RabbitMQ audit queue | replay |
| Alert state (`fw:alert:*`) | the fold | recompute by replaying the window |

Alert state can be deleted at any time and rebuilt from the event history without loss. This is why no extra durability machinery is needed for it — and why retuning `FW_ALERT_DEDUP_WINDOW` is safe: replaying with a new window produces the new rollup, not a corrupted one.

## 8.1 Transport vs semantic dedup — where each lives

| Concern | Mechanism | Owner | Effect if missing |
|---|---|---|---|
| Same *message* redelivered | idempotent `SET`/`ZADD` keyed by `id` | worker (§5.4) | duplicate history rows |
| Same *message* re-published to the browser | gateway LRU on `id` | gateway ([websocket-server.md](websocket-server.md) §5) | duplicate UI rows |
| Same *message* arriving out of order | watermark fold | worker (§5.2) | an old CLOSE retires a newer CRITICAL |
| Same *condition* reporting repeatedly | occurrence window rollup | worker (§5.4) | 600k alert rows per fault, UI unusable |
| Same *condition* re-shown after a UI reconnect | backfill + client dedup on `id` | gateway + frontend | duplicated rows after every reconnect |

All five are implemented. The first two are "dedup" in the usual sense; the last two are what prevent a healthy 1 000 evt/min stream from looking like an outage.

## 9. UI implications

- The gateway forwards events as they arrive; it does not compute alert state. Rendering "this alert just closed" comes from the `severity` field of the event that closed it.
- Because the gateway can receive a stale CLOSE (it is published to `fw:ch:events` immediately, before the fold), the gateway must annotate each frame with the fold's return code:

```json
{ "op": "event", "event": { "id": "...", "severity": "CLOSE", ... }, "applied": 3 }
```

| `applied` | UI behaviour |
|---|---|
| `0` | history only — do **not** touch alert state or counts |
| `1` | new occurrence — update the alert row, increment `count`, refresh `last_seen` |
| `2` | coalesced — refresh `last_seen` only; **do not** increment `count` and do not animate an entry |
| `3` | closed — remove the alert row |

A UI that ignores `applied` will show alerts closing and immediately reopening (stale CLOSE, §4) and will show a permanently-firing alert with a count that increments 1 000 times a minute (§5.4). Both are fixed by honouring the code.

## 10. Verification

```bash
# 1. broker timestamps are present on every message
kubectl -n fire-watch exec rabbitmq-0 -- rabbitmqadmin get queue=q.event.ingest count=5 \
  | grep -E 'timestamp_in_ms|severity'

# 2. watermark guard: a stale CLOSE must not retire a newer alert
kubectl -n fire-watch exec dragonfly-0 -- redis-cli -p 6379 HGETALL fw:alert:probe:test

# 3. occurrence coalescing: publish the same CRITICAL 5x inside the 5-min window,
#    then once outside it. Expect count = 2, not 6.
kubectl -n fire-watch exec dragonfly-0 -- redis-cli -p 6379 HMGET fw:alert:probe:test count first_seen last_seen

# 4. replay invariance: wipe alert keys, replay, compare
kubectl -n fire-watch exec dragonfly-0 -- redis-cli -p 6379 --scan --pattern 'fw:alert:*'
kubectl -n fire-watch create job --from=cronjob/replay-gap replay-verify
```

Scenarios are listed in [validation.md](validation.md) §6.
