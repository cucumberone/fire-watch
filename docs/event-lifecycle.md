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

These are the *internal* severities. A producer does not send them: it sends the
`EVT_Severity` scale (`CRITICAL`/`MAJOR`/`MINOR`/`WARNING`/`INDETERMINATE`/
`INFORMATION`/`CLEAR`/`HARMLESS`) which the HTTP ingress maps down onto these five
before the event ever enters the ingest stream. In particular a producer's `CLEAR` becomes
`CLOSE`, and `INFORMATION` becomes `HARMLESS` — see
[event-schema.md](event-schema.md) §3. The producer's original value is preserved as
`payload.EVT_Severity` but **nothing downstream reads it**; the fold only ever sees
the mapped internal severity.

Two consequences worth stating explicitly:

- `HARMLESS` and `CLOSE` are *not* severities in the same sense as the others — they are **retirement events**. They never create alert state of their own.
- All five still update the record at `event:<key>`; only a retirement changes its `state`.

## 2. The ordering requirement

Closing is inherently a comparison: "was this CLOSE after the CRITICAL it is closing?" That makes event order a correctness property, not a display detail.

The cluster must answer that question with **one total order all three workers agree on**. Two candidate clocks exist and only one is acceptable:

| Clock source | Problem |
|---|---|
| Producer clock (`ts` in the payload) | Every producer host has its own clock. Skew of a few seconds between hosts reorders events. Unfixable without PTP/NTP discipline across all producers. |
| **Valkey stream entry ID** | A single authority per event, assigned by the master when it accepts the `XADD`. All workers read the same value. |

Therefore: **ordering uses the stream-assigned arrival timestamp, never the producer timestamp.** The producer's `ts` is retained for display and for producer-side correlation only. The value is stored as `brokerTs` — the field name predates the stream and is kept everywhere (record, indexes, live frames, history).

## 3. Getting the arrival timestamp

Producers append with an auto-generated ID:

```
XADD fw:stream:ingest * evt <envelope> src http [cid <x-request-id>]
```

`*` makes the **master** assign the ID `<ms>-<seq>`: `<ms>` is the master's clock in
milliseconds, `<seq>` disambiguates entries within the same millisecond. Valkey
guarantees IDs are strictly increasing within a stream — if the clock goes backwards
it reuses the last `<ms>` and bumps `<seq>` — and a failover keeps that property for
everything the promoted replica received, because it has the stream's last ID and continues
after it. Across a failover the new master's wall clock takes over, so the three Valkey
nodes must be NTP-disciplined — three cluster nodes, not hundreds of producer hosts.

No producer can choose its position: http-ingress and the gateway's operator close
always pass `*`.

Ordering key used by the worker (`entryTsMs()` in
[`services/worker/src/event.js`](../services/worker/src/event.js)):

```
brokerTs = Number(entryId.split('-')[0])
```

If the ID carries no timestamp (malformed), the entry can never succeed → moved to
`fw:stream:dlq` ([workers.md](workers.md) §6.1).

## 4. The hazard: a redelivered CLOSE can kill a newer CRITICAL

This is the scenario the design must survive, written out precisely.

```mermaid
sequenceDiagram
  participant P as http-ingress
  participant ST as fw:stream:ingest
  participant W as Worker
  participant DF as Valkey records

  P->>ST: XADD CRITICAL (id 1000-0)
  ST->>W: XREADGROUP CRITICAL (1000)
  W->>DF: fold: state = CRITICAL @1000
  W->>ST: XACK + XDEL

  P->>ST: XADD CLOSE (id 1500-0)
  ST->>W: XREADGROUP CLOSE (1500)
  W--xDF: WRITE FAILS (master failover / NOREPLICAS)
  Note over W,ST: CLOSE stays in this worker's PEL

  P->>ST: XADD CRITICAL (id 2000-0)
  ST->>W: XREADGROUP CRITICAL (2000) to a *different* worker
  W->>DF: fold: state = CRITICAL @2000
  W->>ST: XACK + XDEL

  ST->>W: re-read CLOSE (1500) from the PEL
  W->>DF: apply CLOSE??  <-- would wrongly close the t=2000 CRITICAL
```

The bug is applying the CLOSE by *processing* order instead of by *arrival timestamp*. An old CLOSE retried late would retire a newer FATAL/CRITICAL/WARNING — silently losing an active alarm.

Note the same hazard exists without any failure at all: with **3 consumers in one group, entries of a batch are processed concurrently and batches on different workers race**. Two events appended 10 ms apart can be folded in the opposite order. Stream-assigned timestamps are what make the fold order-independent.
## 5. The fix: monotonic watermark per alert key

Every event carries an **arrival timestamp** (`brokerTs`, from the stream entry ID). The alert state for a given key is only advanced by an event whose timestamp is strictly newer than the last applied one.

### 5.1 Keys

| Key | Type | Contents | TTL |
|---|---|---|---|
| `event:<32 hex>` | hash | the WHOLE record: payload fields, `state`, `severity`, `count`, `firstSeen`, `lastSeen`, `lastId`, ack/closure fields, `watermark` | 7d (rolling) |
| `idx:event:open` | zset | member = bare key, score = last activity ms. The open set, so a client can be given a snapshot without `SCAN event:*` ([websocket-server.md](websocket-server.md) §3.1) | members removed by the fold on retirement |
| `idx:event:all` | zset | member = bare key, score = **arrival** timestamp ms (`brokerTs`). The global timeline, written in the same step as the per-device index so it can never be missing a record that index has. Backs the All Events tab, which cannot be answered from `idx:source:*` without a SCAN | 7d, via the worker's index prune |
| `idx:event:closed` | zset | member = bare key, score = `closedAt` ms. The retired set, written by the same fold call that removes the key from the open index, so a key is in at most one of the two. Removed again when a condition re-opens ([frontend.md](frontend.md) §5.6) | 7d, via the worker's index prune |
| `idx:source:{device}` | zset | member = bare key, score = `brokerTs` ms. Per-device timeline for the filtered tail | 7d, via the worker's index prune |

**There is no fingerprint key and no second record key.** The record used to be two
objects — `fw:evt:<id>` per delivery and `fw:alert:<source>:<type>` for the rollup —
plus `fw:fp:<fingerprint>` to detect content duplicates. One hash per CONDITION
replaces all three: the key is
`sha256(EVT_Device + 0x1f + EVT_Summary)[0:32]`, so a repeat of the same condition
hashes to the same address and there is nothing to detect ([valkey.md](valkey.md) §2).

`event:<key>` fields:

| Field | Meaning |
|---|---|
| `state` | `open` or `closed` |
| `severity` | severity of the last applied event |
| `displaySeverity` | the PRODUCER's original label (`MINOR`, `CLEAR`, …), preserved by the ingress |
| `source`, `type` | `EVT_Device` and `EVT_EventKey`, as fields — the key itself is opaque |
| `msg` | `EVT_Summary`, normalised. Half of the key derivation, so it never varies within a record |
| `payload` | JSON string of the compressed payload object |
| `firstSeen` | arrival ts (`brokerTs`) of the first event in the **current open episode** |
| `lastSeen` | arrival ts (`brokerTs`) of the **most recent** applied event — the "last occurrence" |
| `count` | events applied in the current episode. Every applied event counts; a redelivery never gets here (the watermark drops it, §5.4.1) |
| `ackedBy`, `ackedAt` | operator who acknowledged this episode, cleared on a new episode ([acknowledge.md](acknowledge.md) §6) |
| `lastId` | event id of the most recent applied arrival |
| `closedBy`, `closedAt` | the CLOSE/HARMLESS that retired the episode |
| `watermark`, `watermarkId` | ordering guard (§5.2) |

A CLOSE and the CRITICAL it retires share one key when they share device **and**
summary, which is exactly the intent: retiring is a state change on the record, not a
new record.

### 5.2 The fold, as an atomic Lua script

Three workers can process different events for the same key concurrently, so the
compare-and-set must be atomic. A `WATCH`/`MULTI` dance is not needed; a single `EVAL`
is. The real script is in
[`k8s/base/20-event-fold-config.yaml`](../k8s/base/20-event-fold-config.yaml); this is
its shape.

```lua
-- KEYS[1] = event:<key>         the record
-- KEYS[2] = idx:event:all       KEYS[3] = idx:event:open
-- KEYS[4] = idx:event:closed    KEYS[5] = idx:source:<device>
-- ARGV[1] = broker_ts_ms   ARGV[2] = arrival id   ARGV[3] = severity
-- ARGV[4] = bare key (index member)   ARGV[5] = device   ARGV[6] = ttl ms
-- ARGV[7..] = field, value, field, value …  the record's own fields

local ts     = tonumber(ARGV[1])
local id     = ARGV[2]
local sev    = ARGV[3]
local member = ARGV[4]

-- 1. ordering guard: (timestamp, id) lexicographic, see 5.3
local wm_ts = tonumber(redis.call('HGET', KEYS[1], 'watermark') or '0')
local wm_id = redis.call('HGET', KEYS[1], 'watermarkId') or ''
if ts < wm_ts or (ts == wm_ts and id <= wm_id) then
  return 0                                        -- STALE / REDELIVERY
end

local existed = redis.call('EXISTS', KEYS[1])
local state   = redis.call('HGET', KEYS[1], 'state') or ''

-- 1b. the record's own fields, applied to BOTH branches — a CLEAR can arrive for a
--     condition with no stored record yet, and must not leave a blank row
local sets = {}
for i = 7, #ARGV, 2 do
  sets[#sets + 1] = ARGV[i]
  sets[#sets + 1] = ARGV[i + 1]
end
if #sets > 0 then redis.call('HSET', KEYS[1], unpack(sets)) end

-- 2. retirement events
if sev == 'CLOSE' or sev == 'HARMLESS' then
  redis.call('HSET', KEYS[1], 'state','closed','severity',sev,
                              'closedBy',id,'closedAt',ts,'lastSeen',ts,
                              'watermark',ts,'watermarkId',id)
  redis.call('HDEL', KEYS[1], 'ackedBy', 'ackedAt')      -- the ack dies with the episode
  redis.call('ZREM', KEYS[3], member)                    -- out of "open"
  redis.call('ZADD', KEYS[4], ts, member)                -- into "closed"
  redis.call('ZADD', KEYS[2], ts, member)                -- still an event
  redis.call('ZADD', KEYS[5], ts, member)
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[6]))
  return 3                                               -- CLOSED
end

-- 3. occurrence rollup: THE DEDUP. A repeat updates the SAME hash.
local count = tonumber(redis.call('HGET', KEYS[1], 'count') or '0')
local episode_started = (existed == 0 or state == 'closed' or state == '')

if episode_started then
  count = 1                       -- new condition, or a re-fire: fresh episode
  redis.call('HSET', KEYS[1], 'firstSeen', ts)
  redis.call('HDEL', KEYS[1], 'ackedBy', 'ackedAt', 'closedBy', 'closedAt')
else
  count = count + 1               -- firstSeen deliberately NOT touched
end

redis.call('HSET', KEYS[1], 'state','open','severity',sev,'count',count,
                            'lastSeen',ts,'lastId',id,
                            'watermark',ts,'watermarkId',id)
redis.call('ZADD', KEYS[3], ts, member)   -- open
redis.call('ZREM', KEYS[4], member)       -- no longer closed
redis.call('ZADD', KEYS[2], ts, member)   -- global timeline
redis.call('ZADD', KEYS[5], ts, member)   -- per-device timeline
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[6]))
return 1
```

Return values — these become the `applied` field on the wire:

| Code | Meaning | UI action |
|---|---|---|
| `0` | **stale or redelivered — dropped** | change nothing |
| `1` | applied | bump `count`, move `lastSeen` |
| `3` | applied, record **closed** | remove the row from the open board |

The old code `2` ("coalesced") is gone. There is no coalescing window any more: a
repeat either lands in the same millionth of a key (and is counted) or it is a
redelivery of the same message (and is dropped). See §5.4.3.

### 5.3 Tie-break within the same millisecond

Two events can share a millisecond (their entry IDs differ only in `<seq>`), and concurrent workers give no ordering guarantee between them. The fold needs a deterministic winner so all workers agree, which is why the watermark is the **pair** `(broker_ts_ms, event_id)` compared lexicographically (lines 6–9 above).

Without the tie-break, a CRITICAL and a CLOSE stamped in the same millisecond could resolve differently on different workers depending on delivery order — a race that appears only under load and is nearly impossible to reproduce afterwards.

Alternative considered and rejected: random UUIDv4 ids cannot provide a meaningful tie-break *order*, but they do provide a deterministic one, which is all that is required — the requirement is convergence, not semantic priority. If a same-millisecond CRITICAL and CLOSE must resolve in favour of the CRITICAL, add an explicit severity rank as the second sort key instead of the id.

### 5.4 Two different kinds of duplicate — one rule now

Both cases still exist. What changed is that the SECOND one no longer needs any
machinery, because the record is keyed by the condition.

| | **Transport duplicate** | **Content duplicate** |
|---|---|---|
| Question | "is this the same *message* again?" | "is this the same *condition*, reported again?" |
| Cause | at-least-once delivery, worker crash before `XACK`, `XAUTOCLAIM` | a producer re-reporting the same alarm with a fresh id |
| Identity | `(brokerTs, id)` | the record KEY (§5.4.2) |
| Detection | the fold's ordering guard returns `0` | the arrival hashes to an EXISTING key, so there is no second record to write |
| Result | **nothing at all happens** | `count` +1 and `lastSeen` moves **on the same hash** |

#### 5.4.1 Transport duplicate — same stream entry twice

A redelivery is the same stream entry, keeping its entry ID — so its original
`brokerTs` — and its original `evt.id`. It must be a no-op:

- The fold's `(ts, id)` watermark returns `0` for anything not newer, so no field
  changes.
- `ZADD` on an existing member only sets its score, so the indexes cannot gain a
  duplicate member.
- The gateway's LRU drops a repeated `PUBLISH` before it reaches a browser.

Nothing is counted and nothing is shown. This is correct: the operator already saw
the original.

**The watermark is now the ONLY gate**, and that is sufficient because the key is the
content. A re-sent id whose content matches cannot create a record, and one whose
content differs *should* be recorded — which is what the old `SET … NX` blocked.

#### 5.4.2 Content duplicate — same condition, different id

**This is the case that matters for the board.** An upstream alarm system re-reports a
standing condition every interval, with a new event id each time. Counting those would
mean one row per delivery, and 7-day retention turns a single fault into hundreds of
thousands of records.

Identity is the **record key**:

```text
event:<sha256(EVT_Device + 0x1f + EVT_Summary)[0:32]>
```

computed in [`services/worker/src/keygen.js`](../services/worker/src/keygen.js).

What is included, and why:

| Field | Why included |
|---|---|
| `EVT_Device` | the device is half of "what is broken" |
| `EVT_Summary` | the summary is what an operator reads. Two different faults with identical wording are indistinguishable on the board, so grouping them is the honest choice |

What is **excluded**:

| Field | Why excluded |
|---|---|
| `id` | differs on every delivery — that is the whole problem |
| `ts` | the producer's clock, also differs per delivery |
| `brokerTs` | set on arrival, differs every time |
| `EVT_EventKey` / `type` | the producer's internal key is not what an operator triages on. Two different event keys with the same words on screen are one row |
| `severity` | escalation updates the standing row instead of opening a second one beside it |

Whitespace in device and summary is collapsed before hashing, so a producer that pads
or re-wraps its summary does not mint a new key per report.

The flow is now trivial, which is the point:

```text
key := sha256(device + 0x1f + summary)[0:32]
EVAL event-fold.lua 5 event:<key> idx:event:all idx:event:open idx:event:closed idx:source:<device> …
  -> 1 : the fold counted this arrival on the existing (or new) record
```

There is no claim/verify pair, no owner lookup and no "the copy is discarded" step.
A repeat **cannot** create a second record, because there is no second key to create.

The old design needed `fw:fp:<fingerprint>` because the record key was the message id.
That set could expire independently of the record, and when it did every later
delivery looked new — a bug that took a branch of its own to patch. One content-derived
key removes the possibility.

#### 5.4.3 Why there is no time window any more

An earlier design coalesced only repeats inside `FW_ALERT_DEDUP_WINDOW` (300 s). Three
things were wrong with it:

1. A storm inside the window left `count` frozen at 1 — indistinguishable from a broken
   counter, and reported as one.
2. A repeat *outside* the window created a second row for the same condition, which is
   precisely what the board must not do.
3. It modelled "same condition still firing" from arrival times. That question needs a
   content identity, not a clock.

`FW_ALERT_DEDUP_WINDOW` has been removed from the manifests entirely — keeping a
setting that changes nothing is how a reader ends up believing it does something.

#### 5.4.4 What this means operationally

| Situation | Behaviour |
|---|---|
| Same device+summary at t and t+10s | one record; `count` 2, `lastSeen` = t+10s |
| Same device+summary a week later | still one record (the TTL refreshed on every occurrence) |
| Same device+summary, severity escalated WARNING → CRITICAL | **same** record, `severity` updated. An escalation is a new fact about the same condition, not a second condition |
| Same device, different summary | different key → a second record |
| Same summary, different device | different key → a second record |
| `CLOSE` for a condition | same key (device and summary unchanged) → the record's `state` flips to `closed` |

**Trade-off, stated plainly:** two genuinely distinct faults with the same device and
identical wording are indistinguishable and will be counted as one condition. That is
the cost of content identity, and it is the requested behaviour — an alarm system that
wants them distinguished must vary the summary, which is what a real one does.

### 5.5 What is *not* guarded

There is no separate "event" and "alert" object to guard differently. The record is
the condition, so the fold IS the write: an event that the watermark rejects changes
nothing, and an event it accepts is counted and indexed in one atomic step
([valkey.md](valkey.md) §6). Per-delivery history lives in the `hist:` trail, written
before the fold ([history.md](history.md)).

## 6. Why redelivery is safe

A stream entry keeps its ID while it sits in a consumer's pending list, whether it is re-read by the same worker (`XREADGROUP ... 0` after `FW_RETRY_MS`) or taken over by another (`XAUTOCLAIM`). A redelivered CLOSE therefore arrives with its **original** `brokerTs`, not a fresh one:

| Scenario | Redelivered CLOSE timestamp | Fold result |
|---|---|---|
| CLOSE retried, newer CRITICAL written meanwhile | old (e.g. 1500) | watermark is 2000 → **stale, dropped** ✓ |
| CLOSE retried, nothing newer happened | old (e.g. 1500) | watermark 1000 → **applied** ✓ |
| CLOSE folded, `XACK` lost, delivered again | old, twice | first applies, second is stale → **idempotent** ✓ |
| Two workers process the same CLOSE concurrently (claim race) | old, both | atomic script → applied once ✓ |

**Critical operational rule:** never *re-append* an entry to move it, because a new `XADD` gets a new ID and therefore a new, newer timestamp. Retries stay in the PEL; nothing in the system re-appends. An operator who re-injects an entry from `fw:stream:dlq` creates a **new arrival** — acceptable only after checking that nothing newer exists for that condition.

## 7. Duplicate safety

Not every duplicate is a redelivery of the same entry:

| Duplicate | Same entry ID? | Result |
|---|---|---|
| Worker crash after the fold, before `XACK`+`XDEL` | yes | watermark returns `0`; entry removed; one extra `hist:` row |
| `XACK`+`XDEL` `MULTI` failed after the writes | yes | same |
| nginx re-POSTs after a 503 whose `EXEC` had succeeded (`WAIT` short) | **no** — a new `XADD` | a new arrival on the same condition key: `count` +1, `lastSeen` moves; never a second record |
| Producer re-sends the same event | no | same as above |

The first two are invisible. The last two are counted, because from the stream's point of view they *are* new arrivals; the content key keeps them on one record ([valkey.md](valkey.md) §2), which is the "drop the duplicate, update last occurrence" behaviour the board needs.

## 8. Alert state is derived, not authoritative

| Data | Authority | Recovery |
|---|---|---|
| Arrivals not yet folded | `fw:stream:ingest` (replicated, AOF on every pod) | workers fold them after any restart or failover |
| Records (`event:*`, `idx:*`) | Valkey (replicated, AOF on every pod) | AOF reload; no copy outside Valkey ([persistence-and-durability.md](persistence-and-durability.md) §4) |
| Alert state (`event:*`) | the fold | recomputed by the fold for every arrival, in any order |

Because the fold is order-insensitive (§5), re-applying any set of arrivals — a
backlog after an outage, a claimed PEL, entries a worker processed twice — converges
on the same state regardless of the order they are processed in.
## 8.1 Transport vs semantic dedup — where each lives

| Concern | Mechanism | Owner | Effect if missing |
|---|---|---|---|
| Same *stream entry* redelivered | `(ts, id)` watermark returns `0` | worker (§5.2) | a repeat is counted twice |
| Same *message* re-published to the browser | gateway LRU on `id@brokerTs` | gateway ([websocket-server.md](websocket-server.md) §5) | duplicate UI rows |
| Same *message* arriving out of order | watermark fold | worker (§5.2) | an old CLOSE retires a newer CRITICAL |
| Same *condition* reporting repeatedly | the CONTENT KEY: a repeat hashes to the same record | worker ([valkey.md](valkey.md) §2) | it cannot happen — there is no second key |
| Same *condition* re-shown after a UI reconnect | backfill + client dedup on `id` | gateway + frontend | duplicated rows after every reconnect |

All five are implemented. The third and fourth are what prevent a healthy 1 000 evt/min
stream from looking like an outage.

## 8.2 An operator close IS an event

Closing an alert from the UI does not flip a state flag. The gateway **injects a
real event** into the ingest stream (`XADD fw:stream:ingest * evt <closeEvent> src operator`,
then `WAIT FW_WAIT_REPLICAS 5000` — `enqueueEvent()` in
[`services/ws-gateway/src/valkey.js`](../services/ws-gateway/src/valkey.js)), and the
worker processes it through the ordinary pipeline — validate, compress, `hist:` fork, fold.

Why it is done that way:

| | |
|---|---|
| **One code path** | A closure a human made and a closure a source reported go through the same fold. A second, special-cased write is a second definition of "closed", and the two drift. |
| **It correlates for free** | The injected event echoes the record's own `source` and `payload.msg`, so it hashes onto the SAME condition key ([valkey.md](valkey.md) §2). No key is passed between the two writes, so nothing can disagree about which alert was retired. |
| **It is a real record** | The closure appears in All events and in History (as its own delivery, `src operator` on the stream entry) — so "who ended this, and when" is answerable from storage rather than from a transient frame. |
| **Durability** | The `XADD` succeeded and `WAIT` saw a replica acknowledge before the operator is told it worked — the same rule as the ingress 202. |

### 8.2.1 The two fields that must not be reworded

The record key is

```text
event:<sha256(EVT_Device + 0x1f + EVT_Summary)[0:32]>
```

so the injected event must carry the closed record's `source` and `payload.msg`
**verbatim**. Re-deriving either one — trimming it, re-casing it, prefixing it with
"Closed:" so it reads nicely — would hash to a DIFFERENT key and open a second
condition instead of retiring the one the operator clicked. `operatorCloseEvent()`
in `services/ws-gateway/src/ack.js` is the only place that builds this event, and
its tests assert the echo.

### 8.2.2 `CLOSE` on the wire, `CLOSED` on the badge

| Field | Value | Why |
|---|---|---|
| `severity` | `CLOSE` | the internal retirement value; the worker only accepts this scale |
| `payload.EVT_Severity` | `CLOSED` | the DISPLAY label only |

`CLOSED` is deliberately distinct from the `CLEAR` a producer sends, so an operator
can tell at a glance that a human ended the incident rather than the monitoring.
`isRetirement()` and `colourSeverity()` accept both spellings, because a caller
holds whichever one its path produced.

### 8.2.3 Stream append failed — the fallback

If the `XADD` or the `WAIT` fails, the gateway falls back to the direct write
(`closeAlert`), which retires the record without producing an event. That exists
because an operator must be able to end an incident while the stream append is
refused; the alternative is a board full of alerts nobody can clear. (A `WAIT` that
returned short after a successful `XADD` can leave the close event in the stream
as well; the fold then sees an already-closed record — harmless.)

It is the narrow, older path, it is logged at `warn` when taken, and it is the only
case where a close leaves no event behind. A close taken through the fallback shows
up in the gateway log rather than a metric, because it is an outage symptom rather
than a steady state.

## 9. UI implications

- The gateway forwards events as they arrive; it does not compute alert state. Rendering "this alert just closed" comes from the `severity` field of the event that closed it.
- Because a browser can be handed a stale CLOSE from a fan-out frame, every alert frame carries the fold's return code (`applied`) so the client knows whether to move the row:

```json
{ "op": "event", "event": { "id": "...", "severity": "CLOSE", ... }, "applied": 3 }
```

| `applied` | UI behaviour |
|---|---|
| `0` | history only — do **not** touch alert state or counts |
| `1` | new occurrence — update the row, take the server's `count`, move `lastSeen` |
| `2` | retired — was "coalesced". Clients treat it as `1` so a gateway on the previous build cannot freeze a row mid-rollout |e an entry |
| `3` | closed — remove the alert row |

A UI that ignores `applied` will show alerts closing and immediately reopening (stale CLOSE, §4) and will show a permanently-firing alert with a count that increments 1 000 times a minute (§5.4). Both are fixed by honouring the code.

## 10. Verification

```bash
vk() { kubectl -n fire-watch exec valkey-0 -c valkey -- sh -c "valkey-cli -a \"\$VALKEY_PASSWORD\" --no-auth-warning $*"; }

# 1. entry IDs carry the arrival time; brokerTs on the record equals the ms part
vk XRANGE fw:stream:ingest - + COUNT 5          # only non-empty while a backlog exists
vk HMGET event:<bare-key> lastSeen watermark

# 2. watermark guard: a stale CLOSE must not retire a newer alert
vk HGETALL event:<bare-key>

# 3. occurrence folding: post the same CRITICAL 5x. Expect ONE record, count = 5.
vk HMGET event:<bare-key> count firstSeen lastSeen

# 4. redelivery is a no-op: kill a worker mid-batch, then check count did not jump
#    and History shows the extra delivery rows (validation.md §6)
```

Scenarios are listed in [validation.md](validation.md) §6.

## 11. Operator close through the ingest stream

The gateway's Close (and bulk Close) is an ordinary producer of the ingest stream —
the full rationale is §8.2; this is the wire-level summary the code refers to.

```text
gateway (handleClose)
  closeEvent = operatorCloseEvent(record)            # echoes source + payload.msg verbatim
  XADD fw:stream:ingest * evt <closeEvent> src operator
  WAIT <FW_WAIT_REPLICAS> 5000                        # 1 in production, 0 in the lab
  on any failure -> closeAlert(bareKey, ...)          # direct write, logged at warn (§8.2.3)

worker
  XREADGROUP -> validate -> brokerTs = entry-ID ms -> hist: fork -> fold (applied 3) -> XACK+XDEL
```

| Property | Value |
|---|---|
| Stream key | `FW_STREAM_KEY` (`fw:stream:ingest`) — the same stream http-ingress writes |
| `src` field | `operator` (http-ingress writes `http`) |
| Ordering | the close gets a fresh entry ID, so it is newer than anything already folded and retires the current episode; a re-fire appended after it opens a new one |
| Acknowledgement to the operator | only after `XADD` + `WAIT` succeeded, or after the fallback write succeeded |
| History | one `hist:` row, like any delivery |
