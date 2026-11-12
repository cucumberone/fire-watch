# History — the per-delivery audit trail

Every other read path in this system answers a question about a **condition**.
This one answers questions about a **delivery**, and it exists because the record
model cannot.

## 1. Why a second copy of the data

`event:<key>` is ONE hash per condition, updated **in place**. After an incident
has repeated 400 times the record looks exactly like it did after the first
report, except for `count` and `lastSeen`. That is the correct model for a board
an operator works from — one row per thing that is wrong — but it destroys every
fact about the individual arrivals:

| Question | `event:<key>` | History |
|---|---|---|
| how bad is it now? | yes | no (it has no state) |
| how many times did it fire in total? | yes (`count`) | yes, by counting rows |
| **when** did it fire? | only the last time | **every time** |
| did the broker deliver this twice? | no — the duplicate never reaches the record | **yes**, as two rows |
| what did the source send at 02:14? | only if nothing arrived after | **yes** |
| how many arrivals in this hour? | no | **yes** |

So every delivery writes a second, independent hash — forked BEFORE the fold, so
nothing it does can suppress a row. Roughly doubles storage, which was accepted, and
it is bounded by a shorter retention (§4).

## 2. Shape

```text
hist:<deliveryId>   hash  ONE delivery. Immutable, no state.
idx:history         zset  <deliveryId> -> broker timestamp
```

`<deliveryId>` is a **UUID minted per delivery** (`newDeliveryId()`), not derived
from the message id and not scoped to the condition. Two consequences, both
required:

- two deliveries of the SAME message (a redelivery) are two rows, which is the
  whole reason the fork happens before dedup;
- a condition that fires 400 times keeps 400 rows instead of overwriting itself
  down to one.

The CONDITION key lives inside the hash as the `key` field. Putting it in the key
name would reintroduce exactly that overwrite.

### 2.1 Fields

Written from the same sanitised values the fold is about to write to the record,
so the two cannot drift:

| Field | Note |
|---|---|
| `key` | the CONDITION this delivery belongs to (`<32 hex>`) |
| `id` | the producer's message id — repeated across rows when a message is redelivered |
| `source`, `type`, `msg`, `payload`, `severity`, `displaySeverity` | what arrived |
| `brokerTs` | when WE received it |
| `ts` | when the source claims it fired |

Deliberately **absent**: `count`, `firstSeen`, `lastSeen`, `state`, `ackedBy`,
`closedBy`, `closedAt`. Every one of those describes the condition at read time.
Copying them would create a second, competing source of truth that silently goes
stale the moment the next arrival lands — and an operator reading `count: 1` on an
audit row of a condition with `count: 400` would have no way to know which number
lied.

There is deliberately **no `applied` field**. The fork happens before the fold, so
at write time no verdict exists — and inventing one would mean the trail could only
describe what the dedup accepted, which is the limitation this design removes. The
verdict is a property of the CONDITION and lives on the record; the State column on
Live events / All events is where it is read.

## 3. It is written BEFORE the fold, not inside it

`recordDelivery()` in `services/worker/src/store.js`, called from the worker's
message handler **before** `persistEvent()` runs the fold.

An earlier version wrote the audit row inside `event-fold.lua`, atomically with the
state. That was wrong for this requirement, and wrong in a way that was easy to miss:
the fold is the dedup, so a row written inside it could only ever describe the
arrivals the fold **accepted** (plus redeliveries its watermark happened to
recognise). The one question an audit trail exists to answer —

> what did the broker actually hand us, and how many times?

— was still unanswerable for anything the guard dropped.

So the fork is first and unconditional. The fold then runs and knows nothing about
it. Consequences, all intended:

| | |
|---|---|
| every accepted message produces exactly one row | including the ones the fold will reject |
| a delivery never overwrites another | the id is unique per delivery |
| the write still precedes the ack | a crash after processing but before recording would leave a hole in the log |

It is one `MULTI` of `HSET` + `PEXPIRE` + `ZADD`, so the row and its index member
cannot disagree. A per-command error inside the `MULTI` is checked and re-thrown,
because ioredis reports those in the result tuples rather than throwing — swallowing
them would leave an index member with no hash behind it, rendering as an empty row.

### 3.1 What is lost by moving it out of the script

One thing: the row and the record are no longer written in the same atomic step, so
a crash between them leaves a recorded delivery whose record was never updated. That
is the correct trade — a gap in the log is worse than a gap in the cache, and the
broker will redeliver the message anyway (producing a second audit row, which is
accurate).

## 4. Retention — 3 days, on its own clock

`FW_HISTORY_TTL`, default `259200` (3 days), against the record's 7.

History is one hash per **delivery**, so at the documented 500/s storm peak three
days of it outweighs a week of records. A shorter window keeps the size predictable
while still covering "what happened during the incident this morning".

Two mechanisms, both needed:

- `PEXPIRE` on each `hist:` hash, in the same `MULTI` as the write
- `ZREM rangebyscore idx:history` in `maybePruneIndex`, on the **history** cutoff
  rather than the record's — pruning it with the record cutoff would keep three
  days of members alive for seven, a zset of a million members with no hashes
  behind them

The gateway's ranges stop at `3d` for the same reason: offering a longer window
would promise rows that cannot exist.

## 5. The tab

`History` is the fourth tab, read-only, newest first.

| Column | Note |
|---|---|
| Severity | the arrival's own severity, coloured the same way as everywhere else (`colourSeverity`) |
| **Received** | the broker timestamp — when *we* saw it, not when the source claims it fired |
| **Summary**, **Key** | the arrival's summary, and the condition it belongs to (`Key` carries the producer message id as a tooltip) |

No State column and no Count: an arrival has no state, and a per-arrival count
would be a constant 1. No Result column either — see §2.1.

**The rows cannot be acked, closed or deleted** — those act on the CONDITION, and
that lives on the other tabs. The one action offered is the inspector, because
reading the full payload of a specific delivery is the thing this tab is for.

### 5.1 Fetching, and the page size

Re-queried **every time the tab is selected**, not once per connection.

The earlier rule was "first open only", guarded on `loadedAt`, to avoid re-reading a
large page on every tab switch. That was wrong in a way the operator sees: clicking
the tab a second time showed rows from whenever it was first opened, with nothing on
screen saying they were old — on a trail whose entire value is recency. A page is 50
rows now, so refreshing is cheap enough to do unconditionally.

The guard that replaced it is a **request signature** (`tab`, `status`, `range`,
`limit`), held in a ref and cleared on leaving the tab. It fetches once per entry and
does not re-fire on its own response — the failure mode without it is a loop:
`loading` goes false, the effect runs, `total` resets to null, and the table sits
beside a "showing 50 of 0" notice forever.

**Page size is a separate control from the window.** The window is "how far back",
the page is "how many rows"; conflating them would force an operator who wants more
rows to also change the time range.

| Show | default |
|---|---|
| 50 / 100 / 200 | **50** |

**The tab carries no badge.** Every other tab's badge is a fact about the system
(alerts firing, arrivals received, events in the window); this one's would be the
page size the operator just chose, which tells them nothing they did not type. The
tab opts out via `badge: false`, and `tabBadgeCount('history', …)` throws — so
re-adding a count without re-adding the UI fails loudly instead of silently.

50 rather than the gateway's maximum of 1000: the trail is read by skimming recent
arrivals, and 1000 rows is both a scroll marathon and 1000 hashes out of Valkey per
query. `total` still reports the whole window, so the notice can say
"showing the newest 50 of 812" and point at `Show`.

### 5.2 `op: history`

```json
{"op":"history","range":"24h"}
{"op":"history","range":"1h","key":"<32-hex>","limit":500}
```

| Field | Default | Note |
|---|---|---|
| `range` | `24h` | `15m`, `1h`, `6h`, `24h`, `3d`. Anything else falls back to `24h` rather than erroring |
| `limit` | 1000 | clamped; a full page sets `truncated` |
| `key` | — | one condition's deliveries. A malformed key is **rejected**, not ignored |

```json
{"op":"history","events":[ … ],"total":128,"range":"24h","truncated":false,"sinceMs":…}
```

`total` is a `ZCOUNT` over the same window, so the badge reports the window rather
than the page. Same rule as `allEvents`, and for the same reason: deriving a badge
from fetched rows makes it read 0 before the tab is opened.

`key` is applied server-side (after the bounded read, since the index is scored by
arrival time and a prefix filter would be a full range walk per request). It is
applied server-side so `total` and `truncated` describe the SAME set the table
shows — filtering a page in the client would let the badge say 3 while the table
held 3 of 40.

## 6. What it does NOT do

- **no replay** — History is a read model. `q.audit.events` in RabbitMQ remains the
  durable WAL ([persistence-and-durability.md](persistence-and-durability.md) §4),
  and it is what a rebuild replays from.
- **no ordering guarantee beyond the timestamp** — `brokerTs` comes from the
  ingress, so two arrivals in the same millisecond sort by zset member, which is
  arbitrary. They are the same instant; nothing downstream depends on their order.
- **no dedup at all** — a redelivery is a SECOND ROW, and that is deliberate. The
  trail records what the broker handed us; whether the fold counted it is a
  different question, answered by the condition's `count`.
- **not part of the resize/selection machinery** — no selection, no bulk actions.
  An audit row is a fact, not a target.
