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
| did the broker deliver this twice? | no — the duplicate never reaches the record | **yes**, as a `rejected` row |
| what did the source send at 02:14? | only if nothing arrived after | **yes** |
| how many arrivals in this hour? | no | **yes** |

So each accepted arrival writes a second, independent hash. Roughly doubles
storage — accepted, and bounded by a shorter retention, see §4.

## 2. Shape

```text
hist:<bareKey>|<arrivalId>   hash  ONE delivery. Immutable, no state.
idx:history                  zset  "<bareKey>|<arrivalId>" -> broker timestamp
```

The member is **compound** on purpose. The two zsets answer different questions
(`idx:event:all` is per condition, `idx:history` is per arrival), and two
conditions receiving the same message id must not share an audit hash.

### 2.1 Fields

Written from the same sanitised values the fold is about to write to the record,
so the two cannot drift:

| Field | Note |
|---|---|
| `source`, `type`, `msg`, `payload`, `severity`, `displaySeverity`, `brokerTs` | exactly what the record gets |
| `applied` | the fold's verdict for THIS arrival |

Deliberately **absent**: `count`, `firstSeen`, `lastSeen`, `state`, `ackedBy`,
`closedBy`, `closedAt`. Every one of those describes the condition at read time.
Copying them would create a second, competing source of truth that silently goes
stale the moment the next arrival lands — and an operator reading `count: 1` on an
audit row of a condition with `count: 400` would have no way to know which number
lied.

`applied` is what makes the trail readable:

| Value | Chip | Meaning |
|---|---|---|
| `1` | `counted` | folded in: `count` moved, `lastSeen` moved |
| `3` | `closed` | retired the condition |
| `0` | `rejected` | stale or a redelivery — the record refused it, nothing changed |

`0` is **not** an error and is not coloured as one. At-least-once delivery makes a
redelivery normal; painting it red would send an operator chasing the broker
during a healthy incident.

## 3. It is written inside the fold

`k8s/base/20-event-fold-config.yaml`, steps 1c and the two exits. Not by a second
round trip from the worker.

The reason is the one failure an audit trail cannot have:

> a log written separately can be **skipped** — by a crashed worker, a timeout, a
> rolling restart — leaving the record updated and its audit entry missing, with
> nothing to detect it.

Inside the script both happen or neither does. The same argument the indexes
already rely on.

It is written **before** the ordering guard can reject the arrival. A redelivery is
still an arrival, and "did the broker deliver this twice" is exactly what the
trail is for. The guard then only decides whether the RECORD changes.

## 4. Retention — 3 days, on its own clock

`FW_HISTORY_TTL`, default `259200` (3 days), against the record's 7.

History is one hash per **delivery**, so at the documented 500/s storm peak three
days of it outweighs a week of records. A shorter window is what keeps the size
predictable while still covering "what happened during the incident this
morning".

Two mechanisms, both needed:

- `PEXPIRE` on each `hist:` hash, set inside the fold
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
| **Result** | the verdict chip from §2.1 |
| **Summary**, **Key** | the arrival's summary, and the condition it belongs to |

No State column and no Count: an arrival has no state, and a per-arrival count
would be a constant 1.

**The rows cannot be acked, closed or deleted** — those act on the CONDITION, and
that lives on the other tabs. The one action offered is the inspector, because
reading the full payload of a specific delivery is the thing this tab is for.

### 5.1 Fetching

On first open only, unlike All events (which is fetched on connect so its badge is
honest from the first frame).

The difference is cost and habit: All events is the log an operator scans
constantly, while History is a page of up to 1000 audit rows nobody needs a count
of before they ask for it. Fetching it on every connect would spend a large read on
a tab most sessions never open. This mirrors the old Closed tab's rule, which was
right about *when* to fetch even though the tab itself was redundant.

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
- **no dedup of a redelivery into one row** — a redelivery reuses `(key, id)`, so
  it overwrites its own audit hash rather than adding one. The row then records
  `applied: 0`, which is the honest answer: the arrival was seen and changed
  nothing.
- **not part of the resize/selection machinery** — no selection, no bulk actions.
  An audit row is a fact, not a target.
