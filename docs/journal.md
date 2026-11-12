# Operator Journal

A right-click note on an incident. Small feature, but it touches the two rules this
project is most careful about — who owns a key, and what "no data loss" actually
means for a field a human typed.

**Status: implemented** in the gateway (`journal.js`, `audit.js`), the frontend
(inspector → Journal), and the dev harness. Not yet deployed to the lab.

---

## 1. The two decisions that shape everything

### 1.1 Keyed on the RECORD, and stored beside it

The journal key is the bare record key — the same one ack and close use — **not** a
per-delivery id.

| Keyed on a delivery | Keyed on the record |
|---|---|
| Note vanishes when that delivery is pruned or deleted | Note lives as long as the condition |
| Unreachable from the row an operator works in | Reachable from Open alerts and All events |
| Two notes about "the same outage" are unrelated rows | One thread per condition, which is what an operator means |

Every row for a condition resolves to the same thread, so whichever row you right-click
you land on the same place. With one record per condition there is no longer any
difference between "the alert key" and "the record key": both are the same 32-hex
string ([valkey.md](valkey.md) §2).


### 1.2 Append-only, never a single editable field

`RPUSH` onto a list. There is no "edit note" and no "delete note".

A single `text` field on the alert would have been less code and would have been
wrong: two operators annotating the same incident silently overwrite each other,
last-write-wins, with no trace of what was lost. This codebase has already had to fix
that exact lost-update race twice — the fold owns `count`, and the ack survives
re-fires — and a journal is a **log**, where earlier entries are the entire value.

The consequence is stated plainly: a typo cannot be corrected, only superseded by a
follow-up note.

---

## 2. Storage

```
journal:<32-hex>        LIST   [ {id,by,at,text}, … ]   oldest first
```

Its own key, deliberately **not** fields on `event:<key>`. The worker's fold owns
that hash and rewrites it atomically; a second writer HSET-ing notes into it would
race the fold and leave state that disagrees with itself. Same reasoning that keeps
`count` out of reach — see [event-lifecycle.md](event-lifecycle.md) §5.

| Knob | Default | Why |
|---|---|---|
| `FW_JOURNAL_MAX` | 200 | `LTRIM` on every append. Unbounded operator text is an OOM path |
| `FW_JOURNAL_TTL` | 604800 s (7 days) | Same retention as the alert hash, refreshed per append. A journal that outlived its alert would be an orphan list nothing can read |

Trimming happens **on the write**, not on a timer: a timer cannot know which
incidents are active and would walk every key forever — the same argument as
`maybePruneIndex` in the worker.

### 2.1 Deleting an event does NOT delete its journal

Three key spaces, and they are scoped differently on purpose:

| Key | Scoped to | Removed by |
|---|---|---|
| `event:<key>` | one **record** (a condition) | Delete (the hash and 3 index members) |
| `journal:<key>` | notes about that condition | the 7-day TTL only |

`deleteEvent` removes `event:<key>`, its per-device index member and its two state index
members — nothing else. The journal is a separate LIST, so a record delete cannot touch
it.

**That is the intended behaviour, and it is the reason the journal is a separate key.**
If notes were hash FIELDS on the record, a note would disappear whenever the record was
deleted or expired — the opposite of what a journal is for. Deleting one record also
never implies "forget what operators wrote about this incident": the incident is still
there and may still be firing.

The consequence is stated plainly: **there is no way to delete a journal entry.** Not
one note, not the whole thread. Notes outlive every event in their incident and expire
only with the alert's own 7-day TTL. That was chosen deliberately over two
alternatives:

- **cascade from event delete** — rejected: cleaning up one record would wipe the
  notes for the whole incident
- **a journal delete verb** — not built; if it ever is, it belongs in the journal panel
  with its own confirmation, clearly separate from deleting events

---

## 3. Durability — and the honest limit

**A note is NOT as durable as an event.** Events pass through RabbitMQ and are copied
to `q.audit.events` before the ack, so the event log is recoverable from the broker if
Valkey is lost. Valkey has no AOF. A note written only to Valkey would go
with it.

So each entry is **also published to `fx.audit` / `event.audit`**, the same exchange
and routing key the worker uses for the event audit copy:

```json
{ "kind": "journal", "key": "core-sw-07:link.down",
  "entry": { "id": "…", "by": "alice", "at": 1794326011520, "text": "…" } }
```

`messageId` is `journal:{key}:{id}`, so a redelivery after a reconnect is the same
message rather than a second note.

| Property | Value |
|---|---|
| Broker-confirmed | yes (`createConfirmChannel`) |
| Persistent | yes (`deliveryMode: 2`) |
| Blocking the append | **no** |

**Why the publish is best-effort.** By the time it runs the note is already stored and
readable. Failing the request because the broker is unreachable would turn "your note
may not survive a database loss" into "you cannot write a note" — the wrong trade for
an annotation, and it would report failure for a write that happened. The failure is
logged at **`warn`** and surfaced as the `fw_journal_audit_enabled` gauge, so a
deployment without audit wiring is visible to monitoring rather than living in a
startup log line.

### What is still true even with audit enabled

- The audit copy is **plain text on the broker**. Anyone with broker access can read
  every operator note. It is not a private channel.
- A note deleted from `q.audit.events` is gone. There is no operator delete for
  journal entries at all, by design.
- The note does **not** survive a Valkey restore *in the UI* until replayed from
  the audit log. There is no replay tooling; the audit copy is what makes writing one
  possible.

---

## 4. Wire protocol

### Append

```json
{ "op": "journal", "key": "core-sw-07:link.down", "text": "Swapped the SFP…", "by": "alice" }
```

| Field | Rule |
|---|---|
| `key` | must contain `:`, ≤ 256 chars, no control characters |
| `text` | 1–**1024** chars after cleaning; empty/whitespace-only is refused |
| `by` | optional, ≤ 128 chars, defaults to `anonymous` |
| `id` | **server-minted**, never accepted from the client |

The id is minted server-side because it is both the ordering key and the identity of
the entry: a client that chose it could overwrite or reorder history. It embeds a
process-local counter as well as the clock, so two notes written in the same
millisecond never collide.

Reply to the writer, then fan out to everyone else:

```json
{ "op": "journal", "key": "core-sw-07:link.down",
  "entry": { "id": "…", "by": "alice", "at": 1794326011520, "text": "…" } }
```

The frame carries the **whole entry**, not a "something changed" notice — a notice
would force another round trip to display it, which is the reload-to-see-it bug the
a query-backed tab already had once.

The fan-out publishes `{kind:"journal", origin:POD_NAME}` on `fw:ch:alerts` and is
filtered by `shouldFanOut`, exactly like ack and close. Without that guard the writer
receives its own broadcast and the note appears **twice** — worse than a duplicated
ack, because an ack is idempotent state while a note is a row in a log.

### Bulk read

```json
{ "op": "journals", "keys": ["a:b", "c:d"], "limit": 50 }
```

```json
{ "op": "journals", "journals": [{ "key": "a:b", "entries": [ …newest first… ], "truncated": false }] }
```

| Bound | Value |
|---|---|
| `keys` per frame | `MAX_JOURNAL_QUERY_KEYS` = 200 (matches `op: alerts`) |
| `limit` per key | `MAX_JOURNAL_ENTRIES` = 200, default 50 |

Every requested key comes back even when its journal is empty, so a client can tell
"read it, there is nothing" from "no answer".

---

## 5. Text handling

`cleanNote` in `journal.js` is the only place arbitrary human text enters the system,
so it is the only place these can be handled once rather than at every render:

1. **Strip control characters** — C0 except newline, plus DEL. Left in, they corrupt
   whatever renders the journal next.
2. **Normalise newlines** — `\r\n` → `\n`, so a note pasted from a Windows log is
   stable.
3. **Collapse runs of blank lines** — a pasted terminal dump must not become a wall
   of whitespace and push real notes off the panel.
4. **Trim, then cap at 1024.** Cap last: capping first could leave trailing
   whitespace after the trim.

The limit is enforced in three places, and only the third is a control:

| Where | What it does | Is it a control? |
|---|---|---|
| `maxLength` on the textarea | stops the keystroke | no — a convenience |
| The `n/1024` counter | tells the operator how much room is left | no |
| `MAX_JOURNAL_LEN` in the gateway | truncates for real | **yes** |

The counter turns amber past 90%: reaching 1000 of 1024 is not an error, and colouring
it red would cry wolf.

**Rendered as plain text, never markup.** React escapes it by default and that must not
be "improved": a note is arbitrary operator input, so rendering it as HTML would let
one operator inject content into another operator's panel.

---

## 6. UI

Right-click any row → **Journal (n)**, inside the existing inspector rather than a
modal: the operator is reading the row they are annotating, and a modal would cover it.

- **Newest first**, matching the table convention.
- **Ctrl/Cmd+Enter submits.** Enter stays a newline — notes are often pasted
  multi-line, and a form that submits on Enter mangles them the first time someone
  tries.
- **Optimistic entry**, dimmed and dashed, labelled `saving…`. Shown immediately with
  the operator's own text, then reconciled when the server's echo arrives carrying the
  authoritative id and timestamp. Reconciliation is by **text match against the
  pending entry**, because the server mints the id so the two cannot share one.
- **Signed notes only.** A name prompt appears once and is remembered
  (`localStorage['fw:operator']`). A note attributed to `anonymous` is close to
  useless as an audit trail, and a name field beside every note guarantees people stop
  filling it in.

**The name is not authentication.** There is none in this system. It records who
*claimed* the note — the same standing as the ack's `by` — and the UI must not imply
otherwise.

### Two capture bugs found while building this

Both are the same mistake: putting a **value** in `openInspector`'s closure when only
a **key** belongs there.

| Captured | Symptom | Fix |
|---|---|---|
| `journal.entries` | the count on the menu item was right, but a note added while the panel was open never appeared | resolve `entries` from the `journals` map at render |
| `onAdd` bound to `operatorName` | setting your name **inside** the panel then writing a note still stored it as `anonymous` | build `onAdd` at render |

`openInspector` now stores only `journal: { key }`. Everything else is derived.

---

## 7. Verification

```bash
# append one note through the gateway
kubectl -n fire-watch-lab port-forward svc/ws-gateway 18080:8080 &
node -e '
const WS = require("ws"); const ws = new WS("ws://127.0.0.1:18080/ws");
ws.on("open", () => ws.send(JSON.stringify({op:"journal",
  key:"probe:test", text:"note from the smoke test", by:"operator"})));
ws.on("message", (m) => { const f = JSON.parse(m);
  if (f.op === "journal" || f.op === "error") { console.log(JSON.stringify(f)); process.exit(0); } });'

# the list, and its TTL
PW=$(kubectl -n fire-watch-lab get secret fire-watch-valkey -o jsonpath='{.data.password}' | base64 -d)
kubectl -n fire-watch-lab exec valkey-0 -c valkey -- \
  valkey-cli -a "$PW" --no-auth-warning LRANGE journal:<32-hex> 0 -1
kubectl -n fire-watch-lab exec valkey-0 -c valkey -- \
  valkey-cli -a "$PW" --no-auth-warning TTL journal:<32-hex>       # > 0

# durability: the audit copy
kubectl -n fire-watch-lab exec rabbitmq-0 -- rabbitmqctl list_queues name messages | grep audit
# the entry is in q.audit.events with kind:"journal"
```

Acceptance:

- a second note from a **different** connection appears without a reload (fan-out)
- the note is **not** duplicated for the operator who wrote it (`shouldFanOut`)
- an empty note is refused; a 5000-character note is truncated to 1024
- `/metrics` reports `fw_journal_audit_enabled 1` once audit env vars are set
