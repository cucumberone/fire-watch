# Configuration — data to columns

The first configuration page, reached from the left navigation rail. It maps each
`EVT_*` key the ingress accepts onto the name shown in the GUI.

**Visibility is NOT here.** Which columns an operator sees is a per-browser setting,
chosen from the **column picker** in the header (§10.1) and stored in
`localStorage`. What a header is *called* is a team convention — two operators should
not disagree about that — while which columns an individual wants on screen is
their own business. Storing the two together was the original mistake: one
operator's rename silently changed everyone's visible set.

## 1. Global, not per-user

The configuration lives in Valkey at `config:columns` and is broadcast on change, so
**every user of this Fire-Watch sees one set of columns**.

A per-browser preference would mean two operators looking at the same incident and
seeing different columns, with no way to tell whose view was the intended one. That
is the opposite of what a board is for, so the choice was not offered.

| | |
|---|---|
| Storage | `config:columns`, a hash: field = `EVT_*` key, value = `{label, asColumn}` |
| Lifetime | **no TTL** |
| Who writes | the gateway (`op: setColumns`), after validating against the schema |
| Who reads | the frontend, via `op: columns` on every connect |

**No TTL** is deliberate. Configuration that expired would silently revert every
column name to its default on some later day — indistinguishable from a lost edit,
and only noticeable during an incident when someone wonders why the headers changed.

## 2. An empty hash means "all defaults"

`mergeColumns()` overlays whatever is stored onto the built-in defaults, so:

- a fresh install has **no** `config:columns` key at all, and renders exactly as it
  did before this feature existed;
- a key added to the schema later appears automatically, with its default name,
  instead of being invisible until somebody remembers to edit the config;
- a partially-written store cannot blank a column.

The corollary is that the store is never the source of truth for the DEFAULTS — those
are code, in both the gateway and the client, and a test asserts the two lists match
each other and the schema.

## 3. The allowlist is the schema

`COLUMN_DEFAULTS` mirrors `schemas/event-ingest.schema.json`. The list is duplicated
rather than read at runtime, because the gateway image does not ship `schemas/` — so
a **test** reads the schema file and asserts the key sets are equal, on both sides:

| Test | Asserts |
|---|---|
| `services/ws-gateway/src/columns.test.js` | `COLUMN_KEYS` == the schema's `EVT_*` properties |
| `services/frontend/src/lib.test.js` | the client's `COLUMN_KEYS` == the same schema |

A key added to the contract therefore fails a test until it is configurable, rather
than quietly never appearing in the form.

## 4. Locked rows

`EVT_Severity` and `EVT_Summary` are listed so the form shows the **complete**
mapping, but they are `locked`:

- the table renders them itself — a severity badge and a summary cell, not a generic
  text column — so this configuration has no control over them;
- `locked` always comes from the DEFAULTS and is **never** taken from storage, so a
  stored `locked: false` cannot hide the severity badge;
- the form disables both inputs and captions the row `always shown`.

The form sends only the unlocked rows back. Sending a locked row would let the panel
overwrite a flag it does not own.

## 5. Editing is local until Save

The draft lives in component state; the gateway is only called on **Save for
everyone**.

A form that wrote on every keystroke would broadcast a rename to every open board
mid-word, so a half-typed header would flash onto someone else's screen during an
incident. The panel shows an explicit *"Unsaved changes. Nothing is shared until you
press Save"* notice and offers **Reset**.

After a save the client re-renders from the **server's echo**, not from what it sent:
the gateway merges the submission over the stored copy and replies with the result, so
the form can never show a value the server rejected.

## 6. What `asColumn` does today

| Key | Has a view field? | Renders as a column? |
|---|---|---|
| `EVT_Device`, `EVT_Agent`, `EVT_AgentOwner`, `EVT_EventKey`, `EVT_Environment` | yes | **yes** — Open alerts, Live events and All events, with the configured label |
| `EVT_Identifier`, `EVT_Details`, `EVT_URL` | no | stored, not yet rendered |
| `EVT_Severity`, `EVT_Summary` | always | always, locked |

The bottom three have no `toViewEvent` accessor, so promoting one to a column is
stored but has no visible effect. That is a deliberate limit rather than an oversight:
adding a column means adding the field to the view shape, which is a code change, not
a flag. `configuredColumns()` in `lib.js` is the single place that filter lives, so
extending it is one edit.

**Open alerts renders them too, from the alert's event.** It used to render only the
fixed columns, so ticking a key here changed nothing — and the two tables disagreed
about whether the configuration was in effect. The values come off `alert.event`
(the view event the rollup carries), because the alert itself only holds
severity/first/last/count; `alert.event ?? alert` keeps a row readable while the event
is still in flight.

## 7. Unhiding is not "hidden from Information"

Unchecking **Visible as column** removes the COLUMN. The value is still in the record,
still searchable in the inspector, and still present in the JSON the inspector shows —
which is exactly what the label promises. It is not a redaction, and it is not
access control: everything is visible to anyone who can open the board.

## 8. Protocol

```json
{"op":"columns"}
{"op":"columns","columns":{"EVT_Device":{"label":"Device","asColumn":true,"locked":false}, …}}

{"op":"setColumns","by":"gunnar","columns":{"EVT_Agent":{"label":"Tool","asColumn":true}}}
{"op":"columns","columns":{ …the MERGED result… }}
```

`setColumns` is validated as a whole: one unknown key, one blank name or one
non-boolean `asColumn` rejects the entire frame and names the problem. A
half-applied configuration would leave the form showing one thing and the tables
another, which is worse than a refusal.

A successful write is broadcast on `fw:ch:alerts` as `{"kind":"columns"}`, so every
open board updates without a reload. The writing pod skips its own echo, for the same
reason ack and close do.

## 9. Failure behaviour

| Failure | Effect |
|---|---|
| `op: columns` read fails | the gateway answers with the **defaults**. A board rendered with the built-in names is the pre-feature behaviour, not a broken one — an operator mid-incident needs the table, not a config panel |
| `setColumns` write fails | `columns_failed`; the form keeps its draft and the notice stays |
| broadcast fails | the write is durable; other pods keep their old copy until they reload. Logged at `error` because the state genuinely diverges |
| client not yet answered | it renders the built-in defaults, so the first paint is correct rather than blank |

## 10. Ingest schema — which keys are accepted, and what identifies a record

The second configuration page. It edits two things that are one contract:

1. **Which `EVT_*` keys the ingress accepts.** Starts as
   `schemas/event-ingest.schema.json`; an operator can add keys to it.
2. **Which keys mint an identity when `EVT_Identifier` arrives empty.** The
   resulting hash becomes the record key.

Both are stored in one Valkey hash at `input-schema` and applied by the HTTP
ingress **without a redeploy**.

### 10.1 Storage

| | |
|---|---|
| Key | `input-schema`, a hash |
| `schema` | the whole JSON Schema document, as JSON |
| `identifierKeys` | JSON array of `EVT_*` keys used for the identifier hash |
| Lifetime | **no TTL** |
| Who writes | the gateway (`op: setIngestSchema`), after validating both halves |
| Who reads | the **HTTP ingress**, on a 5 s poll; the frontend via `op: ingestSchema` |

One hash, not two keys: the two fields describe a single decision, and a key-by-key
write could be read between the halves — a stored selection naming a key the stored
schema had just dropped would make the ingress hash a field that never arrives.

**No TTL**, for the same reason `config:columns` has none: a contract that expired
would silently stop accepting a key on some later day, and the producer's `400`
would point at its payload rather than at this.

### 10.2 The file is still the default, and the fallback

`schemas/event-ingest.schema.json` is shipped in the ingress image and remains
load-bearing:

| Situation | Contract in force |
|---|---|
| `input-schema` absent | the file |
| `input-schema` unreadable / not JSON / fails validation | the **previous** contract; the file at boot |
| `input-schema` valid | the stored document |
| Valkey unreachable | the previous contract (ingestion continues) |

A corrupted config therefore degrades to "no new keys accepted", never to "nothing
is accepted". The ingress logs the reason at `error` and keeps serving.

The gateway image does **not** ship `schemas/`, so it cannot answer with a default
document of its own: `op: ingestSchema` answers `{schema: null, source: "file"}` and
the **client** renders its own copy. `lib.test.js` reads the real schema file and
asserts that copy matches, on keys, `required` and the severity enum.

### 10.3 The core six are immutable

`EVT_Device`, `EVT_Summary`, `EVT_Severity`, `EVT_EventKey`, `EVT_Agent`,
`EVT_AgentOwner` must stay present **and required**. The validator refuses a document
that removes one or drops it from `required`, and the form shows those rows with
their Remove button disabled.

They are not preferences — the pipeline is built on them:

| Key | Why it cannot go |
|---|---|
| `EVT_Device`, `EVT_Summary` | they *are* the default record key, `sha256(device + 0x1f + summary)` |
| `EVT_Severity` | the mapping onto the internal scale, and the `enum` that defines it |
| `EVT_EventKey` | the table's type column and the subscription filter |
| `EVT_Agent`, `EVT_AgentOwner` | shown on every row |

Letting a web form remove one would break ingestion for every producer, with the
symptom appearing in the payloads rather than here.

### 10.4 Only `type: "string"` properties

Enforced, not a JSON Schema limitation. Every value is stored as text in a hash
field and rendered as text in a table cell, so a `number` or `object` property would
be silently stringified and a `required` check on it would compare against
`"undefined"`.

Operator-added keys are carried into `payload` with a **1024-character** bound and no
further validation — the point of adding a key is to accept a value the code does not
know about. The loop walks the *contract*, never the request, so a producer posting
an unknown `EVT_Whatever` cannot mint hash fields on the record.

### 10.5 The identifier hash

When `identifierKeys` is non-empty and `EVT_Identifier` arrives blank, the **ingress**
computes

```
event:<sha256(normalise(values of the selected keys, joined by 0x1f))[0:32]>
```

and puts it on the envelope as `key`. The worker uses it in place of the condition
key (`services/worker/src/keygen.js`).

Where it is computed matters: the hash has to be **in the stream entry**. The worker,
the gateway's operator close, the verification scripts and any replay job all need it,
and only the producer sees the raw request. The worker re-validates the shape
(32 lowercase hex) rather than trusting it — a bad key would mint a record no index
member and no client can address.

| Rule | Why |
|---|---|
| **Empty selection** means the legacy identity | an install that never opens the page behaves exactly as before |
| A **supplied** `EVT_Identifier` wins, and no hash key is produced | keying on a per-delivery id would turn every repeat into a new record |
| Values are **whitespace-normalised** before hashing | an upstream tool padding its summary differently would mint a new key each time |
| **All values blank** falls back to a random id | a hash of "everything empty" is a constant, so every such event would collapse onto ONE record |
| Case is **preserved** | merging `Disk Full` and `disk full` is a stronger claim than the data supports |

An operator close sends the record's own `key` when there is one, so a CLOSE retires
the record that was clicked instead of deriving a different key beside it.

### 10.6 Protocol

```json
{"op":"ingestSchema"}
{"op":"ingestSchema","schema":{…},"identifierKeys":["EVT_Device","EVT_EventKey"],
 "identifierKeyChoices":["EVT_Severity",…],"source":"valkey","version":"stored"}

{"op":"setIngestSchema","by":"gunnar","schema":{…},"identifierKeys":["EVT_Device"]}
{"op":"ingestSchema", …the STORED document… }
```

`setIngestSchema` validates the schema and the selection **together** and rejects the
whole frame on any problem. A schema saved without its selection, or a selection
naming a key the new schema dropped, would make the ingress hash a field that never
arrives — one constant key for the whole fleet. A partial save is therefore refused
rather than merged.

A successful write is broadcast on `fw:ch:alerts` as `{"kind":"ingestSchema"}`, so
every open form updates without a reload. The writing pod skips its own echo, for the
same reason ack and close do.

### 10.7 Failure behaviour

| Failure | Effect |
|---|---|
| `op: ingestSchema` read fails | answers `source: "file"` — the ingress is running the file contract, so this is the truth, not a fallback |
| stored document is not JSON | same answer; the ingress logs it and keeps its previous contract |
| `setIngestSchema` write fails | `ingest_schema_failed`; the form keeps its draft |
| broadcast fails | the write is durable and the ingress picks it up on its next poll; other pods' clients keep the old copy |
| an added key is missing from a payload | the key is simply absent; only keys added to `required` are enforced |

## 10.1 The column picker (per browser)

An icon in the header, beside the theme and row-colour toggles, opens a dropdown
listing every unlocked ingest key with a checkbox.

| | |
|---|---|
| Storage | `localStorage['fw:hiddenColumns']`, a JSON array of **hidden** keys |
| Scope | one browser — never broadcast, never in Valkey |
| Default | Severity, Summary and **Event Key** visible; everything else hidden |

**Hidden, not shown.** The stored set is the complement, because that is the smaller
one and it degrades correctly: a key added to the ingest schema later arrives
*visible* rather than needing an entry to appear. `normaliseHidden()` drops locked
keys and non-strings, so stale or hand-edited storage cannot hide the severity badge.

**Absent storage is not "hide nothing".** With no stored value the client applies
`defaultHidden()`, so a new browser opens on the columns that carry the signal. An
empty stored array is a real choice — the operator turned everything on — and is
respected. Unreadable JSON falls back to the defaults rather than to "show all".

**Severity and Summary are not switchable.** They are `locked`: the table renders a
severity badge and a summary cell, not generic cells, so a checkbox for either would
promise a control that does nothing. Count and the two occurrence columns are part of
the fixed layout for the same kind of reason — a row with no time reference is not
useful.

**Renames come from here, visibility does not.** The picker reads `config:columns`
for the labels, so a renamed header shows the operator's name in the menu. It writes
only to `localStorage`.

### 10.1.1 Operator-added keys

Data to columns lists **every key the ingest contract declares**, not just the
client's build-time copy: `COLUMN_KEYS` first (it carries the presentation order and
the `locked` flags), then any key from `input-schema` that it does not know. Without
that union, a key added from Configuration → Ingest schema could never be given a
name — the one thing the page exists to do.

The gateway accepts those names too. Its allowlist was `COLUMN_KEYS` exactly, so it
rejected what the form was happy to send; it now also accepts `EVT_[A-Za-z0-9_]{1,61}`
— the same shape the ingest contract enforces for an added key, so a name that could
not be an ingest key still cannot be stored as a column name.
