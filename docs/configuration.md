# Configuration — data to columns

The first configuration page, reached from the left navigation rail. It maps each
`EVT_*` key the ingress accepts onto the name shown in the GUI, and decides whether
that value gets a **table column** or only appears under **Information**.

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
| `EVT_Device`, `EVT_Agent`, `EVT_AgentOwner`, `EVT_EventKey`, `EVT_Environment` | yes | **yes** — Live events and All events, with the configured label |
| `EVT_Identifier`, `EVT_Details`, `EVT_URL` | no | stored, not yet rendered |
| `EVT_Severity`, `EVT_Summary` | yes | always, locked |

The bottom three have no `toViewEvent` accessor, so promoting one to a column is
stored but has no visible effect. That is a deliberate limit rather than an oversight:
adding a column means adding the field to the view shape, which is a code change, not
a flag. `configuredColumns()` in `lib.js` is the single place that filter lives, so
extending it is one edit.

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
