# Frontend

React single-page app served as static assets, talking to the WebSocket gateway.

## 0. UI framework

The chrome is **MUI** (`@mui/material` + emotion). The tables are not — they are
custom, because they carry behaviour DataGrid does not provide out of the box:
drag-to-resize columns with per-table persistence, a header that cannot scroll
away, shift/ctrl multi-select, and two stacked tables sharing one scroll offset.
Re-implementing those on DataGrid's API would change how the board behaves, so the
surface around the tables is MUI and the tables stay hand-built.

| Layer | Provided by |
|---|---|
| App bar, status chip, theme/palette controls | MUI `AppBar` / `Toolbar` / `Chip` / `IconButton` |
| View selector | MUI `Tabs` / `Tab` |
| Pane headings and actions | MUI `Stack` / `Typography` / `Button` |
| Closed-tab filters | MUI `TextField` / `FormControl` / `Select` |
| Messages | MUI `Alert`, `CircularProgress` |
| Tables | custom (`ScrollTable`, `ResizableTh`) |

`buildTheme(mode)` creates one theme per resolved mode, memoised on `darkMode` —
rebuilding it per render would re-render every styled component on every socket
frame, because MUI propagates a new context value on identity change.

The palette is intentionally tight (48px toolbar, 40px tabs, 13px base, dense
inputs). This is an operations board read at a glance during an incident, not a
content site; Material's default sizing fits half the alerts on the same screen.

Two MUI defaults had to be overridden, both for the same reason — a flex column
shrink-wraps:

- `MuiToolbar` needs `width: 100%`, or it collapses to the width of its content
  and sits centred in the AppBar
- `MuiTabs` (with `variant="scrollable"`) shrink-wraps its labels, so it needs
  `width: 100%` too, and the Toolbar above it inherits the same shrink without it

**One separator, then a dark-green band.** The tab strip used to end with a 1px
border and the AppBar with another, 13px apart, with dead background between them.
The band (`--band:#14532d`, 6px, on `MuiTabs.list`) replaces both: it is the single
divider between the tabs and the data, and it is the app's accent rather than a grey
rule, so the boundary reads as part of the design.

The band needs **10px of `padding-bottom` on `MuiTabs.list`** to detach it from the
selected tab. A tab fills the list's content box, so without padding the rounded
green fill of the selected tab sits directly on the band and the two run together as
one thick block. The gap is deliberately wider than the band: at 6px the eye reads the
fill-plus-band as a single striped rule instead of as a fill and a separate separator.

Three MUI v9 details make this fiddly, and all three fail **silently**:

| Mistake | Symptom |
|---|---|
| styling `MuiTabs.flexContainer` | no effect — the slot was renamed to `list` in v9 |
| `<Stack alignItems="center">` as a prop | computed style stays `normal`; use `sx` |
| a plain `header { }` rule in `build.js` | MUI's AppBar **is** a `<header>`, so pre-MUI chrome CSS re-arms against it |

The third one was the actual cause of the double line: `build.js` still carried a
pre-MUI `header { padding-bottom:.75rem; border-bottom:1px solid; gap:1rem }`, which
drew the lower border and the 12px void. The lesson generalises — a component rename
can silently re-arm dead CSS — so the rule is gone and anything the top bar needs
lives in the theme, scoped to the component that owns it.

The CSP already allowed `style-src 'unsafe-inline'`, which is what emotion's
runtime style injection requires, so no CSP change was needed.

**Cost:** the bundle went from 169 KiB to 459 KiB (unminified) because MUI and
emotion are large. That is the price of the framework, accepted deliberately —
but it is the reason MUI stopped at the chrome. Pulling in DataGrid would have
added more on top of an already-heavy bundle, to replace tables whose behaviour
would then need rebuilding.

## 1. Topology

| Item | Value |
|---|---|
| Kind | `Deployment`, 3 replicas |
| Image | `image-registry.openshift-image-registry.svc:5000/fire-watch/frontend:<sha>` (nginx-unprivileged + built assets) |
| Port | 8080 (nginx listening unprivileged; OpenShift-friendly) |
| Exposure | OpenShift `Route`, edge TLS |
| State | none — 100% static, any pod can serve any request |
| Anti-affinity | **required**, `kubernetes.io/hostname` |
| PDB | `minAvailable: 2` |

## 2. Why a separate Route

The frontend is served over HTTPS by a Route; the gateway is a separate Route with WebSocket support. Keeping them apart means:

- the gateway's 1h route timeout does not apply to asset requests
- a gateway roll-out never invalidates a cached JS bundle
- each can be scaled and rolled back independently

## 3. Runtime configuration

The bundle is built once and configured at runtime, because build-time env vars would force a rebuild per environment:

```text
/usr/share/nginx/html/config.json   (mounted from ConfigMap)
{
  "wsUrl": "wss://ws.fire-watch.example.com",
  "apiUrl": "",
  "backfillDefaultMinutes": 15,
  "resolveWsHostFromPage": false,
  "resyncSeconds": 60
}
```

`resyncSeconds` bounds how long a missed `close` frame can leave a stale row on
screen (see [websocket-server.md](websocket-server.md) §3.3). `0` disables the
timer; the reconnect and tab-visibility resyncs still fire.

nginx serves `config.json` with `Cache-Control: no-store`; `index.html` also `no-store`; hashed assets get `immutable, max-age=31536000`.

## 4. WebSocket client requirements

| Requirement | Detail |
|---|---|
| Reconnect | jittered exponential backoff, 0.5s → 30s cap, infinite retries |
| Resume | on reconnect, send `subscribe` with `since` = last received event broker timestamp minus 5s overlap |
| Resync | send `resync` every `resyncSeconds`, on every reconnect, and when the tab becomes visible. A snapshot **replaces** the alert map, including when it is empty — merging would keep rows that have already retired ([websocket-server.md](websocket-server.md) §3.3) |
| De-duplication (transport) | keyed on event `id` (overlap + at-least-once delivery can duplicate) |
| De-duplication (occurrence) | render `count` from `op: alert` frames; never increment it per received event — the worker coalesces ([event-lifecycle.md](event-lifecycle.md) §5.4) |
| Ordering | sort by broker timestamp, not arrival order and not producer `ts` — cross-worker delivery can invert ([event-lifecycle.md](event-lifecycle.md) §2) |
| `applied` code | **must** be honoured: `0` dropped (duplicate or stale) / `1` applied / `3` closed. `2` is a retired alias for `1`. Treating `0` as an applied event double-counts redeliveries ([event-lifecycle.md](event-lifecycle.md) §9) |
| Alert state | render from `op: alert` frames and, on connect, from the `op: snapshot` frame; never derive "is it open" or the count from the event list ([websocket-server.md](websocket-server.md) §3.1) |
| Initial load | `op: snapshot` **replaces** the alert map, it is not merged: it is the complete open set, so an omitted row is genuinely closed. Merging would resurrect rows retired while the socket was down |
| `msgc` decoding | field may be gzip + base64. Decode with the native `DecompressionStream`; on failure render `[undecodable payload]` and keep the rest of the alert usable ([payload-and-compression.md](payload-and-compression.md) §5) |
| `msg` decoding | always plain — leave it alone (it is below the compression threshold by design) |
| Field naming | the wire says `msg` / `msgc`; the UI says **Summary** / **MsgCompleteString**. Renamed once, at the ingest boundary, by `toViewEvent()` in `src/lib.js` (§4.1) |
| Severity display | `FATAL` > `CRITICAL` > `WARNING` > `HARMLESS` > `CLOSE`; `HARMLESS` / `CLOSE` are retirement events and are green, not grey, because "resolved" is a success state |
| Row colouring | opt-in, persisted; whole row tinted by severity with a 3px left bar. Off by default — at 1 000 events/min a fully tinted table is harder to scan |
| Theme | `auto` (follow `prefers-color-scheme`) / `light` / `dark`, persisted in `localStorage`; `auto` is stored as the *absence* of the key so a later OS change still applies |
| Buffering | render at most `N` rows (default 500) in the live table; older windows come from backfill |
| Live tail | its own **tab**, not a drawer under the alert table. See §5.5 for why the drawer was replaced |
| Acknowledgement | an `Ack` column showing state, and the **action is a right-click menu item** on the row. State arrives on `op: alert` frames carrying `ack`, is re-read on reconnect and when the displayed key set changes, and is dropped locally on `applied: 1` because the fold clears it there ([acknowledge.md](acknowledge.md) §6) |
| Backpressure | if the socket's `bufferedAmount` exceeds a threshold, drop render cadence rather than the socket |
| Visibility | close the socket when the tab is hidden > 5 min, reconnect on focus with backfill |

### 4.1 Wire names vs. UI names

The message contract, Valkey and the audit WAL all use `payload.msg` and `payload.msgc` ([payload-and-compression.md](payload-and-compression.md) §1). Those names do not belong on a screen: `msgc` decodes to nothing useful to an operator, and calling the field "Message" collides with the event as a whole.

| Wire | UI | Where |
|---|---|---|
| `payload.msg` | **Summary** | live-events column header |
| `payload.msgc` | **MsgCompleteString** | live-events disclosure button |
| `count` | **Count** | alert and live tables |

The rename is a single pure function, `toViewEvent()` in `src/lib.js`, applied once in the ingest path. Components never read the wire names. That matters because:

- the vocabulary cannot drift field-by-field across rows
- the mapping is unit-testable without a DOM
- the original fields are preserved on the view event, so any caller still reading `evt.payload.msg` keeps working
- a missing payload yields `''`, never `undefined`, so a row can never render the literal text "undefined"

**The details fold reveals `msgc` and nothing else.** Two bugs lived here, and both were
visible as "there is a details link on every row":

1. The fold fell back to `JSON.stringify(evt.wire, null, 2)`. `toViewEvent` always sets
   `wire`, so the fallback was always truthy — measured **211 of 211** rows had a link —
   and what it opened was a raw debug dump of the record. The right-click menu's
   Information submenu already renders the same fields as a table, so the fallback was
   a second, unreadable way to see them. The link now appears only when the producer
   sent long-form text.
2. `toViewEvent` maps a compressed field with `asText`, and `String({__gz:…})` is
   `"[object Object]"`. The live path passes payloads through `decodePayload` first;
   the **query** paths (`allEvents`, `closed`) did not, so `msgc` reached the mapper
   still compressed and the fold rendered that string. Those paths now decode too, and
   `asPayloadText` yields `''` for anything that is not already text, so a missed
   decode hides the fold instead of showing garbage.

The general rule: **any path that maps a stored record must decode it first.** There
are three of them and they must stay in step — live frames, `op: allEvents`, and
`op: closed`.

### 4.2 Occurrence fields and worker enrichment

The UI shows four fields that the workers add to `payload` before the event reaches Valkey. All are optional — an unenriched event must still render.

| Wire key(s) | UI column | Notes |
|---|---|---|
| `FirstOccurrence`, `firstOccurrence`, `first_occurrence` | First occurrence | epoch ms or ISO 8601 |
| `LastOccurrence`, `lastOccurrence`, `last_occurrence` | Last occurrence | falls back to the event's broker timestamp |
| `Count`, `count` | *(no column)* | parsed for the alert rollup's sake but **no longer displayed**: the `EVT_*` schema has no such field and the ingress never adds one, so it was a constant `1` in every event row. Removed for that reason — see §5.6 |
| `CIF` | CIF | acronym, arrives uppercase |
| `AlarmIDKey` | Alarm ID key | `ID` stays uppercase; all four other casings accepted |
| `AlarmOwner` | Alarm owner | |
| `AutoClose` | Auto close | boolean renders as `yes` / `no` |
| `SupportGroupName` | Support group | |

Accepted spellings are listed explicitly rather than matched case-insensitively: a genuine unrelated `cif` must never be mistaken for the enriched one. `AlarmIDKey` accepts `AlarmIDKey`, `AlarmIdKey`, `alarmIDKey`, `alarmIdKey` and `alarm_id_key`, because `ID` is an acronym upstream tools spell inconsistently.

**Fallback rule.** Without enrichment, a single raw event is its own first *and* last occurrence with `count: 1`. That is the honest answer for one event; extrapolating a wider window would be a guess. `first` and `last` therefore being equal is expected and correct for an unenriched stream — it does not indicate a bug.

### 4.3 Column order

Both tables lead with identity and status, then time, then volume, then the human-readable text, then enrichment. Keeping the two tables in the same order means an operator's eye lands in the same place after switching views.

| Open alerts | Live events / All events |
|---|---|
| Severity | Severity |
| First occurrence | First occurrence |
| Last occurrence | Last occurrence |
| **Count** | *(removed — always 1)* |
| Summary | Summary |
| Ack | CIF, Alarm ID key, Alarm owner, Auto close, Support group |
| **Key** | |

The Count column exists only on the alert table, where it is the fold's occurrence
rollup. The event tables had one too, carrying a per-message `Count` field that no
producer sends; it was a constant `1` and has been removed (§5.6).

`Key` is deliberately **last** in the alert table. It is a stable identifier you read once when you need it, not something scanned down the column on every glance — putting it first pushed the signal (severity, recency, count) off to the right. Ack sits immediately before it because both are "about this specific row" rather than about the condition.

With ten columns the live table is horizontally tight, so:

- **no cell ever wraps.** A wrapped value corrupts the value — `CIF-\n7730` is
  unreadable, and a timestamp split across two lines is worse than a scrollbar.
  Cells are `white-space: nowrap` with `text-overflow: ellipsis`; the container
  scrolls horizontally. The full text of any record is available from the
  right-click **Information** panel (§5.7).
- headers may still wrap, because a wrapped header costs a line while a wrapped
  value corrupts the value
- the tables are `table-layout: fixed` inside a `.tablewrap` with `overflow-x:
  auto`, so `min-width: 100%` keeps them filling the pane until the columns exceed
  it

#### A header that never scrolls away

Both tables pin their header. The mechanism is **two stacked tables** — one for
the header, one for the body — inside a flex column, where only the body is a
scroll container.

`position: sticky` on the `th` does not work here, for two independent reasons:

1. A no-wrap table needs `overflow-x: auto` on its wrapper, and setting
   `overflow-x` makes an element a scroll container in **both** axes. The sticky
   header then sticks to that box, which never scrolls vertically, so the header
   scrolled away with the rows exactly as before.
2. Even with that resolved, sticky is bounded by its containing block: when the
   table ends, the header goes with it.

Splitting the tables makes the behaviour structural rather than dependent on
scroll ancestry — the header cannot scroll because it is not inside a scroller.

Both tables share one `<colgroup>` and are `table-layout: fixed`, so the columns
line up. The body's horizontal scroll offset is mirrored onto the header, or a
sideways scroll would shear the header off its columns.

The height chain matters as much as the split: `#root` → `main` → `section` →
`.tablewrap` are all flex columns with `min-height: 0`. Without the `section`
rule the wrapper keeps its content height, `.tbody` has nothing to scroll, and the
whole section overflows instead — which is what pushed the header out of view to
begin with. Measured after the fix, with both bodies scrolled: the alert header
stayed at y=150 and the live header at y=305.

#### Time format

Every timestamp is rendered as **`YYYY-MM-DD HH:MM:SS`, 24-hour, local time**
(`formatClock` in `src/lib.js`). Hand-rolled, not `toLocale*`:

- locale-dependent output means two operators can see different strings for the
  same alert, and the format can change under them after an OS locale change
- the previous 12-hour clock lost the AM/PM distinction off the edge of a narrow
  column, so 01:05 and 13:05 looked identical
- the date was missing entirely, so yesterday's events were indistinguishable
  from today's

Local time, because an operator correlates against the clock in front of them; the
inspector shows the raw ISO and epoch values for anyone who needs UTC.

#### Resizable columns

Each header cell carries a 7px drag handle on its right edge (`role="separator"`).
Dragging sets a width for that column only; widths are persisted per table in
`localStorage` (`fw:colw:alerts`, `fw:colw:events`, `fw:colw:all`, `fw:colw:closed`).

**Widths are keyed by column LABEL, never by index.** They used to be keyed by
position, which is silently wrong the moment a column is added or removed: deleting
the "Msg count" column shifted every index past it, so an operator's saved widths
came back applied one column to the left. Nothing errored — the table just returned
with the wrong columns wide. A name-keyed entry for a column that no longer exists is
simply ignored, and a new column starts at its default.

A stored layout in the old index-keyed shape is migrated once on load, so an
existing layout is not thrown away on upgrade.

**There is no "reset columns" button.** It only existed to recover from that
bug. With names as keys there is nothing to recover from, and a control that exists
to undo a data-corruption bug is a poor use of a header.

Three deliberate choices:

- **`<colgroup>` with per-column widths, not flexbox.** The no-wrap rule means
  columns are fixed-width boxes; there is no meaningful "flex" when the content
  cannot reflow.
- **A minimum of 48px.** Without a floor a drag can collapse a column to nothing,
  and at zero width there is no handle left to drag it back.
- **Listeners on `document`, not the handle.** The pointer leaves a 7px handle
  almost immediately; a handle-local listener would stop tracking mid-drag.
  `body.resizing` keeps the `col-resize` cursor and suppresses text selection for
  the duration, or the drag selects the whole table.

Only the leading columns of the live table are resizable; the enriched columns are
appended after them with natural widths.

#### Making the two tables the same width

Every table must line up, and a few
pixels of offset is immediately visible as misaligned columns.

Note that resizable columns (§4.3) change this on purpose: once an operator drags
a column, the two tables are *no longer* the same width, because the widths are
per-table state. What stays true is that both containers occupy the full pane, so
the misalignment comes only from content and never from a layout bug.

Three things are needed for the containers to match, and the first two were each
wrong at some point:

1. **No `max-width` cap.** `main` used to be limited to 1200px, which wasted most
   of a wide monitor and forced the ten-column live table to wrap. It now fills
   the viewport.
2. **`width: 100%` on `main`.** In a flex column, removing `max-width` is not
   enough: the item shrink-to-fits its content and ended up narrower than its
   container. `#root` states `align-items: stretch` explicitly for the same reason.
3. **`scrollbar-gutter: stable` on the pane bodies.** Each tab's `.panelbody` owns
   the scroll, and reserving the gutter keeps the columns from shifting by the
   scrollbar width when a table crosses the threshold that needs one.

Measured after the fix (table widths, px):

| Viewport | Alert table | Live table | Delta |
|---|---|---|---|
| 1024 | 964 | 999 | −35 |
| 1152 | 1092 | 1092 | **0** |
| 1280 | 1220 | 1220 | **0** |
| 1536 | 1476 | 1476 | **0** |

Below ~1100px the ten-column live table cannot shrink past its nowrap minimum, so
it overflows (and scrolls) while the alert table still fits. That is the intended
trade-off: keeping identifiers unbroken matters more than matching widths on a
narrow window.

## 5. UI preferences

Three presentation preferences are persisted. All are client-side only — they are
not sent to the gateway and they do not affect `applied` handling.

| Preference | Values | Default | Storage |
|---|---|---|---|
| Theme | `auto`, `light`, `dark` | `auto` | `localStorage['fw:theme']` |
| Colour rows | on, off | off | `localStorage['fw:rowColor']` |
| Selected tab | `alerts`, `events`, `closed` | `alerts` | `localStorage['fw:tab']` |

Column widths are persisted separately per table (`fw:colw:alerts`,
`fw:colw:events`, `fw:colw:all`, `fw:colw:closed` — §4), keyed by column label.

The selected tab is persisted because it is a **workspace preference, not
transient UI state**: an operator working the Closed tab should not be reset to
Alerts on every reload. An unknown stored value falls back to `alerts` rather than
throwing or rendering an empty page.

Both toggles are **icon buttons**, not checkboxes:

| Control | Icon | On-state |
|---|---|---|
| Theme | crescent (dark) / sun (light) — the icon shows the mode you would switch *to* | blue pill |
| Colour rows | palette | amber pill |

Implementation notes:

- real `<button>` with `aria-pressed`, not a styled `<input type=checkbox>` — dressing up a native checkbox means faking its box, and a fake box loses the keyboard and screen-reader behaviour the native control had
- inline SVG, not an icon font or emoji: no extra request, no font-loading flash, inherits `currentColor` so the theme handles it, and an emoji renders in the OS emoji font on one platform and as tofu on another
- the on-state is driven by `[aria-pressed="true"]` rather than a separate class, so the visual state and the announced state cannot disagree
- the two on-states are different hues (blue / amber). Two identical blue pills told you *that* both were on but not *which* was which
- the theme icon shows the destination mode, and its `aria-label` says `Switch to dark mode` / `Switch to light mode` — it never announces what is currently on, because `aria-pressed` already does

Theme is applied as `html[data-theme="<value>"]`, or no attribute at all for `auto`. The CSS keys off the attribute *and* `prefers-color-scheme`, so:

- no attribute + dark OS → dark
- no attribute + light OS → light
- attribute beats the OS, in both directions

`localStorage` access is wrapped in `try`/`catch`: Safari private mode throws on read, and a lost preference must never break the render.

### 5.1 Severity colours

Every severity is a token pair, so both themes stay legible:

| Severity | Badge | Row tint | Meaning |
|---|---|---|---|
| `FATAL` | **black**, white text | black | unrecoverable |
| `CRITICAL` | red, white text | red | service affecting |
| `WARNING` | amber, dark text | amber | degraded |
| `HARMLESS` | **green**, white text | green | informational retirement |
| `CLOSE` | **green**, white text | green | resolution |

`FATAL` is black in both themes — it is the one severity that must read as "this is not a colour, this is an outage". Because a black badge on a black row would lose its outline, the badge carries a 1px `rgba(255,255,255,.45)` inset ring, and the row's left bar is red rather than a darker black.

`HARMLESS` and `CLOSE` are green because resolution is a success state, not an absence of state. They are still ranked last in the alert table so an open `WARNING` never sorts below a resolved `CRITICAL`.

#### The badge shows the PRODUCER's severity, not the internal one

These five are **internal** values — the ingress maps a producer's eight-value
scale onto them ([event-schema.md](event-schema.md) §3). A producer never sends
`CLOSE` or `HARMLESS`; it sends `CLEAR` and `INFORMATION`. Rendering the mapped
value therefore showed operators a severity that does not exist in their own
vocabulary.

So the badge splits its two jobs:

| | Source | Why |
|---|---|---|
| **Colour** | the severity being DISPLAYED (`colourSeverity`) | colour is the primary signal, so it must agree with the word next to it |
| **Text** | `payload.EVT_Severity` | what the producer actually reported |

`toViewEvent` puts the producer value on `displaySeverity`, falling back to the
internal one for a record that carries no `EVT_*` payload (a legacy or internal
producer). A `CLEAR` therefore renders as a green **CLEAR** badge on a dimmed
retirement row: `CLEAR` maps onto the retirement colour, so nothing about the
retirement behaviour changes.

**The colour is NOT taken from the stored `severity` blindly.** That was the bug:
`colourSeverity(stored, displayed)` maps the displayed label back onto a colour
class, because the two sources can disagree on a CLOSED row.

An operator close sets `severity: CLOSE` on the record and **leaves `payload`
alone**, so the badge said **WARNING** in **green** — a colour belonging to a
different severity than the word beside it, on the one table where an operator is
reading the badge to decide how bad the incident was. The same happened to a
re-read row carrying an older event whose payload was still `MAJOR`.

The mapping mirrors `SEVERITY_MAP` in `services/http-ingress/src/normalise.js`:

| displayed | colour |
|---|---|
| `FATAL` | `FATAL` (black) |
| `CRITICAL`, `MAJOR` | `CRITICAL` |
| `WARNING`, `MINOR`, `INDETERMINATE` | `WARNING` |
| `INFORMATION` | `HARMLESS` |
| `CLEAR`, `HARMLESS`, `CLOSE` | `CLOSE` (green) |

An unrecognised label (a producer inventing its own value, an empty label, a
legacy record) falls back to the stored severity, which is what the colour classes
were defined for. Genuine retirements are unaffected: `CLEAR` and `HARMLESS` both
land on green.

Deliberately **not** renamed in the data: `severity` on the wire, in the fold and
in the alert hash stays internal. Only the label an operator reads is translated —
renaming the stored value would break every comparison in `event-fold.lua` and
`SEVERITY_RANK`.

#### A retirement row is dimmed, but NOT its severity badge

`tr.retirement td { opacity:.55 }` marks an ended episode, and it deliberately does
not apply to the first cell (`tr.retirement td:first-child { opacity:1 }`).

Opacity is multiplicative and cannot be undone by a child, so the badge has to be
exempted at the cell. Dimming it was the wrong trade: at 55 % an amber `WARNING`
reads as grey/unknown, which is the one thing the badge exists to prevent. The row
is still visibly retired through its timestamps, summary and key — the cells where
nobody is reading colour.

Column 1 is Severity in **every** table (`ALERT_COLS`, `CLOSED_COLS`,
`EVENT_COLS`, `ALL_EVENT_COLS` all begin with it), so this is one rule in
`build.js` rather than a class sprinkled across three row components. Adding a
table whose first column is not Severity means revisiting it.

### 5.2 Whole-row colouring

Off by default. When on, every cell of a row gets the severity tint and the first cell gets a 3px inset bar of the same hue. The bar is not decoration — it keeps the severity readable when the table is scrolled horizontally and for anyone who cannot separate the tints.

Two deliberate interactions with the always-on styles:

- retirement rows lose their `opacity:.55`, because dimming would undo the tint that is the point of the mode
- `.dim` cells switch to `color:inherit; opacity:.7`, because the grey type colour has no contrast against a tinted background

Colour is never the only signal: the `sev` badge still carries the severity as text on every row.

### 5.3 Eventless frames — and the blank page they caused

A **content duplicate** is absorbed by the worker: the copy is discarded and only
the rollup moves, so the gateway fans out `{kind:'occurrence', key, applied}` with
**no event attached** ([event-lifecycle.md](event-lifecycle.md) §5.4). The client
still has to increment that row's count — `count` is owned by the fold.

The first version of `applyAlert` handled that by falling back to `undefined` for
`severity` when the row was new. That produced a row with no severity, and the
Badge rendered it with:

```js
<span className={`sev sev-${severity.toLowerCase()}`}>{severity}</span>
```

`severity` was `undefined`, so this threw **during render**. React unmounts the
whole tree when a render throws, so the page went completely blank — the app
looked crashed, and a browser refresh was the only way back. Firing
`send-same-event.mjs` was enough to trigger it, because every repeat after the
first is a content duplicate.

Two independent fixes, because either alone would leave the other latent:

1. `applyAlert` will not create a row from a frame with **no event** when the key
   has never been seen. The first delivery of a condition always carries its
   event, so an eventless frame for an unseen key means this client missed the
   original — the next `resync` supplies the real row. Ignoring it cannot make the
   server's state wrong, only this client briefly stale.
2. `Badge` tolerates a missing severity and renders an `unknown` badge instead of
   throwing. A single malformed frame must never be able to blank the page, so no
   render path is allowed to assume a field is present.

An eventless frame for a row that **does** already exist still increments the
count and moves `lastSeen`, and never downgrades a known severity. Three tests in
`lib.test.js` pin all of this, including one that walks a frame sequence and
asserts every resulting row is renderable.

**Count is the server's, not the client's.** When a frame carries `row` (see
[websocket-server.md](websocket-server.md) §3.2), `row.count` is used verbatim —
it already includes the occurrence that caused the frame. Adding one on top
double-counted, so a CLEAR followed by a re-send showed `Count 2` for a brand-new
episode. The client only increments its own copy when no row is supplied, which is
the case against an older gateway.

### 5.4 Error boundaries: a bad row must not blank the board

Fixing the one `toLowerCase` call was not enough, for a structural reason: **a
throw during render unmounts the entire React tree.** Any malformed field,
anywhere, took the whole page black — which reads as a crashed app and is only
recoverable with a refresh. In a monitoring UI that is the worst possible failure,
because it happens precisely during an incident.

So there are now boundaries at two levels:

| Boundary | Contains |
|---|---|
| around `<App>` | a failure in the shell (header, config) |
| around each table | a failure in one pane's rows |

A broken row therefore costs **one pane**, and the boundary renders a fault panel
naming what failed. The intent is that a pane never goes silently empty: an empty
table reads as "nothing is happening", which is the most dangerous message to send
by accident, so the panel says explicitly that the rest of the board is still live.

Deliberately no automatic retry on catch — re-rendering the same bad data would
loop. The panel waits for the next state change or a reload.

This was verified by forcing a render throw in the alert table and confirming the
header, the other pane and the socket all stayed functional.

### 5.5 Four tabs: Open alerts, Live events, All events, Closed

The drawer is gone. It was a primary view with an accessory bolted underneath: it
could only be expanded or invisible, it competed with the alert table for vertical
space, and at `max-height:38vh` it either hid during an incident or ate a third of
the screen.

Three peers replace it, because "what is firing", "what did we receive", and "what
already finished" are parallel questions rather than a hierarchy:

| Tab | Answers | Source |
|---|---|---|
| **Open alerts** | what is firing right now | `op: snapshot` on connect + `op: alert` live |
| **Live events** | what did we actually receive, and when | `op: event` frames, arrival order |
| **All events** | what happened in the last N, searchable | `op: allEvents`, a windowed query |
| **Closed** | what ended in the last 7 days | `op: closed`, a windowed query |

Badges come from `tabBadgeCount(tabId, counts)` in `lib.js`, which switches
explicitly per tab and **throws on an unknown id**. It used to be a ternary chain
ending in `: closed.count`, so adding the All events tab silently gave it the
Closed count — a number beside an empty table, which reads as a broken query rather
than a wrong badge. A new tab must now decide what its badge counts.

**All events vs Live events** is the distinction worth stating, because they look
alike: the live tail is an in-memory buffer of **arrivals since this page loaded**,
which dies on reload and holds at most 500 rows. All events reads **storage**, so it
survives a reload, reaches the full 7-day retention, and can be asked for a specific
window. Search is over the fetched page (bounded, so it stays instant); the window
is applied server-side, so a 1-hour query is cheaper than a 7-day one.

**The State column answers "is this incident open now".** An event record is immutable
— it carries no state of its own. Open/closed is a property of the **alert**
(the 32-hex record key), which the fold maintains on `event:<key>`, and every frame carries it
onto each row as `alertState` / `alertClosedAt` using one pipelined `HMGET` over the
**distinct** alerts in the page (400 events from 4 incidents costs 4 lookups, not 400).

Three states, and `unknown` is deliberately **not** rendered as closed:

| Chip | Meaning |
|---|---|
| `open` (filled, error colour) | the alert this event belongs to is still firing |
| `closed` (outlined, success colour) | retired; the tooltip says when |
| `unknown` (dimmed) | no alert hash — expired, or the event never opened one |

Colouring `unknown` as `closed` would claim an incident ended when we simply do not
know, which is the same mistake the connection chip's `unknown` state exists to avoid.

A row can read `CLEAR` for severity and `open` for state, and that is correct: the chip
reports the alert's **current** state, not its state when the event happened. A CLEAR
from last Tuesday belongs to an incident that may have re-fired since, and "is it open
now" is the question an operator scanning a log is actually asking.

The chip stays live. An `op: alert` frame with `applied: 3` flips every All events row
for that key to `closed` immediately, and `1`/`2` flips them back to `open` — otherwise
the column would be a snapshot from whenever the tab was queried, and closing an alert
from Open alerts would leave its All events rows claiming `open`.

**Two filters on one row, severity first.** The severity picker leads because it is
the coarse cut an operator makes before typing anything — "only the criticals in the
last 24 hours". Its options are derived from the loaded rows (`eventSeverities`) so a
two-event window does not offer eight severities that match nothing, and the option
labels are the **displayed** ones, because those are the names on the badges.

That last point is the whole rule of the filter: **it matches the label the badge
renders**, `displaySeverity` falling back to the internal name. Matching the stored
`severity` as well looked helpful and was actively wrong — `MAJOR` maps down to
internal `CRITICAL` on ingest, so selecting `CRITICAL` returned rows badged `MAJOR`.
An operator who picks a value and gets rows showing a different one concludes the
filter is broken. One label, one meaning.

The mock had a matching defect: `mkEvent` derived `payload.EVT_Severity` from its
index and then let `overrides.severity` replace the stored severity, so the live
stream emitted `severity:'CRITICAL'` with `EVT_Severity:'MINOR'` — a badge that
contradicted its own row. That made the filter look broken in the harness while it
was correct in production. A fixture that contradicts itself hides the bug it exists
to exercise, which is why the producer value is now derived from the final severity.

**The tab is a query that stays live.** A query alone was a still photograph of a
live board: it answered once, on first open, and the list and its badge then sat at
whatever the count was at that instant. An operator watching the tab saw nothing
move. So a stored event that arrives while the tab is loaded is appended through
`insertAllEvent`, which de-duplicates on **id alone** — deliberately unlike the live
tail's `insertEvent`, which also compares `brokerTs`.

The asymmetry is the whole point, and it follows from the two questions:

| List | Question | An absorbed repeat |
|---|---|---|
| Live events | what *arrived* | a distinct arrival — include it |
| All events | what is *stored* | never written — exclude it |

Storage is one record per CONDITION at `event:<key>`, so a second row for the same key would
be a row the tab's own query could never return. Frames with `applied: 0` are
therefore skipped outright; their ids are new, so no de-duplication rule could have
caught them.

The query reply is still authoritative for its window, but it is **merged** rather
than assigned: it only covers up to `untilMs`, so replacing the list wholesale
discarded anything that landed during the round trip and the list visibly went
backwards a moment after the operator saw it move.

**The list and its badge grow before the tab is ever opened.** The append used to be
gated on `loadedAt`, reasoning that a list of "events since page load" is a partial
answer and the authoritative query would overwrite it anyway. But `loadedAt` stays
`null` until the tab is opened, so the badge sat at **0** and the pane said "No events
in this window" while every other view was visibly busy — measured: Open alerts 4→5
and Live events 76→79 with All events frozen at 0.

A badge reading zero while the system is producing events is simply wrong. A partial
count that becomes exact on first open is not.

**The badge counts the WINDOW, not the page.** `op: allEvents` replies with a separate
`total` — one `ZCOUNT` on the same range — and the badge reads that. Counting the
fetched rows was the same bug in a different guise: the list is capped at
`ALL_EVENTS_LIMIT`, so a full window would have shown the cap, and a refresh on
another tab showed **0** because nothing had been fetched yet. `total` is `null` until
a query answers, because "we have not asked" is not "there are none"; the badge falls
back to the row count only during that first in-flight query.

**The window is fetched once on CONNECT, not on first open.** One bounded query per
connection is a fair price for a badge that is never wrong. `total` is cleared when the
range changes — it described the *previous* window — and decremented on a delete, so it
cannot outlive the rows it counts. A live arrival increments it once a query has
answered, or the badge would freeze at whatever the last fetch said while the list
visibly grew.

**No heading and no count chip in a pane header.** The tab strip sits directly above
and already names the pane, and its badge already carries the count. A heading
restating the selected tab is the one control on screen that tells the operator
nothing, and on a live tab the two counts could disagree for a frame — two numbers
side by side, one of them stale, is worse than one.

This applies to all four panes. **Open alerts** lost its whole header row, because
after the heading and chip were removed there was nothing left in it — an empty 40px
band of vertical space above the data. **Live events** kept its row for the
`N of last 500` chip, which is NOT redundant with the badge: it carries the buffer
LIMIT, the difference between "all of it" and "the newest slice", which matters when
reading a tail. **Closed** and **All events** keep a row for their own controls.

**The refresh button belongs next to the control it re-runs**, not pushed to the far
right. On the Closed tab that is the severity picker: the button re-reads the list
that the picker filters, so they are one group. The `N of M loaded` caption is what
stays at the right edge — it is a status line, not a control, and separating it from
the buttons is what makes the grouping read correctly.

**Two chips joined by a line: connection → database.** They are separate because
they fail separately — the socket can be perfectly healthy while the gateway's reads
fail, which is a stale board behind a green chip. The line between them shows the
COMBINED state (`linkState` in `lib.js`), so "everything is fine" is one glance rather
than two readings:

| Line | Meaning |
|---|---|
| green, pulsing | socket connected **and** database readable |
| red, static | either side has definitely failed |
| grey, static | not known yet — nothing attempted, or no health report received |

Three states, not two, because "not told yet" is neither healthy nor broken.
Rendering unknown as red would cry wolf on every page load; rendering it green would
be a lie. A **dropped socket is red, not grey**: we know the path is down, and dressing
a definite failure up as uncertainty helps nobody.

The pulse runs **only** when green. A slow pulse reads as "this path is live"; pulsing
a failure would add motion at the moment an operator needs to concentrate. It is
disabled entirely under `prefers-reduced-motion`, since it carries no information the
colour and the chips do not already carry.

The connection chip is a MUI `Chip` labelled with the socket state, which has five
values:

| Value | Colour | Meaning |
|---|---|---|
| `idle` | amber outline | before the first connect attempt |
| `connecting` | amber outline | first attempt in flight |
| `connected` | green filled | subscribed; snapshot delivered, live frames flowing |
| `reconnecting` | amber outline | dropped, backoff retry in progress |
| `misconfigured` | red outline | `wsUrl` empty — will never connect |

It was `open` until recently, which read as the **Open alerts** tab directly
underneath it; the value was renamed rather than relabelled, so no comparison is left
referring to a state name that no longer exists.

The chip reports the SOCKET, not the cluster: it can read `connected` while Valkey
is down and the board is stale.

The tab bar is a real `role=tablist` with `aria-selected` and Arrow key
navigation. The active tab is marked by **both** a colour change and a filled
background, because colour alone fails for anyone who cannot separate the two hues.
MUI's `TabIndicator` is **disabled** (`display: none`) rather than kept alongside it:
a 2px underline drawn directly under a rounded fill reads as a rendering seam, and it
is two signals for one state.

The fill's alpha is deliberately low (`.12` light, `.18` dark). A solid primary block
would compete with the severity colours in the table below, which are the actual
signal being scanned; the selected tab only has to be unmistakable, not loud. Hover
gets its own faint tint, and `&.Mui-selected:hover` is set explicitly — otherwise
hovering the current tab washes it toward the unselected hover colour and it looks
like it is being deselected.

MUI marks the selected tab with `.Mui-selected` on the Tab root and applies the label
colour through `.MuiTab-root.Mui-selected`, so the fill lives on `MuiTab.root` rather
than on the `label` node. Styling the label instead centred the background on the
text, not on the clickable area.

The selected tab is persisted (`localStorage['fw:tab']`) so a reload lands where the
operator left off, and an unknown stored value falls back to `alerts` rather than
rendering an empty page.

### 5.6 Live events tab

**What it is for.** It is the raw ARRIVAL feed — one row per delivery, in arrival
order — and it is a diagnostic view, not the working surface. The alert table
answers "what is firing"; the tail answers "what did we actually receive, and
when", which is what you need when the two disagree. Concretely:

- prove the pipeline is alive when no alert changed (producers send `CLEAR` or
  `HARMLESS` continuously, which retire rather than open)
- see an event that was **absorbed** as a duplicate — it moved the alert's Count
  and Last occurrence but created no alert row of its own, so the tail is the only
  place its arrival is visible
- see events that never reached the alert fold at all (`applied: 0`)
- inspect the full record, including fields the tables do not show

**Count is on All events, not on the tail.** It is the **alert's** occurrence rollup —
how many times the condition has been reported — fetched in the same `HMGET` that
carries `state`, so it costs nothing extra. It is NOT a per-event number: the `EVT_*`
schema has no `Count` field and the ingress never adds one, so anything per-event would
render a constant 1. A cell with no alert hash renders `—`, not `0`, because an expired
alert is unknown rather than zero.

**A live arrival must fill Count from the client's own alert map.** Only
`op: allEvents` replies are enriched with `alertCount`; an `op: event` frame carries no
alert state. So a row that arrived live showed `—` for Count while a row for the SAME
alert, fetched by a query, showed the real number — the same alert reading `3` or `—`
depending on how its row got on screen. The render path now falls back to the `alerts`
map the client already maintains from the fold's frames, with server-supplied values
still winning where present (`docs/frontend.md` §5.5).

The two tables therefore have **different column lists** despite sharing a row
component:

| | Columns |
|---|---|
| Live events | Severity · State · First · Last · Summary · enrichment |
| All events | Severity · State · First · Last · **Count** · Summary · enrichment |

The tail is one row per DELIVERY, so an alert's rollup repeated on every row of it would
invite exactly the misreading "three rows, one number, which is which". All events is a
window over stored events, where "how often did this fire" is a real thing to scan.

The old `Count` column this replaces was labelled "Msg count" and read the producer's
own payload field — which does not exist, so it rendered a constant **1**. The dev
harness fabricated `Count: (i % 5) + 1`, which made the dead field look meaningful and
hid the problem for as long as it existed; it now emits no `Count`, so the mock has the
same shape production does.

**The tail is NOT seeded from the alert snapshot.** It used to be: on connect the
gateway sends a `snapshot` of open alerts carrying each one's newest record, and the
client pushed those into the tail so the pane was not empty. The result was exactly
one row of unexplained age after every refresh — the alert's `lastId` record, which
can be hours old, sitting in a feed that otherwise means "arrivals since you
connected". An operator cannot tell whether that row just arrived or was left over,
and there was no way to distinguish it because it carried no marker.

So the tail shows **arrivals only**, and history on connect comes from the real
backfill instead: `subscribe` replays the events in the window since `since`
(15 minutes by default) from the global index. A refresh therefore shows a genuine
time range, and the newest row is a real arrival rather than whatever the fold
happened to leave as `lastId`.

### 5.7 Closed tab

Shows what retired in the last **7 days**, newest closure first. It is a
**query**, not a stream: `{"op":"closed"}` is fetched on demand — when the tab is
first opened, or on `refresh` — because the window changes slowly and a client
that never opens the tab should not pay for a 7-day read
([websocket-server.md](websocket-server.md) §3.5).

| Column | Note |
|---|---|
| Severity | the severity that RETIRED the alert (`CLEAR` / `INFORMATION`), not the one it was firing at. The original lives in the event record, reachable through Information |
| **Closed at** | the timestamp the window is measured from, and the sort key |
| First / Last occurrence, Count | kept, so a closed row still explains how bad it got |
| Summary, Key | as elsewhere |

**No Ack column.** The ack belonged to the episode and the fold deletes it when
the episode ends, so a column of `—` would imply a state that does not exist.

**Filtering is client-side** (`filterClosedAlerts` in `lib.js`, exported so it is
tested directly rather than through a rendered table):

- free text, case-insensitive and trimmed, matched across every visible column
  plus the summary and the enriched fields
- an exact severity select whose options come from the **data** — a fixed list
  would offer dead ends
- both the internal severity and the producer's value are searchable, so searching
  `CLEAR` finds a row whose stored severity is `CLOSE`
- sorting is newest-first *after* filtering, so a filter cannot silently reorder
  the table

`truncated` from the server is surfaced as a notice rather than a silent cut: a
busy week must not look smaller than it is.

**The tab badge counts closures, not loaded rows.** `closedState` carries a
separate `count` that a retirement frame increments immediately, so the badge is
honest the moment something closes. Deriving it from `alerts.length` made it look
frozen until the tab was opened, because the badge was reporting "rows I happen to
have fetched" rather than "closures that have happened". A fetch resets `count` to
what the server actually holds, which also corrects drift and the case where a
`truncated` page caps `alerts`.

#### The badge is seeded on connect, like All events

The badge is rendered on **all four tabs**, so it has to be right after a browser
refresh on Open alerts — but the 7-day row read stays on demand. The two needs are
satisfied separately:

| | when | cost |
|---|---|---|
| the **badge** | a `{"op":"closed","countOnly":true}` on every connect, and on each resync tick | one `ZCOUNT` over `idx:event:closed` |
| the **rows** | on first open of the tab, and on `refresh` | the windowed row read |

The answer is `{"op":"closedCount","count":N,"sinceMs":…}`. It deliberately does
**not** set `loadedAt`: it says how many closures exist, not that their rows were
read, so the tab still fetches on open and still shows its `stale` chip until it
has.

Without this, a refresh on Open alerts left the badge at 0 while storage held
closures, and it only corrected itself once Closed was selected — which reads as
"the closures were lost", not "this number is not loaded yet". The local count is
optimistic (it increments on every retirement frame), so the `ZCOUNT` also
corrects drift **downwards** after a frame is lost across a reconnect.

Because the rows lag the count, the pane says so explicitly with a `stale` chip
when `loadedAt` is null. Without it the chip and the list disagree and both read
as wrong, rather than one reading as out of date.

**There is no count chip in the pane.** It showed `closed.count`, which is exactly
what the tab badge shows — two numbers for one thing, and on a busy board they
disagree for the length of a fetch, so one of them is always behind. The badge is
the live one and it is already visible; `N of M loaded` in the filter row reports
what was actually read, which is the different question.

Two loading states, deliberately distinct: "Nothing has closed in the last 7 days"
is a real answer (the list was fetched), whereas "Open this tab to load" means it
has not been fetched yet. Conflating them would make an unfetched tab look like a
quiet week.

**The auto-load effect depends on the connection status, not just the tab.** On a
reload with this tab already selected the effect runs *before* the socket opens;
without the `status === 'connected'` guard the request was dropped and nothing changed,
so the effect never ran again and the list stayed empty until `refresh` was
pressed.

### 5.8 Pause

The `❚❚ Pause` button freezes the **render**, not the socket. The socket keeps draining into the buffer, so resuming shows everything that arrived while paused instead of silently skipping a window.

- a snapshot is taken on the transition into pause, so the frozen list is exactly what was on screen and does not creep forward
- the button is `aria-pressed` and changes to `▶ Resume` plus a `paused` marker, so a paused view is never mistaken for a quiet stream
- the count in the handle shows the frozen length while paused; on resume it jumps to the true length, which is itself evidence that nothing was dropped

### 5.8.1 Row selection

Both the **Open alerts** and **All events** tables support multi-select, through one
shared hook (`useRowSelection`) rather than two copies. The convention is the
file-manager one:

| Gesture | Effect |
|---|---|
| plain click | select only this row, and move the anchor |
| shift+click | select the range from the anchor to this row |
| ctrl/cmd+click | toggle this row, leaving the rest alone |
| Escape | clear the selection |

The **anchor** is what makes shift-click predictable. Without it, shift-click would
range from whatever was selected last — which, after a shift-range, is the far end of
that range rather than where the operator started.

Two properties worth knowing:

- **Shift ranges resolve against the rendered row order**, not the selection, so
  "everything between these two on screen" is literally what gets selected.
- **Only rows still on screen count.** The selection is filtered against the current
  row keys on every render, so a bulk action cannot fire at a row that has been
  retired, deleted, or filtered out from under the operator.

Right-click respects the selection: inside a multi-row selection the menu acts on
**all** of it and says so (`Delete 3 events`), and outside it the menu targets that
one row. That is the same rule the alert table uses, and it exists because a menu that
silently acts on less than the screen shows as selected is worse than no menu.

The two selections are **independent**. Selecting alerts and selecting events are
different intents, and a single set spanning two id spaces would make every consumer
filter a mixed collection.

**The Live events tab has neither.** No selection, no right-click, no row actions. It
is a diagnostic tail — rows scroll past at stream rate, so a menu opened on one would
usually describe a record that is no longer under the pointer. Every action a live row
could offer is an action on the ALERT or on the stored RECORD, and both are one click
away in Open alerts and All events, where the row is the thing being acted on. Three
action surfaces for one row was three places to keep in step for no gain; the row now
carries no `aria-selected` and no handler at all.

### 5.9 Record inspector

Right-clicking a row in **Open alerts** or **All events** opens a **two-level menu**:

```text
level 1                          level 2
┌──────────────────────┐         ┌──────────────────────┐
│ Acknowledge          │  ──────▶│ ← record key         │
│ Information          │         │ every field, flat    │
└──────────────────────┘         └──────────────────────┘
```

The split exists because the previous panel showed actions and the whole field
dump together: a routine acknowledge sat next to ~40 lines of JSON. Actions are
now one click and the detail is opt-in, and there is room to add actions later
without the menu becoming a wall of text.

`Information` lists **every key/value pair of that record** — including `payload`
keys the table does not show.

| Detail | Behaviour |
|---|---|
| Source record | the untouched wire event (`evt.wire`), not the view event. Otherwise every field would appear twice, once as `payload.CIF` and once as the derived `cif` |
| Nesting | objects and arrays are flattened to dotted / indexed paths (`payload.nested.depth`, `tags[0]`) rather than stringified — the point is to see inside `payload`, not `[object Object]` |
| Timestamps | rendered as `YYYY-MM-DD HH:MM:SS (raw)` — the same clock the tables use, with the original value kept in parentheses. Matched by FIELD NAME (`brokerTs`, `closedAt`, `firstSeen`, `ts`, `FirstOccurrence`, …), not by sniffing values, because a bare number is ambiguous: a count of `1794319171950` and a timestamp of the same value are indistinguishable, and guessing would rewrite a legitimate number. Epoch ms, epoch seconds and ISO strings are all accepted; an unparseable value falls back to the raw string rather than printing `Invalid Date` |
| Absent vs. null | `undefined` keys are omitted (an absent field is noise); `null` is shown, because absence and explicit null are different facts. `0` and `false` are values and are shown |
| Still-compressed fields | labelled `[gzip, N base64 chars]` rather than dumping base64 |
| Cycles | detected with a `WeakSet` and reported as `[circular]`; a malformed payload must never blow the stack on a right-click |
| Size | `620px` wide, `max-height:min(560px, 100vh-1rem)`, from the stylesheet only. The action list is short, so the panel only reaches full height on the Information view — which is the view that needs it: 16 typical fields fit with no scrolling, where the previous `460x320` cap forced a scrollbar through the very values the panel exists to reveal |
| No horizontal scroll | `table-layout:fixed` with a 34% key column and `overflow-wrap:anywhere` on values. Without it the table sized to its longest cell and the panel grew a sideways scrollbar |
| Positioning | clamped to the viewport **using the element's measured size** (`offsetWidth/offsetHeight` after layout), re-run when the view toggles because the Information table is a different height from the action list. The size is deliberately not duplicated in JS — the two copies disagreed when the panel grew, and the stale clamp pushed it off the bottom of the screen |
| Information is intrinsic | the panel always renders its own **Information** row, and caller-supplied actions are filtered for that label. A row that added one anyway produced TWO identical entries, and because the injected one was not wired to `setView('info')` it did nothing when clicked — that exact bug shipped on the Closed tab |
| Dismissal | Escape, click outside, or the ✕ button. The ✕ is present in both levels; the back link returns to the action list |

**Why Information exists instead of wrapping cells.** Cells never wrap (see §4.3),
so a long value is necessarily clipped at a narrow width. The record is therefore
read here, in a panel that can be as tall as it needs, rather than by reflowing the
row and pushing every other column out of alignment.

### 5.10 Acknowledge is a menu action, not a button

The `Ack` column shows *state*; the *action* lives in the right-click menu. Rationale:

- acknowledging is destructive-ish and one-way for the episode, so it should not sit one stray click away in a dense table row
- the menu already opens on every row and already shows the record, so the action sits next to the evidence for it
- it keeps the alert table 7 columns wide instead of 8, which matters at 1 000 events/min

It is available on **both** tables. Every row carries the record key, so a live-event
row and the alert table acknowledge the same object and the two entry points cannot
diverge. The key arrives on the frame rather than being derived in the browser: it is
a hash of device + summary, and a second implementation of that hash is exactly how the
two views would drift ([valkey.md](valkey.md) §2).

When the alert is already acked the menu item is present but disabled and reads `Acked by <name>`, with the timestamp in its tooltip. A disabled item is used rather than removing it, so the menu does not change shape between rows.

An acked row is marked with a left bar and an `acked · <name>` tag. It is deliberately **not** dimmed, struck through or hidden: *acked* is not *resolved*, and a struck-through key on a black FATAL row reads as closed while the condition is still firing.

The alert table resolves the inspector record from the **latest applied event** for that key, so right-clicking an alert shows the enriched fields of the occurrence that put the row there.

## 6. nginx configuration notes

```nginx
# unprivileged image: no user directive, listen 8080
server {
  listen 8080;
  root /usr/share/nginx/html;

  location = /config.json      { add_header Cache-Control "no-store"; }
  location = /index.html       { add_header Cache-Control "no-store"; }
  location /assets/            { add_header Cache-Control "immutable, max-age=31536000"; }
  location /                   { try_files $uri $uri/ /index.html; }
  location = /healthz          { access_log off; return 200 "ok\n"; }
}
```

Never `listen 80` in the image: OpenShift runs with an arbitrary UID and cannot bind < 1024 without a capability grant that the SCC will refuse.

## 7. Probes

| Probe | Path |
|---|---|
| readiness | `GET /healthz` — static 200 |
| liveness | `GET /healthz` |

The probes are intentionally trivial: the frontend has no dependencies, and a dependency-aware probe would restart pods during a gateway outage, which is exactly when the UI must stay up to show the reconnect state.

## 8. CSP and security headers

```nginx
add_header Content-Security-Policy "default-src 'self'; connect-src 'self' wss://ws.fire-watch.example.com; img-src 'self' data:; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'" always;
add_header X-Content-Type-Options "nosniff" always;
add_header Referrer-Policy "no-referrer" always;
```

`connect-src` must list the gateway origin explicitly, otherwise the browser silently blocks the WebSocket.

The CSP has one constraint for theming: `data-theme` is an attribute, not inline style, so it needs no `script-src` or `style-src` relaxation. `style-src 'unsafe-inline'` is already present and is required by React's inline styles, not by the theme.

## 9. Sizing

| Aspect | Value |
|---|---|
| Requests | 20m CPU, 64Mi RAM |
| Limits | 200m CPU, 128Mi RAM |
| HPA | none — static serving is cheap; 3 replicas is the H/A requirement |

## 10. Verification checklist

```bash
kubectl -n fire-watch get route frontend -o jsonpath='{.spec.host}{"\n"}'
curl -sI "https://$(kubectl -n fire-watch get route frontend -o jsonpath='{.spec.host}')/" | head -20
curl -s  "https://$(kubectl -n fire-watch get route frontend -o jsonpath='{.spec.host}')/config.json"

# delete a pod; the app must keep loading from the other two
kubectl -n fire-watch delete pod -l app.kubernetes.io/name=frontend --field-selector status.phase=Running
```

Theme and row-colour preferences survive a pod replacement: they are in `localStorage`, not on the server. Only a user clearing site data loses them.
