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

The message contract, Dragonfly and the audit WAL all use `payload.msg` and `payload.msgc` ([payload-and-compression.md](payload-and-compression.md) §1). Those names do not belong on a screen: `msgc` decodes to nothing useful to an operator, and calling the field "Message" collides with the event as a whole.

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

### 4.2 Occurrence fields and worker enrichment

The UI shows four fields that the workers add to `payload` before the event reaches Dragonfly. All are optional — an unenriched event must still render.

| Wire key(s) | UI column | Notes |
|---|---|---|
| `FirstOccurrence`, `firstOccurrence`, `first_occurrence` | First occurrence | epoch ms or ISO 8601 |
| `LastOccurrence`, `lastOccurrence`, `last_occurrence` | Last occurrence | falls back to the event's broker timestamp |
| `Count`, `count` | Count | falls back to `1`; a zero, negative or non-numeric value is rejected rather than rendered as `NaN` |
| `CIF` | CIF | acronym, arrives uppercase |
| `AlarmIDKey` | Alarm ID key | `ID` stays uppercase; all four other casings accepted |
| `AlarmOwner` | Alarm owner | |
| `AutoClose` | Auto close | boolean renders as `yes` / `no` |
| `SupportGroupName` | Support group | |

Accepted spellings are listed explicitly rather than matched case-insensitively: a genuine unrelated `cif` must never be mistaken for the enriched one. `AlarmIDKey` accepts `AlarmIDKey`, `AlarmIdKey`, `alarmIDKey`, `alarmIdKey` and `alarm_id_key`, because `ID` is an acronym upstream tools spell inconsistently.

**Fallback rule.** Without enrichment, a single raw event is its own first *and* last occurrence with `count: 1`. That is the honest answer for one event; extrapolating a wider window would be a guess. `first` and `last` therefore being equal is expected and correct for an unenriched stream — it does not indicate a bug.

### 4.3 Column order

Both tables lead with identity and status, then time, then volume, then the human-readable text, then enrichment. Keeping the two tables in the same order means an operator's eye lands in the same place after switching views.

| Open alerts | Live events |
|---|---|
| Severity | Severity |
| First occurrence | First occurrence |
| Last occurrence | Last occurrence |
| Count | Count |
| Summary | Summary |
| Ack | CIF, Alarm ID key, Alarm owner, Auto close, Support group |
| **Key** | |

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
`localStorage` (`fw:colw:alerts`, `fw:colw:events`) and a **reset columns** link
restores the defaults.

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
`fw:colw:events`, `fw:colw:closed` — §4).

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
| **Colour** | internal `severity` | the five colour classes are defined for these |
| **Text** | `payload.EVT_Severity` | what the producer actually reported |

`toViewEvent` puts the producer value on `displaySeverity`, falling back to the
internal one for a record that carries no `EVT_*` payload (a legacy or internal
producer). A `CLEAR` therefore renders as a green **CLEAR** badge on a dimmed
retirement row: colour and styling still come from the internal `CLOSE`, so
nothing about the retirement behaviour changes.

Deliberately **not** renamed in the data: `severity` on the wire, in the fold and
in the alert hash stays internal. Only the label an operator reads is translated —
renaming the stored value would break every comparison in `alert-fold.lua` and
`SEVERITY_RANK`.

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

### 5.5 Three tabs: Open alerts, Live events, Closed

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

**All events vs Live events** is the distinction worth stating, because they look
alike: the live tail is an in-memory buffer of **arrivals since this page loaded**,
which dies on reload and holds at most 500 rows. All events reads **storage**, so it
survives a reload, reaches the full 7-day retention, and can be asked for a specific
window. Search is over the fetched page (bounded, so it stays instant); the window
is applied server-side, so a 1-hour query is cheaper than a 7-day one.

The tab bar is a real `role=tablist` with `aria-selected` and Arrow key
navigation. The active tab is marked by **both** a colour change and an underline,
because colour alone fails for anyone who cannot separate the two hues and an
underline alone is easy to miss. The selected tab is persisted
(`localStorage['fw:tab']`) so a reload lands where the operator left off, and an
unknown stored value falls back to `alerts` rather than rendering an empty page.

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

Because the rows lag the count, the pane says so explicitly with a `stale` chip
when `loadedAt` is null. Without it the chip and the list disagree and both read
as wrong, rather than one reading as out of date.

Two loading states, deliberately distinct: "Nothing has closed in the last 7 days"
is a real answer (the list was fetched), whereas "Open this tab to load" means it
has not been fetched yet. Conflating them would make an unfetched tab look like a
quiet week.

**The auto-load effect depends on the connection status, not just the tab.** On a
reload with this tab already selected the effect runs *before* the socket opens;
without the `status === 'open'` guard the request was dropped and nothing changed,
so the effect never ran again and the list stayed empty until `refresh` was
pressed.

### 5.8 Pause

The `❚❚ Pause` button freezes the **render**, not the socket. The socket keeps draining into the buffer, so resuming shows everything that arrived while paused instead of silently skipping a window.

- a snapshot is taken on the transition into pause, so the frozen list is exactly what was on screen and does not creep forward
- the button is `aria-pressed` and changes to `▶ Resume` plus a `paused` marker, so a paused view is never mistaken for a quiet stream
- the count in the handle shows the frozen length while paused; on resume it jumps to the true length, which is itself evidence that nothing was dropped

### 5.9 Record inspector

Right-clicking a row in either table opens a **two-level menu**:

```text
level 1                          level 2
┌──────────────────────┐         ┌──────────────────────┐
│ Acknowledge          │  ──────▶│ ← source:type        │
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

It is available on **both** tables. A live-event row acknowledges `source:type` — the same key the alert table uses — so the two entry points are the same action and cannot diverge.

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
