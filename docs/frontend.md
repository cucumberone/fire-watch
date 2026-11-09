# Frontend

React single-page app served as static assets, talking to the WebSocket gateway.

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
  "backfillDefaultMinutes": 15
}
```

nginx serves `config.json` with `Cache-Control: no-store`; `index.html` also `no-store`; hashed assets get `immutable, max-age=31536000`.

## 4. WebSocket client requirements

| Requirement | Detail |
|---|---|
| Reconnect | jittered exponential backoff, 0.5s → 30s cap, infinite retries |
| Resume | on reconnect, send `subscribe` with `since` = last received event broker timestamp minus 5s overlap |
| De-duplication (transport) | keyed on event `id` (overlap + at-least-once delivery can duplicate) |
| De-duplication (occurrence) | render `count` from `op: alert` frames; never increment it per received event — the worker coalesces ([event-lifecycle.md](event-lifecycle.md) §5.4) |
| Ordering | sort by broker timestamp, not arrival order and not producer `ts` — cross-worker delivery can invert ([event-lifecycle.md](event-lifecycle.md) §2) |
| `applied` code | **must** be honoured: `0` stale / `1` new occurrence / `2` coalesced / `3` closed. Treating `2` as new causes entry churn; treating `0` as a close causes flicker ([event-lifecycle.md](event-lifecycle.md) §9) |
| Alert state | render from `op: alert` frames and `fw:alert:*` on reconnect; never derive "is it open" or the count from the event list |
| `msgc` decoding | field may be gzip + base64. Decode with the native `DecompressionStream`; on failure render `[undecodable payload]` and keep the rest of the alert usable ([payload-and-compression.md](payload-and-compression.md) §5) |
| `msg` decoding | always plain — leave it alone (it is below the compression threshold by design) |
| Field naming | the wire says `msg` / `msgc`; the UI says **Summary** / **MsgCompleteString**. Renamed once, at the ingest boundary, by `toViewEvent()` in `src/lib.js` (§4.1) |
| Severity display | `FATAL` > `CRITICAL` > `WARNING` > `HARMLESS` > `CLOSE`; `HARMLESS` / `CLOSE` are retirement events and are green, not grey, because "resolved" is a success state |
| Row colouring | opt-in, persisted; whole row tinted by severity with a 3px left bar. Off by default — at 1 000 events/min a fully tinted table is harder to scan |
| Theme | `auto` (follow `prefers-color-scheme`) / `light` / `dark`, persisted in `localStorage`; `auto` is stored as the *absence* of the key so a later OS change still applies |
| Buffering | render at most `N` rows (default 500) in the live table; older windows come from backfill |
| Live tail | a collapsed-by-default bottom drawer, not a second page section. The alert table is the primary signal and owns the screen; the collapsed handle carries the row count so the stream is visibly alive while closed |
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
| Key | First occurrence |
| First occurrence | Last occurrence |
| Last occurrence | Count |
| Count | Summary |
| Summary | CIF, Alarm ID key, Alarm owner, Auto close, Support group |

With ten columns the drawer is horizontally tight, so:

- identifier and timestamp cells are `white-space: nowrap` — a mid-token break reads as `CIF-\n7730`, which is worse than a scrollbar
- headers may still wrap, because a wrapped header costs a line while a wrapped value corrupts the value
- the Summary cell is capped (`min-width:12rem; max-width:26rem`) so a long message cannot inflate the table and force a scrollbar on an otherwise wide window
- the drawer body scrolls horizontally when the viewport is narrow; that is the intended behaviour, not an overflow bug

## 5. UI preferences

Two presentation toggles live in the header. Both are client-side only — they are not sent to the gateway and they do not affect `applied` handling.

| Preference | Values | Default | Storage |
|---|---|---|---|
| Theme | `auto`, `light`, `dark` | `auto` | `localStorage['fw:theme']` |
| Colour rows | on, off | off | `localStorage['fw:rowColor']` |

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

### 5.2 Whole-row colouring

Off by default. When on, every cell of a row gets the severity tint and the first cell gets a 3px inset bar of the same hue. The bar is not decoration — it keeps the severity readable when the table is scrolled horizontally and for anyone who cannot separate the tints.

Two deliberate interactions with the always-on styles:

- retirement rows lose their `opacity:.55`, because dimming would undo the tint that is the point of the mode
- `.dim` cells switch to `color:inherit; opacity:.7`, because the grey type colour has no contrast against a tinted background

Colour is never the only signal: the `sev` badge still carries the severity as text on every row.

### 5.3 Live events drawer

The live tail is a bottom drawer, `max-height:45vh`, closed by default. Reasons:

- at 1 000 events/min the tail is the noisiest thing on screen, and it is a diagnostic view rather than the thing an operator acts on
- keeping it closed by default means the alert table owns the full page height
- the collapsed handle still shows `(n of last 500)`, so a closed drawer is not a dead one

**A real bottom bar, never an overlay.** `position: sticky; bottom: 0` was tried and removed: sticky does not reserve space, it *lifts* the element out of its natural position to keep it visible, so as soon as `<main>` was taller than the viewport the drawer floated up over the alert table and clipped the row beneath it. `padding-bottom` on `<main>` cannot compensate, because the drawer was never in that space.

The page is therefore a fixed-height shell:

| Element | Role |
|---|---|
| `html, body, #root` | `height: 100%` |
| `#root` | `display:flex; flex-direction:column; overflow:hidden` — the viewport-locked shell |
| `main` | `flex:1 1 auto; min-height:0; overflow-y:auto` — takes the remaining height and scrolls internally |
| `.drawer` | `flex:0 0 auto; position:static` — a normal flex item, so it *consumes* layout height |

Because the drawer occupies real space, it cannot cover `<main>` at any scroll position or window size. Verified by measurement: `main.bottom === drawer.top` exactly, with `overlap: false`.

`min-height: 0` on `main` is load-bearing — a flex item's default `min-height: auto` refuses to shrink below its content, which would push the drawer off the bottom of the viewport instead of scrolling.

A partially visible row at the bottom of `main` is the scroll boundary, not the drawer covering it; that row is fully reachable by scrolling. An `inset` box-shadow on `main` marks the edge so the difference is visible rather than ambiguous — it is painted against the scroll container, so it does not scroll away with the content.

| Concern | Handling |
|---|---|
| Overlap | none possible — the drawer is in the flow |
| Share of viewport | closed: handle bar only (~36px). Open: drawer ≈ 44%, `main` ≈ 56% |
| Inner scrolling | `.drawer-body` has `overflow:auto` and `max-height:38vh`, so the tail scrolls inside the drawer rather than growing the page |
| Accessibility | the handle is a real `<button>` with `aria-expanded` and `aria-controls`; the body uses `hidden`, so it leaves the tab order and the accessibility tree when closed |
| Reflow | not persisted — every page load starts closed |

### 5.4 Pause

The `❚❚ Pause` button freezes the **render**, not the socket. The socket keeps draining into the buffer, so resuming shows everything that arrived while paused instead of silently skipping a window.

- a snapshot is taken on the transition into pause, so the frozen list is exactly what was on screen and does not creep forward
- the button is `aria-pressed` and changes to `▶ Resume` plus a `paused` marker, so a paused view is never mistaken for a quiet stream
- the count in the handle shows the frozen length while paused; on resume it jumps to the true length, which is itself evidence that nothing was dropped

### 5.5 Record inspector

Right-clicking a row in either table opens a fixed panel listing **every key/value pair of that record** — including `payload` keys the table does not show.

| Detail | Behaviour |
|---|---|
| Source record | the untouched wire event (`evt.wire`), not the view event. Otherwise every field would appear twice, once as `payload.CIF` and once as the derived `cif` |
| Nesting | objects and arrays are flattened to dotted / indexed paths (`payload.nested.depth`, `tags[0]`) rather than stringified — the point is to see inside `payload`, not `[object Object]` |
| Absent vs. null | `undefined` keys are omitted (an absent field is noise); `null` is shown, because absence and explicit null are different facts. `0` and `false` are values and are shown |
| Still-compressed fields | labelled `[gzip, N base64 chars]` rather than dumping base64 |
| Cycles | detected with a `WeakSet` and reported as `[circular]`; a malformed payload must never blow the stack on a right-click |
| Positioning | coordinates clamped to the viewport so a right-click near an edge cannot open a panel off-screen |
| Dismissal | Escape, click outside, or the ✕ button |

### 5.6 Acknowledge is a menu action, not a button

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
