/**
 * Build with esbuild — no bundler config to keep correct, no framework CLI.
 *
 * Produces:
 *   dist/index.html      shell (no-store, so a deploy is picked up immediately)
 *   dist/app.[hash].js   the bundle (immutable cache)
 *   dist/app.css
 *
 * config.json is NOT built here: it is mounted from a ConfigMap at runtime so
 * one image works in every environment (docs/frontend.md §3).
 */
import { build, context } from 'esbuild';
import { mkdir, writeFile, rm } from 'node:fs/promises';
import { createHash } from 'node:crypto';

const watch = process.argv.includes('--watch');

const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Fire-Watch</title>
<link rel="stylesheet" href="/app.css" />
</head>
<body>
<div id="root"></div>
<script type="module" src="/app.js"></script>
</body>
</html>
`;

const css = `
/* Theme tokens. The absence of html[data-theme] means "follow the OS", so a
   later system change still takes effect; the toggle writes an explicit value.
   Colour is never the only signal: every severity also carries a text badge. */
:root {
  color-scheme: light;
  --fg:#1a1a1a; --dim:#6b7280; --line:#e3e3e3; --bg:#fff;
  --accent:#2563eb; --notice:#b45309; --error:#dc2626;
  --sev-fatal-bg:#000;       --sev-fatal-fg:#fff;
  --sev-critical-bg:#dc2626; --sev-critical-fg:#fff;
  --sev-warning-bg:#f59e0b;  --sev-warning-fg:#1a1a1a;
  --sev-harmless-bg:#16a34a; --sev-harmless-fg:#fff;
  --sev-close-bg:#15803d;    --sev-close-fg:#fff;
  --row-fatal:#000;          --row-fatal-fg:#f5f5f5;
  --row-critical:#fdeaea;    --row-critical-fg:#7f1d1d;
  --row-warning:#fdf3dc;     --row-warning-fg:#5c3d05;
  --row-harmless:#e3f6e8;    --row-harmless-fg:#14532d;
  --row-close:#d9f2e1;       --row-close-fg:#14532d;
  --row-unknown:#f4f4f5;     --row-unknown-fg:#3f3f46;
  --pre-bg:rgba(127,127,127,.12);
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    color-scheme: dark;
    --fg:#e8e8e8; --dim:#9ca3af; --line:#333; --bg:#141414;
    --accent:#60a5fa; --notice:#fbbf24; --error:#f87171;
    --sev-fatal-bg:#000;       --sev-fatal-fg:#fff;
    --sev-critical-bg:#dc2626; --sev-critical-fg:#fff;
    --sev-warning-bg:#f59e0b;  --sev-warning-fg:#1a1a1a;
    --sev-harmless-bg:#16a34a; --sev-harmless-fg:#fff;
    --sev-close-bg:#15803d;    --sev-close-fg:#fff;
    --row-fatal:#000;          --row-fatal-fg:#f5f5f5;
    --row-critical:#381414;    --row-critical-fg:#fecaca;
    --row-warning:#33280c;     --row-warning-fg:#fde68a;
    --row-harmless:#12301e;    --row-harmless-fg:#bbf7d0;
    --row-close:#0f2b1b;       --row-close-fg:#bbf7d0;
    --row-unknown:#27272a;     --row-unknown-fg:#d4d4d8;
    --pre-bg:rgba(127,127,127,.18);
  }
}
:root[data-theme="dark"] {
  color-scheme: dark;
  --fg:#e8e8e8; --dim:#9ca3af; --line:#333; --bg:#141414;
  --accent:#60a5fa; --notice:#fbbf24; --error:#f87171;
  --sev-fatal-bg:#000;       --sev-fatal-fg:#fff;
  --sev-critical-bg:#dc2626; --sev-critical-fg:#fff;
  --sev-warning-bg:#f59e0b;  --sev-warning-fg:#1a1a1a;
  --sev-harmless-bg:#16a34a; --sev-harmless-fg:#fff;
  --sev-close-bg:#15803d;    --sev-close-fg:#fff;
  --row-fatal:#000;          --row-fatal-fg:#f5f5f5;
  --row-critical:#381414;    --row-critical-fg:#fecaca;
  --row-warning:#33280c;     --row-warning-fg:#fde68a;
  --row-harmless:#12301e;    --row-harmless-fg:#bbf7d0;
  --row-close:#0f2b1b;       --row-close-fg:#bbf7d0;
  --row-unknown:#27272a;     --row-unknown-fg:#d4d4d8;
  --pre-bg:rgba(127,127,127,.18);
}
* { box-sizing: border-box; }
body { margin:0; font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
       background:var(--bg); color:var(--fg); }
/* The drawer is in the flow, so no bottom padding is needed to clear it. */
h1 { font-size:1.25rem; margin:0; }
h2 { font-size:1rem; margin:2rem 0 .5rem; font-weight:600; }
header { display:flex; gap:1rem; align-items:center; flex-wrap:wrap;
         border-bottom:1px solid var(--line); padding-bottom:.75rem; }
.controls { margin-left:auto; display:flex; gap:.6rem; align-items:center; font-size:.8rem; }
/* Icon toggles. A real <button> with aria-pressed, not a styled checkbox:
   the native control cannot be dressed up without faking its box, and a fake
   box would lose the keyboard and screen-reader behaviour it had. */
.togglegroup { display:inline-flex; gap:.25rem; padding:.15rem;
               border:1px solid var(--line); border-radius:999px; }
.iconbtn { display:inline-flex; align-items:center; justify-content:center;
           width:1.9rem; height:1.9rem; padding:0; border:0; border-radius:999px;
           background:transparent; color:var(--dim); cursor:pointer;
           transition: background .15s ease, color .15s ease, opacity .15s ease; }
.iconbtn:hover { background:var(--pre-bg); color:var(--fg); }
.iconbtn:focus-visible { outline:2px solid var(--accent); outline-offset:1px; }
/* aria-pressed is the source of truth for the visual on-state, so the two can
   never disagree. Each toggle gets its own hue: two identical blue pills gave
   no clue which control was which at a glance, only that both were on. */
.iconbtn { opacity:.75; }
.iconbtn:hover { opacity:1; }
.iconbtn[aria-pressed="true"] { opacity:1; background:var(--accent); color:#fff; }
.iconbtn-theme[aria-pressed="true"] { background:var(--accent); }
.iconbtn-rows[aria-pressed="true"] { background:var(--sev-warning-bg); color:var(--sev-warning-fg); }
.icon { width:1.05rem; height:1.05rem; display:block; }
table { width:100%; border-collapse:collapse; font-size:.85rem; }
th { text-align:left; font-weight:600; color:var(--dim); border-bottom:1px solid var(--line);
     padding:.4rem .5rem; position:relative; background:var(--bg); }

/* NO CELL EVER WRAPS.
   A wrapped value corrupts the value: "CIF-\n7730" is unreadable, and a time
   split across two lines is worse than a scrollbar. The trade-off is accepted
   deliberately — a narrow viewport scrolls horizontally, and the full text of
   any record is available from the right-click Information panel rather than by
   reflowing it in place. */
td { padding:.4rem .5rem; border-bottom:1px solid var(--line); vertical-align:top;
     white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
/* The expanded detail row is the ONE cell that must not be clipped: the global
   td rule above sets overflow:hidden so values ellipsize instead of wrapping,
   which would cut the disclosure block off at the column width. */
td.detailcell { overflow:visible; white-space:normal; padding:0 .5rem .6rem; }
td.detailcell pre.detail { margin:0; max-width:none; }
/* Headers may still wrap: a wrapped header costs one line, a wrapped value
   corrupts the value. */
th { white-space:normal; }

/* A table whose header stays put while its BODY scrolls.

   This is deliberately NOT position:sticky on the th.

   Sticky failed here, twice over:
   1. .tablewrap needs overflow-x:auto so a no-wrap table can scroll sideways,
      and an element with overflow-x set becomes a scroll container in BOTH
      axes. The sticky header then sticks to that box, which never scrolls
      vertically, so it travelled with the content anyway.
   2. Even with that fixed, sticky is bounded by its containing block: the
      table ends and the header goes with it.

   Splitting header and body into two stacked flex children makes the behaviour
   structural instead of dependent on scroll ancestry: the header is a normal
   block that never scrolls, and only .tbody is a scroll container. */
.tablewrap { display:flex; flex-direction:column; min-height:0; flex:1 1 auto;
             overflow:hidden; }
.thead { overflow:hidden; flex:0 0 auto; }
.tbody { overflow:auto; min-height:0; flex:1 1 auto; scrollbar-gutter:stable; }

/* The chain that gives .tbody a bounded height: main is a flex column, each
   section is a flex column that can shrink, and .tablewrap takes what is left.
   Without the section rules the table wrapper kept its natural (content) height,
   so .tbody had nothing to scroll and the whole section overflowed instead —
   which is what pushed the header out of view before. */
/* Both tables are table-layout:fixed with the same colgroup, so the header and
   body columns line up exactly. */
.tablewrap > div > table { table-layout:fixed; min-width:100%; border-collapse:separate;
                     border-spacing:0; }
/* The header row must not scroll horizontally with the body, so the header and
   body are separate scrollers sharing one horizontal offset — see the
   scroll-sync note below. */
/* The header table does not scroll, so nothing needs to be sticky here. Leaving
   position:sticky on the th made it a containing block whose height:100% children
   sized against the table, not the cell. */
.tablewrap thead { position:static; }
.tablewrap thead th { position:relative; }

/* Column resize handle. A real element, not a pseudo-element, because it needs
   its own hit area and cursor. Sits over the right edge of the header cell so
   the pointer target is 7px wide however narrow the column is.

   The th must be position:relative for the absolutely-positioned handle to be
   bounded by the CELL. It used to be position:sticky, which also establishes a
   containing block — but with height:100% the handle sized against the table
   rather than the header cell and measured 722px tall, so it covered the whole
   body and no longer sat on the column edge. It is static now because the
   header table does not scroll, so sticky bought nothing anyway.

   The handle is drawn INSIDE the cell edge rather than half outside it. With
   right:-3px it extended into the neighbouring th, and because a later sibling
   paints on top, the next header intercepted every pointer event aimed at the
   handle — the drag was unclickable. Keeping it in-bounds means there is no
   overlap to lose. */
.th-wrap { position:relative; }
.colresize { position:absolute; top:0; bottom:0; right:0; width:7px;
             cursor:col-resize; user-select:none; z-index:3; }
.colresize::after { content:''; position:absolute; top:20%; bottom:20%; left:3px;
                    width:1px; background:var(--line); }
.colresize:hover::after, body.resizing .colresize::after { background:var(--accent); width:2px; }
/* While dragging, the cursor must stay col-resize even over cells, and text
   selection must be suppressed or the drag selects the whole table. */
body.resizing { cursor:col-resize; user-select:none; }
body.resizing * { cursor:col-resize !important; }
.colreset { margin-left:.75rem; font-size:.75rem; font-weight:400; }
/* main scrolls, so a sticky header sits over the row that was scrolled to the
   top. Without this, scrolling a row into view (keyboard focus, anchor jump)
   lands it underneath the header where it cannot be clicked. */
tbody tr { scroll-margin-top: 2.2rem; }
tr.retirement td { opacity:.55; }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size:.8rem; }
.nowrap { white-space:nowrap; }
.dim { color:var(--dim); }
.num { text-align:right; font-variant-numeric: tabular-nums; }
.sev { display:inline-block; padding:.1rem .4rem; border-radius:.25rem; font-size:.7rem;
       font-weight:700; letter-spacing:.03em; }
.sev-fatal    { background:var(--sev-fatal-bg);    color:var(--sev-fatal-fg);
                /* FATAL is black, and a black badge on a black Fatal row would
                   lose its outline in colour-rows mode. */
                box-shadow: inset 0 0 0 1px rgba(255,255,255,.45); }
.sev-critical { background:var(--sev-critical-bg); color:var(--sev-critical-fg); }
.sev-warning  { background:var(--sev-warning-bg);  color:var(--sev-warning-fg); }
.sev-harmless { background:var(--sev-harmless-bg); color:var(--sev-harmless-fg); }
.sev-close    { background:var(--sev-close-bg);    color:var(--sev-close-fg); }
/* A row whose severity was never learned: it was incremented by a content
   duplicate that carried no event, so there is nothing to colour it by.
   Without this rule the badge would be unstyled rather than obviously unknown. */
.sev-unknown  { background:var(--row-unknown);     color:var(--row-unknown-fg); }
/* The connection chip is a MUI Chip now, so the .status* rules that used to style
   it are gone. Keeping dead rules is how a later change "fixes" a class that no
   longer exists. */
/* The line between the connection chip and the database chip.

   A 2px rule with a dot at each end, so it reads as a link rather than a divider.
   Its colour is the COMBINED health of the two chips — green only when both are
   healthy, amber when either is not yet known, red when either has failed. */
.healthlink { position:relative; display:inline-block; width:40px; height:3px;
              border-radius:2px; background:var(--dim); flex:0 0 auto;
              /* centred on the chips' vertical mid-line — as a flex item it defaults
                 to stretch/top, which pinned the rule to the top edge of the chips. */
              align-self:center; margin:0 .5rem; }
.healthlink::before, .healthlink::after { content:''; position:absolute; top:50%;
              width:7px; height:7px; border-radius:50%; background:inherit;
              transform:translateY(-50%); }
.healthlink::before { left:0; }
.healthlink::after  { right:0; }
.healthlink-ok   { background:#16a34a; }
.healthlink-bad  { background:#dc2626; }
.healthlink-unknown { background:var(--dim); }

/* Pulse only when everything is healthy.

   A slow, low-contrast pulse reads as "this path is live". Pulsing a FAILURE would
   trade the operator's attention for decoration at exactly the moment they need to
   concentrate, so red and amber are deliberately static.

   Motion is disabled outright under prefers-reduced-motion: an indefinite animation
   is one of the most common triggers for vestibular discomfort, and this one carries
   no information that the colour and the chips do not already carry. */
@media (prefers-reduced-motion: no-preference) {
  .healthlink-ok { animation:healthlink-pulse 2.4s ease-in-out infinite; }
}
@keyframes healthlink-pulse {
  0%, 100% { opacity:1; }
  50%      { opacity:.35; }
}
.notice { font-size:.8rem; color:var(--notice); }
.error { color:var(--error); }
.link { background:none; border:0; color:var(--accent); cursor:pointer; font-size:.75rem;
        text-decoration:underline; margin-left:.5rem; padding:0; }
pre.detail { margin:.25rem 0 0; padding:.6rem; background:var(--pre-bg);
             border-radius:.25rem; font-size:.75rem; overflow-x:auto; white-space:pre-wrap; }

/* Whole-row colouring, opt-in. The 3px left bar keeps the severity visible when
   the row is scrolled horizontally and for anyone who cannot separate the
   tints. Dimming retirement rows is suppressed here: the green tint already
   says "retired", and dimming would undo the point of the mode. */
.color-rows tr.row-fatal    td { background:var(--row-fatal);    color:var(--row-fatal-fg); }
.color-rows tr.row-critical td { background:var(--row-critical); color:var(--row-critical-fg); }
.color-rows tr.row-warning  td { background:var(--row-warning);  color:var(--row-warning-fg); }
.color-rows tr.row-harmless td { background:var(--row-harmless); color:var(--row-harmless-fg); }
.color-rows tr.row-close    td { background:var(--row-close);    color:var(--row-close-fg); }
.color-rows tr.row-unknown  td { background:var(--row-unknown);  color:var(--row-unknown-fg); }
/* FATAL is pure black in both themes, so its bar is red rather than a darker
   shade of black, which would be invisible. */
.color-rows tr.row-fatal    td:first-child { box-shadow: inset 3px 0 0 #dc2626; }
.color-rows tr.row-critical td:first-child { box-shadow: inset 3px 0 0 #dc2626; }
.color-rows tr.row-warning  td:first-child { box-shadow: inset 3px 0 0 #f59e0b; }
.color-rows tr.row-harmless td:first-child { box-shadow: inset 3px 0 0 #16a34a; }
.color-rows tr.row-close    td:first-child { box-shadow: inset 3px 0 0 #15803d; }
.color-rows tr.retirement td { opacity:1; }
.color-rows td .dim { color:inherit; opacity:.7; }

/* Live-events drawer: a real bottom bar, never an overlay.
   position:sticky with bottom:0 looked like it worked, but sticky LIFTS the
   element out of its natural position to keep it visible — so as soon as main
   was taller than the viewport the drawer floated up over the alert table and
   clipped the row underneath it. Padding on main cannot compensate, because the
   drawer was never in that space to begin with.

   Instead the page is a fixed-height shell: #root is a flex column locked to
   the viewport, main takes the remaining space and scrolls internally, and the
   drawer sits after it as a normal flex item. The drawer therefore consumes
   layout height, so it cannot cover main at any scroll position, at any window
   size. */
html, body, #root { height:100%; }
/* align-items:stretch is the default, but it is stated here because the whole
   two-table layout depends on it: main and the drawer are siblings in this flex
   column and must have the same width. Without it a shrink-to-fit main ended up
   narrower than the drawer and the two tables stopped lining up. */
#root { display:flex; flex-direction:column; align-items:stretch; overflow:hidden; }
/* Shell layout now comes from the MUI Box in app.jsx, so the old main block
   is gone. It set overflow:hidden on a fixed-height column, which is what
   produced the stray horizontal scrollbar once the content moved into panes. */
/* ---------------------------------------------------------------- tab bar

   Replaces the old bottom drawer. The drawer competed with the alert table for
   vertical space and could only be "open" or "invisible"; three peers can all
   exist without any of them being hidden. The bar is a real tablist (role=tab,
   aria-selected, arrow keys), so it is usable without a mouse. */
.tabbar { display:flex; gap:.25rem; padding:0 1.5rem; flex:0 0 auto;
          border-bottom:1px solid var(--line); }
.tab { font:inherit; font-size:.82rem; font-weight:600; cursor:pointer;
       background:none; border:0; border-bottom:2px solid transparent;
       color:var(--dim); padding:.5rem .7rem; margin-bottom:-1px; }
.tab:hover { color:var(--fg); }
.tab:focus-visible { outline:2px solid var(--accent); outline-offset:-2px; }
/* The active tab is marked by BOTH a colour change and an underline. Colour alone
   fails for anyone who cannot separate the two hues, and an underline alone is
   easy to miss on a dense screen. */
.tab-active { color:var(--fg); border-bottom-color:var(--accent); }
.tab .dim { font-weight:400; }

/* The pane body. Replaces .drawer-body: it takes the remaining height and owns
   the scroll, so a table can be as tall as the viewport allows. */
.panelbody { flex:1 1 auto; min-height:0; overflow:hidden; padding:0 1.5rem .5rem;
             display:flex; flex-direction:column; scrollbar-gutter:stable; }

/* Actions that belong to the pane heading (reset columns, pause, refresh) rather
   than to a row. Kept in the h2 so they never shift the table down the page. */
.h2actions { display:inline-flex; align-items:center; gap:.6rem; margin-left:auto; }

/* Closed-tab filters. One row above the table: a free-text box that matches every
   visible column, plus a severity select whose options come from the data. */
.closedfilters { display:flex; align-items:center; gap:.5rem; margin:.1rem 0 .45rem;
                 flex-wrap:wrap; flex:0 0 auto; }
.filterinput { font:inherit; font-size:.78rem; padding:.3rem .5rem; min-width:16rem;
               background:var(--pre-bg); color:var(--fg);
               border:1px solid var(--line); border-radius:.25rem; }
.filterinput:focus-visible { outline:2px solid var(--accent); outline-offset:1px; }
.filterselect { font:inherit; font-size:.78rem; padding:.3rem .4rem;
                background:var(--pre-bg); color:var(--fg);
                border:1px solid var(--line); border-radius:.25rem; }
/* The live table is inside .tablewrap now, so the old per-column caps are gone:
   widths are user-controlled and nothing wraps (see the table rules above). */

/* Selection. A left bar plus a tint, matching the ack treatment so the two
   states read as the same kind of marker rather than competing highlights.
   Deliberately NOT a heavier background than the severity colours: on a
   colour-rowed board a solid selection fill would hide the severity, which is
   the one thing the row exists to communicate. */
tr.row-selected td { background:var(--pre-bg); }
tr.row-selected td:first-child { box-shadow: inset 3px 0 0 var(--accent); }
.color-rows tr.row-selected td { background:rgba(127,127,127,.25); }
/* Rows are a selection surface, not a document.
   Without this, shift-click also runs the browser's text-selection gesture and
   paints the cell text with the native highlight — which on a dark board looks
   exactly like the row text has been selected for copying, and competes with the
   selection colour so it is unclear what is selected.
   Text stays copyable from the right-click Information panel, which is where a
   cell's full value is read anyway. */
.tbody { user-select:none; -webkit-user-select:none; }
tbody tr { cursor:default; }

/* Acknowledgement. Acked rows stay visible — silencing an alert is not the
   same as resolving it, and hiding it would let real trouble disappear. The row
   is marked, not removed, so an operator can still see and act on it. */
.ackcell { text-align:right; white-space:nowrap; }
.ack { font:inherit; font-size:.72rem; cursor:pointer; padding:.2rem .6rem;
       border-radius:.25rem; border:1px solid var(--line);
       background:var(--bg); color:var(--fg); white-space:nowrap; }
.ack:hover { background:var(--pre-bg); border-color:var(--accent); color:var(--accent); }
.ack:focus-visible { outline:2px solid var(--accent); outline-offset:1px; }
.acked-tag { display:inline-block; font-size:.7rem; font-weight:600;
             padding:.15rem .5rem; border-radius:999px;
             background:var(--row-unknown); color:var(--row-unknown-fg);
             max-width:11rem; overflow:hidden; text-overflow:ellipsis;
             vertical-align:middle; }
/* A left bar and a tag mark the acked row. Deliberately NO dimming and NO
   strikethrough: "acked" is not "resolved", and a struck-through key on a black
   FATAL row reads as closed while the condition is still firing. The row keeps
   full contrast so it still reads as an active problem. */
tr.acked td:first-child { box-shadow: inset 3px 0 0 var(--dim); }
/* Pause lives in the pane heading now, so it only needs its own styling. */
.pause { flex:0 0 auto; font:inherit; font-size:.75rem; cursor:pointer;
         padding:.25rem .7rem; border-radius:.25rem; white-space:nowrap;
         border:1px solid var(--line); background:var(--bg); color:var(--fg); }
.pause:hover { background:var(--pre-bg); }
.pause-on { background:var(--sev-warning-bg); color:var(--sev-warning-fg);
            border-color:transparent; font-weight:600; }
.hint { font-size:.72rem; margin:-.25rem 0 .5rem; }

/* Right-click record inspector. Fixed, so it is never clipped by a table's
   scroll container; the click coordinates are clamped in JS to keep it on
   screen. */
.inspector { position:fixed; z-index:50; width:620px; max-width:calc(100vw - 1rem);
             max-height:min(560px, calc(100vh - 1rem)); overflow:auto;
             background:var(--bg); color:var(--fg);
             border:1px solid var(--line); border-radius:.375rem;
             box-shadow:0 .5rem 1.5rem rgba(0,0,0,.28); font-size:.78rem; }
.inspector-head { display:flex; align-items:center; justify-content:space-between;
                  gap:.5rem; position:sticky; top:0; background:var(--bg);
                  padding:.45rem .6rem; border-bottom:1px solid var(--line);
                  font-weight:600; }
.inspector-head .link { margin-left:auto; text-decoration:none; font-size:.85rem; }
/* The back link replaces the title in the Information view, so it must NOT be
   pushed to the right with the close button. */
.inspector-head .inspector-back { margin-left:0; margin-right:auto;
                                  text-decoration:none; font-size:.85rem;
                                  overflow:hidden; text-overflow:ellipsis; }
/* table-layout:fixed plus a percentage split keeps a long value (a gzipped
   detail body, a full URL) INSIDE the panel. Without it the table sized to its
   longest cell and the panel grew a horizontal scrollbar — the value the
   operator opened the panel to read was the one forcing them to scroll. */
.inspector-table { width:100%; border-collapse:collapse; table-layout:fixed; }
.inspector-table th { text-align:left; font-weight:600; color:var(--dim);
                      padding:.25rem .6rem; width:34%; vertical-align:top;
                      border-bottom:1px solid var(--line); background:transparent;
                      position:static; word-break:break-word; }
.inspector-table td { padding:.25rem .6rem; border-bottom:1px solid var(--line);
                      word-break:break-word; overflow-wrap:anywhere; }
.inspector-table tr:last-child th, .inspector-table tr:last-child td { border-bottom:0; }
/* Action row. Deliberately at the top of the menu: it is what the operator
   right-clicked for, and burying it under 20 field rows would be hostile. */
.inspector-actions { display:flex; flex-direction:column; gap:.25rem;
                     padding:.4rem .6rem; border-bottom:1px solid var(--line); }
.inspector-action { font:inherit; font-size:.78rem; text-align:left; cursor:pointer;
                    padding:.3rem .5rem; border-radius:.25rem;
                    border:1px solid var(--line); background:var(--bg); color:var(--fg); }
.inspector-action:hover:not(:disabled) { background:var(--pre-bg);
                                         border-color:var(--accent); color:var(--accent); }
.inspector-action:focus-visible { outline:2px solid var(--accent); outline-offset:1px; }
.inspector-action:disabled { cursor:default; opacity:.7; color:var(--dim); }
/* Destructive actions. Red text and a red border, because a permanent delete
   sitting one row under Acknowledge must not look like it belongs to the same
   class of action. The colour is the only warning the item gets — the modal
   confirm is what actually stops an accident. */
.inspector-danger { color:#ef4444; border-color:rgba(239,68,68,.5); }
.inspector-danger:hover:not(:disabled) { background:rgba(239,68,68,.12);
                                         border-color:#ef4444; color:#ef4444; }
.inspector-hint { position:sticky; bottom:0; background:var(--bg);
                  padding:.25rem .6rem; border-top:1px solid var(--line);
                  font-size:.68rem; color:var(--dim); }
/* A render error inside one pane. Deliberately looks like a fault report rather
   than an empty table: an empty pane would read as "nothing is happening", which
   during an incident is the most dangerous possible message. */
.pane-error { margin:.5rem 0; padding:.6rem .8rem; border-radius:.3rem;
              border:1px solid rgba(239,68,68,.5);
              background:rgba(239,68,68,.1); font-size:.8rem; line-height:1.5; }
.pane-error strong { color:#ef4444; }
`;

async function main() {
  await rm('dist', { recursive: true, force: true });
  await mkdir('dist', { recursive: true });

  const result = await build({
    entryPoints: ['src/app.jsx'],
    bundle: true,
    minify: !watch,
    sourcemap: watch,
    format: 'esm',
    target: ['es2020'],
    outfile: 'dist/app.js',
    jsx: 'automatic',
    loader: { '.js': 'jsx' },
    define: { 'process.env.NODE_ENV': watch ? '"development"' : '"production"' },
    logLevel: 'info',
    metafile: true,
  });

  // Fingerprint the bundle so assets can be cached immutably while index.html
  // stays no-store. Without this a deploy can serve a stale bundle next to a
  // fresh shell.
  const { readFile } = await import('node:fs/promises');
  const js = await readFile('dist/app.js');
  const hash = createHash('sha256').update(js).digest('hex').slice(0, 12);
  await writeFile(`dist/app.${hash}.js`, js);
  await rm('dist/app.js');
  await writeFile('dist/app.css', css.trim() + '\n');
  await writeFile(
    'dist/index.html',
    html.replace('/app.js', `/app.${hash}.js`),
  );

  if (Object.keys(result.metafile?.outputs || {}).length === 0) {
    throw new Error('esbuild produced no output');
  }
  console.log(`built dist/index.html + dist/app.${hash}.js`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
