/**
 * React UI.
 *
 * Responsibilities and their sources:
 *   - reconnect with jittered backoff      docs/frontend.md §4
 *   - honour `applied` codes               docs/event-lifecycle.md §9
 *   - decode `MsgCompleteString` in the browser   docs/payload-and-compression.md §5
 *   - sort by broker timestamp             docs/event-lifecycle.md §2
 */
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import {
  ThemeProvider, createTheme, CssBaseline,
  AppBar, Toolbar, Tabs, Tab, TextField, MenuItem, Select, FormControl,
  InputLabel, Button, IconButton, Tooltip, Chip, Box, Stack, Typography,
  Paper, Alert as MuiAlert, CircularProgress,
} from '@mui/material';
import {
  DarkMode as DarkModeIcon,
  LightMode as LightModeIcon,
  Palette as PaletteIcon,
  Refresh as RefreshIcon,
  Pause as PauseIcon,
  PlayArrow as PlayArrowIcon,
} from '@mui/icons-material';
import {
  resolveWsUrl,
  decodePayload,
  toViewEvent,
  applyAlert,
  insertEvent,
  backoffDelay,
  isRetirement,
  resolveTheme,
  severityRowClass,
  colourSeverity,
  recordEntries,
  formatClock,
  ENRICHED_FIELDS,
  SEVERITY_RANK,
  isUnsupportedOpError,
  filterEvents,
  eventSeverities,
  tabBadgeCount,
  linkState,
  insertAllEvent,
  allEventRowKey,
} from './lib.js';

const EVENT_LIMIT = 500;
// Matches the gateway's MAX_ALL_EVENTS_LIMIT, so a list grown by live arrivals is
// bounded the same way a query's reply is.
const ALL_EVENTS_LIMIT = 1000;
const MSG_MAX_BYTES = 1048576;
// One `op: alerts` frame asks about this many keys. Matches the gateway's
// FW_MAX_ALERT_QUERY limit so a large alert table degrades to "acks refresh a
// little later" instead of erroring (docs/acknowledge.md §5).
const MAX_ACK_KEYS = 200;

/**
 * Longest journal note, in characters.
 *
 * Mirrors `MAX_JOURNAL_LEN` in the gateway. The server enforces it for real; this
 * copy exists so the field can stop the keystroke and show a counter instead of
 * letting an operator type 3000 characters and then lose the tail on save.
 */
const JOURNAL_MAX_CHARS = 1024;

/**
 * How many frames a bulk action may send per second.
 *
 * The gateway rejects a connection sending more than `FW_MAX_FRAMES_PER_SEC`
 * (default 10) inbound frames in a rolling second, and the limiter counts
 * EVERY frame on the socket — including the ack-state refresh and heartbeat.
 * Staying at 8 leaves headroom so a bulk action cannot starve them and trigger
 * `rate_limited: too many frames`.
 */
const FRAME_BUDGET_PER_SEC = 8;

// Column labels, defined once so the header, the colgroup and the resize state
// cannot drift out of order. Order is documented in docs/frontend.md §4.3.
const ALERT_COLS = ['Severity', 'First occurrence', 'Last occurrence', 'Count', 'Summary', 'Ack', 'Key'];

// The enriched columns are fixed-width labels, so only the leading four are
// user-resizable; ENRICHED_FIELDS is appended after them.
//
// No Count column. There used to be one labelled "Msg count", carrying the
// producer's own `Count` payload field — a per-MESSAGE value that no producer in
// this system sends, so it rendered a constant 1 on every row. The number an
// operator cares about is the alert's occurrence rollup, which lives on the alert
// hash and is the alert table's Count. A column that always reads 1 is noise that
// invites the reader to look for meaning in it.
// Live events and All events share a row component but NOT a column list.
//
// All events gets a Count column and Live events does not, because the two answer
// different questions: the tail is "what arrived", one row per delivery, so a count
// there would repeat the alert's rollup on every row. All events is "what happened in
// this window", where "how many times did this fire" is a real thing to scan.
const EVENT_COLS = ['Severity', 'State', 'First occurrence', 'Last occurrence', 'Summary'];
const ALL_EVENT_COLS = ['Severity', 'State', 'First occurrence', 'Last occurrence', 'Count', 'Summary'];

// The History tab: one row per DELIVERY, newest first.
//
// No State column, and no Count. An audit row describes an ARRIVAL, not a
// condition: `state` belongs to the record (which may have moved on since), and a
// per-arrival count would be a constant 1.
//
// No `Result` column either, and that is the point of writing the fork BEFORE the
// fold: at the moment the row is written no verdict exists, so the trail records
// what the broker handed us rather than what the dedup decided. A `CLOSE` that the
// watermark rejects still appears here — which is exactly how "did it arrive twice"
// becomes answerable.
const HISTORY_COLS = ['Severity', 'Received', 'Summary', 'Key'];
const HISTORY_DEFAULT_W = [96, 172, 380, 240];

/**
 * Default widths, in px, for every real column.
 *
 * Every column needs an explicit width, not just the ones the operator has
 * dragged. With `table-layout: fixed` and `width: 100%`, an UNSIZED column is
 * simply "whatever is left over" — so dragging one column grew it and silently
 * shrank all the others (measured: they collapsed to 42px, below the 48px floor).
 * Sizing them all makes a drag change exactly one column.
 *
 * A trailing spacer column soaks up any leftover space, so the table still fills
 * the pane when the columns are narrower than it, and overflows (scrolls) when
 * they are wider. Without the spacer, `width: 100%` redistributes again.
 */
const ALERT_DEFAULT_W = [96, 172, 172, 64, 340, 88, 240];
const EVENT_DEFAULT_W = [96, 84, 172, 172, 340];
const ALL_EVENT_DEFAULT_W = [96, 84, 172, 172, 64, 340];


// ------------------------------------------------------------ UI preferences
// Persisted in localStorage so a reload keeps them. `auto` is stored as the
// ABSENCE of a key, so a later OS change still takes effect.
const THEME_KEY = 'fw:theme';
const ROW_COLOR_KEY = 'fw:rowColor';
// Who is at the keyboard, used to sign journal notes. A preference, not a login.
const OPERATOR_KEY = 'fw:operator';
// Which tab was last open. A workspace preference: an operator who works from
// Live events should not be reset to Alerts every reload.
const TAB_KEY = 'fw:tab';
const TABS = [
  { id: 'alerts', label: 'Open alerts' },
  { id: 'events', label: 'Live events' },
  { id: 'all', label: 'All events' },
  { id: 'history', label: 'History' },
];

/**
 * Time ranges offered by All Events.
 *
 * Mirrors `ALL_EVENTS_RANGES` in the gateway. Presets rather than free pickers:
 * the question is "what happened in the last N", and a preset is one click with no
 * timezone ambiguity. The window is applied server-side, so a short range is
 * genuinely cheaper.
 */
const ALL_RANGES = [
  { id: '1h', label: 'last hour' },
  { id: '6h', label: 'last 6 hours' },
  { id: '24h', label: 'last 24 hours' },
  { id: '7d', label: 'last 7 days' },
];

/**
 * Time ranges offered by History.
 *
 * Mirrors `HISTORY_RANGES` in the gateway and STOPS AT THREE DAYS, which is the
 * audit retention (`FW_HISTORY_TTL`). Offering a longer window would promise rows
 * that cannot exist, and the badge would then report fewer than the label says for
 * a reason the operator cannot see.
 */
const HISTORY_RANGES = [
  { id: '15m', label: 'last 15 minutes' },
  { id: '1h', label: 'last hour' },
  { id: '6h', label: 'last 6 hours' },
  { id: '24h', label: 'last 24 hours' },
  { id: '3d', label: 'last 3 days' },
];

/**
 * MUI theme.
 *
 * MUI replaces the hand-rolled chrome — header, tab strip, filters, buttons — but
 * NOT the tables. The tables carry behaviour MUI's DataGrid does not provide out
 * of the box (drag-to-resize columns with per-table persistence, a header that
 * cannot scroll away, shift/ctrl multi-select, and two stacked tables sharing one
 * scroll offset), and re-implementing that on DataGrid's API would change how the
 * board behaves. So the tables stay custom and the surface around them is MUI.
 *
 * Built per render from the resolved mode rather than once at module scope, so the
 * existing light/dark/auto preference keeps working: `auto` follows
 * `prefers-color-scheme` and resolves to one of these two palettes.
 *
 * The density is deliberately tight. This is an operations board read at a glance
 * during an incident, not a content site: a 56px app bar and 48px rows would fit
 * half the alerts on the same screen.
 */
function buildTheme(mode) {
  const dark = mode === 'dark';
  return createTheme({
    palette: {
      mode,
      primary: { main: dark ? '#60a5fa' : '#2563eb' },
      secondary: { main: dark ? '#c084fc' : '#7c3aed' },
      error: { main: dark ? '#f87171' : '#dc2626' },
      warning: { main: dark ? '#fbbf24' : '#d97706' },
      success: { main: dark ? '#4ade80' : '#16a34a' },
      background: {
        default: dark ? '#0d1117' : '#ffffff',
        paper: dark ? '#161b22' : '#ffffff',
      },
      divider: dark ? 'rgba(240,246,252,.12)' : 'rgba(0,0,0,.12)',
      text: {
        primary: dark ? '#e6edf3' : '#1f2328',
        secondary: dark ? '#8b949e' : '#59636e',
      },
    },
    shape: { borderRadius: 6 },
    typography: {
      // System stack, not Roboto: no extra font request, and the numerals align
      // with the monospace columns the tables already use.
      fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif',
      fontSize: 13,
      h1: { fontSize: '1.05rem', fontWeight: 700, letterSpacing: '.01em' },
      h2: { fontSize: '.92rem', fontWeight: 600 },
      button: { textTransform: 'none', fontWeight: 600 },
    },
    components: {
      MuiCssBaseline: {
        styleOverrides: { body: { overflow: 'hidden' } },
      },
      MuiAppBar: {
        defaultProps: { elevation: 0, color: 'default' },
        styleOverrides: {
          root: {
            // No border on the AppBar itself: the tab strip's band IS the
            // separator (see MuiTabs below). Two lines with a gap between them
            // read as an accident, not as a frame.
            backgroundImage: 'none',
            // A tight column so the band can sit flush under the tab strip.
            padding: 0,
          },
        },
      },
      MuiToolbar: {
        styleOverrides: {
          root: { minHeight: 48, gap: 12, paddingInline: 16, width: '100%' },
        },
      },
      MuiTab: {
        styleOverrides: {
          root: {
            minHeight: 40, textTransform: 'none', fontWeight: 600,
            fontSize: '.85rem', paddingInline: 14,
            // A rounded FILL for the selected tab instead of relying on the 2px
            // underline alone. The underline is a 2px mark at the very bottom edge
            // of a 40px bar, which is easy to lose at a glance across a dark
            // screen — the whole point of this strip is knowing which pane you
            // are looking at during an incident.
            //
            // The alpha is deliberately low: a solid primary block would compete
            // with the severity colours in the table below, which are the actual
            // signal an operator is scanning for. The fill only has to be
            // distinguishable, not loud.
            borderRadius: 6,
            marginInline: 2,
            transition: 'background-color 120ms ease, color 120ms ease',
            '&:hover': {
              backgroundColor: dark ? 'rgba(240,246,252,.06)' : 'rgba(0,0,0,.04)',
            },
            '&.Mui-selected': {
              color: dark ? '#60a5fa' : '#2563eb',
              backgroundColor: dark ? 'rgba(96,165,250,.18)' : 'rgba(37,99,235,.12)',
            },
            // Hovering an already-selected tab must not lighten it toward the
            // unselected hover colour, which would make the current pane look like
            // it is being deselected.
            '&.Mui-selected:hover': {
              backgroundColor: dark ? 'rgba(96,165,250,.24)' : 'rgba(37,99,235,.18)',
            },
          },
        },
      },
      MuiTabs: {
        styleOverrides: {
          // width:100% because a scrollable Tabs strip shrink-wraps its content;
          // without it the bar (and the Toolbar above it) collapse to the width
          // of the tab labels and sit centred in the AppBar.
          root: { minHeight: 40, width: '100%' },
          // The indicator is HIDDEN because the selected tab now carries a filled
          // background. Keeping both would draw a 2px underline directly beneath a
          // rounded fill, which reads as a rendering seam rather than as emphasis —
          // two signals for the same state, one of them ugly.
          indicator: { display: 'none' },
          // `list` carries the band, not `root`: root is the flex COLUMN that also
          // holds the scroller's overflow, and a border there is drawn over the
          // scroll area. The slot was renamed from `flexContainer` in v9 — styling
          // `flexContainer` silently does nothing, the same class of silent no-op
          // as the Stack `alignItems` prop. The band is a 6px dark-green rule that
          // replaces BOTH the old tab underline and the AppBar border, so there is
          // exactly one separator between the tabs and the data.
          //
          // `paddingBottom` is what detaches the band from the selected tab. A tab
          // fills the list's content box, so with no padding its rounded green fill
          // sat directly on the band and the two colours ran together as one thick
          // block. 10px, not 6: at 6 the gap was the same thickness as the band, so
          // the eye read the pair as one striped rule rather than a fill and a
          // separate separator.
          list: {
            paddingBottom: 10,
            borderBottom: '6px solid var(--band)',
          },
        },
      },
      MuiButton: { defaultProps: { size: 'small', disableElevation: true } },
      MuiTextField: { defaultProps: { size: 'small', margin: 'dense' } },
      MuiFormControl: { defaultProps: { size: 'small', margin: 'dense' } },
      MuiChip: { styleOverrides: { root: { height: 22, fontSize: '.72rem' } } },
    },
  });
}

function readPref(key, fallback) {
  try {
    return window.localStorage.getItem(key) ?? fallback;
  } catch {
    return fallback;                  // Safari private mode throws on access
  }
}

function writePref(key, value) {
  try {
    if (value === null) window.localStorage.removeItem(key);
    else window.localStorage.setItem(key, value);
  } catch {
    // Preferences are a convenience; never fail a render over storage.
  }
}

const DARK_QUERY = '(prefers-color-scheme: dark)';

function useSystemDark() {
  const [dark, setDark] = useState(() => window.matchMedia?.(DARK_QUERY).matches ?? false);
  useEffect(() => {
    const mq = window.matchMedia?.(DARK_QUERY);
    if (!mq?.addEventListener) return undefined;
    const onChange = (e) => setDark(e.matches);
    mq.addEventListener('change', onChange);
    return () => mq.removeEventListener('change', onChange);
  }, []);
  return dark;
}

function usePreferences() {
  const systemDark = useSystemDark();
  const [themePreference, setThemePreference] = useState(() => {
    const stored = readPref(THEME_KEY, 'auto');
    return stored === 'dark' || stored === 'light' ? stored : 'auto';
  });
  const [rowColor, setRowColor] = useState(() => readPref(ROW_COLOR_KEY, 'off') === 'on');
  // Who the operator is, for signing journal notes.
  //
  // Asked once and remembered, rather than a field beside every note: a note signed
  // "anonymous" is close to useless as an audit trail, and retyping a name per note
  // guarantees people stop bothering. Not authentication — there is none in this
  // system — so the UI must not imply it proves authorship; it records who claimed
  // the note, exactly like the ack's `by`.
  const [operatorName, setOperatorNameState] = useState(() => readPref(OPERATOR_KEY, ''));

  const darkMode = resolveTheme(themePreference, systemDark) === 'dark';

  useEffect(() => {
    const root = document.documentElement;
    // Absent attribute = CSS follows the OS via its prefers-color-scheme block.
    if (themePreference === 'auto') root.removeAttribute('data-theme');
    else root.setAttribute('data-theme', themePreference);
    writePref(THEME_KEY, themePreference === 'auto' ? null : themePreference);
  }, [themePreference]);

  useEffect(() => {
    writePref(ROW_COLOR_KEY, rowColor ? 'on' : null);
  }, [rowColor]);

  const setTheme = useCallback((next) => {
    setThemePreference(next === 'dark' || next === 'light' ? next : 'auto');
  }, []);

  const setOperatorName = useCallback((next) => {
    const clean = String(next || '').trim().slice(0, 128);
    setOperatorNameState(clean);
    writePref(OPERATOR_KEY, clean || null);
  }, []);

  return {
    themePreference, darkMode, rowColor, setTheme, setRowColor,
    operatorName, setOperatorName,
  };
}

// --------------------------------------------------------- column resizing
// Widths are stored per table in localStorage so a reload keeps the layout an
// operator has tuned.
const COL_WIDTH_KEY = (table) => `fw:colw:${table}`;

function readWidths(table) {
  try {
    const raw = window.localStorage.getItem(COL_WIDTH_KEY(table));
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch {
    return {};
  }
}

/**
 * Widths are stored per COLUMN NAME, never by index.
 *
 * They used to be keyed by position, which is silently wrong the moment a column
 * is added or removed: deleting the "Msg count" column shifted every index past it,
 * so an operator's saved widths were reapplied one column to the left. Nothing
 * errored — the table just came back with the wrong columns wide, and the only way
 * to notice was to remember what it looked like before.
 *
 * Keying by label means a removed column's entry is simply ignored, and a new one
 * starts at its default.
 */
function migrateWidths(table, cols) {
  const saved = readWidths(table);
  const byName = {};
  let changed = false;
  for (let i = 0; i < cols.length; i += 1) {
    // A name-keyed entry wins; a legacy index-keyed one is accepted once, so an
    // existing layout is not thrown away on upgrade.
    const v = saved[cols[i]] ?? saved[String(i)];
    if (typeof v === 'number' && Number.isFinite(v)) byName[cols[i]] = v;
    if (String(i) in saved) changed = true;
  }
  if (changed) writeWidths(table, byName);   // rewrite in the new shape
  return byName;
}

function writeWidths(table, widths) {
  try {
    window.localStorage.setItem(COL_WIDTH_KEY(table), JSON.stringify(widths));
  } catch {
    // Losing a column width is not worth failing a render over.
  }
}

/**
 * Column widths for one table, drag-resizable from the header separator.
 *
 * Implemented with `<colgroup>` and a per-column width rather than flexing the
 * table: the requirement is that a cell NEVER wraps, so the only sane layout is
 * fixed-width columns with horizontal overflow on the container.
 *
 * `defaults` is required: see ALERT_DEFAULT_W for why every column must carry a
 * width rather than only the dragged ones.
 */
const MIN_COL_WIDTH = 48;

/**
 * Row selection for one table: plain / shift / ctrl-click, plus Escape to clear.
 *
 * Extracted because it is used by TWO tables now (Open alerts and All events) and
 * a third copy would be a third chance to disagree about what shift-click means.
 * The behaviour is the file-manager convention:
 *
 *   plain click   select only this row (and move the anchor)
 *   shift+click   select the range from the anchor to this row
 *   ctrl/cmd+click toggle this row, leaving the rest alone
 *
 * The anchor is what makes shift-click predictable. Without it, shift+click would
 * select from whatever happened to be selected last, which after a shift-range is
 * the far end of that range and not where the operator started.
 *
 * @param {string[]} orderedKeys  row keys in the order they are RENDERED. Shift
 *   ranges are resolved against this, not against the selection, so the range means
 *   "everything between these two on screen" — which is what an operator sees.
 */
function useRowSelection(orderedKeys) {
  const [selected, setSelected] = useState(() => new Set());
  // Anchor for shift-range selection: the last row clicked without shift.
  const anchorRef = useRef(null);

  // Keep the latest key order available to the click handler without making it a
  // dependency of that handler: rebinding on every arriving event would re-create
  // the callback on every frame, and the rows do not need to know.
  const keysRef = useRef(orderedKeys);
  keysRef.current = orderedKeys;

  const selectRow = useCallback((key, e) => {
    const additive = e.metaKey || e.ctrlKey;
    const range = e.shiftKey;
    const keys = keysRef.current;

    setSelected((prev) => {
      const next = new Set(prev);
      if (range && anchorRef.current !== null) {
        const from = keys.indexOf(anchorRef.current);
        const to = keys.indexOf(key);
        if (from === -1 || to === -1) {
          next.clear();
          next.add(key);
          return next;
        }
        // An additive shift-range adds to the existing selection; a plain one
        // replaces it. Both include the anchor and the clicked row.
        if (!additive) next.clear();
        const [lo, hi] = from <= to ? [from, to] : [to, from];
        for (let i = lo; i <= hi; i += 1) next.add(keys[i]);
        return next;
      }
      if (additive) {
        if (next.has(key)) next.delete(key);
        else next.add(key);
        anchorRef.current = key;
        return next;
      }
      next.clear();
      next.add(key);
      anchorRef.current = key;
      return next;
    });
  }, []);

  const clearSelection = useCallback(() => {
    setSelected(new Set());
    anchorRef.current = null;
  }, []);

  // Escape clears the selection, matching the convention that Escape backs out of
  // a mode rather than leaving rows armed.
  useEffect(() => {
    if (selected.size === 0) return undefined;
    const onKey = (e) => { if (e.key === 'Escape') clearSelection(); };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [selected.size, clearSelection]);

  // Only rows still on screen, so a bulk action cannot fire at something that has
  // been retired, deleted or filtered out from under the operator's selection.
  const liveKeys = useMemo(() => {
    const present = new Set(orderedKeys);
    return [...selected].filter((k) => present.has(k));
  }, [selected, orderedKeys]);

  return { selected, selectRow, clearSelection, liveKeys };
}

function useColumnWidths(table, cols, defaults) {
  const [overrides, setOverrides] = useState(() => migrateWidths(table, cols));

  // Effective width for a column: the operator's value, else the default. Keyed by
  // LABEL, so adding or removing a column cannot shift a saved width onto its
  // neighbour.
  const widthOf = useCallback(
    (index) => Math.max(MIN_COL_WIDTH, overrides[cols[index]] ?? defaults[index] ?? MIN_COL_WIDTH),
    [overrides, cols, defaults],
  );

  const beginResize = useCallback((index) => (event) => {
    event.preventDefault();
    event.stopPropagation();
    const name = cols[index];
    const startX = event.clientX;
    const startWidth = widthOf(index);

    // Listeners on the document, not the handle: the pointer leaves the 7px
    // handle almost immediately, and a handle-local listener would stop tracking.
    const onMove = (e) => {
      const next = Math.max(MIN_COL_WIDTH, startWidth + (e.clientX - startX));
      setOverrides((prev) => ({ ...prev, [name]: next }));
    };
    const onUp = () => {
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
      document.body.classList.remove('resizing');
      // Persist on drop, not per pixel: a drag is dozens of updates and
      // localStorage is synchronous.
      setOverrides((prev) => {
        writeWidths(table, prev);
        return prev;
      });
    };

    // Suppresses text selection and holds the col-resize cursor while dragging.
    document.body.classList.add('resizing');
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
  }, [table, cols, widthOf]);

  // No `reset`. The "reset columns" button is gone: widths persist per column NAME
  // now, so a stored layout can never be corrupted by adding or removing a column —
  // which was the only thing the button was recovering from.
  return { widthOf, beginResize };
}

/**
 * A header cell with a drag handle on its right edge.
 *
 * The handle is a separate element rather than a CSS pseudo-element because it
 * needs its own hit area, its own cursor and its own mousedown target; a
 * `::after` would still need pointer-events juggling to distinguish "click the
 * header" from "drag the edge".
 */
function ResizableTh({ index, width, onBegin, children }) {
  return (
    <th style={{ width: `${width}px`, minWidth: `${width}px` }}>
      {children}
      <span
        className="colresize"
        role="separator"
        aria-orientation="vertical"
        title="Drag to resize this column"
        onMouseDown={onBegin(index, 0, width || 0)}
      />
    </th>
  );
}

// ---------------------------------------------------------------- gunzip shim
async function gunzip(bytes) {
  if (typeof DecompressionStream === 'undefined') {
    throw new Error('DecompressionStream unavailable');
  }
  const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

// ------------------------------------------------------------------- config
function useConfig() {
  const [config, setConfig] = useState(null);
  const [error, setError] = useState(null);
  useEffect(() => {
    fetch('config.json', { cache: 'no-store' })
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then(setConfig)
      .catch((e) => setError(String(e)));
  }, []);
  return { config, error };
}

// --------------------------------------------------------------------- socket
function useSocket(config) {
  const [status, setStatus] = useState('idle');
  // Dragonfly health as reported by the gateway. `null` means "not told yet", which
  // renders differently from "told, and it is down" — an unknown state must never
  // look like a healthy one.
  const [dbUp, setDbUp] = useState(null);
  const [events, setEvents] = useState([]);
  const [alerts, setAlerts] = useState(new Map());
  // Ack state is keyed by alert key and lives apart from the alert rows: the
  // rows are derived from the fold verdicts, while acks are an operator fact
  // that arrives on its own frames (docs/acknowledge.md §5).
  const [acks, setAcks] = useState(new Map());
  // Operator journal, keyed by record key, newest entry first.
  //
  // Keyed by the alert rather than an event id because the note is about the
  // incident: an event record is pruned after retention, and a note attached to
  // one would disappear with it. Held in one map so every tab that can open a
  // journal reads the same copy — two copies would be two chances to disagree.
  //
  // Absent key means "not read yet"; an empty array means "read, and there is
  // nothing". The distinction matters because the panel shows a spinner for the
  // first and "no notes yet" for the second.
  const [journals, setJournals] = useState(new Map());
  const [notice, setNotice] = useState(null);

  // The All Events tab's data: a windowed server query over the raw event log.
  // Held here so switching tabs does not re-fetch, and keyed by the range so
  // changing range is an explicit reload.
  const [allEvents, setAllEvents] = useState({
    events: [], loading: false, error: null, truncated: false,
    range: '24h', loadedAt: null,
    // How many events the WINDOW held when the last query answered. `null` until one
    // has, because "we have not asked" is not "there are none".
    total: null,
    // Rows genuinely appended since that query. Kept separate from `total` so the
    // badge can be `total + windowAdded` and therefore cannot drift from the table:
    // anything the list already holds, or the cap drops, adds nothing here.
    windowAdded: 0,
  });

  // The History tab's data: the AUDIT trail, one row per DELIVERY.
  //
  // Deliberately its own reducer rather than a second view of `allEvents`. The two
  // read different indexes and answer different questions: All events enumerates
  // CONDITIONS (a record is updated in place, so it shows the last time something
  // fired and its total count), History enumerates ARRIVALS (so it can show that
  // something fired eleven times in four minutes). Sharing state would force one to
  // lie about what it holds.
  const [history, setHistory] = useState({
    events: [], loading: false, error: null, truncated: false,
    range: '24h', loadedAt: null,
    // How many arrivals the WINDOW held when the last query answered. `null` until
    // one has, because "we have not asked" is not "there are none".
    total: null,
  });

  const wsRef = useRef(null);
  const attemptRef = useRef(0);
  const timerRef = useRef(null);
  const lastTsRef = useRef(null);
  // Set when WE close the socket (unmount), so `onclose` does not schedule a
  // reconnect. The name predates the removal of the Closed tab; it is about
  // connection teardown, not about closed alerts.
  const closedByUsRef = useRef(false);
  // Set when the gateway rejects `resync` as an unknown op — i.e. the deployed
  // ws-gateway predates this bundle. Stops the client from asking every interval
  // for an op that can only work after a redeploy.
  const noResyncRef = useRef(false);
  // Alert keys we currently display, so a reconnect can re-read their ack state.
  const knownKeysRef = useRef([]);
  // Last ARRIVAL folded into each alert key.
  //
  // ONE arrival produces TWO frames: `op: event` on the events channel and, for a
  // content duplicate, `op: alert` (kind: occurrence) on the alerts channel. Both
  // reach this client and both used to fold into the alert map, so a single repeat
  // incremented Count twice — measured 1 -> 2 -> 3 -> 2 across one arrival's frames.
  //
  // The pair is identified by the arrival's broker timestamp (an `op: alert` frame
  // without one cannot be paired, and folds as before). Value is the brokerTs.
  const lastFoldedRef = useRef(new Map());

  const ingest = useCallback(async (frame) => {
    if (frame.op === 'event' && frame.event) {
      const evt = frame.event;
      const payload = await decodePayload(evt.payload, gunzip, MSG_MAX_BYTES);
      // Rename at the boundary: components only ever see summary /
      // msgCompleteString (lib.js WIRE_FIELD).
      const decorated = toViewEvent({ ...evt, payload });
      setEvents((prev) => insertEvent(prev, decorated, EVENT_LIMIT));
      // The All Events tab is a windowed QUERY, but a query alone froze it: it
      // answered once, on first open, and never again. An operator who opened the
      // tab and then watched it saw a still image of a live board, with the badge
      // pinned at whatever the count was at that instant.
      //
      // Every arrival is a stored record now, including a content duplicate — the
      // worker keeps the rollup deduplicated but indexes each delivery. So there is
      // no `applied: 0` case to skip: the tab is literally "all events", and the
      // tab would be lying if it omitted the repeats the live tail shows.
      //
      // THE BADGE AND THE LIST MOVE TOGETHER — they are derived, not incremented
      // independently.
      //
      // `total` used to be `prev.total + 1` on every event frame, which was wrong in
      // two ways that both showed as the badge outrunning the table:
      //
      //   1. `insertAllEvent` de-duplicates on id, so a frame the client already holds
      //      adds NO row but still bumped the count. A redelivery, or the same record
      //      arriving twice, drifted the two apart permanently.
      //   2. Once the list hits `ALL_EVENTS_LIMIT` it stops growing while the count
      //      keeps climbing.
      //
      // Measured before this fix: badge 464 against 396 rows, a gap that grew through
      // a session and vanished on refresh (397/397). A count nobody can reconcile with
      // the rows underneath it is worse than no count.
      //
      // Now the badge is `windowBase + rows added since the last query`, so it can only
      // differ from the table by what the window held but the page did not carry (a
      // `truncated` reply). That difference is real data, and it is stated in words.
      setAllEvents((prev) => {
        const next = insertAllEvent(prev.events, decorated, ALL_EVENTS_LIMIT);
        const added = next === prev.events ? 0 : next.length - prev.events.length;
        return {
          ...prev,
          events: next,
          // `added` is 0 for anything the list already had, or for an arrival the cap
          // dropped — in both cases the table did not change, so the count must not.
          windowAdded: prev.loadedAt === null ? prev.windowAdded : (prev.windowAdded ?? 0) + added,
        };
      });
      if (frame.applied !== null && frame.applied !== undefined) {
        // The record key travels on the frame. It is a hash of device + summary, so
        // it CANNOT be re-derived here — and a second implementation of the hash is
        // exactly how the tail and the board would drift apart.
        const alertKey = evt.key ?? null;
        if (!alertKey) {
          // A frame from an older worker. Nothing to join on, so the tail row is
          // still rendered but the alert map is left alone; the next `resync`
          // repairs it. Loud only at debug: this is a rolling-update case.
          if (evt.brokerTs) lastTsRef.current = evt.brokerTs;
          return;
        }
        // ------------------------------------------------------------------
        // ONE ARRIVAL, ONE COUNT — and this half of it was missing.
        //
        // A single arrival produces TWO frames: `op: event` here and `op: alert` on
        // the alerts channel. Both reach this browser. The `op: alert` handler
        // already dropped a frame whose `brokerTs` matched the last folded one, but
        // THIS handler folded unconditionally and then overwrote the marker.
        //
        // So the count was correct only while the frames arrived `event` then
        // `alert`. When they arrived the other way round — which happens, because
        // they are two separate PUBLISHes on two channels — the `op: alert` frame
        // folded first, then this one folded the same arrival again. Measured on
        // the cluster: three identical events showed Count 6.
        //
        // The guard is the same test the other handler uses: same key, same broker
        // timestamp = same delivery.
        const arrival = Number.isFinite(evt.brokerTs) && evt.brokerTs > 0
          ? evt.brokerTs
          : null;
        if (arrival && lastFoldedRef.current.get(alertKey) === arrival) {
          // Already folded by the `op: alert` frame. The row still belongs in the
          // live tail — that happened above — but not in the rollup a second time.
          if (evt.brokerTs) lastTsRef.current = evt.brokerTs;
          return;
        }
        // Remember this arrival so the paired `op: alert` frame for the SAME
        // delivery does not count it a second time.
        if (arrival) lastFoldedRef.current.set(alertKey, arrival);
        setAlerts((prev) =>
          applyAlert(prev, {
            key: alertKey,
            applied: frame.applied,
            event: decorated,
          }),
        );
      }
      if (evt.brokerTs) lastTsRef.current = evt.brokerTs;
      return;
    }
    if (frame.op === 'alert') {
      // Keep the State chip on existing EVENT rows in step with the alert.
      //
      // Without this the chip is a snapshot from whenever the tab was queried: close
      // an alert from Open alerts and the All events rows for it keep saying `open`
      // until the next fetch. The chip's whole value is answering "is this incident
      // open NOW", so it has to move when the fold says it moved.
      //
      // `applied === 3` is a retirement, `1`/`2` an occurrence, which means an alert
      // that was closed has re-fired. Only rows for THIS key are touched.
      if (typeof frame.key === 'string' && (frame.applied === 1 || frame.applied === 2 || frame.applied === 3)) {
        // Match on the RECORD KEY. Splitting `source:type` out of the key was only
        // possible while the key WAS `source:type`; it is a 32-hex hash now, so the
        // comparison has to be on the key itself.
        const nowState = frame.applied === 3 ? 'closed' : 'open';
        setAllEvents((prev) => {
          let changed = false;
          const events = prev.events.map((e) => {
            if (e.key !== frame.key) return e;
            // The record fields come from the row, not from this frame: `op: event`
            // carries the producer's message and nothing else, while the row is the
            // FOLD's view of the condition — `count`, `firstSeen`, `lastSeen`.
            //
            // Without this merge a live arrival rendered Count `—` and
            // First == Last, because `op: event` has no counters at all; only a
            // fresh `allEvents` QUERY filled them in. Measured: the same record read
            // `3 | 19:33:21 -> 19:33:23` from a query and `— | 19:33:36 -> 19:33:36`
            // after one live delivery — two tabs disagreeing about one record, and
            // the All events tab contradicting itself depending on how it was loaded.
            const enriched = frame.row
              ? {
                ...e,
                count: frame.row.count ?? e.count,
                firstOccurrence: frame.row.firstOccurrence ?? e.firstOccurrence,
                lastOccurrence: frame.row.lastOccurrence ?? e.lastOccurrence,
                alertCount: frame.row.count ?? e.alertCount,
                alertClosedAt: frame.row.alertClosedAt ?? e.alertClosedAt,
                alertState: frame.row.alertState ?? nowState,
              }
              : { ...e, alertState: nowState };
            if (enriched.alertState === e.alertState
                && enriched.count === e.count
                && enriched.lastOccurrence === e.lastOccurrence) {
              return e;
            }
            changed = true;
            return enriched;
          });
          return changed ? { ...prev, events } : prev;
        });
      }
      // An `op: alert` frame carries `row`: the record itself, with its payload,
      // summary and timestamps inline. It is NOT a thin rollup plus a nested
      // `event` — one hash per condition means the row has no separate event to
      // embed.
      //
      // Reading `row.event` here produced a blank row on the first repeat (no
      // Summary, no First/Last occurrence) while Count still worked, because Count
      // is read from `row.count`. That is the reported screenshot.
      const row = frame.row;
      const rowEvent = row ? toViewEvent(row) : undefined;
      // ONE ARRIVAL, ONE COUNT.
      //
      // A content duplicate is published on BOTH channels: `op: event` carries the
      // arrival with its brokerTs, and `op: alert` (kind: occurrence) carries the
      // rollup. Both reach this client, so folding both incremented Count twice for
      // a single repeat — measured 1 -> 2 -> 3 -> 2 as the two frames interleaved.
      //
      // `brokerTs` is the arrival identity both frames share. It is preferred over
      // `row.lastSeen` because the gateway may fail to read the row, and then the
      // row supplies nothing at all.
      const arrival = Number.isFinite(frame.brokerTs) && frame.brokerTs > 0
        ? frame.brokerTs
        : (row?.lastSeen ?? null);
      if (arrival && lastFoldedRef.current.get(frame.key) === arrival) {
        return;
      }
      if (arrival) lastFoldedRef.current.set(frame.key, arrival);
      setAlerts((prev) => applyAlert(prev, {
        key: frame.key,
        applied: frame.applied,
        event: rowEvent,
        // The row is authoritative for a key we have not seen: it carries the
        // real severity, first/last seen and the fold's count.
        row,
      }));
      // An ack frame is an `alert` frame carrying `ack` instead of `applied`.
      if ('ack' in frame) {
        setAcks((prev) => {
          const next = new Map(prev);
          if (frame.ack) next.set(frame.key, frame.ack);
          else next.delete(frame.key);
          return next;
        });
        return;
      }
      // `applied === 1` means the fold counted a NEW OCCURRENCE, and the fold
      // clears the ack at exactly that point (event-fold.lua). Mirror it here
      // so the row stops showing "acked" the instant it re-fires, instead of up
      // to one debounce later. Without this an operator would see a stale
      // "acked by alice" on an alert nobody is handling
      // (docs/acknowledge.md §6).
      if (frame.applied === 1) {
        setAcks((prev) => {
          if (!prev.has(frame.key)) return prev;
          const next = new Map(prev);
          next.delete(frame.key);
          return next;
        });
      }
      // A retirement means the alert has left the board. Its record stays in
      // storage (the `Closed` tab that used to list it has been removed), so
      // there is nothing to do here beyond the alert-map delete `applyAlert`
      // already performed above.
      return;
    }
    if (frame.op === 'alerts') {
      // Bulk ack state for the keys we asked about.
      setAcks((prev) => {
        const next = new Map(prev);
        for (const a of frame.alerts || []) {
          if (a.ack) next.set(a.key, a.ack);
          else next.delete(a.key);
        }
        return next;
      });
      return;
    }
    if (frame.op === 'allEvents') {
      // The answer to `op: allEvents`. The reply is AUTHORITATIVE for the window:
      // it is a bounded query, so an event it omits is outside the window rather
      // than missing from the response.
      //
      // Live events that arrived while the request was in flight are re-applied on
      // top, though. The reply only covers up to `untilMs`, so replacing wholesale
      // would silently discard anything that landed in the round trip — the list
      // would go backwards a moment after the operator saw it move, and the same
      // event would reappear only on the next query. Merging by id keeps the reply
      // authoritative for everything it covers while not throwing away what it
      // could not have known about.
      const rows = await Promise.all(
        (Array.isArray(frame.events) ? frame.events : []).map(async (e) => {
          // Payloads are decoded here for the same reason the live path decodes
          // them: `msgc` arrives gzipped base64 whenever a producer sends
          // `EVT_Details`, and `toViewEvent` is synchronous so it cannot decode.
          // Skipping this on the QUERY paths is what made the details fold render
          // "[object Object]" on this tab while the live tab was fine.
          const payload = await decodePayload(e?.payload, gunzip, MSG_MAX_BYTES);
          return toViewEvent({ ...e, payload });
        }),
      );
      const newest = rows.reduce((m, e) => Math.max(m, e.brokerTs ?? 0), 0);
      setAllEvents((prev) => {
        const inFlight = prev.events.filter((e) => (e.brokerTs ?? 0) > newest);
        const merged = inFlight.reduce(
          (acc, e) => insertAllEvent(acc, e, ALL_EVENTS_LIMIT),
          rows,
        );
        return {
          events: merged,
          loading: false,
          error: null,
          truncated: Boolean(frame.truncated),
          range: frame.range || '24h',
          loadedAt: Date.now(),
          // The server's own count for the whole window. Kept as-is rather than
          // recomputed from `merged`: the list is capped at ALL_EVENTS_LIMIT and
          // grows from live frames, so `merged.length` answers a different question.
          total: Number.isFinite(frame.total) ? frame.total : merged.length,
          // A fresh query re-baselines: everything the window holds is now in `total`,
          // so the post-query arrivals start counting from zero.
          windowAdded: 0,
        };
      });
      return;
    }
    if (frame.op === 'history') {
      // The answer to `op: history` — one row per DELIVERY.
      //
      // Replaced wholesale, not merged: it is a windowed query, so a row it omits
      // is outside the window rather than missing from this reply.
      //
      // Decode before mapping, like every other read path: `hist:` rows carry the
      // same (possibly gzipped) payload, so skipping this leaves Summary blank and
      // the inspector showing `{__gz:…}`.
      const rows = Array.isArray(frame.events) ? frame.events : [];
      const next = [];
      for (const row of rows) {
        if (!row || typeof row.key !== 'string') continue;
        const payload = await decodePayload(row.payload, gunzip, MSG_MAX_BYTES);
        next.push(toViewEvent({ ...row, payload }));
      }
      setHistory({
        events: next,
        loading: false,
        error: null,
        truncated: Boolean(frame.truncated),
        range: frame.range || '24h',
        // The server's window total, not the page size. `null` is preserved so the
        // badge can tell "not asked" from "none".
        total: Number.isFinite(frame.total) ? frame.total : next.length,
        loadedAt: Date.now(),
      });
      return;
    }
    if (frame.op === 'snapshot') {
      // The state of the world as of connect, or as of the last resync. Without
      // this the board is empty on load until something new fires, because events
      // only create rows as they arrive — a reload would look like a dead cluster.
      //
      // A snapshot REPLACES the alert map rather than merging into it: it is the
      // full open set, so a row it omits is genuinely closed. Merging would
      // resurrect rows retired while the socket was down.
      //
      // `alerts` may be an EMPTY array, and that is a real answer, not a missing
      // one. Bailing out on an empty snapshot (as an earlier length check did)
      // left rows on screen that no longer existed — the stale-board bug that
      // only a browser refresh cleared.
      if (!Array.isArray(frame.alerts)) return;
      const next = new Map();
      for (const row of frame.alerts) {
        if (!row || typeof row.key !== 'string') continue;
        // The row IS the record now, so it carries the fields a row renders:
        // `summary`, `payload`, `source`, `type`, `severity`, `count`, timestamps.
        //
        // It used to be a thin rollup plus a nested `event` record, and this code
        // read only `row.event` — which the gateway no longer sends, because one
        // hash per condition means the row has no separate event to embed. The
        // result was a fully blank row: no Summary, no First/Last occurrence, while
        // Count (read from `row.count`) still worked. That is exactly the shape of
        // the reported screenshot.
        //
        // `toViewEvent` is still the right constructor: it derives `summary` from
        // `payload.msg`, normalises the timestamps and keeps the wire record for the
        // inspector. It just gets the row itself instead of a nested event.
        const payload = await decodePayload(row.payload, gunzip, MSG_MAX_BYTES);
        const evt = toViewEvent({ ...row, payload });
        next.set(row.key, {
          key: row.key,
          severity: row.severity,
          firstSeen: row.firstOccurrence ?? row.firstSeen,
          lastSeen: row.lastOccurrence ?? row.lastSeen,
          count: row.count,
          event: evt,
        });
      }
      setAlerts(next);

      // Ack state rides along, so a reload does not lose "acked by alice".
      const acked = new Map();
      for (const row of frame.alerts) {
        if (row?.key && row.ackedBy) {
          acked.set(row.key, { by: row.ackedBy, at: row.ackedAt });
        }
      }
      setAcks(acked);

      // NOTE: the live tail is NOT seeded from the snapshot.
      //
      // It used to be — the newest record of each open alert was pushed into the
      // tail so the pane was not empty on load. That produced exactly one row of
      // unexplained age after every refresh: the alert's `last_id` record, which can
      // be hours old, presented in a feed that otherwise means "arrivals since you
      // connected". An operator sees one row and cannot tell whether it just
      // arrived or was left over.
      //
      // The tail now shows ARRIVALS ONLY. Nothing has arrived yet on a fresh
      // connection, so it is empty, and `op: allEvents` is where history lives —
      // with a window picker, a filter and a server-reported total. The backfill
      // below is what fills it with real recent events when a source filter is set.
      return;
    }
    if (frame.op === 'db') {
      // The gateway probes Dragonfly and pushes only on CHANGE, so this frame is a
      // transition worth showing, not routine traffic.
      setDbUp(Boolean(frame.up));
      return;
    }
    if (frame.op === 'hello') {
      if (frame.truncated) setNotice('Backfill was truncated — narrow the time window.');
      // The initial database state, so a client joining mid-outage does not have to
      // wait for the next probe tick to find out.
      if (frame.db && typeof frame.db.up === 'boolean') setDbUp(frame.db.up);
      return;
    }
    if (frame.op === 'journal') {
      // One new entry, either echoed back to the operator who wrote it or fanned out
      // from another pod. Reconciling against the optimistic copy is by TEXT match on
      // the pending entry: the server mints the id, so the two cannot share one, and
      // matching on text is the only identity the client has until the echo lands.
      const entry = frame?.entry;
      if (typeof frame?.key === 'string' && entry && typeof entry.text === 'string') {
        setJournals((prev) => {
          const next = new Map(prev);
          const list = next.get(frame.key) || [];
          // Already have this exact entry id (a re-delivered fan-out): ignore.
          if (list.some((e) => e.id === entry.id)) return prev;
          const withoutPending = list.filter(
            (e) => !(e.pending && e.text === entry.text && e.at <= entry.at),
          );
          next.set(frame.key, [entry, ...withoutPending]);
          return next;
        });
      }
      return;
    }
    if (frame.op === 'journals') {
      // A bulk read. Replaces each key wholsesale: it is a bounded newest-N query,
      // so an entry it omits is older than the window, not missing.
      const rows = Array.isArray(frame.journals) ? frame.journals : [];
      if (rows.length === 0) return;
      setJournals((prev) => {
        const next = new Map(prev);
        for (const row of rows) {
          if (!row || typeof row.key !== 'string') continue;
          const entries = Array.isArray(row.entries) ? row.entries : [];
          next.set(row.key, entries);
        }
        return next;
      });
      return;
    }
    if (frame.op === 'deleted') {
      // An event record is gone for good, so every list that was showing it must
      // stop claiming it exists. This used to touch the live tail only, which made
      // delete look broken on any other tab: the record was gone from storage while
      // the All events row sat there.
      //
      // The alert ROW stays. `count` is owned by the fold, and decrementing it here
      // would disagree with the stored state until the next write
      // (docs/acknowledge.md §10.1). What is dropped is the row's reference to the
      // dead record, so it stops offering an id that resolves to nothing.
      // A row is identified by its RECORD KEY here, not by its message id. A live
      // `op: event` row carries the arrival's id in `id` (so two deliveries to the
      // same condition are distinguishable in the tail) while `key` holds the record
      // key — and `op: delete` addresses the RECORD. Filtering on `id` therefore
      // removed the row only when a query had loaded it (where id === key) and left
      // it in place when it had arrived live: delete looked like it did nothing.
      const isTarget = (e) => (e.key ?? e.id) === frame.id;
      setEvents((prev) => prev.filter((e) => !isTarget(e)));
      setAllEvents((prev) => (prev.events.some(isTarget)
        ? {
          ...prev,
          events: prev.events.filter((e) => !isTarget(e)),
          // The window no longer holds it either, so the count must follow or the
          // badge would outlive the row it was counting.
          // The window total plus whatever has arrived since, so the badge and the
          // table cannot drift apart. `windowAdded` counts only rows the list actually
          // gained — see the ingest handler for why incrementing on every frame was
          // wrong (measured gap: badge 464 vs 396 rows).
          total: prev.total === null ? null : Math.max(0, prev.total - 1),
          // A deleted row was in the window too, so it leaves both numbers together.
          windowAdded: Math.max(0, (prev.windowAdded ?? 0) - 1),
        }
        : prev));
      setAlerts((prev) => {
        // The row IS the record now: one hash per condition, and the gateway's
        // snapshot/reply rows carry the record fields directly, not under `event`.
        // This still asked for `row.event.id`, which no row has had since the
        // Valkey migration — so the branch matched nothing, the alert stayed in the
        // map, and "Open alerts" kept a badge of 1 with `dbsize: 0` behind it. The
        // row must be REMOVED, not blanked: a condition with no record has nothing
        // to display, and the old code left a skeleton row behind.
        const has = prev.has(frame.id);
        if (!has) return prev;
        const next = new Map(prev);
        next.delete(frame.id);
        return next;
      });
      return;
    }
    if (frame.op === 'error') {      // A gateway older than this bundle does not know `resync`. That is a deploy
      // skew, not something the operator can act on, and retrying cannot fix it:
      // stop sending it and say so once. Showing the raw frame (
      // `bad_op: unknown op resync`) every 60 seconds reads as a frontend bug.
      if (isUnsupportedOpError(frame, 'resync')) {
        if (!noResyncRef.current) {
          noResyncRef.current = true;
          setNotice(
            'Auto-refresh is off: the ws-gateway is older than this page and does ' +
            'not support it. Reload the page to retry after the gateway is updated.',
          );
        }
        return;
      }
      // A failed read belongs to its own pane, not to the whole page: an operator
      // reading the alert board should not get a banner about a tab they are not
      // looking at.
      if (frame.code === 'all_events_failed') {
        setAllEvents((prev) => ({
          ...prev, loading: false, error: frame.message || 'could not read events',
        }));
        return;
      }
      if (frame.code === 'history_failed') {
        setHistory((prev) => ({
          ...prev, loading: false, error: frame.message || 'could not read history',
        }));
        return;
      }
      setNotice(`${frame.code}: ${frame.message}`);
      return;
    }
    if (frame.op === 'shutdown') {
      setNotice('Server is shutting down, reconnecting…');
    }
  }, []);

  const connect = useCallback(() => {
    if (!config) return;
    const url = resolveWsUrl(config, window.location.hostname, window.location.protocol === 'https:');
    if (!url) {
      setStatus('misconfigured');
      setNotice('wsUrl is empty in config.json');
      return;
    }

    setStatus(attemptRef.current === 0 ? 'connecting' : 'reconnecting');
    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => {
      attemptRef.current = 0;
      setStatus('connected');
      setNotice(null);
      // Resume from the last event we saw, minus a small overlap, so a reconnect
      // does not leave a gap. Overlap is de-duplicated by id on the server side
      // and by insertEvent here.
      const since = lastTsRef.current
        ? new Date(lastTsRef.current - 5000).toISOString()
        : new Date(Date.now() - (config.backfillDefaultMinutes || 15) * 60000).toISOString();
      ws.send(JSON.stringify({ op: 'subscribe', since }));

      // Re-read ack state on every connect. A client that was disconnected may
      // have missed an ack frame, and the gateway has no per-client replay.
      if (knownKeysRef.current.length > 0) {
        ws.send(JSON.stringify({ op: 'alerts', keys: knownKeysRef.current.slice(0, MAX_ACK_KEYS) }));
      }

      // A reconnect follows a window in which frames were definitely missed, so
      // the board is resynced straight away rather than waiting for the next
      // timer tick. `subscribe` above already sends a snapshot, so this is
      // redundant on the happy path — but it costs one small frame and covers
      // the case where the subscribe was accepted while the snapshot read was
      // still in flight and returned nothing. Skipped entirely once the gateway
      // has shown it does not know the op.
      if (!noResyncRef.current) ws.send(JSON.stringify({ op: 'resync' }));
    };

    ws.onmessage = (msg) => {
      let frame;
      try {
        frame = JSON.parse(msg.data);
      } catch {
        return;
      }
      ingest(frame);
    };

    ws.onclose = () => {
      if (closedByUsRef.current) return;
      setStatus('reconnecting');
      const delay = backoffDelay(attemptRef.current);
      attemptRef.current += 1;
      timerRef.current = setTimeout(connect, delay);
    };

    ws.onerror = () => {
      // onclose always follows; no action needed here.
    };
  }, [config, ingest]);

  useEffect(() => {
    if (!config) return undefined;
    connect();
    return () => {
      closedByUsRef.current = true;
      if (timerRef.current) clearTimeout(timerRef.current);
      if (wsRef.current) wsRef.current.close();
    };
  }, [config, connect]);

  /**
   * Ask for ack state whenever the set of displayed alerts changes.
   *
   * Debounced, because arrivals are bursty: without it, every event would send
   * a frame. The client sends its own keys rather than asking the server to
   * list everything, which would be the unbounded scan the design forbids
   * (docs/acknowledge.md §5).
   */
  useEffect(() => {
    const keys = [...alerts.keys()].slice(0, MAX_ACK_KEYS);
    knownKeysRef.current = keys;
    const ws = wsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN || keys.length === 0) return undefined;
    const t = setTimeout(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ op: 'alerts', keys }));
      }
    }, 400);
    return () => clearTimeout(t);
  }, [alerts]);

  /**
   * Periodic resync of the open-alert set.
   *
   * The live stream is best-effort in one direction: a `close` fan-out frame that
   * is lost leaves a retired row on screen until something else touches it. The
   * observed failure was a row that disappeared on a browser refresh but not on
   * its own — meaning the alert HAD retired in Dragonfly and the frame telling
   * this client had not arrived (a missed pong, a dropped pub/sub message across
   * a reconnect, a throttled tab).
   *
   * React cannot "reload" a websocket; the equivalent is to re-ask for the
   * authoritative set and replace local state with it, which is exactly what the
   * `snapshot` handler already does on connect. So this sends `resync` on a timer
   * and lets the existing snapshot path do the work — no second code path, no
   * merge rules, nothing that can disagree with a fresh page load.
   *
   * Interval is deliberately not tight: the cost is one Redis read of the open
   * index plus an HMGET per alert, and a stale row is an annoyance rather than an
   * emergency. Five minutes bounds the staleness while keeping the gateway idle
   * for a quiet board. A resync is also sent immediately on reconnect, because
   * the disconnect window is when frames are most likely to have been missed.
   */
  useEffect(() => {
    if (!config) return undefined;
    const ms = (config.resyncSeconds ?? 300) * 1000;
    if (!(ms > 0)) return undefined;   // 0 disables it
    const tick = () => {
      if (noResyncRef.current) return;   // gateway rejected it; see the error handler
      const ws = wsRef.current;
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ op: 'resync' }));
      }
    };
    const id = setInterval(tick, ms);
    // A backgrounded tab has its timers throttled by the browser, so the interval
    // above can be minutes late — and that is the state an operator returns to.
    // Resync the moment the tab is visible again instead of waiting for the next
    // tick.
    const onVisible = () => {
      if (document.visibilityState === 'visible') tick();
    };
    document.addEventListener('visibilitychange', onVisible);
    return () => {
      clearInterval(id);
      document.removeEventListener('visibilitychange', onVisible);
    };
  }, [config]);

  /**
   * Bulk ack/close.
   *
   * Frames are PACED, not fired in a loop. The gateway rate-limits inbound
   * frames per connection (`FW_MAX_FRAMES_PER_SEC`, default 10), and one frame
   * per key in a tight loop blows straight through it: selecting 200 alerts and
   * closing them emitted 201 frames in a single tick and the client got back
   *
   *   rate_limited: too many frames
   *
   * Pacing to just under the limit keeps every frame inside the budget. It also
   * matches how the writes already behave — each one is an independent Lua
   * script, so there is nothing to gain from queueing 200 of them at once, and
   * a steady drip lets the confirmations update rows as they land instead of all
   * at the end.
   *
   * `FRAME_BUDGET_PER_SEC` is deliberately below the server's 10: the ack-state
   * refresh and heartbeat also send frames, and the limiter counts all of them.
   */
  const bulk = useCallback((op, keys) => {
    const ws = wsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      setNotice(`not connected — ${op} not sent`);
      return 0;
    }
    if (keys.length <= FRAME_BUDGET_PER_SEC) {
      for (const key of keys) ws.send(JSON.stringify({ op, key }));
      return keys.length;
    }

    // More than one second's budget: drip them out.
    let i = 0;
    const pump = setInterval(() => {
      if (ws.readyState !== WebSocket.OPEN) {
        clearInterval(pump);
        setNotice(`connection lost — ${keys.length - i} ${op} frame(s) not sent`);
        return;
      }
      const slice = keys.slice(i, i + FRAME_BUDGET_PER_SEC);
      for (const key of slice) ws.send(JSON.stringify({ op, key }));
      i += slice.length;
      if (i >= keys.length) clearInterval(pump);
    }, 1000);
    return keys.length;
  }, []);

  /**
   * Permanently delete one event.
   *
   * NO CONFIRMATION DIALOG. It had a `window.confirm()`, on the reasoning that the
   * only irreversible action in the UI should ask first. In practice the dialog was
   * the wrong affordance here: delete already requires right-click → a menu → a red
   * item naming the event id, which is three deliberate steps, and a native modal on
   * top of that is friction that trains an operator to dismiss it without reading.
   * The menu item is styled red and names the id; that is the confirmation.
   *
   * Confirmed in the same place as ack and close — the `deleted` broadcast — so a
   * delete made on another pod removes the row here too.
   */
  const deleteEvent = useCallback((id) => {
    const ws = wsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      setNotice('not connected — delete not sent');
      return;
    }
    ws.send(JSON.stringify({ op: 'delete', id }));
  }, []);

  /**
   * Read the journal for one alert key.
   *
   * A query, not a subscription: notes are written rarely and read on demand when
   * an operator opens the panel, so a live subscription would spend a round trip
   * per alert on every board refresh to deliver nothing.
   */
  const loadJournal = useCallback((key) => {
    const ws = wsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    ws.send(JSON.stringify({ op: 'journals', keys: [key] }));
  }, []);

  /**
   * Append one note to an alert's journal.
   *
   * Optimistic in the one direction that is safe: the entry is shown immediately
   * with the text the operator typed, then reconciled when the server's echo
   * arrives carrying the authoritative id and timestamp. Without this a note would
   * appear to vanish for a round trip, and on a slow link an operator would type it
   * again.
   */
  const addJournalEntry = useCallback((key, text, by) => {
    const ws = wsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      setNotice('not connected — note not saved');
      return false;
    }
    const trimmed = String(text || '').trim();
    if (trimmed === '') return false;
    const pending = {
      id: `pending-${Date.now()}`,
      by: by || 'you',
      at: Date.now(),
      text: trimmed,
      pending: true,
    };
    setJournals((prev) => {
      const next = new Map(prev);
      next.set(key, [pending, ...(prev.get(key) || [])]);
      return next;
    });
    ws.send(JSON.stringify({ op: 'journal', key, text: trimmed, by }));
    return true;
  }, []);

  /**
   * Ask for every event in a time window.
   *
   * A QUERY, not a stream: the window is bounded, it changes slowly relative to a
   * 24-hour span, and re-reading it on demand is cheaper and simpler than
   * maintaining a second subscription. The range is sent to the server so
   * filtering happens there — a 1-hour request reads far less than a 7-day one.
   */
  const loadAllEvents = useCallback((range) => {
    const ws = wsRef.current;
    const nextRange = range || '24h';
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      setNotice('not connected — events not requested');
      return;
    }
    // The range is recorded immediately, so the picker shows what was asked for
    // even while the reply is in flight. `total` is cleared because it described
    // the PREVIOUS window: leaving it would make the badge quote a count for a
    // window the operator is no longer looking at.
    setAllEvents((prev) => ({
      ...prev, range: nextRange, loading: true, error: null,
      // Both describe a window that is no longer selected.
      total: null, windowAdded: 0,
    }));
    ws.send(JSON.stringify({ op: 'allEvents', range: nextRange }));
  }, []);

  /**
   * Ask for the AUDIT trail in a time window.
   *
   * Same shape as `loadAllEvents` — a bounded, on-demand query rather than a
   * stream — but a different index: one row per DELIVERY instead of one per
   * condition.
   *
   * `key` optionally narrows to one incident's arrivals. It is sent to the server
   * rather than filtered here because the badge and the `truncated` notice must
   * describe the SAME set the table shows; filtering a page client-side would let
   * the badge say 3 while the table held 3 of 40.
   */
  const loadHistory = useCallback((range, key = null) => {
    const ws = wsRef.current;
    const nextRange = range || '24h';
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      setNotice('not connected — history not requested');
      return;
    }
    setHistory((prev) => ({
      ...prev, range: nextRange, loading: true, error: null, total: null,
    }));
    const frame = { op: 'history', range: nextRange };
    if (key) frame.key = key;
    ws.send(JSON.stringify(frame));
  }, []);

  return {
    status, events, alerts, acks, notice, bulk, deleteEvent,
    loadAllEvents, allEvents,
    loadHistory, history,
    journals, loadJournal, addJournalEntry,
    dbUp,
  };
}

// ------------------------------------------------------------------------ UI
/**
 * The line between the connection chip and the database chip.
 *
 * Its colour is the COMBINED state, so the pair reads as one statement instead of
 * two independent ones. The rule lives in `lib.js` (`linkState`) so it can be tested
 * without a DOM.
 *
 * The pulse is CSS-only (`@keyframes`), and it runs ONLY when everything is healthy.
 * A pulsing line means "this path is live"; pulsing it in a failure state would add
 * motion at the moment an operator is trying to read the screen. `prefers-reduced-
 * motion` switches it off entirely.
 */
function HealthLink({ socket, db }) {
  const state = linkState(socket, db);
  return (
    <span
      className={`healthlink healthlink-${state}`}
      aria-hidden="true"
      // Not announced: it carries no information a screen reader user does not
      // already get from the two chips either side of it.
    />
  );
}

function Badge({ severity, label }) {
  // Severity can be absent on a row that was incremented by a content-duplicate
  // frame: that frame carries no event, so there is nothing to read it from
  // (docs/frontend.md §5.3.1). Rendering `undefined` here used to throw
  // `Cannot read properties of undefined (reading 'toLowerCase')` inside render,
  // which unmounts the whole tree and leaves a BLANK PAGE — the failure looked
  // like a crashed app rather than one unknown row.
  if ((severity === undefined || severity === null || severity === '')
    && (label === undefined || label === null || label === '')) {
    return <span className="sev sev-unknown">—</span>;
  }
  // Colour comes from the severity being DISPLAYED, not blindly from the stored
  // one. On a closed row an operator close overwrites the stored severity with
  // `CLOSE` while the record's payload still says `WARNING`, so a stored-only
  // colour painted the word WARNING on a green chip — a colour that belongs to a
  // different severity than the text beside it. `colourSeverity` maps the label a
  // producer used back onto a colour class, and falls back to the stored value for
  // an unrecognised label.
  //
  // Text is the producer's own value when there is one: a producer sends CLEAR,
  // never CLOSE, so showing the mapped name would present a severity that does not
  // exist in their vocabulary.
  const colour = colourSeverity(severity, label) ?? severity;
  if (colour === undefined || colour === null || colour === '') {
    return <span className="sev sev-unknown">{label || '—'}</span>;
  }
  return (
    <span className={`sev sev-${String(colour).toLowerCase()}`}>{label || severity || colour}</span>
  );
}

// ---------------------------------------------------------------------- icons
// Inline SVG rather than an icon font or emoji: no extra request, no font
// loading flash, inherits currentColor so the theme handles it, and an emoji
// would render in the OS emoji font on one platform and as tofu on another.
const ICON_PROPS = {
  viewBox: '0 0 24 24',
  fill: 'none',
  stroke: 'currentColor',
  strokeWidth: 1.8,
  strokeLinecap: 'round',
  strokeLinejoin: 'round',
  'aria-hidden': 'true',
  focusable: 'false',
};

function IconSun() {
  return (
    <svg {...ICON_PROPS} className="icon">
      <circle cx="12" cy="12" r="4" />
      <path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4" />
    </svg>
  );
}

function IconMoon() {
  return (
    <svg {...ICON_PROPS} className="icon">
      {/* Filled crescent: reads as "dark" at 18px, where an outline does not. */}
      <path d="M21 12.8A8.5 8.5 0 1 1 11.2 3a6.8 6.8 0 0 0 9.8 9.8z" fill="currentColor" stroke="none" />
    </svg>
  );
}

function IconPalette() {
  return (
    <svg {...ICON_PROPS} className="icon">
      <path d="M12 3a9 9 0 1 0 0 18c1.1 0 1.8-.8 1.8-1.8 0-.5-.2-.9-.5-1.2-.3-.3-.5-.7-.5-1.2 0-1 .8-1.8 1.8-1.8H16a5 5 0 0 0 5-5c0-3.9-4-7-9-7z" />
      <circle cx="7.5" cy="11" r="1.1" fill="currentColor" stroke="none" />
      <circle cx="10" cy="7.2" r="1.1" fill="currentColor" stroke="none" />
      <circle cx="14.6" cy="7.6" r="1.1" fill="currentColor" stroke="none" />
    </svg>
  );
}

// --------------------------------------------------------------- inspector
/**
 * Right-click menu for a record.
 *
 * Two levels, deliberately:
 *
 *   level 1  the ACTIONS on this row — Acknowledge, Information
 *   level 2  Information expands to every field of the record
 *
 * The previous version showed actions and the full field dump at once, which
 * made a routine acknowledge sit next to 40 lines of JSON. Splitting them means
 * the common action is one click and the detail is opt-in, and it leaves room
 * for more actions later without the menu becoming a wall of text.
 *
 * Closed on Escape, on a click outside, and on scroll of the container it sits
 * in — a menu anchored to a row that has scrolled away is worse than no menu.
 */
function RecordInspector({ state, onClose, journal }) {
  const ref = useRef(null);
  // Which submenu is expanded. Reset whenever a different row is opened, or the
  // panel would reopen showing the previous row's Information.
  const [view, setView] = useState('menu');

  useEffect(() => {
    setView('menu');
  }, [state?.record, state?.title]);

  useEffect(() => {
    if (!state) return undefined;
    const onKey = (e) => { if (e.key === 'Escape') onClose(); };
    const onDown = (e) => { if (!ref.current?.contains(e.target)) onClose(); };
    document.addEventListener('keydown', onKey);
    document.addEventListener('mousedown', onDown);
    return () => {
      document.removeEventListener('keydown', onKey);
      document.removeEventListener('mousedown', onDown);
    };
  }, [state, onClose]);

  /**
   * Keep the panel fully on screen, measured from the REAL element.
   *
   * The size is a CSS decision (620x560), so hard-coding it here as well meant
   * two copies that could disagree — and when they did, the clamp used the old,
   * smaller numbers and pushed a taller panel off the bottom of the viewport.
   *
   * Runs on every open and whenever the view toggles, because the Information
   * table is a different height from the action list. `offsetWidth/Height` is
   * read after layout, so the browser has already applied the CSS max-height.
   */
  useEffect(() => {
    const el = ref.current;
    if (!el || !state) return;
    const margin = 8;
    const w = el.offsetWidth;
    const h = el.offsetHeight;
    const x = Math.max(margin, Math.min(state.x, window.innerWidth - w - margin));
    const y = Math.max(margin, Math.min(state.y, window.innerHeight - h - margin));
    el.style.left = `${x}px`;
    el.style.top = `${y}px`;
  }, [state, view]);

  if (!state) return null;
  const entries = recordEntries(state.record);
  // Drop any caller-supplied `Information`.
  //
  // This component ALWAYS renders its own Information row at the bottom, because
  // that submenu is intrinsic to the inspector rather than something a row should
  // have to remember to add. A caller that adds one anyway gets two identical
  // entries — and because the injected one is not wired to `setView('info')`, it
  // does nothing when clicked. That exact bug shipped: a context menu once showed
  // two Information rows and only the second worked.
  //
  // Filtering here rather than trusting callers means the mistake cannot recur:
  // the panel owns its own affordance.
  const actions = (state.actions || []).filter((a) => a.label !== 'Information');

  return (
    <div
      ref={ref}
      className="inspector"
      // Positioned by the effect below once the real size is known. Until then
      // it is laid out off-view so it can be measured without flashing at the
      // wrong place.
      style={{ left: state.x, top: state.y }}
      role="dialog"
      aria-label={state.title}
    >
      <div className="inspector-head">
        {view === 'info' ? (
          <button className="link inspector-back" onClick={() => setView('menu')}>
            ← {state.title}
          </button>
        ) : (
          <span>{state.title}</span>
        )}
        <button className="link" onClick={onClose} aria-label="Close">✕</button>
      </div>

      {view === 'menu' ? (
        <div className="inspector-actions">
          {actions.map((a) => (            <button
              key={a.label}
              // Destructive actions get their own class: an irreversible item must
              // not look identical to Acknowledge sitting above it.
              className={a.danger ? 'inspector-action inspector-danger' : 'inspector-action'}
              onClick={() => { a.run(); onClose(); }}
              disabled={a.disabled}
              title={a.title}
            >
              {a.label}
            </button>
          ))}
          {journal ? (
            <button
              className="inspector-action"
              onClick={() => setView('journal')}
              title="Notes left by operators about this incident"
            >
              {journal?.entries?.length
                ? `Journal (${journal.entries.length})`
                : 'Journal'}
            </button>
          ) : null}
          <button
            className="inspector-action"
            onClick={() => setView('info')}
            title="Show every field of this record"
          >
            Information
          </button>
        </div>
      ) : view === 'journal' ? (
        <JournalView
          journal={journal}
          onBack={() => setView('menu')}
        />
      ) : (
        <table className="inspector-table">
          <tbody>
            {entries.length === 0
              ? <tr><td className="dim">No fields.</td></tr>
              : entries.map(([k, v]) => (
                <tr key={k}>
                  <th scope="row">{k}</th>
                  <td className="mono">{v}</td>
                </tr>
              ))}
          </tbody>
        </table>
      )}
      <div className="inspector-hint">
        {view === 'info' || view === 'journal'
          ? 'Esc or click outside to close'
          : 'Right-click action for this row'}
      </div>
    </div>
  );
}

/**
 * The journal for one incident: newest notes first, plus a composer.
 *
 * Deliberately inside the inspector rather than a modal. The operator is reading
 * the row they are annotating — a modal would cover it, and notes are usually
 * written *about* something visible on screen.
 *
 * The character limit is enforced three times on purpose: `maxLength` stops the
 * keystroke, the counter tells the operator how much room is left, and the SERVER
 * enforces it again (`MAX_JOURNAL_LEN`) because a client-side limit is a
 * convenience, never a control.
 */
function JournalView({ journal, onBack }) {
  const [text, setText] = useState('');
  const { key, entries = [], loading, error, onAdd, operatorName, setOperatorName } = journal || {};

  return (
    <div className="journal">
      {/* Signed notes only. A note attributed to "anonymous" is not an audit
          trail, and making it a prerequisite once is cheaper than a name field on
          every note — which people stop filling in. Not authentication: this
          records who CLAIMED the note, exactly like the ack's `by`. */}
      {!operatorName ? (
        <div className="journal-signature">
          <label htmlFor="journal-operator">Sign notes as</label>
          <input
            id="journal-operator"
            className="journal-name"
            placeholder="your name"
            maxLength={64}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && e.currentTarget.value.trim()) {
                setOperatorName?.(e.currentTarget.value);
              }
            }}
            onBlur={(e) => { if (e.currentTarget.value.trim()) setOperatorName?.(e.currentTarget.value); }}
          />
          <span className="dim">press Enter</span>
        </div>
      ) : null}
      <div className="journal-compose">
        <textarea
          className="journal-input"
          value={text}
          onChange={(e) => setText(e.target.value.slice(0, JOURNAL_MAX_CHARS))}
          placeholder="Add a note about this incident…"
          maxLength={JOURNAL_MAX_CHARS}
          rows={3}
          aria-label="New journal note"
          // Ctrl/Cmd+Enter submits, because Enter must stay a newline: notes are
          // often pasted multi-line text and a form that submits on Enter mangles
          // them the first time someone tries.
          onKeyDown={(e) => {
            if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
              e.preventDefault();
              if (text.trim() && onAdd?.(text)) setText('');
            }
          }}
        />
        <div className="journal-compose-actions">
          <span className={text.length > JOURNAL_MAX_CHARS * 0.9
            ? 'journal-counter journal-counter-warn' : 'journal-counter'}>
            {text.length}/{JOURNAL_MAX_CHARS}
          </span>
          <button
            className="inspector-action journal-save"
            disabled={!text.trim() || !onAdd || !operatorName}
            onClick={() => { if (onAdd?.(text)) setText(''); }}
            title={operatorName
              ? 'Save this note (Ctrl/Cmd+Enter)'
              : 'Set your name above first — a note nobody is willing to sign is not a journal'}
          >
            Add note
          </button>
        </div>
      </div>

      {error ? <p className="error">{error}</p> : null}

      {loading && entries.length === 0 ? (
        <p className="dim">Loading notes…</p>
      ) : entries.length === 0 ? (
        <p className="dim">No notes yet for {key}.</p>
      ) : (
        <ul className="journal-list">
          {entries.map((e) => (
            <li key={e.id} className={e.pending ? 'journal-entry journal-pending' : 'journal-entry'}>
              <div className="journal-meta">
                <strong>{e.by}</strong>
                <span>{formatClock(e.at)}</span>
                {e.pending ? <span className="journal-saving">saving…</span> : null}
              </div>
              {/* Plain text, never HTML. React escapes it by default and that is
                  exactly what is wanted: a note is arbitrary operator input, and
                  rendering it as markup would let one operator inject content into
                  another operator's panel. */}
              <div className="journal-text">{e.text}</div>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

/**
 * A table whose header never scrolls out of view.
 *
 * Two stacked tables — one for the header, one for the body — inside a flex
 * column. Only the body scrolls.
 *
 * Why not `position: sticky` on the `th`:
 *
 * 1. A horizontally scrollable wrapper needs `overflow-x: auto`, and that makes
 *    the element a scroll container in BOTH axes. The sticky header then sticks
 *    to that box, which never scrolls vertically, so the header scrolled away
 *    with the rows anyway.
 * 2. Even with that fixed, sticky is bounded by its containing block: when the
 *    table ends, the header goes with it.
 *
 * Splitting the tables makes the behaviour structural: the header cannot scroll
 * because it is not inside a scroller.
 *
 * Both tables must share one column geometry, so the same `<colgroup>` is passed
 * to both, and both are `table-layout: fixed`. The horizontal scroll offset is
 * mirrored from the body to the header, so a sideways scroll does not shear the
 * header off its columns.
 */
function ScrollTable({ cols, head, rows, bodyClass }) {
  const headRef = useRef(null);
  const bodyRef = useRef(null);

  const onBodyScroll = useCallback(() => {
    if (headRef.current && bodyRef.current) {
      headRef.current.scrollLeft = bodyRef.current.scrollLeft;
    }
  }, []);

  return (
    <div className="tablewrap">
      <div className="thead" ref={headRef}>
        <table>
          <colgroup>{cols}</colgroup>
          <thead>
            <tr>{head}</tr>
          </thead>
        </table>
      </div>
      <div className={bodyClass ? `tbody ${bodyClass}` : 'tbody'} ref={bodyRef} onScroll={onBodyScroll}>
        <table>
          <colgroup>{cols}</colgroup>
          <tbody>{rows}</tbody>
        </table>
      </div>
    </div>
  );
}

/**
 * One row in the History tab: a single DELIVERY.
 *
 * Distinct from EventRow because the facts are different. A delivery has no Count
 * (it is one message), no State and no verdict — the trail is forked BEFORE the
 * fold runs, so at write time nothing has been decided about it. What it does carry
 * is the condition it belongs to (`key`), the producer's message id, and the broker
 * timestamp, which is when WE received it rather than when the source claims it
 * fired.
 */
function HistoryRow({ evt, onContextMenu }) {
  const label = evt.displaySeverity ?? evt.severity;
  return (
    <tr
      className={severityRowClass(colourSeverity(evt.severity, label))}
      onContextMenu={onContextMenu}
    >
      <td><Badge severity={evt.severity} label={label} /></td>
      {/* When WE received it. `evt.ts` (the source's own claim) is in the
          inspector: the two can disagree, and the broker timestamp is the one
          that decides ordering. */}
      <td className="mono">{formatClock(evt.brokerTs)}</td>
      <td className="summary">
        {evt.summary || <span className="dim">—</span>}
      </td>
      <td className="mono nowrap" title={evt.id ? `message id ${evt.id}` : undefined}>
        {evt.key}
      </td>
      <td />
    </tr>
  );
}

function AlertRow({ alert, ack, selected, onSelect, onContextMenu }) {
  const acked = Boolean(ack);
  // Colour follows what the badge shows, so the row tint and the badge cannot
  // disagree (see colourSeverity).
  const label = alert.displaySeverity ?? alert.event?.displaySeverity;
  return (
    <tr
      className={`${severityRowClass(colourSeverity(alert.severity, label), acked ? 'acked' : '')}${selected ? ' row-selected' : ''}`}
      onContextMenu={onContextMenu}
      onClick={onSelect}
      // Selection is pointer state, so the row is not focusable and carries no
      // button semantics: making a data row behave like a button breaks
      // screen-reader table navigation. Keyboard users get the same actions via
      // the context menu's Acknowledge/Information, and bulk actions are an
      // explicit affordance (see the selection bar) rather than a hidden mode.
      aria-selected={selected || undefined}
    >
      <td><Badge severity={alert.severity} label={label} /></td>
      {/* Order follows the live table: severity, first, last, count, summary.
          Key is last: it is a stable identifier, read once when you need it,
          not scanned down the column on every glance. */}
      <td className="mono">{formatClock(alert.firstSeen)}</td>
      <td className="mono">{formatClock(alert.lastSeen)}</td>
      <td className="num">{alert.count}</td>
      <td className="summary">{alert.summary ?? alert.msg ?? alert.event?.summary ?? <span className="dim">—</span>}</td>
      <td className="ackcell">
        {acked
          ? <span className="acked-tag" title={`${ack.by} at ${formatClock(ack.at)}`}>acked · {ack.by}</span>
          : <span className="dim">—</span>}
      </td>
      <td className="mono nowrap">{alert.key}</td>
      {/* Spacer cell, matching the spacer column in the header. */}
      <td />
    </tr>
  );
}

function EventRow({ evt, selected, onSelect, onContextMenu, showCount = false }) {
  // `evt` is a view event: `summary`, `msgCompleteString`, the occurrence fields
  // and the worker-enriched fields are already mapped by toViewEvent().
  //
  // NO DISCLOSURE FOLD IN THE SUMMARY COLUMN. It went through three stages and the
  // first two were both wrong:
  //
  //   1. A `details` toggle gated on `msgc` — never appeared for real producers,
  //      which do not send `EVT_Details`, so rows looked inert.
  //   2. A fallback to `JSON.stringify(evt.wire, null, 2)` — always truthy, so every
  //      row in every table grew a link (measured 211 of 211) that opened a raw
  //      debug dump.
  //   3. Removed entirely, on the operator's judgement that the column is for the
  //      summary and a fold inside a table cell is noise.
  //
  // The full record, including `EVT_Details`, is one right-click away in the
  // inspector's Information submenu. That is the single place to read a whole
  // record, and having a second partial one here was the mistake.
  const { summary } = evt;
  const retirement = isRetirement(evt.severity);
  // `retirement` stays keyed to the INTERNAL severity (it drives the dimming), but
  // the COLOUR class follows the displayed label so the row tint and the badge
  // never disagree.
  const rowClass = severityRowClass(colourSeverity(evt.severity, evt.displaySeverity),
    retirement ? 'retirement' : '');
  return (
    <tr
      className={`${rowClass}${selected ? ' row-selected' : ''}`}
      onClick={onSelect}
      onContextMenu={onContextMenu}
      // Selection is pointer state, so the row is not focusable and carries no
      // button semantics: making a data row behave like a button breaks
      // screen-reader table navigation. The same actions are on the context menu.
      //
      // `aria-selected` is only claimed when the row CAN be selected. The live tail
      // passes neither handler, and a row advertising a selection state it does not
      // support is a lie to a screen reader.
      aria-selected={onSelect ? Boolean(selected) : undefined}
    >
      <td><Badge severity={evt.severity} label={evt.displaySeverity} /></td>
      <td><AlertStateChip state={evt.alertState} closedAt={evt.alertClosedAt} /></td>
      <td className="mono">{formatClock(evt.firstOccurrence)}</td>
      <td className="mono">{formatClock(evt.lastOccurrence)}</td>
      {showCount ? (
        <td
          className="num"
          // The ALERT's rollup, not a per-event number: the `EVT_*` schema has no
          // Count field, so a per-event count would always read 1. `null` means the
          // alert hash is gone and we do not know, which is not the same as 0.
          title={evt.alertCount == null
            ? 'No alert state for this record — the alert hash has expired'
            : `This alert has been reported ${evt.alertCount} time(s)`}
        >
          {evt.alertCount == null ? <span className="dim">—</span> : evt.alertCount}
        </td>
      ) : null}
      <td className="summary">
        {summary || <span className="dim">—</span>}
      </td>
      {ENRICHED_FIELDS.map((f) => (
        <td key={f.key} className="mono dim nowrap">{evt[f.key] || '—'}</td>
      ))}
      {/* Spacer cell, matching the spacer column in the header. */}
      <td />
    </tr>
  );
}

/**
 * Open / closed chip for an EVENT row.
 *
 * An event record is immutable — it has no state of its own. "Open or closed" is a
 * property of the RECORD it belongs to (the 32-hex key), which the fold maintains on
 * `fw:alert:{key}`, and the gateway joins it onto each row as `alertState`.
 *
 * Three distinct meanings, and they must not look alike:
 *
 *   open            the alert this event belongs to is still firing
 *   closed          it was retired, and `closedAt` says when
 *   unknown         the alert hash has expired or was never seen
 *
 * `unknown` is deliberately not rendered as "closed". A missing hash means we do not
 * know, and colouring that as a resolution would claim the incident ended — the same
 * mistake as the connection chip's `unknown` state.
 *
 * Note the chip reports the alert's CURRENT state, not the state at the time of the
 * event. A CLEAR event from a week ago belongs to an alert that may have re-fired
 * since, so the row can read `open` while its own severity reads `CLEAR`. That is
 * correct: the chip answers "is this incident open now", which is the question an
 * operator scanning a log is asking.
 */
function AlertStateChip({ state, closedAt }) {
  if (state !== 'open' && state !== 'closed') {
    return (
      <Chip
        label="unknown"
        size="small"
        variant="outlined"
        title="No alert state for this record — the alert hash has expired, or this event never opened an alert"
        sx={{ opacity: 0.6 }}
      />
    );
  }
  const open = state === 'open';
  return (
    <Chip
      label={open ? 'open' : 'closed'}
      size="small"
      color={open ? 'error' : 'success'}
      variant={open ? 'filled' : 'outlined'}
      title={open
        ? 'This event belongs to an alert that is still open'
        : `This event belongs to an alert closed${closedAt ? ` ${formatClock(closedAt)}` : ''}. The event itself is unchanged — closing acts on the alert`}
    />
  );
}

function App() {
  const { config, error } = useConfig();
  const {
    status, events, alerts, acks, notice, bulk, deleteEvent,
    loadAllEvents, allEvents, loadHistory, history, dbUp,
    journals, loadJournal, addJournalEntry,
  } = useSocket(config);
  const { themePreference, darkMode, rowColor, setTheme, setRowColor, operatorName, setOperatorName } = usePreferences();
  // Which tab is showing. Persisted as a workspace preference, in the same spirit
  // as the theme: reopening the app should land where the operator left off.
  const [tab, setTabState] = useState(() => {
    const saved = readPref(TAB_KEY, 'alerts');
    return TABS.some((t) => t.id === saved) ? saved : 'alerts';
  });
  const setTab = useCallback((id) => {
    setTabState(id);
    writePref(TAB_KEY, id);
  }, []);
  // Pause freezes the RENDER, not the socket. The socket keeps draining and the
  // buffer keeps accumulating, so resuming shows everything that arrived while
  // paused instead of silently skipping a window of events.
  const [paused, setPaused] = useState(false);
  const frozenRef = useRef(null);
  const [inspector, setInspector] = useState(null);

  // Column widths are per table and persisted, so an operator tunes the layout
  // once. Each table owns its own state: sizing the alert table must not move
  // the live table's columns.
  const {
    widthOf: alertWidthOf,
    beginResize: beginAlertResize,
  } = useColumnWidths('alerts', ALERT_COLS, ALERT_DEFAULT_W);
  const {
    widthOf: eventWidthOf,
    beginResize: beginEventResize,
  } = useColumnWidths('events', EVENT_COLS, EVENT_DEFAULT_W);

  // Its own width state: the All Events table has the same columns as the live
  // tail but is a different table, and sizing one must not resize the other.
  const {
    widthOf: allWidthOf,
    beginResize: beginAllResize,
  } = useColumnWidths('all', ALL_EVENT_COLS, ALL_EVENT_DEFAULT_W);

  // Its own width state again: History has the same columns as All events but is a
  // different table, and sizing one must not resize the other.
  const {
    widthOf: historyWidthOf,
    beginResize: beginHistoryResize,
  } = useColumnWidths('history', HISTORY_COLS, HISTORY_DEFAULT_W);

  // All Events filter. The page is bounded, so filtering client-side keeps typing
  // instant.
  const [eventFilter, setEventFilter] = useState('');
  const [eventSeverity, setEventSeverity] = useState('');
  const allEventRows = useMemo(
    () => filterEvents(allEvents.events, eventFilter, eventSeverity),
    [allEvents.events, eventFilter, eventSeverity],
  );
  // Options come from the DATA, not a fixed list of eight severities: a window that
  // holds two events should not offer six options that match nothing.
  const eventSeverityOptions = useMemo(
    () => eventSeverities(allEvents.events),
    [allEvents.events],
  );

  // History filter. Same client-side reasoning as All events: the page is bounded
  // (1000 rows) so a round trip per keystroke would only make typing feel broken.
  const [historyFilter, setHistoryFilter] = useState('');
  const [historySeverity, setHistorySeverity] = useState('');
  const historyRows = useMemo(
    () => filterEvents(history.events, historyFilter, historySeverity),
    [history.events, historyFilter, historySeverity],
  );
  const historySeverityOptions = useMemo(
    () => eventSeverities(history.events),
    [history.events],
  );

  // Same contract for All Events: re-fetch on an explicit range change or refresh.
  //
  // Also fetched once on CONNECT, regardless of which tab is showing. The badge has
  // to be right before the tab is opened: deriving it from "rows I have fetched"
  // meant a browser refresh on another tab showed All events 0 while the window was
  // full, and only opening the tab corrected it. One bounded query per connection
  // is a fair price for a badge that is not lying.
  useEffect(() => {
    if (status !== 'connected') return;
    if (allEvents.loadedAt || allEvents.loading) return;
    loadAllEvents(allEvents.range);
  }, [status, allEvents.loadedAt, allEvents.loading, allEvents.range, loadAllEvents]);

  // History is fetched on first open ONLY, unlike All events.
  //
  // The two differ in cost and in how often they are read: All events is the log an
  // operator scans habitually, so its badge is kept honest from the first frame.
  // History is the deeper audit trail — a page of up to 1000 rows — and nobody needs
  // its badge before they open it. Fetching it on every connect would spend a large
  // read on a tab most sessions never touch.
  useEffect(() => {
    if (tab !== 'history') return;
    if (status !== 'connected') return;
    if (history.loadedAt || history.loading) return;
    loadHistory(history.range);
  }, [tab, status, history.loadedAt, history.loading, history.range, loadHistory]);

  // Snapshot on the transition into pause, so the frozen list is exactly what
  // was on screen at that moment and does not creep forward.
  if (!paused) frozenRef.current = events;
  const shownEvents = paused && frozenRef.current ? frozenRef.current : events;

  const openInspector = useCallback((e, record, title, actions = [], journalKey = null) => {
    e.preventDefault();
    // Opening the journal panel is a READ, so it is issued here rather than on the
    // submenu click: the note count is rendered on the menu item itself ("Journal
    // (3)"), and that number has to be there before the operator commits to the
    // click. One frame per open, not per row — the inspector only ever shows one.
    if (journalKey) loadJournal(journalKey);
    // Open at the pointer, then let the panel reposition itself once it has been
    // measured (see the clamp effect in RecordInspector). The size lives in the
    // stylesheet ONLY: duplicating 620x560 here meant two places to keep in sync,
    // and a stale copy silently pushed the menu off-screen.
    setInspector({
      record,
      title,
      actions,
      journal: journalKey
        ? {
          key: journalKey,
        }
        : null,
      x: e.clientX,
      y: e.clientY,
    });
  }, [loadJournal]);

  const closeInspector = useCallback(() => setInspector(null), []);

  // One theme instance per mode, not per render: MUI's ThemeProvider propagates
  // a new context value on identity change, so rebuilding it every render would
  // re-render every styled component on every socket frame.
  const theme = useMemo(() => buildTheme(darkMode ? 'dark' : 'light'), [darkMode]);

  const sortedAlerts = useMemo(
    () => [...alerts.values()].sort(
      (a, b) => (SEVERITY_RANK[a.severity] ?? 9) - (SEVERITY_RANK[b.severity] ?? 9)
        || b.lastSeen - a.lastSeen,
    ),
    [alerts],
  );

  // ------------------------------------------------------------- selection
  // Selection is deliberately client-side only: it is a transient pointer state,
  // not a fact about the alert, so persisting it (or syncing it) would only create
  // disagreement between operators.
  //
  // Two independent selections, one per table, via the same hook. They are not
  // shared: selecting an alert and selecting an event are different intents, and a
  // single set keyed by two different id spaces would collide on nothing while
  // making every consumer filter a mixed set.
  const alertRowKeys = useMemo(() => sortedAlerts.map((a) => a.key), [sortedAlerts]);
  const eventRowKeys = useMemo(() => allEventRows.map(allEventRowKey), [allEventRows]);
  const alertsSel = useRowSelection(alertRowKeys);
  const eventsSel = useRowSelection(eventRowKeys);
  const { selected } = alertsSel;
  const selectedAlertKeys = alertsSel.liveKeys;

  if (error) {
    return <main><h1>Fire-Watch</h1><p className="error">Cannot load config.json: {error}</p></main>;
  }
  if (!config) return <main><h1>Fire-Watch</h1><p className="dim">Loading…</p></main>;

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ display: 'flex', flexDirection: 'column', height: '100vh',
                 overflow: 'hidden' }} className={rowColor ? 'color-rows' : ''}>
        <AppBar position="static">
          <Toolbar variant="dense">
            <Typography variant="h1" sx={{ mr: 1.5 }}>Fire-Watch</Typography>
            {/*
              Two chips joined by a line: connection → database.

              They are separate because they fail separately — the socket can be
              perfectly healthy while the gateway's reads fail, which is a stale
              board with a green chip. The line between them is the COMBINED state,
              so "everything is fine" is one glance rather than two readings.

              The line pulses only when it is green: a slow steady pulse reads as
              "live", while a flashing red would be noise during exactly the incident
              where an operator needs to concentrate. Motion is off entirely under
              `prefers-reduced-motion`.
            */}
            {/* alignItems goes in `sx`, not as a prop: MUI v9's Stack silently
                IGNORES the alignItems prop (computed style stays `normal`), which
                left the health link and every control row vertically stretched.
                spacing and direction still work. */}
            <Stack direction="row" spacing={0} sx={{ alignItems: 'center', mr: 1 }}>
              <Chip
                label={status}
                size="small"
                color={status === 'connected' ? 'success'
                  : status === 'misconfigured' ? 'error' : 'warning'}
                variant={status === 'connected' ? 'filled' : 'outlined'}
              />
              <HealthLink socket={status} db={dbUp} />
              <Chip
                label={dbUp === null ? 'database ?' : dbUp ? 'database' : 'database down'}
                size="small"
                color={dbUp === null ? 'default' : dbUp ? 'success' : 'error'}
                variant={dbUp === true ? 'filled' : 'outlined'}
                title={dbUp === null
                  ? 'The gateway has not reported database health yet'
                  : dbUp
                    ? 'The gateway can read and write Dragonfly'
                    : 'The gateway cannot reach Dragonfly — the board may be stale'}
              />
            </Stack>
            {notice ? (
              <MuiAlert severity="info" sx={{ py: 0, flex: '0 1 auto',
                                              '& .MuiAlert-message': { py: .25 } }}>
                {notice}
              </MuiAlert>
            ) : null}
            <Box sx={{ flex: 1 }} />
            {themePreference === 'auto' ? null : (
              <Button onClick={() => setTheme('auto')}
                      title="Follow the operating system">auto</Button>
            )}
            <Tooltip title={darkMode ? 'Switch to light mode' : 'Switch to dark mode'}>
              <IconButton size="small" color="inherit"
                          onClick={() => setTheme(darkMode ? 'light' : 'dark')}
                          aria-label={darkMode ? 'Switch to light mode' : 'Switch to dark mode'}>
                {darkMode ? <LightModeIcon fontSize="small" /> : <DarkModeIcon fontSize="small" />}
              </IconButton>
            </Tooltip>
            <Tooltip title={rowColor ? 'Severity row colours: on' : 'Severity row colours: off'}>
              <IconButton size="small"
                          color={rowColor ? 'primary' : 'inherit'}
                          onClick={() => setRowColor(!rowColor)}
                          aria-label="Toggle severity row colours">
                <PaletteIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          </Toolbar>

          {/* Tabs live in the AppBar so the header and the view selector are one
              block, not two stacked bars eating vertical space. `value` is the
              tab id, so the mapping from state to selection is direct. */}
          <Tabs
            value={tab}
            onChange={(_, v) => setTab(v)}
            variant="scrollable"
            scrollButtons="auto"
            aria-label="Views"
          >
            {TABS.map((t) => {
              // Every tab has its own branch. The old form was a chain of ternaries
              // ending in a closed-alert count, so a NEW tab silently inherited it —
              // which is why "All events" once showed a number with an empty table.
              const count = tabBadgeCount(t.id, {
                alerts: sortedAlerts.length,
                events: shownEvents.length,
                // Server window total + rows appended since that query. Deriving the
                // badge from the fetched rows alone made it read 0 after a refresh on
                // another tab; incrementing it per frame made it outrun the table
                // (badge 464 vs 396 rows) because absorbed repeats added to the count
                // but not to the list.
                allEvents: allEvents.total === null
                  ? allEvents.events.length
                  : allEvents.total + (allEvents.windowAdded ?? 0),
                // Not asked yet is NOT zero: `total` stays null until the first
                // query answers, so the badge shows the rows it holds (none) rather
                // than claiming the trail is empty.
                history: history.total === null
                  ? history.events.length
                  : history.total,
              });
              return (
                <Tab
                  key={t.id}
                  value={t.id}
                  id={`tab-${t.id}`}
                  aria-controls={`panel-${t.id}`}
                  label={(
                    <Stack direction="row" spacing={0.75} sx={{ alignItems: 'center' }}>
                      <span>{t.label}</span>
                      <Chip label={count} size="small" variant="outlined" />
                    </Stack>
                  )}
                />
              );
            })}
          </Tabs>
        </AppBar>

      {tab === 'alerts' ? (
      <Box role="tabpanel" id="panel-alerts" aria-labelledby="tab-alerts"
           sx={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column',
                 p: 2, pt: 1.5 }}>
        {/* No heading and no count chip: the tab strip sits directly above and its
            badge already carries both the name and the number. The row held
            nothing else, so it is gone rather than left as an empty 40px band of
            vertical space above the data. */}
        {/* No selection banner by design. Selecting rows is purely a pointer
            state; every action lives in the right-click menu, which is where the
            operator already goes for a single row. A toolbar that appears on
            selection duplicates those actions and shifts the table down the page
            at the exact moment the operator is aiming at it. */}
        {sortedAlerts.length === 0
          ? <p className="dim">No open alerts.</p>
          : (
            <PaneBoundary name="Open alerts">
            <ScrollTable
              cols={(
                <>
                  {ALERT_COLS.map((c, i) => (
                    <col key={c} style={{ width: `${alertWidthOf(i)}px` }} />
                  ))}
                  {/* Spacer: absorbs leftover width so the table still fills the
                      pane, instead of stretching the real columns. */}
                  <col />
                </>
              )}
              head={(
                <>
                  {ALERT_COLS.map((label, i) => (
                    <ResizableTh
                      key={label}
                      index={i}
                      width={alertWidthOf(i)}
                      onBegin={beginAlertResize}
                    >
                      {label}
                    </ResizableTh>
                  ))}
                  {/* Spacer header, aligned with the spacer col. */}
                  <th />
                </>
              )}
              rows={sortedAlerts.map((a) => {
                const ack = acks.get(a.key);
                // Right-clicking inside a multi-row selection acts on the whole
                // selection, not just the row under the pointer: otherwise the
                // menu would silently contradict what the board shows as
                // selected. Right-clicking outside it targets that row alone.
                const isSelected = selected.has(a.key);
                const targets = isSelected && selectedAlertKeys.length > 1
                  ? selectedAlertKeys
                  : [a.key];
                const ackTargets = targets.filter((k) => !acks.has(k));
                return (
                  <AlertRow
                    key={a.key}
                    alert={a}
                    ack={ack}
                    selected={isSelected}
                    onSelect={(e) => alertsSel.selectRow(a.key, e)}
                    onContextMenu={(e) => openInspector(
                      e,
                      a.event?.wire ?? a.event ?? a,
                      targets.length > 1 ? `${targets.length} alerts` : a.key,
                      [
                        {
                          label: ackTargets.length > 1
                            ? `Acknowledge ${ackTargets.length}`
                            : (ack ? `Acked by ${ack.by}` : 'Acknowledge'),
                          disabled: ackTargets.length === 0,
                          title: ackTargets.length === 0
                            ? 'Already acknowledged — the ack survives until the alert re-fires'
                            : 'Claim these alerts; an ack clears when the alert re-fires',
                          run: () => bulk('ack', ackTargets),
                        },
                        {
                          label: targets.length > 1 ? `Close ${targets.length}` : 'Close',
                          disabled: false,
                          title: 'Retire these alerts. Same end state as a CLOSE event from the source',
                          run: () => bulk('close', targets),
                        },
                        // NO Delete on an alert row.
                        //
                        // It "worked" — the record went — but nothing visible changed: the
                        // alert stayed, its Count stayed, and one record disappeared from a
                        // list the operator was not looking at. An alert with Count 113 has
                        // 113 records, all identical, and deleting one of them is not an
                        // action anyone means by "delete".
                        //
                        // The verb that removes an incident is Close, and the verb that
                        // removes a RECORD lives on All events, where a row IS one record.
                        // Keeping Delete here offered an action whose effect was invisible
                        // and whose scope was surprising — worse than not offering it.
                        //
                        // (History: it was removed once for this reason, briefly restored,
                        // and is now removed again with the evidence: All events 219 -> 218
                        // and no other change on screen.)
                      ],
                      // The journal belongs to the INCIDENT, so a multi-row selection
                      // gets no journal: one panel cannot annotate several alerts at
                      // once, and silently picking the first would hide the others.
                      targets.length === 1 ? a.key : null,
                    )}
                  />
                );
              })}
            />
            </PaneBoundary>
          )}
      </Box>
      ) : null}

      {tab === 'events' ? (
      <Box role="tabpanel" id="panel-events" aria-labelledby="tab-events"
           sx={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column',
                 p: 2, pt: 1.5 }}>
        {/* No heading: the tab above names the pane. The chip stays because
            "75 of last 500" carries the buffer LIMIT, which the badge does not
            show — it is the difference between "all of it" and "the newest
            slice", and that distinction matters when reading a tail. */}
        <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 1 }}>
          <Chip label={`${shownEvents.length} of last ${EVENT_LIMIT}`} size="small" variant="outlined" />
          {paused ? <Chip label="paused" size="small" color="warning" /> : null}
          <Box sx={{ flex: 1 }} />
          <Button
            variant={paused ? 'contained' : 'outlined'}
            color={paused ? 'warning' : 'primary'}
            startIcon={paused ? <PlayArrowIcon /> : <PauseIcon />}
            onClick={() => setPaused((p) => !p)}
          >
            {paused ? 'Resume' : 'Pause'}
          </Button>
        </Stack>
        <Box sx={{ flex: 1, minHeight: 0, overflow: 'hidden',
                   display: 'flex', flexDirection: 'column' }} id="live-events-panel">
          {shownEvents.length === 0
            ? <p className="dim">Nothing yet.</p>
            : (
              <PaneBoundary name="Live events">
              <ScrollTable
                cols={(
                  <>
                    {EVENT_COLS.map((c, i) => (
                      <col key={c} style={{ width: `${eventWidthOf(i)}px` }} />
                    ))}
                    {ENRICHED_FIELDS.map((f) => (
                      <col key={f.key} />
                    ))}
                    {/* Spacer, same reasoning as the alert table. */}
                    <col />
                  </>
                )}
                head={(
                  <>
                    {EVENT_COLS.map((label, i) => (
                      <ResizableTh
                        key={label}
                        index={i}
                        width={eventWidthOf(i)}
                        onBegin={beginEventResize}
                      >
                        {label}
                      </ResizableTh>
                    ))}
                    {ENRICHED_FIELDS.map((f) => <th key={f.key}>{f.label}</th>)}
                    {/* Spacer header, aligned with the spacer col. */}
                    <th />
                  </>
                )}
                rows={[...shownEvents].reverse().map((e) => {
                  // The live tail's state comes from the client's OWN alert map, not
                  // from the frame: `op: event` carries no alertState (the gateway only
                  // joins it onto query replies), so relying on the field left every
                  // tail row reading `unknown` while the board was visibly firing.
                  //
                  // The client already knows: `alerts` is the open set, maintained from
                  // the fold's own frames. Absent means the alert is not open — which is
                  // `closed`, not `unknown`, because this client has been tracking the
                  // fold and would hold the row if it were open. `unknown` stays reserved
                  // for "we have never heard of this alert", i.e. a gap in this client's
                  // own knowledge rather than a state it can vouch for.
                  const alertKey = e.key ?? null;
                  const liveState = e.alertState
                    ?? (alertKey && alerts.has(alertKey) ? 'open' : null);
                  return (
                    <EventRow
                      // The LIVE TAIL keys on the ARRIVAL, so two deliveries of
                      // one record are told apart by `brokerTs`. `allEventRowKey`
                      // is the same composition All events uses — one helper
                      // rather than three copies, which is how `id` here and `key`
                      // there drifted apart in the first place.
                      key={allEventRowKey(e)}
                      evt={liveState === e.alertState ? e : { ...e, alertState: liveState }}
                      // No onContextMenu: the live tail is a DIAGNOSTIC view, not a
                      // working surface. Every action on it — ack, close, delete,
                      // journal — belongs to the alert or the stored record, and the
                      // operator reaches those from Open alerts and All events, where
                      // the row is the thing being acted on. Three action surfaces for
                      // one row was three places to keep in step for no gain.
                    />
                  );
                })}
              />
              </PaneBoundary>
            )}
        </Box>
      </Box>
      ) : null}

      {tab === 'all' ? (
      <Box role="tabpanel" id="panel-all" aria-labelledby="tab-all"
           sx={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column',
                 p: 2, pt: 1.5 }}>
        <Stack direction="row" spacing={1}
               sx={{ alignItems: 'center', mb: 1, flexWrap: 'wrap', useFlexGap: true }}>
          {/* No heading: the tab strip directly above already names the pane, and
              the tab badge carries its count. A heading repeating the selected
              tab is the one control on screen that tells the operator nothing. */}
          <FormControl sx={{ width: 170, flex: '0 0 auto' }}>
            <InputLabel id="all-range-label">Window</InputLabel>
            <Select
              labelId="all-range-label"
              label="Window"
              value={allEvents.range}
              onChange={(e) => loadAllEvents(e.target.value)}
            >
              {ALL_RANGES.map((r) => (
                <MenuItem key={r.id} value={r.id}>{r.label}</MenuItem>
              ))}
            </Select>
          </FormControl>
          {/* The filter sits on the SAME row as the window picker: both narrow what
              the table shows, and stacking them put two sparse control rows above
              the data on a screen whose whole job is showing rows. The `flex: 1`
              with a `maxWidth` lets it absorb the free space up to a readable
              width, pushing the refresh button to the right edge. */}
          {/* Severity picker FIRST on the row, then free text. Severity is the coarse
              cut an operator makes before typing anything — "only the criticals in
              the last 24 hours" — and putting it after the search box made it read
              as an afterthought, below the fold on a narrow window. */}
          <FormControl sx={{ width: 150, flex: '0 0 auto' }}>
            <InputLabel id="evt-sev-label">Severity</InputLabel>
            <Select
              labelId="evt-sev-label"
              label="Severity"
              value={eventSeverity}
              onChange={(e) => setEventSeverity(e.target.value)}
            >
              <MenuItem value="">All severities</MenuItem>
              {eventSeverityOptions.map((sev) => (
                <MenuItem key={sev} value={sev}>{sev}</MenuItem>
              ))}
            </Select>
          </FormControl>
          <TextField
            type="search"
            value={eventFilter}
            onChange={(e) => setEventFilter(e.target.value)}
            placeholder="Filter by key, device, severity, summary, agent…"
            inputProps={{ 'aria-label': 'Filter all events' }}
            sx={{ flex: '1 1 260px', minWidth: 0, maxWidth: 460 }}
          />
          <Tooltip title={allEvents.loading ? 'Loading…' : 'Re-read this window'}>
            {/* marginLeft:auto, not a flex spacer: a spacer Box and the search
                field both carrying flex:1 SPLIT the free width between them, so
                the field stayed 228px wide next to a 240px void. `auto` on the
                last item absorbs whatever the capped field leaves over. */}
            <span style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center' }}>
              {/* Icon-only and compact: the tab strip already says what the pane is,
                  so a labelled filled button was the loudest control on the screen
                  for a read-only refresh. `span` wrapper because a disabled
                  IconButton swallows pointer events and the tooltip would stop
                  showing. */}
              <IconButton
                size="small"
                onClick={() => loadAllEvents(allEvents.range)}
                disabled={allEvents.loading}
                aria-label="Reload events"
                sx={{ flex: '0 0 auto' }}
              >
                {allEvents.loading
                  ? <CircularProgress size={16} />
                  : <RefreshIcon fontSize="small" />}
              </IconButton>
            </span>
          </Tooltip>
        </Stack>

        {allEvents.error ? (
          <MuiAlert severity="error">Events failed: {allEvents.error}</MuiAlert>
        ) : allEvents.loading && allEvents.events.length === 0 ? (
          <Stack direction="row" spacing={1} sx={{ alignItems: 'center', p: 2 }}>
            <CircularProgress size={16} />
            <Typography variant="body2" color="text.secondary">Loading…</Typography>
          </Stack>
        ) : allEvents.events.length === 0 ? (
          <Typography variant="body2" color="text.secondary">
            {allEvents.loadedAt
              // A completed query that returned nothing: the window really is quiet.
              ? 'No events in this window.'
              // Nothing has ARRIVED since the page loaded either, so the first
              // query is still pending. Saying "No events" here would be a claim
              // we have not verified — this pane has not read storage yet.
              : 'Waiting for the first events, or open this tab to read the window.'}
          </Typography>
        ) : allEventRows.length === 0 ? (
          <Typography variant="body2" color="text.secondary">
            No events match the {eventFilter.trim() && eventSeverity
              ? 'filter and severity'
              : eventSeverity ? 'selected severity' : 'filter'}.
          </Typography>
        ) : (
          <Box sx={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}>
            {allEvents.truncated ? (
              <MuiAlert severity="warning" sx={{ mb: 1 }}>
                {/* State the arithmetic. The badge counts the WINDOW; this table
                    holds the newest slice of it, so a large window legitimately shows
                    fewer rows. Saying only "showing the newest N" left the reader to
                    work out why the two numbers disagreed. */}
                Showing the newest {allEvents.events.length} of{' '}
                {(allEvents.total ?? 0) + (allEvents.windowAdded ?? 0)} events in this
                window — narrow the window or the filter to see older ones.
              </MuiAlert>
            ) : null}
            <PaneBoundary name="All events">
              <ScrollTable
                cols={(
                  <>
                    {ALL_EVENT_COLS.map((c, i) => (
                      <col key={c} style={{ width: `${allWidthOf(i)}px` }} />
                    ))}
                    {ENRICHED_FIELDS.map((f) => <col key={f.key} />)}
                    <col />
                  </>
                )}
                head={(
                  <>
                    {ALL_EVENT_COLS.map((label, i) => (
                      <ResizableTh
                        key={label}
                        index={i}
                        width={allWidthOf(i)}
                        onBegin={beginAllResize}
                      >
                        {label}
                      </ResizableTh>
                    ))}
                    {ENRICHED_FIELDS.map((f) => <th key={f.key}>{f.label}</th>)}
                    <th />
                  </>
                )}
                rows={allEventRows.map((e) => {
                  // Same key the selection set uses, so a row and its selection
                  // entry cannot describe different records.
                  const rowKey = allEventRowKey(e);
                  const isSelected = eventsSel.selected.has(rowKey);
                  // Right-clicking inside a multi-row selection acts on the whole
                  // selection: otherwise the menu would contradict what the board
                  // shows as selected. Outside it, the menu targets that row alone.
                  const targets = isSelected && eventsSel.liveKeys.length > 1
                    ? eventsSel.liveKeys
                    : [rowKey];
                  // A record key, NOT a message id.
                  //
                  // `op: delete` addresses `event:<key>` — one record per
                  // CONDITION — so the gateway validates the id as a 32-hex record
                  // key and rejects anything else with `bad_id`. A live row carries
                  // BOTH: `id` is the message id the producer sent and `key` is the
                  // record. Forwarding `id` therefore failed for every row that had
                  // not been re-read from a query, which is every row in a session
                  // that started from the live tail.
                  //
                  // Falling back to `id` keeps a row from an older worker (no `key`
                  // field) from becoming undeletable.
                  const ids = targets
                    .map((k) => {
                      const row = allEventRows.find((x) => allEventRowKey(x) === k);
                      return row?.key ?? row?.id;
                    })
                    .filter(Boolean);
                  // Events selected -> the distinct alerts they belong to.
                  //
                  // One RECORD per condition; several rows can share one, which is
                  // exactly what content dedup records. So a selection of 6 events can
                  // legitimately touch 4 alerts, and the two menu items then quote
                  // different numbers. That is correct and it looked like a bug, so the
                  // Close label now states the collapse explicitly rather than leaving
                  // the operator to reconcile two counts.
                  const selectedEvents = targets
                    .map((k) => allEventRows.find((x) => allEventRowKey(x) === k))
                    .filter(Boolean);
                  const alertKeys = [...new Set(
                    selectedEvents
                      .map((x) => x.key)
                      .filter(Boolean),
                  )];
                  const collapsing = targets.length > alertKeys.length;
                  // Fill the alert-derived columns from the client's OWN alert map
                  // when the row came from a live frame rather than a query.
                  //
                  // `op: event` carries no alert state — only `op: allEvents` replies
                  // are enriched — so a row that arrived live showed `—` for Count and
                  // could show a stale State, while a queried row for the same alert
                  // showed the real numbers. Measured: the same 3-occurrence alert read
                  // `3` from a query and `—` after one live arrival.
                  //
                  // The client already holds this: `alerts` is maintained from the
                  // fold's own frames. Server-supplied values win where present, so a
                  // query reply is still authoritative.
                  const alertKeyForRow = e.key ?? null;
                  const mapRow = alertKeyForRow ? alerts.get(alertKeyForRow) : null;
                  const enriched = e.alertCount == null && mapRow
                    ? {
                      ...e,
                      alertCount: mapRow.count ?? null,
                      alertState: e.alertState ?? (alerts.has(alertKeyForRow) ? 'open' : null),
                      alertClosedAt: e.alertClosedAt ?? mapRow.lastSeen ?? null,
                    }
                    : e;
                  return (
                    <EventRow
                      // Id plus brokerTs: a stored event id is unique, but the same
                      // rule as the live tail keeps the two tables consistent.
                      key={rowKey}
                      evt={enriched}
                      selected={isSelected}
                      onSelect={(ev) => eventsSel.selectRow(rowKey, ev)}
                      showCount
                      onContextMenu={(ev) => openInspector(
                        ev,
                        e.wire ?? e,
                        // State BOTH counts when they differ. "6 events" alone invites
                        // the reader to expect 6 of everything; the menu then quotes 4
                        // for Close and looks broken.
                        targets.length > 1
                          ? (collapsing
                            ? `${targets.length} events · ${alertKeys.length} alerts`
                            : `${targets.length} events`)
                          : `${e.source} · ${e.type}`,
                        [
                          {
                            // Close the ALERT this event belongs to. Offered here because
                            // the operator is looking at an arrival and may be concluding
                            // the incident from it — forcing a tab switch to Open alerts to
                            // act on the same record is friction for no reason.
                            //
                            // The key is the PAIR, so a multi-row selection collapses to the
                            // distinct alerts it touches: closing the same alert twice is one
                            // close, and the label reports the deduplicated count so the
                            // operator is not told they are closing five when they are
                            // closing two.
                            label: alertKeys.length > 1
                              ? (collapsing
                                ? `Close ${alertKeys.length} alerts (${targets.length} events)`
                                : `Close ${alertKeys.length} alerts`)
                              : 'Close alert',
                            disabled: alertKeys.length === 0,
                            title: collapsing
                              ? `These ${targets.length} events belong to ${alertKeys.length} alert(s); one close per alert. Closing is not deleting — the events stay.`
                              : 'Retire the alert these events belong to. Same end state as a CLOSE event from the source',
                            run: () => bulk('close', alertKeys),
                          },
                          {
                            label: ids.length > 1
                              ? `Delete ${ids.length} events`
                              : 'Delete event',
                            disabled: ids.length === 0,
                            title: ids.length > 1
                              ? `Permanently delete these ${ids.length} event records. One per event, because a record IS an event — unlike Close, which acts on the alert.`
                              : `Permanently delete event ${e.id}`,
                            run: () => { for (const id of ids) deleteEvent(id); },
                            danger: true,
                          },
                        ],
                        // One journal per incident, so a multi-row selection has no
                        // single key to open. Silently picking the first would hide
                        // the others.
                        targets.length === 1 && e.key
                          ? e.key
                          : null,
                      )}
                    />
                  );
                })}
              />
            </PaneBoundary>
          </Box>
        )}
      </Box>
      ) : null}

      {tab === 'history' ? (
      <Box role="tabpanel" id="panel-history" aria-labelledby="tab-history"
           sx={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column',
                 p: 2, pt: 1.5 }}>
        {/* The AUDIT trail: one row per DELIVERY, newest first.
            It answers questions no other tab can, because every other tab lists
            CONDITIONS (a record is updated in place):
              "how many times did this actually fire?"
              "did the broker deliver it twice?"  (a rejected row)
              "what exactly did the source send at 02:14?"
            Retention is 3 days, so the range options stop there. */}
        <Stack direction="row" spacing={1}
               sx={{ alignItems: 'center', mb: 1, flexWrap: 'wrap', useFlexGap: true }}>
          <FormControl sx={{ width: 170, flex: '0 0 auto' }}>
            <InputLabel id="hist-range-label">Window</InputLabel>
            <Select
              labelId="hist-range-label"
              label="Window"
              value={history.range}
              onChange={(e) => loadHistory(e.target.value)}
            >
              {HISTORY_RANGES.map((r) => (
                <MenuItem key={r.id} value={r.id}>{r.label}</MenuItem>
              ))}
            </Select>
          </FormControl>
          <FormControl sx={{ width: 150, flex: '0 0 auto' }}>
            <InputLabel id="hist-sev-label">Severity</InputLabel>
            <Select
              labelId="hist-sev-label"
              label="Severity"
              value={historySeverity}
              onChange={(e) => setHistorySeverity(e.target.value)}
            >
              <MenuItem value="">All severities</MenuItem>
              {historySeverityOptions.map((sev) => (
                <MenuItem key={sev} value={sev}>{sev}</MenuItem>
              ))}
            </Select>
          </FormControl>
          <TextField
            type="search"
            value={historyFilter}
            onChange={(e) => setHistoryFilter(e.target.value)}
            placeholder="Filter by key, device, severity, summary…"
            inputProps={{ 'aria-label': 'Filter history' }}
            sx={{ flex: '1 1 240px', minWidth: 0, maxWidth: 420 }}
          />
          <Tooltip title={history.loading ? 'Loading…' : 'Re-read this window'}>
            <span style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center' }}>
              <IconButton
                size="small"
                onClick={() => loadHistory(history.range)}
                disabled={history.loading}
                aria-label="Reload history"
                sx={{ flex: '0 0 auto' }}
              >
                {history.loading
                  ? <CircularProgress size={16} />
                  : <RefreshIcon fontSize="small" />}
              </IconButton>
            </span>
          </Tooltip>
        </Stack>

        {history.error ? (
          <MuiAlert severity="error">History failed: {history.error}</MuiAlert>
        ) : history.loading && history.events.length === 0 ? (
          <Stack direction="row" spacing={1} sx={{ alignItems: 'center', p: 2 }}>
            <CircularProgress size={16} />
            <Typography variant="body2" color="text.secondary">Loading…</Typography>
          </Stack>
        ) : history.events.length === 0 ? (
          <Typography variant="body2" color="text.secondary">
            {history.loadedAt
              ? 'No arrivals in this window.'
              : 'Waiting for the first arrivals, or open this tab to read the trail.'}
          </Typography>
        ) : historyRows.length === 0 ? (
          <Typography variant="body2" color="text.secondary">
            No arrivals match the filter.
          </Typography>
        ) : (
          <Box sx={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}>
            {history.truncated ? (
              <MuiAlert severity="warning" sx={{ mb: 1 }}>
                Showing the newest {history.events.length} of {history.total ?? 0}
                {' '}arrivals in this window — narrow the window or the filter to see
                older ones.
              </MuiAlert>
            ) : null}
            <PaneBoundary name="History">
              <ScrollTable
                cols={(
                  <>
                    {HISTORY_COLS.map((c, i) => (
                      <col key={c} style={{ width: `${historyWidthOf(i)}px` }} />
                    ))}
                    <col />
                  </>
                )}
                head={(
                  <>
                    {HISTORY_COLS.map((label, i) => (
                      <ResizableTh
                        key={label}
                        index={i}
                        width={historyWidthOf(i)}
                        onBegin={beginHistoryResize}
                      >
                        {label}
                      </ResizableTh>
                    ))}
                    <th />
                  </>
                )}
                rows={historyRows.map((evt, i) => (
                  <HistoryRow
                    // The DELIVERY id: unique per row, so a condition that fired
                    // twice cannot collapse into one rendered row.
                    key={evt.deliveryId || `${evt.key}|${i}`}
                    evt={evt}
                    // Read-only: an audit row cannot be acked, closed or deleted —
                    // those act on the CONDITION, which lives on the other tabs.
                    // The inspector is offered because reading the whole payload of
                    // a specific delivery is the one action that belongs here.
                    onContextMenu={(ev) => openInspector(
                      ev,
                      evt.wire ?? evt,
                      `${evt.source || 'event'} · ${evt.id || 'delivery'}`,
                      [],
                      evt.key || null,
                    )}
                  />
                ))}
              />
            </PaneBoundary>
          </Box>
        )}
      </Box>
      ) : null}
      </Box>

      <RecordInspector
        state={inspector}
        // Resolved at RENDER, not captured when the panel opened. The inspector
        // holds a snapshot of `journal` from the open call, so an entry appended
        // while it is open would never appear — the stale-copy bug a captured
        // `loadedAt` caused on a query-backed tab.
        journal={inspector?.journal
          ? {
            key: inspector.journal.key,
            entries: journals.get(inspector.journal.key),
            loading: journals.get(inspector.journal.key) === undefined,
            operatorName,
            setOperatorName,
            // Built at RENDER, not when the panel opened. Capturing it in
            // `openInspector` meant the closure held whatever `operatorName` was at
            // open time — so setting your name inside the panel and then writing a
            // note still stored it as "anonymous".
            onAdd: (text) => addJournalEntry(inspector.journal.key, text, operatorName),
          }
          : null}
        onClose={closeInspector}
      />
    </ThemeProvider>
  );
}

/**
 * Error boundary: contain a render failure instead of letting it blank the app.
 *
 * A throw during render unmounts the ENTIRE React tree, so one malformed field
 * anywhere turned the whole page black — which reads as "the app crashed" and is
 * only recoverable by a browser refresh. That is exactly what happened when a
 * content-duplicate frame created a row with no severity and `Badge` called
 * `toLowerCase` on `undefined` (docs/frontend.md §5.3).
 *
 * The specific bug is fixed at its source, but the CLASS of bug must not be able
 * to take the page down again: a monitoring UI that goes blank during an incident
 * is worse than one that shows an error for the part that failed. So each table
 * is wrapped separately, meaning a broken row costs one pane and the rest of the
 * board stays usable.
 *
 * Deliberately not `componentDidCatch` + retry: silently re-rendering the same
 * bad data would loop. The panel reports and waits for the next state change.
 */
class PaneBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { error: null };
  }

  static getDerivedStateFromError(error) {
    return { error };
  }

  componentDidCatch(error, info) {
    // Kept in the console on purpose: without it the UI would be the only
    // evidence and the stack would be lost.
    console.error(`render failed in ${this.props.name}`, error, info?.componentStack);
  }

  render() {
    if (this.state.error) {
      return (
        <div className="pane-error" role="alert">
          <strong>{this.props.name} failed to render.</strong>
          <span className="dim"> {String(this.state.error?.message || this.state.error)}</span>
          <div className="dim">The rest of the board is still live. Reload to retry this panel.</div>
        </div>
      );
    }
    return this.props.children;
  }
}

createRoot(document.getElementById('root')).render(
  <PaneBoundary name="Fire-Watch">
    <App />
  </PaneBoundary>,
);
