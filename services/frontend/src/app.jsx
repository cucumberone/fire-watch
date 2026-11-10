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
  resolveWsUrl,
  decodePayload,
  toViewEvent,
  applyAlert,
  insertEvent,
  backoffDelay,
  isRetirement,
  resolveTheme,
  severityRowClass,
  recordEntries,
  formatClock,
  ENRICHED_FIELDS,
  SEVERITY_RANK,
} from './lib.js';

const EVENT_LIMIT = 500;
const MSG_MAX_BYTES = 1048576;
// One `op: alerts` frame asks about this many keys. Matches the gateway's
// FW_MAX_ALERT_QUERY limit so a large alert table degrades to "acks refresh a
// little later" instead of erroring (docs/acknowledge.md §5).
const MAX_ACK_KEYS = 200;

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
// The enriched columns are fixed-width labels, so only the leading five are
// user-resizable; ENRICHED_FIELDS is appended after them.
const EVENT_COLS = ['Severity', 'First occurrence', 'Last occurrence', 'Count', 'Summary'];

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
const EVENT_DEFAULT_W = [96, 172, 172, 64, 340];

// ------------------------------------------------------------ UI preferences
// Persisted in localStorage so a reload keeps them. `auto` is stored as the
// ABSENCE of a key, so a later OS change still takes effect.
const THEME_KEY = 'fw:theme';
const ROW_COLOR_KEY = 'fw:rowColor';

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

  return { themePreference, darkMode, rowColor, setTheme, setRowColor };
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

function useColumnWidths(table, defaults) {
  const [overrides, setOverrides] = useState(() => readWidths(table));

  // Effective width for a column: the operator's value, else the default.
  const widthOf = useCallback(
    (index) => Math.max(MIN_COL_WIDTH, overrides[index] ?? defaults[index] ?? MIN_COL_WIDTH),
    [overrides, defaults],
  );

  const beginResize = useCallback((index) => (event) => {
    event.preventDefault();
    event.stopPropagation();
    const startX = event.clientX;
    const startWidth = widthOf(index);

    // Listeners on the document, not the handle: the pointer leaves the 7px
    // handle almost immediately, and a handle-local listener would stop tracking.
    const onMove = (e) => {
      const next = Math.max(MIN_COL_WIDTH, startWidth + (e.clientX - startX));
      setOverrides((prev) => ({ ...prev, [index]: next }));
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
  }, [table, widthOf]);

  const reset = useCallback(() => {
    setOverrides({});
    writeWidths(table, {});
  }, [table]);

  return { widthOf, beginResize, reset };
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
  const [events, setEvents] = useState([]);
  const [alerts, setAlerts] = useState(new Map());
  // Ack state is keyed by alert key and lives apart from the alert rows: the
  // rows are derived from the fold verdicts, while acks are an operator fact
  // that arrives on its own frames (docs/acknowledge.md §5).
  const [acks, setAcks] = useState(new Map());
  const [notice, setNotice] = useState(null);

  const wsRef = useRef(null);
  const attemptRef = useRef(0);
  const timerRef = useRef(null);
  const lastTsRef = useRef(null);
  const closedByUsRef = useRef(false);
  // Alert keys we currently display, so a reconnect can re-read their ack state.
  const knownKeysRef = useRef([]);

  const ingest = useCallback(async (frame) => {
    if (frame.op === 'event' && frame.event) {
      const evt = frame.event;
      const payload = await decodePayload(evt.payload, gunzip, MSG_MAX_BYTES);
      // Rename at the boundary: components only ever see summary /
      // msgCompleteString (lib.js WIRE_FIELD).
      const decorated = toViewEvent({ ...evt, payload });
      setEvents((prev) => insertEvent(prev, decorated, EVENT_LIMIT));
      if (frame.applied !== null && frame.applied !== undefined) {
        setAlerts((prev) =>
          applyAlert(prev, {
            key: `${evt.source}:${evt.type}`,
            applied: frame.applied,
            event: decorated,
          }),
        );
      }
      if (evt.brokerTs) lastTsRef.current = evt.brokerTs;
      return;
    }
    if (frame.op === 'alert') {
      setAlerts((prev) => applyAlert(prev, { key: frame.key, applied: frame.applied }));
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
      // clears the ack at exactly that point (alert-fold.lua). Mirror it here
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
    if (frame.op === 'snapshot') {
      // The state of the world as of connect. Without this the board is empty on
      // load until something new fires, because events only create rows as they
      // arrive — a reload would look like a dead cluster.
      //
      // A snapshot REPLACES the alert map rather than merging into it: it is the
      // full open set, so a row it omits is genuinely closed. Merging would
      // resurrect rows retired while the socket was down.
      if (!Array.isArray(frame.alerts)) return;
      const next = new Map();
      for (const row of frame.alerts) {
        if (!row || typeof row.key !== 'string') continue;
        // The snapshot's event record carries brokerTs, but a row whose record
        // has expired or was never stored still needs a Last occurrence. Fall
        // back to the alert's own last_seen so the column is never blank.
        const evt = row.event ? toViewEvent(row.event) : null;
        next.set(row.key, {
          key: row.key,
          severity: row.severity,
          firstSeen: row.firstSeen,
          lastSeen: row.lastSeen,
          count: row.count,
          event: evt ?? undefined,
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

      // The snapshot carries the newest record per alert; seed the live tail with
      // them too so the drawer is not empty before the first live event.
      const seeded = frame.alerts
        .map((r) => (r?.event ? toViewEvent(r.event) : null))
        .filter(Boolean);
      if (seeded.length > 0) {
        setEvents((prev) => seeded.reduce((acc, e) => insertEvent(acc, e, EVENT_LIMIT), prev));
      }
      return;
    }
    if (frame.op === 'hello') {
      if (frame.truncated) setNotice('Backfill was truncated — narrow the time window.');
      return;
    }
    if (frame.op === 'deleted') {
      // An event record is gone for good. Drop it from the live tail so the row
      // stops claiming a record exists. The alert row is NOT touched: `count` is
      // owned by the fold, and silently decrementing it here would disagree with
      // the stored state until the next write.
      setEvents((prev) => prev.filter((e) => e.id !== frame.id));
      return;
    }
    if (frame.op === 'error') {
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
      setStatus('open');
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
   * Acknowledge an alert.
   *
   * Optimistic: the server confirms with an `alert` frame carrying the ack, but
   * waiting for a round trip would make the button feel broken on a slow link.
   * The frame therefore overwrites whatever we rendered in the meantime, so a
   * rejection (bad_key, no_such_alert, ack_failed) is corrected rather than
   * silently believed — the error is shown in the notice line too.
   */
  const acknowledge = useCallback((key) => {
    const ws = wsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      setNotice('not connected — acknowledgement not sent');
      return;
    }
    ws.send(JSON.stringify({ op: 'ack', key }));
  }, []);

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
   * The only irreversible action in the UI, so it asks first. `confirm()` rather
   * than a custom dialog deliberately: the browser's modal is unmissable, cannot
   * be dismissed by an outside click, and cannot be styled into something that
   * looks like a normal menu item.
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
    // eslint-disable-next-line no-alert
    const ok = window.confirm(
      `Permanently delete event ${id}?\n\n` +
      'This removes the stored record and cannot be undone. There is no audit ' +
      'copy for an operator delete.',
    );
    if (!ok) return;
    ws.send(JSON.stringify({ op: 'delete', id }));
  }, []);

  return { status, events, alerts, acks, notice, acknowledge, bulk, deleteEvent };
}

// ------------------------------------------------------------------------ UI
function Badge({ severity }) {
  return <span className={`sev sev-${severity.toLowerCase()}`}>{severity}</span>;
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
function RecordInspector({ state, onClose }) {
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

  if (!state) return null;
  const entries = recordEntries(state.record);
  const actions = state.actions || [];

  return (
    <div
      ref={ref}
      className="inspector"
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
          {actions.map((a) => (
            <button
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
          <button
            className="inspector-action"
            onClick={() => setView('info')}
            title="Show every field of this record"
          >
            Information
          </button>
        </div>
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
        {view === 'info' ? 'Esc or click outside to close' : 'Right-click action for this row'}
      </div>
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

function AlertRow({ alert, ack, selected, onSelect, onContextMenu }) {
  const acked = Boolean(ack);
  return (
    <tr
      className={`${severityRowClass(alert.severity, acked ? 'acked' : '')}${selected ? ' row-selected' : ''}`}
      onContextMenu={onContextMenu}
      onClick={onSelect}
      // Selection is pointer state, so the row is not focusable and carries no
      // button semantics: making a data row behave like a button breaks
      // screen-reader table navigation. Keyboard users get the same actions via
      // the context menu's Acknowledge/Information, and bulk actions are an
      // explicit affordance (see the selection bar) rather than a hidden mode.
      aria-selected={selected || undefined}
    >
      <td><Badge severity={alert.severity} /></td>
      {/* Order follows the live table: severity, first, last, count, summary.
          Key is last: it is a stable identifier, read once when you need it,
          not scanned down the column on every glance. */}
      <td className="mono">{formatClock(alert.firstSeen)}</td>
      <td className="mono">{formatClock(alert.lastSeen)}</td>
      <td className="num">{alert.count}</td>
      <td className="summary">{alert.event?.summary || <span className="dim">—</span>}</td>
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

function EventRow({ evt, onContextMenu }) {
  // `evt` is a view event: `summary`, `msgCompleteString`, the occurrence fields
  // and the worker-enriched fields are already mapped by toViewEvent().
  const { summary, msgCompleteString } = evt;
  const [open, setOpen] = useState(false);

  /**
   * What the fold reveals.
   *
   * `msgc` when the producer sent one, otherwise the whole record — which is
   * always worth expanding, because the row shows a clipped summary and a few
   * enriched columns while the inspector shows everything. Previously the fold
   * was gated on `msgc`, so an event without that optional field had no fold at
   * all and the row appeared inert.
   */
  const detailText = msgCompleteString
    || (evt.wire ? JSON.stringify(evt.wire, null, 2) : '');
  const retirement = isRetirement(evt.severity);
  const rowClass = severityRowClass(evt.severity, retirement ? 'retirement' : '');
  return (
    <>
      <tr className={rowClass} onContextMenu={onContextMenu}>
        <td><Badge severity={evt.severity} /></td>
        <td className="mono">{formatClock(evt.firstOccurrence)}</td>
        <td className="mont">{formatClock(evt.lastOccurrence)}</td>
        <td className="num">{evt.count}</td>
        <td className="summary">
          {summary || <span className="dim">—</span>}
          {/*
            The toggle is gated on there being something to reveal, not on the
            `msgc` field existing.

            `msgc` is optional: it is the "complete string" a producer may send,
            and the ones we have do not send it at all, so gating on it meant the
            fold never appeared for real events — while the dev harness, whose
            mock always includes msgc, looked fine. Gate on content.
          */}
          {detailText ? (
            <button
              className="link"
              onClick={(e) => { e.stopPropagation(); setOpen((o) => !o); }}
              aria-expanded={open}
            >
              {open ? 'hide details' : 'details'}
            </button>
          ) : null}
        </td>
        {ENRICHED_FIELDS.map((f) => (
          <td key={f.key} className="mono dim nowrap">{evt[f.key] || '—'}</td>
        ))}
        {/* Spacer cell, matching the spacer column in the header. */}
        <td />
      </tr>
      {open && detailText ? (
        <tr className={rowClass}>
          {/* +1 for the spacer column, or the expanded row stops short. */}
          <td className="detailcell" colSpan={6 + ENRICHED_FIELDS.length}>
            <pre className="detail">{detailText}</pre>
          </td>
        </tr>
      ) : null}
    </>
  );
}

function App() {
  const { config, error } = useConfig();
  const { status, events, alerts, acks, notice, acknowledge, bulk, deleteEvent } = useSocket(config);
  const { themePreference, darkMode, rowColor, setTheme, setRowColor } = usePreferences();
  // The live tail is a diagnostic view, not the primary signal. Closed by
  // default so the alert table owns the screen; the count stays in the handle
  // so a collapsed drawer still tells you the stream is alive.
  const [drawerOpen, setDrawerOpen] = useState(false);
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
    reset: resetAlertWidths,
  } = useColumnWidths('alerts', ALERT_DEFAULT_W);
  const {
    widthOf: eventWidthOf,
    beginResize: beginEventResize,
    reset: resetEventWidths,
  } = useColumnWidths('events', EVENT_DEFAULT_W);

  // Snapshot on the transition into pause, so the frozen list is exactly what
  // was on screen at that moment and does not creep forward.
  if (!paused) frozenRef.current = events;
  const shownEvents = paused && frozenRef.current ? frozenRef.current : events;

  const openInspector = useCallback((e, record, title, actions = []) => {
    e.preventDefault();
    // Keep the panel inside the viewport; a right-click near the right edge
    // would otherwise open a menu that runs off-screen.
    const width = 460;
    const height = 380;
    setInspector({
      record,
      title,
      actions,
      x: Math.max(8, Math.min(e.clientX, window.innerWidth - width - 8)),
      y: Math.max(8, Math.min(e.clientY, window.innerHeight - height - 8)),
    });
  }, []);

  const closeInspector = useCallback(() => setInspector(null), []);

  const sortedAlerts = useMemo(
    () => [...alerts.values()].sort(
      (a, b) => (SEVERITY_RANK[a.severity] ?? 9) - (SEVERITY_RANK[b.severity] ?? 9)
        || b.lastSeen - a.lastSeen,
    ),
    [alerts],
  );

  // ------------------------------------------------------------- selection
  // A Set of alert keys. Selection is deliberately client-side only: it is a
  // transient pointer state, not a fact about the alert, so persisting it (or
  // syncing it) would only create disagreement between operators.
  const [selected, setSelected] = useState(() => new Set());
  // Anchor for shift-range selection: the last row clicked without shift.
  const anchorRef = useRef(null);

  /**
   * Click behaviour, the usual file-manager convention:
   *
   *   plain click   select only this row (and move the anchor)
   *   shift+click   select the range from the anchor to this row
   *   ctrl/cmd+click toggle this row, leaving the rest alone
   *
   * The anchor is what makes shift-click predictable. Without it, shift+click
   * would select from whatever happened to be selected last, which after a
   * shift-range is the far end of that range and not where the operator started.
   */
  const selectRow = useCallback((key, e) => {
    const additive = e.metaKey || e.ctrlKey;
    const range = e.shiftKey;

    setSelected((prev) => {
      const next = new Set(prev);
      if (range && anchorRef.current !== null) {
        const keys = sortedAlerts.map((a) => a.key);
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
  }, [sortedAlerts]);

  const clearSelection = useCallback(() => {
    setSelected(new Set());
    anchorRef.current = null;
  }, []);

  // Escape clears the selection, matching the convention that Escape backs out
  // of a mode rather than leaving rows armed.
  useEffect(() => {
    if (selected.size === 0) return undefined;
    const onKey = (e) => { if (e.key === 'Escape') clearSelection(); };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [selected.size, clearSelection]);

  // Drop keys that are no longer on the board, so a bulk action cannot fire at
  // an alert that has already been closed by a source.
  const liveSelection = useMemo(
    () => [...selected].filter((k) => alerts.has(k)),
    [selected, alerts],
  );

  const selectedAlertKeys = liveSelection;

  if (error) {
    return <main><h1>Fire-Watch</h1><p className="error">Cannot load config.json: {error}</p></main>;
  }
  if (!config) return <main><h1>Fire-Watch</h1><p className="dim">Loading…</p></main>;

  return (
    <>
      <main className={rowColor ? 'color-rows' : ''}>
        <header>
        <h1>Fire-Watch</h1>
        <span className={`status status-${status}`}>{status}</span>
        {notice ? <span className="notice">{notice}</span> : null}
        <div className="controls">
          {themePreference === 'auto' ? null : (
            <button className="link" onClick={() => setTheme('auto')} title="Follow the operating system">
              auto
            </button>
          )}
          <div className="togglegroup">
            <button
              className="iconbtn iconbtn-theme"
              onClick={() => setTheme(darkMode ? 'light' : 'dark')}
              aria-pressed={darkMode}
              aria-label={darkMode ? 'Switch to light mode' : 'Switch to dark mode'}
              title={darkMode ? 'Switch to light mode' : 'Switch to dark mode'}
            >
              {darkMode ? <IconSun /> : <IconMoon />}
            </button>
            <button
              className="iconbtn iconbtn-rows"
              onClick={() => setRowColor(!rowColor)}
              aria-pressed={rowColor}
              aria-label={rowColor ? 'Turn off severity row colours' : 'Turn on severity row colours'}
              title={rowColor ? 'Severity row colours: on' : 'Severity row colours: off'}
            >
              <IconPalette />
            </button>
          </div>
        </div>
      </header>

      <section>
        <h2>
          Open alerts <span className="dim">({sortedAlerts.length})</span>
          <button
            className="link colreset"
            onClick={resetAlertWidths}
            title="Restore the default column widths"
          >
            reset columns
          </button>
        </h2>
        {/* No selection banner by design. Selecting rows is purely a pointer
            state; every action lives in the right-click menu, which is where the
            operator already goes for a single row. A toolbar that appears on
            selection duplicates those actions and shifts the table down the page
            at the exact moment the operator is aiming at it. */}
        {sortedAlerts.length === 0
          ? <p className="dim">No open alerts.</p>
          : (
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
                    onSelect={(e) => selectRow(a.key, e)}
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
                        {
                          // Delete targets the event RECORD behind this alert,
                          // not the alert itself: an alert is an aggregate over
                          // many events, and "remove the alert" is Close.
                          //
                          // Only offered when the row's backing event is known.
                          // An alert loaded from a snapshot may have no `event`
                          // attached (its record expired), and offering a delete
                          // that cannot resolve an id would be a menu item that
                          // always errors.
                          label: targets.length > 1
                            ? `Delete ${targets.length} event(s)`
                            : 'Delete event',
                          disabled: !a.event?.id && targets.length === 1,
                          title: targets.length > 1
                            ? 'Permanently delete the event record behind each selected alert'
                            : (a.event?.id
                              ? `Permanently delete event ${a.event.id}. Cannot be undone.`
                              : 'No event record is attached to this alert'),
                          run: () => {
                            const ids = targets
                              .map((k) => alerts.get(k)?.event?.id)
                              .filter(Boolean);
                            for (const id of ids) deleteEvent(id);
                          },
                          danger: true,
                        },
                      ],
                    )}
                  />
                );
              })}
            />
          )}
      </section>
      </main>

      {/* Outside <main> so it can take real layout space at the viewport bottom
          instead of overlaying the alert table. */}
      <section
        className={drawerOpen ? 'drawer drawer-open' : 'drawer'}
        aria-label="Live events"
      >
        <div className="drawer-bar">
          <button
            className="drawer-handle"
            onClick={() => setDrawerOpen((o) => !o)}
            aria-expanded={drawerOpen}
            aria-controls="live-events-panel"
          >
            <span className="drawer-chevron" aria-hidden="true">{drawerOpen ? '▾' : '▸'}</span>
            <span>Live events</span>
            <span className="dim">({shownEvents.length} of last {EVENT_LIMIT})</span>
          </button>
          <button
            className={paused ? 'pause pause-on' : 'pause'}
            onClick={() => setPaused((p) => !p)}
            aria-pressed={paused}
            title={paused ? 'Resume the live tail' : 'Freeze the live tail'}
          >
            {paused ? '▶ Resume' : '❚❚ Pause'}
          </button>
          {paused ? <span className="notice">paused</span> : null}
        </div>
        <div className="drawer-body" id="live-events-panel" hidden={!drawerOpen}>
          {shownEvents.length === 0
            ? <p className="dim">Nothing yet.</p>
            : (
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
                  // An event row can also start an ack: the alert it belongs to
                  // is `source:type`, and the operator is looking right at it.
                  // Acking from here and from the alert table are the same
                  // action on the same key.
                  const ackKey = `${e.source}:${e.type}`;
                  const ack = acks.get(ackKey);
                  return (
                    <EventRow
                      key={e.id}
                      evt={e}
                      onContextMenu={(ev) => openInspector(
                        ev,
                        e.wire ?? e,
                        `${e.source} · ${e.type}`,
                        [
                          {
                            label: ack ? `Acked by ${ack.by}` : 'Acknowledge alert',
                            disabled: Boolean(ack),
                            title: ack
                              ? `Acknowledged by ${ack.by} at ${formatClock(ack.at)}`
                              : `Acknowledge ${ackKey}`,
                            run: () => acknowledge(ackKey),
                          },
                          {
                            label: 'Close alert',
                            disabled: false,
                            title: `Retire ${ackKey}`,
                            run: () => bulk('close', [ackKey]),
                          },
                          {
                            // On an EVENT row, delete targets this event id, not
                            // the alert: the operator is removing one record.
                            label: 'Delete event',
                            disabled: false,
                            title: `Permanently delete event ${e.id}`,
                            run: () => deleteEvent(e.id),
                            danger: true,
                          },
                        ],
                      )}
                    />
                  );
                })}
              />
            )}
        </div>
      </section>

      <RecordInspector state={inspector} onClose={closeInspector} />
    </>
  );
}

createRoot(document.getElementById('root')).render(<App />);
