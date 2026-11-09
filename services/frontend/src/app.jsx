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

  return { status, events, alerts, acks, notice, acknowledge };
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
 * Right-click menu for a record: the actions available on that row, then every
 * key/value pair of the record.
 *
 * Closed on Escape, on a click outside, and on scroll of the container it sits
 * in — a menu anchored to a row that has scrolled away is worse than no menu.
 */
function RecordInspector({ state, onClose }) {
  const ref = useRef(null);

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
        <span>{state.title}</span>
        <button className="link" onClick={onClose} aria-label="Close">✕</button>
      </div>
      {actions.length > 0 ? (
        <div className="inspector-actions">
          {actions.map((a) => (
            <button
              key={a.label}
              className="inspector-action"
              onClick={() => { a.run(); onClose(); }}
              disabled={a.disabled}
              title={a.title}
            >
              {a.label}
            </button>
          ))}
        </div>
      ) : null}
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
      <div className="inspector-hint">Esc or click outside to close</div>
    </div>
  );
}

function AlertRow({ alert, ack, onContextMenu }) {
  const acked = Boolean(ack);
  return (
    <tr
      className={severityRowClass(alert.severity, acked ? 'acked' : '')}
      onContextMenu={onContextMenu}
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
    </tr>
  );
}

function EventRow({ evt, onContextMenu }) {
  // `evt` is a view event: `summary`, `msgCompleteString`, the occurrence fields
  // and the worker-enriched fields are already mapped by toViewEvent().
  const { summary, msgCompleteString } = evt;
  const [open, setOpen] = useState(false);
  const retirement = isRetirement(evt.severity);
  const rowClass = severityRowClass(evt.severity, retirement ? 'retirement' : '');
  return (
    <>
      <tr className={rowClass} onContextMenu={onContextMenu}>
        <td><Badge severity={evt.severity} /></td>
        <td className="mono">{formatClock(evt.firstOccurrence)}</td>
        <td className="mono">{formatClock(evt.lastOccurrence)}</td>
        <td className="num">{evt.count}</td>
        <td className="summary">
          {summary || <span className="dim">—</span>}
          {msgCompleteString ? (
            <button
              className="link"
              onClick={(e) => { e.stopPropagation(); setOpen((o) => !o); }}
              aria-expanded={open}
            >
              {open ? 'hide MsgCompleteString' : 'MsgCompleteString'}
            </button>
          ) : null}
        </td>
        {ENRICHED_FIELDS.map((f) => (
          <td key={f.key} className="mono dim nowrap">{evt[f.key] || '—'}</td>
        ))}
      </tr>
      {open && msgCompleteString ? (
        <tr className={rowClass}>
          <td colSpan={5 + ENRICHED_FIELDS.length}>
            <pre className="detail">{msgCompleteString}</pre>
          </td>
        </tr>
      ) : null}
    </>
  );
}

function App() {
  const { config, error } = useConfig();
  const { status, events, alerts, acks, notice, acknowledge } = useSocket(config);
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
        <h2>Open alerts <span className="dim">({sortedAlerts.length})</span></h2>
        <p className="hint dim">Right-click a row to acknowledge it or see every field.</p>
        {sortedAlerts.length === 0
          ? <p className="dim">No open alerts.</p>
          : (
            <table>
              <thead>
                <tr>
                  <th>Severity</th>
                  <th>First occurrence</th>
                  <th>Last occurrence</th>
                  <th>Count</th>
                  <th>Summary</th>
                  <th>Ack</th>
                  <th>Key</th>
                </tr>
              </thead>
              <tbody>
                {sortedAlerts.map((a) => {
                  const ack = acks.get(a.key);
                  return (
                    <AlertRow
                      key={a.key}
                      alert={a}
                      ack={ack}
                      onContextMenu={(e) => openInspector(
                        e,
                        a.event?.wire ?? a.event ?? a,
                        a.key,
                        [{
                          label: ack ? `Acked by ${ack.by}` : 'Acknowledge',
                          disabled: Boolean(ack),
                          title: ack
                            ? `Acknowledged by ${ack.by} at ${formatClock(ack.at)}`
                            : 'Claim this alert; it clears when the alert re-fires',
                          run: () => acknowledge(a.key),
                        }],
                      )}
                    />
                  );
                })}
              </tbody>
            </table>
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
              <table>
                <thead>
                  <tr>
                    <th>Severity</th>
                    <th>First occurrence</th>
                    <th>Last occurrence</th>
                    <th>Count</th>
                    <th>Summary</th>
                    {ENRICHED_FIELDS.map((f) => <th key={f.key}>{f.label}</th>)}
                  </tr>
                </thead>
                <tbody>
                  {[...shownEvents].reverse().map((e) => {
                    // An event row can also start an ack: the alert it belongs
                    // to is `source:type`, and the operator is looking right at
                    // it. Acking from here and from the alert table are the
                    // same action on the same key.
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
                          [{
                            label: ack ? `Acked by ${ack.by}` : 'Acknowledge alert',
                            disabled: Boolean(ack),
                            title: ack
                              ? `Acknowledged by ${ack.by} at ${formatClock(ack.at)}`
                              : `Acknowledge ${ackKey}`,
                            run: () => acknowledge(ackKey),
                          }],
                        )}
                      />
                    );
                  })}
                </tbody>
              </table>
            )}
        </div>
      </section>

      <RecordInspector state={inspector} onClose={closeInspector} />
    </>
  );
}

createRoot(document.getElementById('root')).render(<App />);
