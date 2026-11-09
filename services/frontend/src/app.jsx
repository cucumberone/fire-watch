/**
 * React UI.
 *
 * Responsibilities and their sources:
 *   - reconnect with jittered backoff      docs/frontend.md §4
 *   - honour `applied` codes               docs/event-lifecycle.md §9
 *   - decode `msgc` in the browser         docs/payload-and-compression.md §5
 *   - sort by broker timestamp             docs/event-lifecycle.md §2
 */
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import {
  resolveWsUrl,
  decodePayload,
  applyAlert,
  insertEvent,
  backoffDelay,
  isRetirement,
  SEVERITY_RANK,
} from './lib.js';

const EVENT_LIMIT = 500;
const MSG_MAX_BYTES = 1048576;

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
  const [notice, setNotice] = useState(null);

  const wsRef = useRef(null);
  const attemptRef = useRef(0);
  const timerRef = useRef(null);
  const lastTsRef = useRef(null);
  const closedByUsRef = useRef(false);

  const ingest = useCallback(async (frame) => {
    if (frame.op === 'event' && frame.event) {
      const evt = frame.event;
      const payload = await decodePayload(evt.payload, gunzip, MSG_MAX_BYTES);
      const decorated = { ...evt, payload };
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

  return { status, events, alerts, notice };
}

// ------------------------------------------------------------------------ UI
function Badge({ severity }) {
  return <span className={`sev sev-${severity.toLowerCase()}`}>{severity}</span>;
}

function AlertRow({ alert }) {
  return (
    <tr>
      <td><Badge severity={alert.severity} /></td>
      <td className="mono">{alert.key}</td>
      <td className="num">{alert.count}</td>
      <td className="mono">{new Date(alert.firstSeen).toLocaleTimeString()}</td>
      <td className="mono">{new Date(alert.lastSeen).toLocaleTimeString()}</td>
    </tr>
  );
}

function EventRow({ evt }) {
  const { msg, msgc } = evt.payload || {};
  const [open, setOpen] = useState(false);
  const retirement = isRetirement(evt.severity);
  return (
    <>
      <tr className={retirement ? 'retirement' : ''}>
        <td className="mono">{new Date(evt.brokerTs ?? evt.ts).toLocaleTimeString()}</td>
        <td><Badge severity={evt.severity} /></td>
        <td className="mono">{evt.source}</td>
        <td className="mono dim">{evt.type}</td>
        <td>
          {msg || <span className="dim">—</span>}
          {msgc ? (
            <button className="link" onClick={() => setOpen((o) => !o)}>
              {open ? 'hide detail' : 'detail'}
            </button>
          ) : null}
        </td>
      </tr>
      {open && msgc ? (
        <tr>
          <td colSpan={5}><pre className="detail">{msgc}</pre></td>
        </tr>
      ) : null}
    </>
  );
}

function App() {
  const { config, error } = useConfig();
  const { status, events, alerts, notice } = useSocket(config);

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
    <main>
      <header>
        <h1>Fire-Watch</h1>
        <span className={`status status-${status}`}>{status}</span>
        {notice ? <span className="notice">{notice}</span> : null}
      </header>

      <section>
        <h2>Open alerts <span className="dim">({sortedAlerts.length})</span></h2>
        {sortedAlerts.length === 0
          ? <p className="dim">No open alerts.</p>
          : (
            <table>
              <thead>
                <tr><th>Severity</th><th>Key</th><th>Occurrences</th><th>First seen</th><th>Last seen</th></tr>
              </thead>
              <tbody>{sortedAlerts.map((a) => <AlertRow key={a.key} alert={a} />)}</tbody>
            </table>
          )}
      </section>

      <section>
        <h2>Live events <span className="dim">(last {EVENT_LIMIT})</span></h2>
        {events.length === 0
          ? <p className="dim">Nothing yet.</p>
          : (
            <table>
              <thead>
                <tr><th>Time</th><th>Severity</th><th>Source</th><th>Type</th><th>Message</th></tr>
              </thead>
              <tbody>{[...events].reverse().map((e) => <EventRow key={e.id} evt={e} />)}</tbody>
            </table>
          )}
      </section>
    </main>
  );
}

createRoot(document.getElementById('root')).render(<App />);
