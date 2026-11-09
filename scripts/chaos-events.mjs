#!/usr/bin/env node
/**
 * chaos-events.mjs — end-to-end duplicate-detection drill.
 *
 * Two phases, and you control the gap between them:
 *
 *   phase 1  open   N alerts, with a fraction of the events sent twice
 *   wait     press any key to continue, so you correlate at your own pace
 *   phase 2  close  every one of those N alerts, with a fraction closed 2-6 times
 *
 * What it is actually testing
 * ---------------------------
 * The alert fold (k8s/base/20-alert-fold-config.yaml) rolls repeats up inside
 * FW_ALERT_DEDUP_WINDOW (5 min by default) instead of creating new occurrences:
 *
 *   applied = 1  new occurrence        -> count += 1
 *   applied = 2  coalesced             -> count unchanged, last_seen moves
 *   applied = 3  closed                -> alert retired
 *
 * So a genuine duplicate is invisible in `count` and visible only as a moving
 * `last_seen`. That is the property this script exercises: phase 1 sends exact
 * copies of some events, and phase 2 sends exact copies of some CLOSE events. If
 * dedup works, the alert count never exceeds N and no alert re-opens after being
 * closed.
 *
 * Two design constraints worth knowing before editing this file
 * ------------------------------------------------------------
 * 1. The alert key is `source:type` (services/worker/src/config.js). Two events
 *    with the same pair are the SAME alert, not two alerts. To get N distinct
 *    alerts the script therefore gives every opening event a unique type.
 * 2. `HARMLESS` retires an alert exactly like `CLOSE` (see the fold script). It
 *    must NOT be used in phase 1, or alerts close the moment they open.
 *
 * Usage
 * -----
 *   node scripts/chaos-events.mjs                        # defaults
 *   node scripts/chaos-events.mjs --count 50              # quicker smoke test
 *   node scripts/chaos-events.mjs --pause 5               # auto-continue after 5s
 *   node scripts/chaos-events.mjs --no-wait               # no pause at all
 *   node scripts/chaos-events.mjs --dry-run               # print, send nothing
 *
 * The wait is interactive by default: ingestion of phase 1 finishes, then the
 * script blocks on stdin until you press a key. Closing events are only sent after
 * that, so you can inspect the board and correlate before the storm of CLOSEs.
 *
 * Configuration (env, or read from .env when present)
 * ---------------------------------------------------
 *   INGEST_URL        override the endpoint entirely
 *   FW_INGEST_TOKEN   required unless --token is passed
 *
 * Endpoint default: the ingress is a NodePort, so it is NOT on localhost. The
 * script asks kubectl for the node addresses and tries each in turn. Falling back
 * to `localhost:30900` was wrong on the author's machine and produced a bare
 * `TypeError: fetch failed` with no hint as to why.
 *
 * No dependencies: Node 22 has global fetch.
 */

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');

// --------------------------------------------------------------------- config
const SOURCES = ['shop', 'db', 'net', 'auth', 'mail', 'edge'];
// Opening severities. Deliberately excludes HARMLESS, which retires an alert.
const OPEN_SEVERITIES = ['CRITICAL', 'WARNING', 'FATAL'];

const DEFAULTS = {
  count: 200,          // distinct alerts opened in phase 1
  pauseSec: null,      // null = wait for a keypress; a number = auto-continue after N s
  duplicates: 0.10,    // fraction of phase-1 events re-sent verbatim
  closeDuplicates: 0.10, // fraction of CLOSE events sent 2-6 times
  minRepeats: 2,
  maxRepeats: 6,
  // Left null so it can be discovered from the cluster. A hardcoded localhost
  // is wrong whenever the cluster is remote, which it is here.
  url: process.env.INGEST_URL || null,
  nodePort: 30900,
  token: process.env.FW_INGEST_TOKEN || '',
  seed: null,
  dryRun: false,
};

/** Minimal .env reader. Only used to pick up FW_INGEST_TOKEN. */
function readDotEnv() {
  try {
    const text = readFileSync(join(ROOT, '.env'), 'utf8');
    const out = {};
    for (const line of text.split('\n')) {
      const m = /^\s*([A-Z0-9_]+)\s*=\s*(.*)$/.exec(line);
      if (!m) continue;
      out[m[1]] = m[2].trim().replace(/^["']|["']$/g, '');
    }
    return out;
  } catch {
    return {};
  }
}

/** Deterministic PRNG (mulberry32) so a failing run can be replayed. */
function makeRandom(seed) {
  let a = seed >>> 0;
  return function random() {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ------------------------------------------------------------------ endpoint
/**
 * Work out which URL to POST to.
 *
 * The http-ingress Service is a NodePort (30900), so the endpoint only exists on
 * a cluster node — never on localhost. Asking kubectl for the node addresses is
 * far more useful than guessing, and when kubectl is unavailable we say so plainly
 * instead of failing later with an opaque `fetch failed`.
 */
async function resolveEndpoint(cfg, dotenv) {
  if (cfg.url) return cfg.url;

  const port = cfg.nodePort || Number(dotenv.INGEST_NODEPORT) || 30900;
  const candidates = [];

  // Prefer an explicit host, e.g. INGEST_HOST=192.168.50.3 in .env.
  if (dotenv.INGEST_HOST) {
    candidates.push(`http://${dotenv.INGEST_HOST}:${port}/events`);
  }

  // Then discover the nodes. `-o wide` is deliberately avoided: the internal IP
  // column is what we want and jsonpath keeps it parseable.
  try {
    const { execFileSync } = await import('node:child_process');
    const out = execFileSync(
      'kubectl',
      ['get', 'nodes', '-o', 'jsonpath={range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address}{"\\n"}{end}'],
      { encoding: 'utf8', timeout: 10000, stdio: ['ignore', 'pipe', 'ignore'] },
    );
    for (const ip of out.split('\n').map((s) => s.trim()).filter(Boolean)) {
      candidates.push(`http://${ip}:${port}/events`);
    }
  } catch {
    // kubectl missing or not pointing at a cluster — handled below.
  }

  if (candidates.length === 0) {
    fail(
      'cannot determine the ingest endpoint.\n' +
      '  The http-ingress Service is a NodePort, so it is not reachable as localhost.\n' +
      '  Either set INGEST_HOST=<node-ip> in .env, or pass --url, e.g.\n' +
      '    --url http://192.168.50.3:30900/events',
    );
  }

  // Probe each candidate so we fail here, with a clear message, rather than in the
  // middle of phase 1 with half the events sent.
  for (const url of candidates) {
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'X-Ingest-Token': 'probe' },
        body: '{}',
        signal: AbortSignal.timeout(4000),
      });
      // 401 is the expected answer to a bogus token: it proves the endpoint is up.
      if (res.status === 401 || res.status === 202 || res.status === 400) return url;
    } catch {
      // try the next candidate
    }
  }

  fail(
    'no reachable ingest endpoint. Tried:\n' +
    candidates.map((u) => `  ${u}`).join('\n') +
    '\n  Check the Service: kubectl -n fire-watch-lab get svc http-ingress',
  );
  return null;
}

// ---------------------------------------------------------------------- args
function parseArgs(argv) {
  const cfg = { ...DEFAULTS };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    const next = () => {
      const v = argv[i + 1];
      if (v === undefined) fail(`${a} needs a value`);
      i += 1;
      return v;
    };
    switch (a) {
      case '--count': cfg.count = Number(next()); break;
      case '--pause': cfg.pauseSec = Number(next()); break;
      case '--no-wait': cfg.pauseSec = 0; break;
      case '--duplicates': cfg.duplicates = Number(next()); break;
      case '--close-duplicates': cfg.closeDuplicates = Number(next()); break;
      case '--url': cfg.url = next(); break;
      case '--token': cfg.token = next(); break;
      case '--seed': cfg.seed = Number(next()); break;
      case '--dry-run': cfg.dryRun = true; break;
      case '-h':
      case '--help': usage(); process.exit(0); break;
      default: fail(`unknown argument: ${a}`);
    }
  }
  if (!Number.isInteger(cfg.count) || cfg.count < 1) fail('--count must be a positive integer');
  if (cfg.pauseSec !== null && !(cfg.pauseSec >= 0)) fail('--pause must be >= 0');
  if (!(cfg.duplicates >= 0 && cfg.duplicates <= 1)) fail('--duplicates must be 0..1');
  if (!(cfg.closeDuplicates >= 0 && cfg.closeDuplicates <= 1)) fail('--close-duplicates must be 0..1');
  return cfg;
}

function usage() {
  process.stdout.write(
    'chaos-events.mjs — open N alerts with duplicates, wait, then close them\n\n' +
    '  --count N             distinct alerts to open (default 200)\n' +
    '  --pause SECONDS       auto-continue after N seconds instead of waiting for a key\n' +
    '  --no-wait             send the close phase immediately\n' +
    '  --duplicates F        fraction of phase-1 events re-sent verbatim (default 0.10)\n' +
    '  --close-duplicates F  fraction of CLOSE events sent 2-6 times (default 0.10)\n' +
    '  --url URL             ingest endpoint (default $INGEST_URL or http://localhost:30900/events)\n' +
    '  --token T             ingest token (default $FW_INGEST_TOKEN)\n' +
    '  --seed N              reproduce a previous run exactly\n' +
    '  --dry-run             build and print, send nothing\n\n' +
    'Default behaviour is interactive: after phase 1 is ingested the script waits\n' +
    'for any keypress, so you can inspect the board before the CLOSE storm.\n',
  );
}

function fail(msg) {
  process.stderr.write(`chaos-events: ${msg}\n`);
  process.exit(2);
}

// ------------------------------------------------------------------- payloads
/**
 * Build the opening events.
 *
 * Every event gets a UNIQUE type, because the alert key is `source:type`. Without
 * that, N events sharing a pair would fold into one alert and the drill would
 * prove nothing.
 */
function buildOpenEvents(cfg, random) {
  const events = [];
  for (let i = 0; i < cfg.count; i += 1) {
    const n = String(i + 1).padStart(3, '0');
    const severity = OPEN_SEVERITIES[Math.floor(random() * OPEN_SEVERITIES.length)];
    const source = SOURCES[Math.floor(random() * SOURCES.length)];
    events.push({
      id: `chaos-open-${n}`,
      source,
      type: `check-${n}`,          // unique -> unique alert key
      severity,
      payload: {
        msg: `Chaos drill ${n}/${cfg.count}: ${source} check-${n} reports ${severity}`,
        // Helpful context for the operator reading the inspector.
        drill: 'open',
        ordinal: i + 1,
      },
    });
  }
  return events;
}

/**
 * Pick a fraction of the opening events and re-send them verbatim, but label the
 * Summary so a human can tell an intentional duplicate from an accident.
 *
 * The duplicate keeps the SAME id, source and type. That is the whole point: it
 * must land on the same alert key so the fold can coalesce it.
 */
function buildOpenDuplicates(events, cfg, random) {
  const picks = [];
  for (const evt of events) {
    if (random() < cfg.duplicates) picks.push(evt);
  }
  // Guarantee at least one duplicate in a non-empty run, otherwise the drill
  // silently stops testing the thing it exists to test.
  if (events.length > 0 && picks.length === 0) {
    picks.push(events[Math.floor(random() * events.length)]);
  }
  return picks.map((evt) => ({
    ...evt,
    payload: {
      ...evt.payload,
      // Written into Summary so it shows up in the UI, as requested.
      msg: `[DUPLICATE of ${evt.id}] ${evt.payload.msg}`,
      duplicate_of: evt.id,
      drill: 'open-duplicate',
    },
  }));
}

/**
 * Build the CLOSE events — one per opening event, matching its source and type so
 * it retires exactly that alert.
 */
function buildCloseEvents(openEvents) {
  return openEvents.map((evt) => ({
    id: `chaos-close-${evt.type.replace('check-', '')}`,
    source: evt.source,
    type: evt.type,
    severity: 'CLOSE',
    payload: {
      msg: `Resolved: ${evt.source} check ${evt.type} recovered`,
      drill: 'close',
      closes: evt.id,
    },
  }));
}

/** Duplicate a fraction of the CLOSE events, each 2-6 times. */
function buildCloseDuplicates(closeEvents, cfg, random) {
  const out = [];
  const repeats = [];
  for (const evt of closeEvents) {
    if (random() >= cfg.closeDuplicates) continue;
    const n = cfg.minRepeats + Math.floor(random() * (cfg.maxRepeats - cfg.minRepeats + 1));
    repeats.push({ id: evt.id, times: n });
    for (let k = 0; k < n; k += 1) {
      out.push({
        ...evt,
        // Same id: a re-sent CLOSE must be idempotent, not a new event.
        payload: {
          ...evt.payload,
          msg: `[DUPLICATE ${k + 1}/${n} of ${evt.id}] ${evt.payload.msg}`,
          duplicate_of: evt.id,
          drill: 'close-duplicate',
        },
      });
    }
  }
  return { events: out, repeats };
}

// --------------------------------------------------------------------- ingest
/**
 * POST a batch and return the accepted count.
 *
 * The ingress answers 202 only after the broker confirms, so a 202 here means the
 * events are durable, not merely queued in a socket. Anything else is a failure
 * worth stopping for — a silent partial drill is worse than no drill.
 */
async function post(url, token, events, { batchSize = 100, dryRun } = {}) {
  let accepted = 0;
  let sent = 0;
  for (let i = 0; i < events.length; i += batchSize) {
    const batch = events.slice(i, i + batchSize);
    if (dryRun) {
      accepted += batch.length;
      continue;
    }
    const body = batch.length === 1 ? batch[0] : batch;
    let res;
    try {
      res = await fetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'X-Ingest-Token': token },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(30000),
      });
    } catch (err) {
      // A bare "TypeError: fetch failed" hides the actual cause (ECONNREFUSED,
      // DNS, TLS, timeout) in err.cause, which is the only part worth reading.
      const cause = err?.cause;
      const detail = cause?.code || cause?.errno || cause?.message || err?.message || String(err);
      fail(
        `cannot reach ${url}: ${detail}\n` +
        `  ${sent} event(s) were already accepted before this failure.\n` +
        `  Is the ingress reachable from here?  kubectl -n fire-watch-lab get svc http-ingress\n` +
        `  Override with --url or INGEST_HOST in .env.`,
      );
    }
    const text = await res.text();
    if (res.status !== 202) {
      fail(`ingest failed: HTTP ${res.status} ${text.slice(0, 300)}`);
    }
    let parsed = {};
    try { parsed = JSON.parse(text); } catch { /* 202 with an odd body is fine */ }
    const n = Number(parsed.accepted ?? batch.length);
    accepted += n;
    sent += batch.length;
  }
  return accepted;
}

// ----------------------------------------------------------------------- wait
/**
 * Block until the operator presses a key, or until `seconds` elapse.
 *
 * Reading stdin is the part that actually matters here. The close phase is
 * destructive to the interesting state (it retires every alert), so it must not
 * begin until a human says so — a fixed sleep either cuts the inspection short or
 * wastes time, and neither is "correlate at your own pace".
 *
 * Three cases have to work:
 *   - interactive TTY:  raw mode, resume once, continue on the first byte
 *   - non-TTY (CI/pipe): there is nobody to press a key, so do not hang forever;
 *     a timer is the only sensible fallback
 *   - Ctrl-C while waiting: exit cleanly instead of leaving the terminal in raw
 *     mode, which would swallow echo in the user's shell afterwards
 */
function waitForContinue(seconds) {
  if (seconds === 0) {
    process.stdout.write('\n--no-wait: continuing immediately\n');
    return Promise.resolve();
  }

  if (seconds !== null && seconds > 0) {
    process.stdout.write(`\npausing ${seconds}s…\n`);
    return new Promise((resolve) => setTimeout(resolve, seconds * 1000));
  }

  const stdin = process.stdin;
  // A pipe or /dev/null can never produce a keypress. Falling back to the old
  // timed behaviour keeps the script usable from a wrapper while never hanging.
  if (!stdin.isTTY) {
    process.stdout.write(
      '\nstdin is not a TTY, so a keypress is impossible — waiting 60s instead.\n' +
      'Pass --pause N or --no-wait to control this explicitly.\n',
    );
    return new Promise((resolve) => setTimeout(resolve, 60_000));
  }

  process.stdout.write(
    '\n'.repeat(1) +
    '─'.repeat(70) + '\n' +
    '  Phase 1 is ingested and durable (the ingress answered 202).\n' +
    '  Inspect the board now: open alerts, Counts, Summaries.\n' +
    '\n' +
    '  >>> press any key to send the CLOSE events <<<\n' +
    '─'.repeat(70) + '\n',
  );

  return new Promise((resolve) => {
    const finish = () => {
      stdin.removeListener('data', finish);
      stdin.removeListener('error', finish);
      process.removeListener('SIGINT', onSigint);
      // Leaving the terminal in raw mode would break the user's shell echo.
      try { stdin.setRawMode(false); } catch { /* not a TTY any more */ }
      try { stdin.pause(); } catch { /* ignore */ }
      resolve();
    };
    const onSigint = () => {
      try { stdin.setRawMode(false); } catch { /* ignore */ }
      process.stdout.write('\ninterrupted — close phase not sent\n');
      process.exit(130);
    };

    stdin.setRawMode(true);
    stdin.resume();
    stdin.once('data', finish);
    stdin.once('error', finish);
    process.once('SIGINT', onSigint);
  });
}

// ----------------------------------------------------------------------- main
async function main() {
  const cfg = parseArgs(process.argv.slice(2));

  const dotenv = readDotEnv();
  if (!cfg.token) cfg.token = dotenv.FW_INGEST_TOKEN || '';
  if (!cfg.token && !cfg.dryRun) {
    fail('no ingest token: set FW_INGEST_TOKEN, or pass --token, or use --dry-run');
  }

  // Resolve the endpoint before generating anything: a bad endpoint should fail
  // immediately, not after phase 1 has been half-sent.
  cfg.url = await resolveEndpoint(cfg, dotenv);

  const seed = cfg.seed ?? Math.floor(Math.random() * 1e9);
  const random = makeRandom(seed);

  process.stdout.write(`chaos-events: seed=${seed}${cfg.seed === null ? ' (re-run with --seed ' + seed + ' to reproduce)' : ''}\n`);
  process.stdout.write(`chaos-events: endpoint=${cfg.url}${cfg.dryRun ? '  [DRY RUN]' : ''}\n`);

  // ---- phase 1: open ------------------------------------------------------
  const openEvents = buildOpenEvents(cfg, random);
  const openDupes = buildOpenDuplicates(openEvents, cfg, random);
  const phase1 = [...openEvents, ...openDupes];

  process.stdout.write(
    `\nphase 1: opening ${openEvents.length} alerts ` +
    `(${openDupes.length} of them re-sent as duplicates) -> ${phase1.length} events\n`,
  );
  const a1 = await post(cfg.url, cfg.token, phase1, { dryRun: cfg.dryRun });
  process.stdout.write(`phase 1: accepted ${a1}/${phase1.length}\n`);

  process.stdout.write(
    `\nEvery event above has a unique (source, type) pair, so the board should show\n` +
    `${openEvents.length} open alerts and no more. A duplicate must NOT add an alert\n` +
    `and must NOT increment Count: the fold coalesces it and only last_seen moves.\n`,
  );

  // ---- wait for the operator ----------------------------------------------
  await waitForContinue(cfg.pauseSec);

  // ---- phase 2: close -----------------------------------------------------
  const closeEvents = buildCloseEvents(openEvents);
  const { events: closeDupes, repeats } = buildCloseDuplicates(closeEvents, cfg, random);
  const phase2 = [...closeEvents, ...closeDupes];

  process.stdout.write(
    `\nphase 2: closing ${closeEvents.length} alerts ` +
    `(${repeats.length} closed 2-${cfg.maxRepeats} times, ${closeDupes.length} extra events) ` +
    `-> ${phase2.length} events\n`,
  );
  const a2 = await post(cfg.url, cfg.token, phase2, { dryRun: cfg.dryRun });
  process.stdout.write(`phase 2: accepted ${a2}/${phase2.length}\n`);

  // ---- summary ------------------------------------------------------------
  process.stdout.write(
    `\n${'-'.repeat(70)}\n` +
    `expected final state\n` +
    `  alerts opened      ${openEvents.length}\n` +
    `  duplicate opens    ${openDupes.length}  (coalesced: applied=2, Count unchanged)\n` +
    `  alerts closed      ${closeEvents.length}\n` +
    `  repeated closes    ${closeDupes.length}\n` +
    `  fully closed total ${openEvents.length}\n` +
    `\nrepeated CLOSE events (idempotency check):\n` +
    (repeats.length ? repeats.map((r) => `  ${r.id} x${r.times}`).join('\n') : '  (none)') +
    `\n\nverify with:\n` +
    `  kubectl -n fire-watch-lab exec dragonfly-0 -c dragonfly -- \\\n` +
    `    sh -c 'redis-cli -p 6379 --no-auth-warning -a "$DRAGONFLY_PASSWORD" --scan --pattern "fw:alert:open:*" | wc -l'\n` +
    `  # expect 0 — every alert retired\n`,
  );
}

main().catch((err) => {
  process.stderr.write(`chaos-events: ${err?.stack || err}\n`);
  process.exit(1);
});
