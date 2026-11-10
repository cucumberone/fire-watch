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
 * Dedup happens at TWO levels, and they are worth separating because they answer
 * different questions and fail differently.
 *
 * 1. TRANSPORT dedup — keyed on the event id (`SET fw:evt:{id} NX`). The same
 *    MESSAGE delivered twice is idempotent: nothing changes.
 *
 * 2. CONTENT dedup — keyed on a fingerprint of
 *    `{source, type, severity, payload}` (services/worker/src/event.js). The same
 *    CONDITION reported again under a NEW id must not create a second record: the
 *    original absorbs it, `count` increments and `last_seen` moves.
 *
 * That split is why this script has three distinct dials, and it matters that
 * `EVT_Identifier` is NOT part of the fingerprint while `EVT_Summary` (which lands
 * in `payload.msg`) IS:
 *
 *   --copies N            identical content, NEW id
 *                         -> absorbed. 1 record, Count reaches N.
 *   --copies N --same-id  identical content, SAME id
 *                         -> dropped as a redelivery. 1 record, Count stays 1.
 *   --mutate N            same type, DIFFERENT EVT_Summary
 *                         -> a genuinely different report. N+1 records, Count N+1.
 *
 * The third case is the control. Without it, a run where every copy were absorbed
 * would look like a pass even if the gate had been widened to "same type = same
 * event", which would silently lose every genuinely repeated report of an
 * incident.
 *
 * Two design constraints worth knowing before editing this file
 * ------------------------------------------------------------
 * 1. The alert key is `source:type` (services/worker/src/config.js). Two events
 *    with the same pair are the SAME alert, not two alerts. To get N distinct
 *    alerts the script therefore gives every alert a unique type.
 * 2. `CLEAR` and `HARMLESS` retire an alert (the ingress maps both onto the
 *    internal CLOSE severity). They must NOT be used in phase 1, or alerts close
 *    the moment they open.
 *
 * Usage
 * -----
 *   node scripts/chaos-events.mjs                        # defaults
 *   node scripts/chaos-events.mjs --count 3 --copies 5 --no-wait
 *   node scripts/chaos-events.mjs --count 2 --copies 3 --mutate 2 --no-wait --seed 1
 *   node scripts/chaos-events.mjs --count 2 --copies 5 --same-id --no-wait   # dropped
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
// Opening severities, as a producer reports them. Excludes CLEAR/HARMLESS,
// which RETIRE an alert — using one here would close the alert the moment it
// opened, and the drill would prove nothing.
const OPEN_SEVERITIES = ['CRITICAL', 'MAJOR', 'MINOR', 'WARNING'];

const DEFAULTS = {
  count: 200,          // distinct alerts opened in phase 1
  pauseSec: null,      // null = wait for a keypress; a number = auto-continue after N s
  duplicates: 0.10,    // fraction of phase-1 events re-sent verbatim
  closeDuplicates: 0.10, // fraction of CLOSE events sent 2-6 times
  // How many IDENTICAL events to send for each opened alert. This is the
  // same-type dedup drill: `--copies 5` sends the same content five times, so the
  // alert must show Count 5 while only ONE event record exists.
  copies: 1,
  // How many DIFFERENT-content events to send for each opened alert. Deliberately
  // the opposite of `copies`: same (source, type) but a different fingerprint, so
  // the gate must LET IT THROUGH — one alert, several records. Existing to prove
  // dedup keys on content and not merely on "this looks like that other one".
  mutate: 0,
  closeCopies: 1,      // identical CLEAR events per alert (idempotency)
  minRepeats: 2,
  maxRepeats: 6,
  // Left null so it can be discovered from the cluster. A hardcoded localhost
  // is wrong whenever the cluster is remote, which it is here.
  url: process.env.INGEST_URL || null,
  nodePort: 30900,
  token: process.env.FW_INGEST_TOKEN || '',
  seed: null,
  dryRun: false,
  // When false (default) re-sent copies get a fresh id and are therefore counted
  // as distinct events. --same-id keeps the original id, so the fold drops them.
  sameId: false,
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
      case '--copies': cfg.copies = Number(next()); break;
      case '--mutate': cfg.mutate = Number(next()); break;
      case '--close-copies': cfg.closeCopies = Number(next()); break;
      case '--url': cfg.url = next(); break;
      case '--token': cfg.token = next(); break;
      case '--seed': cfg.seed = Number(next()); break;
      case '--dry-run': cfg.dryRun = true; break;
      case '--same-id': cfg.sameId = true; break;
      case '-h':
      case '--help': usage(); process.exit(0); break;
      default: fail(`unknown argument: ${a}`);
    }
  }
  if (!Number.isInteger(cfg.count) || cfg.count < 1) fail('--count must be a positive integer');
  if (cfg.pauseSec !== null && !(cfg.pauseSec >= 0)) fail('--pause must be >= 0');
  if (!(cfg.duplicates >= 0 && cfg.duplicates <= 1)) fail('--duplicates must be 0..1');
  if (!(cfg.closeDuplicates >= 0 && cfg.closeDuplicates <= 1)) fail('--close-duplicates must be 0..1');
  for (const [flag, v] of [['--copies', cfg.copies], ['--close-copies', cfg.closeCopies]]) {
    if (!Number.isInteger(v) || v < 1) fail(`${flag} must be a positive integer (at least 1)`);
  }
  // Zero is meaningful for --mutate: it turns the control case OFF, which is the
  // pure "identical copies only" drill.
  if (!Number.isInteger(cfg.mutate) || cfg.mutate < 0) {
    fail('--mutate must be 0 or a positive integer (0 = no distinct-content events)');
  }
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
    '  --copies N            send N IDENTICAL events per alert (same type, same\n' +
    '                        content, new id). Count must reach N while only ONE\n' +
    '                        record is stored -- this is the dedup drill.\n' +
    '  --mutate N            send N DIFFERENT-content events per alert (same type).\n' +
    '                        Count reaches N+1 AND N+1 records exist: dedup keys on\n' +
    '                        content, not on type.\n' +
    '  --close-copies N      send N identical CLEAR events per alert (default 1)\n' +
    '  --url URL             ingest endpoint (default $INGEST_URL or discovered from kubectl)\n' +
    '  --token T             ingest token (default $FW_INGEST_TOKEN)\n' +
    '  --seed N              reproduce a previous run exactly\n' +
    '  --same-id             re-sent copies keep the original event id, so the fold\n' +
    '                        drops them as a redelivery (Count does NOT move)\n' +
    '  --dry-run             build and print, send nothing\n\n' +
    'The three dedup cases, in order of what they prove:\n' +
    '  1. --copies N          identical content, NEW id   -> absorbed: 1 record, Count N\n' +
    '  2. --copies N --same-id identical content, SAME id -> dropped: 1 record, Count 1\n' +
    '  3. --mutate N          different content, same type -> stored: N+1 records\n\n' +
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
 * Every alert gets a UNIQUE type, because the alert key is `source:type`. Without
 * that, N alerts sharing a pair would fold into one and the drill would prove
 * nothing.
 *
 * Each alert contributes `copies + mutate` events, all with the SAME
 * (device, event key) — which is the whole point. The group is what tests dedup:
 *
 *   `copies`  identical content, fresh EVT_Identifier  -> absorbed by the first
 *   `mutate`  same type, different EVT_Summary         -> a DIFFERENT fingerprint
 *
 * `EVT_Summary` is the lever for the mutate case, and it is not arbitrary: the
 * fingerprint is `{source, type, severity, payload}`, and `EVT_Summary` lands in
 * `payload.msg`, while `EVT_Identifier` becomes the envelope `id` and is NOT
 * hashed (services/worker/src/event.js). So changing the summary changes the
 * fingerprint; changing only the identifier does not.
 *
 * Returns `[{ event, alertIndex, kind }]` — `alertIndex` lets the CLOSE phase send
 * exactly one retirement per alert rather than one per event.
 */
function buildOpenEvents(cfg, random) {
  const out = [];
  for (let i = 0; i < cfg.count; i += 1) {
    const n = String(i + 1).padStart(3, '0');
    const severity = OPEN_SEVERITIES[Math.floor(random() * OPEN_SEVERITIES.length)];
    const device = SOURCES[Math.floor(random() * SOURCES.length)];
    const base = {
      // The upstream schema. The alert key is `EVT_Device:EVT_EventKey`, so a
      // unique EVT_EventKey per alert is what gives N distinct alerts.
      EVT_Device: device,
      EVT_EventKey: `check-${n}`,
      EVT_Severity: severity,
      EVT_Summary: `Chaos drill ${n}/${cfg.count}: ${device} check-${n} reports ${severity}`,
      EVT_Details: `Chaos drill opening event ${i + 1} of ${cfg.count}`,
      EVT_Agent: 'ChaosDrill',
      EVT_AgentOwner: 'AM-000001',
      EVT_Environment: 'development',
    };

    for (let c = 0; c < cfg.copies; c += 1) {
      out.push({
        alertIndex: i,
        kind: 'base',
        event: {
          ...base,
          // First copy keeps the plain identifier; later copies get a suffix.
          // The id is NOT part of the fingerprint, so this cannot turn an
          // identical copy into a distinct one.
          EVT_Identifier: c === 0
            ? `chaos-open-${n}`
            : `chaos-open-${n}-c${c + 1}`,
        },
      });
    }

    for (let m = 0; m < cfg.mutate; m += 1) {
      out.push({
        alertIndex: i,
        kind: 'mutate',
        event: {
          ...base,
          EVT_Identifier: `chaos-open-${n}-m${m + 1}`,
          // Different `payload.msg` -> different fingerprint -> the gate lets it
          // through and it becomes its own record. This is the control case: if
          // this were absorbed too, dedup would be keyed on the alert rather than
          // on the content, and two genuinely different reports would be lost.
          EVT_Summary: `${base.EVT_Summary} (report ${m + 2} — different detail)`,
        },
      });
    }
  }
  return out;
}

/**
 * Pick a fraction of the opening events and re-send them.
 *
 * Both modes now land on the same outcome, because dedup is on CONTENT:
 *
 *   default      identical content, fresh EVT_Identifier -> absorbed by the
 *                original record. Count increments, Last occurrence moves, and no
 *                second record is stored.
 *   --same-id    identical content, same EVT_Identifier -> identical outcome. The
 *                flag is retained only to demonstrate that the message id is not
 *                what dedup keys on.
 */
/**
 * Pick a fraction of the opening events and re-send them verbatim.
 *
 * This is the original random-sampling mode, kept because it produces a messy,
 * realistic board — copies scattered across alerts rather than a uniform N of
 * each. `--copies` is the deterministic version when you want an exact Count to
 * assert against.
 *
 * Both modes rely on the same rule: the CONTENT must match exactly, including
 * `EVT_Summary`, or the copy becomes a different condition and is stored as its
 * own record. Only `EVT_Identifier` may vary, because it is the envelope id and is
 * not part of the fingerprint.
 */
function buildOpenDuplicates(events, cfg, random) {
  const picks = [];
  for (const item of events) {
    if (random() < cfg.duplicates) picks.push(item);
  }
  // Guarantee at least one copy in a non-empty run, otherwise the drill
  // silently stops testing the thing it exists to test.
  if (events.length > 0 && picks.length === 0) {
    picks.push(events[Math.floor(random() * events.length)]);
  }
  return picks.map((item, i) => ({
    ...item,
    kind: 'random',
    event: {
      ...item.event,
      // A fresh identifier makes this a new MESSAGE carrying the same content;
      // keeping it makes it the same message. Either way the content matches, so
      // the original record absorbs it.
      EVT_Identifier: cfg.sameId
        ? item.event.EVT_Identifier
        : `${item.event.EVT_Identifier}-copy${i + 1}`,
      // NOTE: EVT_Summary is part of the fingerprint, so it must stay IDENTICAL
      // or the copy becomes a different condition and gets its own record —
      // which would make the drill test the wrong thing.
    },
  }));
}

/**
 * Build the CLEAR events — one per opening event, matching its device and event
 * key so it retires exactly that alert.
 *
 * `CLEAR` is the upstream schema's resolution signal; the ingress maps it onto
 * the internal CLOSE severity.
 */
function buildCloseEvents(openEvents, copies) {
  // One retirement per ALERT, not per event: N occurrences of one alert are one
  // alert, and sending N CLEARs would only test CLOSE idempotency — which
  // `--close-copies` does explicitly and deliberately.
  const seen = new Set();
  const out = [];
  for (const item of openEvents) {
    if (seen.has(item.alertIndex)) continue;
    seen.add(item.alertIndex);
    const evt = item.event;
    for (let c = 0; c < copies; c += 1) {
      out.push({
        EVT_Device: evt.EVT_Device,
        EVT_EventKey: evt.EVT_EventKey,
        EVT_Identifier: c === 0
          ? `chaos-clear-${evt.EVT_EventKey.replace('check-', '')}`
          : `chaos-clear-${evt.EVT_EventKey.replace('check-', '')}-c${c + 1}`,
        EVT_Severity: 'CLEAR',
        EVT_Summary: `Resolved: ${evt.EVT_Device} check ${evt.EVT_EventKey} recovered`,
        EVT_Details: 'Chaos drill resolution',
        EVT_Agent: 'ChaosDrill',
        EVT_AgentOwner: 'AM-000001',
        EVT_Environment: 'development',
      });
    }
  }
  return out;
}

/** Duplicate a fraction of the CLEAR events, each 2-6 times. */
function buildCloseDuplicates(closeEvents, cfg, random) {
  const out = [];
  const repeats = [];
  for (const evt of closeEvents) {
    if (random() >= cfg.closeDuplicates) continue;
    const n = cfg.minRepeats + Math.floor(random() * (cfg.maxRepeats - cfg.minRepeats + 1));
    repeats.push({ id: evt.EVT_Identifier, times: n });
    for (let k = 0; k < n; k += 1) {
      out.push({
        ...evt,
        // Same content, so it is absorbed: a re-sent CLEAR must be idempotent,
        // not a new event.
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
  const openItems = buildOpenEvents(cfg, random);
  const openDupes = buildOpenDuplicates(openItems, cfg, random);
  const phase1 = [...openItems, ...openDupes].map((it) => it.event);

  const alertsOpened = new Set(openItems.map((it) => it.alertIndex)).size;
  const identicalCopies = cfg.count * (cfg.copies - 1);
  const mutated = cfg.count * cfg.mutate;
  // One record per alert for the base content, plus one per DISTINCT mutation.
  // The identical copies add none: they are absorbed by the base record.
  const expectedRecords = alertsOpened * (1 + cfg.mutate);
  const expectedCountMax = cfg.copies + cfg.mutate;

  process.stdout.write(
    `\nphase 1: ${alertsOpened} alerts <- ${phase1.length} events\n` +
    `  per alert: ${cfg.copies} identical + ${cfg.mutate} mutated\n` +
    `  identical copies beyond the first: ${identicalCopies}` +
      (cfg.sameId ? ' (SAME id: dropped as a redelivery, Count stays 1)\n'
        : ' (new id: ABSORBED, Count reaches ' + expectedCountMax + ')\n') +
    `  mutated (different EVT_Summary): ${mutated} -> ${expectedRecords} records stored` +
      ` (${alertsOpened} base + ${mutated} mutated)\n` +
    (openDupes.length ? `  random extra copies: ${openDupes.length} (also absorbed)\n` : ''),
  );
  const a1 = await post(cfg.url, cfg.token, phase1, { dryRun: cfg.dryRun });
  process.stdout.write(`phase 1: accepted ${a1}/${phase1.length}\n`);

  process.stdout.write(
    `\nexpected on the board now:\n` +
    `  Open alerts           ${alertsOpened}\n` +
    `  Count on every row    ${cfg.sameId ? 1 : cfg.copies + cfg.mutate}` +
      (cfg.sameId ? '   (copies were dropped, not counted)\n' : '\n') +
    `  fw:evt:* records      ${expectedRecords}` +
      `   <- identical copies must NOT add records\n` +
    `  All events rows       ${expectedRecords}   <- same set, read from the global index\n`,
  );

  // ---- wait for the operator ----------------------------------------------
  await waitForContinue(cfg.pauseSec);

  // ---- phase 2: close -----------------------------------------------------
  const closeEvents = buildCloseEvents(openItems, cfg.closeCopies);
  const { events: closeDupes, repeats } = buildCloseDuplicates(closeEvents, cfg, random);
  const phase2 = [...closeEvents, ...closeDupes];

  const closeAlerts = new Set(closeEvents.map((e) => `${e.EVT_Device}:${e.EVT_EventKey}`)).size;
  process.stdout.write(
    `\nphase 2: closing ${closeAlerts} alerts ` +
    `(${cfg.closeCopies} CLEAR per alert; ${repeats.length} sent an extra 2-${cfg.maxRepeats} times, ` +
    `+${closeDupes.length} events) -> ${phase2.length} events\n`,
  );
  const a2 = await post(cfg.url, cfg.token, phase2, { dryRun: cfg.dryRun });
  process.stdout.write(`phase 2: accepted ${a2}/${phase2.length}\n`);

  // ---- summary ------------------------------------------------------------
  process.stdout.write(
    `\n${'-'.repeat(70)}\n` +
    `expected final state\n` +
    `  alerts opened        ${alertsOpened}\n` +
    `  alerts closed        ${closeAlerts}\n` +
    `  records stored       ${expectedRecords}` +
      (cfg.mutate ? '   (one per DISTINCT content)\n' : '   (one per alert)\n') +
    `  repeated CLEARs      ${closeDupes.length} ` +
      '  (a repeat retirement must not close twice or reopen anything)\n' +
    `\nrepeated CLOSE events (idempotency check):\n` +
    (repeats.length ? repeats.map((r) => `  ${r.id} x${r.times}`).join('\n') : '  (none)') +
    `\n\nverify — counts first, they are the actual assertions:\n` +
    `  PW=$(kubectl -n fire-watch-lab get secret fire-watch-dragonfly -o jsonpath='{.data.password}' | base64 -d)\n` +
    `  R() { kubectl -n fire-watch-lab exec dragonfly-0 -c dragonfly -- \\\n` +
    `         redis-cli -a "$PW" --no-auth-warning "$@"; }\n` +
    `\n  # 1. every alert retired\n` +
    `  R ZCARD fw:idx:alert:open                  # 0\n` +
    `\n  # 2. dedup: one record per DISTINCT content, NOT one per delivery\n` +
    `  R --scan --pattern 'fw:evt:chaos-open-*' | wc -l   # ${expectedRecords}\n` +
    `  #    with --copies 5 and no --mutate this stays ${alertsOpened}, not ${alertsOpened * 5}:\n` +
    `  #    the extra four are absorbed by the first record of each alert.\n` +
    `\n  # 3. the live Count reached ${expectedCountMax}, so the copies were COUNTED even though\n` +
    `  #    they were not STORED. That asymmetry is the design, not a bug:\n` +
    `  #    Count is how often the condition was reported; a record is one stored event.\n` +
    `\n  # 4. the audit trail still holds every delivery\n` +
    `  R --scan --pattern 'fw:audit*' | head\n`,
  );
}

main().catch((err) => {
  process.stderr.write(`chaos-events: ${err?.stack || err}\n`);
  process.exit(1);
});
