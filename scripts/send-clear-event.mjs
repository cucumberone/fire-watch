#!/usr/bin/env node
/**
 * send-clear-event.mjs — retire the alert opened by send-same-event.mjs.
 *
 *   node scripts/send-clear-event.mjs
 *   node scripts/send-clear-event.mjs --show        # print the event, send nothing
 *   node scripts/send-clear-event.mjs --count 5     # send it 5 times in one batch
 *   node scripts/send-clear-event.mjs --reset       # clear this event's own state first
 *
 * This is the counterpart to send-same-event.mjs: same device, same event key,
 * same agent, but `EVT_Severity: CLEAR` instead of `WARNING`. Because the alert
 * identity is `EVT_Device:EVT_EventKey` (docs/event-schema.md §2.1), it lands on
 * exactly the row the probe opened — and `CLEAR` maps to the internal `CLOSE`
 * (docs/event-schema.md §3), so the fold retires that alert.
 *
 * ## Why the identifier is DIFFERENT from the probe's
 *
 * `send-same-event.mjs` uses `static-probe-0001`. This script uses
 * `static-probe-clear-0001`, and that is not cosmetic. Reusing the probe's id
 * would be actively wrong:
 *
 *   - The content fingerprint differs (severity and payload both changed), so the
 *     dedup gate would still let it through — but then `SET fw:evt:{id} … NX`
 *     would find the probe's record already there, take the "existing record
 *     absorbed a repeat" path, and never store the CLEAR at all.
 *   - The fold would still run with severity CLOSE and the alert would still
 *     retire, so it would *look* like it worked. But the audit trail would have
 *     no CLEAR record, and the fingerprint table would be left pointing at a
 *     record that represents a WARNING. That is a silent lie in the history.
 *
 * A retirement event is a real event with its own identity. It is not "the same
 * event as before but with a different severity".
 *
 * ## Repeat runs are harmless
 *
 * Run it twice and the second run is a content duplicate: absorbed by the first
 * CLEAR's record, count increments, and the fold returns 3 again. The alert stays
 * retired. Sending a CLEAR when nothing is open is also safe — the fold's
 * retirement branch returns before it ever checks whether the alert was open.
 *
 * Endpoint discovery and the token are the same as the other scripts here: read
 * `.env`, then ask kubectl for a node address, because http-ingress is a NodePort
 * and is never reachable as localhost.
 */

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { execFileSync } from 'node:child_process';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');

/**
 * The retirement event, in the upstream schema.
 *
 * `EVT_Device` and `EVT_EventKey` are copied verbatim from send-same-event.mjs —
 * those two fields ARE the alert identity, so they are what makes this retire the
 * probe's alert rather than create a second, unrelated one.
 *
 * `EVT_Severity: 'CLEAR'` is the resolution signal. The schema has no `CLOSE`;
 * `CLEAR` is what maps onto the internal retirement severity.
 *
 * `EVT_Identifier` is fixed (so this is a reproducible event like the probe) but
 * deliberately distinct from the probe's — see the note above.
 */
const EVENT = {
  EVT_Device: 'probe-device',
  EVT_EventKey: 'static.probe',
  EVT_Identifier: 'static-probe-clear-0001',
  // Source severity. Maps to the internal CLOSE, which retires the alert.
  EVT_Severity: 'CLEAR',
  EVT_Summary: 'Static probe event cleared by operator',
  EVT_Details: 'The condition reported by static.probe is no longer present.',
  EVT_Agent: 'FireWatchProbe',
  EVT_AgentOwner: 'AM-000001',
  EVT_Environment: 'development',
  EVT_URL: 'https://example.invalid/probe/static',
};

function dotenv() {
  try {
    const out = {};
    for (const line of readFileSync(join(ROOT, '.env'), 'utf8').split('\n')) {
      const m = /^\s*([A-Z0-9_]+)\s*=\s*(.*)$/.exec(line);
      if (m) out[m[1]] = m[2].trim().replace(/^["']|["']$/g, '');
    }
    return out;
  } catch {
    return {};
  }
}

const args = process.argv.slice(2);
const has = (f) => args.includes(f);
const get = (f, d) => { const i = args.indexOf(f); return i === -1 ? d : args[i + 1]; };
const SHOW = has('--show');
const RESET = has('--reset');
const COUNT = Number(get('--count', 1));

if (has('-h') || has('--help')) {
  console.log('send-clear-event.mjs — send a CLEAR for the static probe alert\n\n' +
              '  --show        print the event and exit, send nothing\n' +
              '  --count N     send it N times in one request (default 1)\n' +
              '  --reset       delete this CLEAR\'s own record + fingerprint first,\n' +
              '                so a re-run is stored fresh rather than absorbed.\n' +
              '                It does NOT reopen or alter the alert itself.\n' +
              '  --url URL     override the ingest endpoint\n\n' +
              'Retires the alert opened by send-same-event.mjs, because it uses the\n' +
              'same EVT_Device:EVT_EventKey and EVT_Severity=CLEAR.\n');
  process.exit(0);
}
if (!Number.isInteger(COUNT) || COUNT < 1) {
  console.error('send-clear-event.mjs: --count must be a positive integer');
  process.exit(2);
}

if (SHOW) {
  console.log(JSON.stringify(EVENT, null, 2));
  process.exit(0);
}

const env = dotenv();
const token = env.FW_INGEST_TOKEN || '';
if (!token) {
  console.error('send-clear-event.mjs: FW_INGEST_TOKEN missing from .env');
  process.exit(2);
}

let nodeIp = env.INGEST_HOST;
if (!nodeIp) {
  try {
    const out = execFileSync('kubectl', [
      'get', 'nodes', '-o',
      'jsonpath={range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address}{"\\n"}{end}',
    ], { encoding: 'utf8', timeout: 10000 });
    nodeIp = out.split('\n').map((s) => s.trim()).filter(Boolean)[0];
  } catch {
    // handled below
  }
}
if (!nodeIp) {
  console.error(
    'send-clear-event.mjs: cannot determine the ingest endpoint.\n' +
    '  Set INGEST_HOST=<node-ip> in .env, or pass --url.\n',
  );
  process.exit(2);
}

const url = get('--url', `http://${nodeIp}:30900/events`);
const body = COUNT === 1 ? EVENT : Array.from({ length: COUNT }, () => EVENT);

// The canonical mapping is imported rather than re-derived, so the alert key and
// fingerprint always match what the ingress and worker compute.
const { fromUpstream } = await import('../services/http-ingress/src/normalise.js');
const { contentFingerprint } = await import('../services/worker/src/event.js');

const canonical = fromUpstream(EVENT);
const key = `${canonical.source}:${canonical.type}`;
const fp = contentFingerprint(canonical);
const ns = env.NAMESPACE || 'fire-watch-lab';
const pw = env.DRAGONFLY_PASSWORD || '';

// ------------------------------------------------------------------- reset
/**
 * Clear only what THIS event owns.
 *
 * Deliberately narrower than send-same-event.mjs's reset, which also wipes the
 * alert hash, the open flag and the open-index member. Here the alert is the
 * thing being retired — deleting it first would make the CLEAR a no-op against
 * a row that no longer exists, and the run would prove nothing.
 *
 *   fw:evt:{id}          this CLEAR's record
 *   fw:fp:{fingerprint}  the content-dedup gate; while it exists a re-run is
 *                        absorbed by the first CLEAR instead of being stored
 */
if (RESET) {
  if (!pw) {
    console.error('send-clear-event.mjs: --reset needs DRAGONFLY_PASSWORD in .env');
    process.exit(2);
  }
  const script = [
    `redis-cli -p 6379 --no-auth-warning -a '${pw}' --raw del`,
    `fw:evt:${canonical.id}`,
    `fw:fp:${fp}`,
    `> /dev/null; echo reset-ok`,
  ].join(' ');
  let out = '';
  try {
    out = execFileSync(
      'kubectl',
      ['-n', ns, 'exec', 'dragonfly-0', '-c', 'dragonfly', '--', 'sh', '-c', script],
      { encoding: 'utf8', timeout: 20000 },
    ).trim();
  } catch (err) {
    console.error(`send-clear-event.mjs: --reset failed\n  ${err?.message || err}`);
    process.exit(1);
  }
  if (!out.includes('reset-ok')) {
    console.error(`send-clear-event.mjs: --reset produced no confirmation: ${out}`);
    process.exit(1);
  }
  console.log(`reset: cleared record ${canonical.id} (alert state untouched)`);
}

const res = await fetch(url, {
  method: 'POST',
  headers: { 'content-type': 'application/json', 'X-Ingest-Token': token },
  body: JSON.stringify(body),
  signal: AbortSignal.timeout(30000),
});
const text = await res.text();

console.log(`sent to ${url} -> HTTP ${res.status} ${text}`);
console.log(
  `event id: ${canonical.id}   alert key: ${key}   severity: ${canonical.severity}` +
  (canonical.severity === 'CLOSE' ? ' (retires)' : ' (NOT a retirement severity!)'),
);

if (canonical.severity !== 'CLOSE') {
  // Guard against exactly the mistake this script exists to avoid: a CLEAR that
  // was edited into a severity that opens an alert instead of ending one.
  console.error(
    '\nsend-clear-event.mjs: EVT_Severity did not map to CLOSE, so this event\n' +
    'CANNOT retire anything. Expected CLEAR -> CLOSE in the ingress SEVERITY_MAP.',
  );
  process.exit(1);
}

// ------------------------------------------------- read back the alert state
/**
 * The only way to see that a CLEAR actually worked, without opening a browser.
 *
 * Best-effort: a missing password or an unreachable pod prints a note rather than
 * failing, because the event was already accepted and exit 1 would wrongly imply
 * the send failed.
 *
 * The worker is asynchronous, so the fold may not have run yet at the instant the
 * ingress answers 202. Retry briefly rather than reporting a stale `state=open`.
 */
if (!pw) {
  console.log('\nalert state: skipped (DRAGONFLY_PASSWORD not in .env)');
} else {
  const readState = () => {
    const script =
      `redis-cli -p 6379 --no-auth-warning -a '${pw}' --raw ` +
      `hget 'fw:alert:${key}' state; ` +
      `redis-cli -p 6379 --no-auth-warning -a '${pw}' --raw ` +
      `hget 'fw:alert:${key}' severity; ` +
      `redis-cli -p 6379 --no-auth-warning -a '${pw}' --raw ` +
      `exists 'fw:alert:open:${key}'`;
    try {
      return execFileSync(
        'kubectl',
        ['-n', ns, 'exec', 'dragonfly-0', '-c', 'dragonfly', '--', 'sh', '-c', script],
        { encoding: 'utf8', timeout: 15000 },
      ).split('\n').map((s) => s.trim());
    } catch {
      return null;
    }
  };

  let state = null;
  for (let i = 0; i < 10; i += 1) {
    const got = readState();
    if (got) {
      const [st, sev, openFlag] = got;
      state = { st, sev, openFlag };
      // `closed` plus a missing open flag is the fully settled outcome.
      if (st === 'closed' && openFlag === '0') break;
    }
    await new Promise((r) => setTimeout(r, 300));
  }

  if (!state) {
    console.log(`\nalert state: unreadable (could not exec into dragonfly-0)`);
  } else if (state.st === 'closed' && state.openFlag === '0') {
    console.log(
      `\nalert state: closed (severity=${state.sev}, open flag cleared) — the alert is retired.`,
    );
  } else if (state.st === 'closed') {
    console.log(
      `\nalert state: closed but the open flag is still set (exists=${state.openFlag}).\n` +
      '  That is the failure docs/event-lifecycle.md §5.2 guards against: the index\n' +
      '  would hand a closed row to every new client. Check that the worker and the\n' +
      '  alert-fold ConfigMap are rolled out together.',
    );
    process.exitCode = 1;
  } else {
    console.log(
      `\nalert state: ${state.st || '(none)'} — NOT retired.\n` +
      '  If no alert was ever open, run send-same-event.mjs first.\n' +
      '  If one was open, check the worker log for a fold watermark rejection.',
    );
    process.exitCode = 1;
  }
}

console.log(
  COUNT > 1
    ? `\n${COUNT} CLEAR copies sent: the first stores the record, the rest are absorbed by it. The alert stays retired.`
    : '\nRepeat runs are safe: the second CLEAR is absorbed by the first and the alert stays retired.',
);
