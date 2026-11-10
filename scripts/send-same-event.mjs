#!/usr/bin/env node
/**
 * send-same-event.mjs — replay ONE identical event, every time it runs.
 *
 *   node scripts/send-same-event.mjs
 *   node scripts/send-same-event.mjs --show        # print the event, send nothing
 *   node scripts/send-same-event.mjs --count 5     # send it 5 times in one batch
 *   node scripts/send-same-event.mjs --reset       # clear its state, then send
 *
 * Why this exists: it exercises BOTH kinds of duplicate, which are now handled
 * differently (docs/event-lifecycle.md §5.5):
 *
 *   1. TRANSPORT duplicate — the same message id delivered twice. Dropped
 *      entirely: no count, no Last occurrence change.
 *   2. CONTENT duplicate — the same condition reported again under a fresh id.
 *      The ORIGINAL record absorbs it: count increments and Last occurrence
 *      moves, and the new copy is never stored.
 *
 * So on this script:
 *
 *   run 1  -> one record stored, count 1
 *   run 2+ -> count increments, Last occurrence moves, still ONE record stored
 *
 * That is the requested behaviour. `--new-id` is retained only to demonstrate
 * that a fresh id changes nothing now: dedup is on content, not identity.
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
 * The event template. `id` is fixed — that is what makes run 2+ a duplicate —
 * but `ts` is stamped at execution time so the board shows when you actually ran
 * the script rather than a hard-coded date.
 *
 * This does NOT weaken the duplicate test: the worker dedupes on event ID, not on
 * the timestamp. A changing ts only means the stored record differs between runs,
 * which is the honest representation of "the same event was sent again just now".
 */
const EVENT = {
  // Constant, so the second and later runs are duplicates.
  id: 'static-probe-0001',
  source: 'probe',
  type: 'static.probe',
  severity: 'WARNING',
  // Stamped below, at execution time.
  ts: '',
  payload: {
    msg: 'Static probe event: identical on every run',
    CIF: 'CIF-0001',
    AlarmIDKey: 'ALM-STATIC-0001',
    AlarmOwner: 'probe-owner',
    AutoClose: false,
    SupportGroupName: 'Probe Support',
  },
};

// Stamp the producer timestamp now, so the record reflects when this run happened.
EVENT.ts = new Date().toISOString();
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
const NEW_ID = has('--new-id');
const COUNT = Number(get('--count', 1));

if (has('-h') || has('--help')) {
  console.log('send-same-event.mjs — resend one identical event\n\n' +
              '  --show        print the event and exit, send nothing\n' +
              '  --count N     send it N times in one request (default 1)\n' +
              '  --reset       delete the stored event + alert first, so this run\n' +
              '                creates the alert again instead of being dropped\n' +
              '  --new-id      give this run a unique id, so the event is COUNTED\n' +
              '                (count +1, Last occurrence moves) instead of dropped.\n' +
              '                Same payload and alert key either way.\n');
  process.exit(0);
}
if (!Number.isInteger(COUNT) || COUNT < 1) {
  console.error('send-same-event.mjs: --count must be a positive integer');
  process.exit(2);
}

if (SHOW) {
  console.log(JSON.stringify(EVENT, null, 2));
  process.exit(0);
}

const env = dotenv();
const token = env.FW_INGEST_TOKEN || '';
if (!token) {
  console.error('send-same-event.mjs: FW_INGEST_TOKEN missing from .env');
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
    'send-same-event.mjs: cannot determine the ingest endpoint.\n' +
    '  Set INGEST_HOST=<node-ip> in .env, or pass --url.\n',
  );
  process.exit(2);
}

const url = get('--url', `http://${nodeIp}:30900/events`);

/**
 * With --new-id the id becomes unique per run, which turns the event from a
 * duplicate into a fresh occurrence: the worker's `SET … NX` succeeds, the fold
 * counts it, `count` increments and `last_seen` moves.
 *
 * Why this is a flag rather than the default: the whole point of the script in
 * its original form is to prove that a REPEATED id is dropped. Making that
 * impossible by default would leave no way to test the gate. So both behaviours
 * exist, and the flag decides which one you are exercising:
 *
 *   default    same id      -> dropped on re-run (dedup proof)
 *   --new-id   unique id    -> counted on every run (occurrence proof)
 *
 * The alert key (`source:type`) never changes, so both runs land on the SAME row.
 */
const thisRun = NEW_ID
  ? { ...EVENT, id: `${EVENT.id}-${Date.now()}` }
  : EVENT;

const body = COUNT === 1 ? thisRun : Array.from({ length: COUNT }, () => thisRun);

// ------------------------------------------------------------------- reset
/**
 * Delete everything this event owns, so the next run is a first run again.
 *
 * All four keys matter:
 *
 *   fw:evt:{id}            the stored record (and the transport-dedup gate)
 *   fw:fp:{fingerprint}    the CONTENT-dedup gate — while this exists, a re-run
 *                          is absorbed by the original record instead of being
 *                          stored (this is the key that must be cleared to make
 *                          a re-run create a fresh record)
 *   fw:alert:{key}         the alert record, including state=closed
 *   fw:alert:open:{key}    the "is it firing" flag
 *   fw:idx:alert:open      the snapshot index — leaving a member here means a
 *                          closed alert reappears on the next page load
 *
 * Run through kubectl + redis-cli rather than the app, deliberately: there is no
 * HTTP endpoint that deletes an event, and inventing one for a test helper would
 * add a destructive route to a service that must not have one.
 */
if (RESET) {
  const pw = env.DRAGONFLY_PASSWORD || '';
  if (!pw) {
    console.error('send-same-event.mjs: --reset needs DRAGONFLY_PASSWORD in .env');
    process.exit(2);
  }
  const ns = env.NAMESPACE || 'fire-watch-lab';
  const key = `${EVENT.source}:${EVENT.type}`;
  // The fingerprint has to be computed exactly as the worker computes it, or this
  // deletes a key that does not exist and the re-run is still absorbed. Import the
  // real function rather than re-deriving the hash here.
  const { contentFingerprint } = await import('../services/worker/src/event.js');
  const fp = contentFingerprint({
    source: EVENT.source,
    type: EVENT.type,
    severity: EVENT.severity,
    payload: EVENT.payload,
  });
  const script = [
    `redis-cli -p 6379 --no-auth-warning -a '${pw}' --raw del`,
    `fw:evt:${EVENT.id}`,
    // The content-dedup gate. Without this, a re-run is absorbed by the original
    // record instead of creating a new one, which is correct behaviour but not
    // what --reset is being asked for.
    `fw:fp:${fp}`,
    `fw:alert:${key}`,
    `fw:alert:open:${key}`,
    `> /dev/null;`,
    `redis-cli -p 6379 --no-auth-warning -a '${pw}' --raw zrem fw:idx:alert:open '${key}' > /dev/null;`,
    `echo reset-ok`,
  ].join(' ');

  let out = '';
  try {
    out = execFileSync(
      'kubectl',
      ['-n', ns, 'exec', 'dragonfly-0', '-c', 'dragonfly', '--', 'sh', '-c', script],
      { encoding: 'utf8', timeout: 20000 },
    ).trim();
  } catch (err) {
    console.error(`send-same-event.mjs: --reset failed\n  ${err?.message || err}`);
    process.exit(1);
  }
  if (!out.includes('reset-ok')) {
    console.error(`send-same-event.mjs: --reset produced no confirmation: ${out}`);
    process.exit(1);
  }
  console.log(`reset: cleared event ${EVENT.id} and alert ${key}`);
}

const res = await fetch(url, {
  method: 'POST',
  headers: { 'content-type': 'application/json', 'X-Ingest-Token': token },
  body: JSON.stringify(body),
  signal: AbortSignal.timeout(30000),
});
const text = await res.text();

console.log(`sent to ${url} -> HTTP ${res.status} ${text}`);
console.log(`event id: ${thisRun.id}   alert key: ${thisRun.source}:${thisRun.type}`);
if (NEW_ID) {
  console.log(
    COUNT > 1
      ? `${COUNT} copies all share ONE fresh id, so exactly one is stored and the rest are dropped.`
      : 'New id: this run is a distinct event, so count rises and Last occurrence moves.',
  );
} else {
  console.log(
    COUNT > 1
      ? `${COUNT} copies of the SAME id: the first is stored, the rest are dropped.`
      : 'Same content: the original record absorbs this run — count increments and Last occurrence moves, and no second record is stored.',
  );
}
