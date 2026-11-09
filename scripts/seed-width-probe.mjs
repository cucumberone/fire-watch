#!/usr/bin/env node
/**
 * seed-width-probe.mjs — put N alerts on the board with long summaries.
 *
 * Exists so UI work (column widths, wrapping, row colouring) can be judged
 * against realistic content instead of an empty table. Kept separate from
 * chaos-events.mjs because that script is a dedup drill with its own meaning;
 * this one only cares about producing rows of a known shape.
 *
 *   node scripts/seed-width-probe.mjs                 # 40 alerts
 *   node scripts/seed-width-probe.mjs --count 200
 *   node scripts/seed-width-probe.mjs --close         # retire them again
 *
 * Reads FW_INGEST_TOKEN from .env and the endpoint from the running cluster,
 * same discovery as chaos-events.mjs.
 */
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { execFileSync } from 'node:child_process';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');

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
const get = (flag, dflt) => {
  const i = args.indexOf(flag);
  return i === -1 ? dflt : args[i + 1];
};
const COUNT = Number(get('--count', 40));
const CLOSE = args.includes('--close');

const env = dotenv();
const token = env.FW_INGEST_TOKEN || '';
if (!token) {
  console.error('seed-width-probe: FW_INGEST_TOKEN missing from .env');
  process.exit(2);
}

// Discover a node IP, exactly as chaos-events.mjs does: the ingress is a
// NodePort, so it is never reachable on localhost from this machine.
let nodeIp = env.INGEST_HOST;
if (!nodeIp) {
  const out = execFileSync('kubectl', [
    'get', 'nodes', '-o',
    'jsonpath={range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address}{"\\n"}{end}',
  ], { encoding: 'utf8', timeout: 10000 });
  nodeIp = out.split('\n').map((s) => s.trim()).filter(Boolean)[0];
}
const url = `http://${nodeIp}:30900/events`;

const SOURCES = ['shop', 'db', 'net', 'auth'];
const SEVERITIES = ['CRITICAL', 'WARNING', 'FATAL'];

const events = [];
for (let i = 1; i <= COUNT; i += 1) {
  const n = String(i).padStart(3, '0');
  events.push({
    id: `${CLOSE ? 'wpclose' : 'wp'}-${n}`,
    source: SOURCES[i % SOURCES.length],
    type: `wcheck-${n}`,
    severity: CLOSE ? 'CLOSE' : SEVERITIES[i % SEVERITIES.length],
    payload: {
      msg: CLOSE
        ? `Resolved: width probe ${n}`
        : `Width probe ${n}: a deliberately long summary line so the column has to flex and the layout can be judged`,
    },
  });
}

const res = await fetch(url, {
  method: 'POST',
  headers: { 'content-type': 'application/json', 'X-Ingest-Token': token },
  body: JSON.stringify(events),
  signal: AbortSignal.timeout(30000),
});
const text = await res.text();
console.log(`${CLOSE ? 'closed' : 'opened'} via ${url} -> HTTP ${res.status} ${text}`);
console.log(`rows affected: ${COUNT}`);
