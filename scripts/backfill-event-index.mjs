#!/usr/bin/env node
/**
 * backfill-event-index.mjs — rebuild `fw:idx:evt:all` from the stored records.
 *
 * WHY THIS EXISTS. The All events tab reads ONE zset (`fw:idx:evt:all`), written
 * by the worker in the same step as the per-source index. Records created before
 * that index existed — or written while the index write was failing — are in
 * storage but not in the index, so the tab can never list them:
 *
 *   fw:evt:*  ->  7 records
 *   fw:idx:evt:all -> 3 members
 *
 * Four events existed and were invisible. Nothing was broken in the query; the
 * query reads an index, and the index was incomplete.
 *
 * WHAT IT DOES NOT DO. It does not verify that the indexed ids have records, and it
 * does not prune. It only ADDS missing members, scored by each record's `brokerTs`,
 * so the timeline order matches what the reader expects. Running it twice is safe:
 * ZADD on an existing member only updates its score.
 *
 * THE WORKER NOW DOES THIS AT STARTUP. `reconcileEventIndex` in
 * services/worker/src/store.js runs before consuming, bounded by
 * `FW_INDEX_RECONCILE_MAX`, and logs what it repaired. This script remains for the
 * cases that path cannot cover: a full history larger than the startup cap, and
 * running the repair without restarting a worker that is happily serving.
 *
 * USAGE
 *   node scripts/backfill-event-index.mjs --dry-run
 *   node scripts/backfill-event-index.mjs
 *
 * Requires the Dragonfly password. It reads it from the cluster secret, or from
 * $DRAGONFLY_PASSWORD, or from the local .env.
 */

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { execFileSync } from 'node:child_process';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const args = process.argv.slice(2);
const DRY = args.includes('--dry-run') || args.includes('-n');
const NAMESPACE = (args.find((a) => a.startsWith('--namespace=')) || '').split('=')[1]
  || process.env.NAMESPACE || 'fire-watch-lab';
const POD = (args.find((a) => a.startsWith('--pod=')) || '').split('=')[1]
  || 'dragonfly-0';
const CONTAINER = 'dragonfly';

function fail(msg) {
  process.stderr.write(`backfill-event-index: ${msg}\n`);
  process.exit(2);
}

function readDotEnv() {
  try {
    const text = readFileSync(join(ROOT, '.env'), 'utf8');
    const out = {};
    for (const line of text.split('\n')) {
      const m = /^\s*([A-Z0-9_]+)\s*=\s*(.*)$/.exec(line);
      if (m) out[m[1]] = m[2].trim().replace(/^["']|["']$/g, '');
    }
    return out;
  } catch {
    return {};
  }
}

/** Resolve the Dragonfly password: env, then .env, then the cluster secret. */
function resolvePassword() {
  if (process.env.DRAGONFLY_PASSWORD) return process.env.DRAGONFLY_PASSWORD;
  const fromFile = readDotEnv().DRAGONFLY_PASSWORD;
  if (fromFile) return fromFile;
  try {
    const b64 = execFileSync('kubectl', [
      '-n', NAMESPACE, 'get', 'secret', 'fire-watch-dragonfly',
      '-o', 'jsonpath={.data.password}',
    ], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();
    if (b64) return Buffer.from(b64, 'base64').toString('utf8');
  } catch { /* fall through */ }
  return '';
}

/** Run one redis-cli command inside the Dragonfly pod. */
function redis(password, ...argv) {
  const out = execFileSync('kubectl', [
    '-n', NAMESPACE, 'exec', '-i', POD, '-c', CONTAINER, '--',
    'redis-cli', '-a', password, '--no-auth-warning', ...argv,
  ], { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  return out;
}

function main() {
  const password = resolvePassword();
  if (!password) fail('no Dragonfly password: set DRAGONFLY_PASSWORD or create the secret');

  process.stdout.write(`backfill-event-index: ns=${NAMESPACE} pod=${POD}${DRY ? '  [DRY RUN]' : ''}\n`);

  // ---- what storage holds
  const keys = redis(password, '--scan', '--pattern', 'fw:evt:*')
    .split('\n').map((s) => s.trim()).filter(Boolean);
  if (keys.length === 0) {
    process.stdout.write('no fw:evt:* records — nothing to backfill\n');
    return;
  }
  const ids = keys.map((k) => k.slice('fw:evt:'.length));
  process.stdout.write(`storage holds ${ids.length} record(s)\n`);

  // ---- what the index holds
  const indexed = new Set(
    redis(password, 'ZRANGE', 'fw:idx:evt:all', '0', '-1')
      .split('\n').map((s) => s.trim()).filter(Boolean),
  );
  process.stdout.write(`fw:idx:evt:all holds ${indexed.size} member(s)\n`);

  const missing = ids.filter((id) => !indexed.has(id));
  process.stdout.write(`${missing.length} record(s) missing from the index\n`);
  if (missing.length === 0) {
    process.stdout.write('index is complete — nothing to do\n');
    return;
  }

  // ---- read each missing record for its brokerTs.
  //
  // MGET in chunks rather than one call per id: the pod exec is the expensive part,
  // not the Redis command inside it.
  const CHUNK = 200;
  let added = 0;
  let skipped = 0;
  for (let i = 0; i < missing.length; i += CHUNK) {
    const slice = missing.slice(i, i + CHUNK);
    const raw = redis(password, 'MGET', ...slice.map((id) => `fw:evt:${id}`))
      .split('\n');
    for (let j = 0; j < slice.length; j += 1) {
      const id = slice[j];
      const line = raw[j];
      if (!line || line === '') {
        // Listed by SCAN but the value is gone: expired between the two calls, or
        // a wrong key type. Do not invent a score.
        process.stdout.write(`  skip ${id}: record vanished or unreadable\n`);
        skipped += 1;
        continue;
      }
      let score;
      try {
        const rec = JSON.parse(line);
        score = Number(rec.brokerTs ?? rec.ts ?? Date.now());
        if (!Number.isFinite(score)) throw new Error('no usable timestamp');
      } catch (err) {
        // A record with no timestamp cannot be placed on a timeline. Skipping is
        // honest; scoring it "now" would put a week-old event at the top of the
        // All events list.
        process.stdout.write(`  skip ${id}: ${err.message}\n`);
        skipped += 1;
        continue;
      }
      if (DRY) {
        process.stdout.write(`  would add ${id} @ ${score}\n`);
      } else {
        redis(password, 'ZADD', 'fw:idx:evt:all', String(score), id);
      }
      added += 1;
    }
  }

  process.stdout.write(
    `\n${DRY ? 'would add' : 'added'} ${added}, skipped ${skipped}\n`,
  );
  if (!DRY) {
    const after = redis(password, 'ZCARD', 'fw:idx:evt:all').trim();
    process.stdout.write(`fw:idx:evt:all now holds ${after} member(s)\n`);
    process.stdout.write(
      'reload the All events tab to see them. The pruning cutoff is the worker\'s\n'
      + 'event TTL, so anything older than that will be removed again on the next\n'
      + 'prune as expected.\n',
    );
  }
}

main();
