#!/usr/bin/env node
/**
 * backfill-event-index.mjs — rebuild `idx:event:all` from the stored records.
 *
 * WHY THIS EXISTS. The All events tab reads ONE zset (`idx:event:all`), written by
 * the fold in the same atomic step as the record. Records created before that index
 * existed — or written while the index write was failing — are in storage but not in
 * the index, so the tab can never list them:
 *
 *   record hashes      ->  7
 *   idx:event:all      -> 3 members
 *
 * Four conditions existed and were invisible. Nothing was broken in the query; the
 * query reads an index, and the index was incomplete.
 *
 * WHAT IT DOES NOT DO. It does not verify that the indexed keys have records, and it
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
 * Requires the Valkey password. It reads it from the cluster secret, or from
 * $VALKEY_PASSWORD, or from the local .env.
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
  || 'valkey-0';
const CONTAINER = 'valkey';

/**
 * The record key prefix and the index name.
 *
 * The record is a HASH per condition at `event:<32 hex>`, so `--scan --pattern
 * 'event:*'` returns keys, not ids — the member of the index IS the bare key, so the
 * scan result is used directly rather than sliced into an id.
 */
const PREFIX = 'event:';
const INDEX = 'idx:event:all';

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

/** Resolve the Valkey password: env, then .env, then the cluster secret. */
function resolvePassword() {
  if (process.env.VALKEY_PASSWORD) return process.env.VALKEY_PASSWORD;
  const fromFile = readDotEnv().VALKEY_PASSWORD;
  if (fromFile) return fromFile;
  try {
    const b64 = execFileSync('kubectl', [
      '-n', NAMESPACE, 'get', 'secret', 'fire-watch-valkey',
      '-o', 'jsonpath={.data.password}',
    ], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();
    if (b64) return Buffer.from(b64, 'base64').toString('utf8');
  } catch { /* fall through */ }
  return '';
}

/** Run one valkey-cli command inside the Valkey pod. */
function cli(password, ...argv) {
  return execFileSync('kubectl', [
    '-n', NAMESPACE, 'exec', '-i', POD, '-c', CONTAINER, '--',
    'valkey-cli', '-a', password, '--no-auth-warning', ...argv,
  ], { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
}

/**
 * Read `brokerTs` for a batch of bare keys.
 *
 * HMGET rather than MGET: the record is a hash, so the field has to be named. One
 * exec per batch, because the pod exec is the expensive part, not the command.
 */
function brokerTs(keys, password) {
  const flat = cli(password, 'HMGET', ...keys.flatMap((k) => [`${PREFIX}${k}`]), 'brokerTs');
  // HMGET with many keys and one field returns one line per key in order.
  return flat.split('\n').map((s) => s.trim());
}

function main() {
  const password = resolvePassword();
  if (!password) fail('no Valkey password: set VALKEY_PASSWORD or create the secret');

  process.stdout.write(`backfill-event-index: ns=${NAMESPACE} pod=${POD}${DRY ? '  [DRY RUN]' : ''}\n`);

  // ---- what storage holds
  const keys = cli(password, '--scan', '--pattern', `${PREFIX}*`)
    .split('\n').map((s) => s.trim()).filter(Boolean);
  if (keys.length === 0) {
    process.stdout.write(`no ${PREFIX}* records — nothing to backfill\n`);
    return;
  }
  const bare = keys.map((k) => k.slice(PREFIX.length));
  process.stdout.write(`storage holds ${bare.length} record(s)\n`);

  // ---- what the index holds
  const indexed = new Set(
    cli(password, 'ZRANGE', INDEX, '0', '-1')
      .split('\n').map((s) => s.trim()).filter(Boolean),
  );
  process.stdout.write(`${INDEX} holds ${indexed.size} member(s)\n`);

  const missing = bare.filter((k) => !indexed.has(k));
  process.stdout.write(`${missing.length} record(s) missing from the index\n`);
  if (missing.length === 0) {
    process.stdout.write('index is complete — nothing to do\n');
    return;
  }

  // ---- read each missing record for its brokerTs.
  const CHUNK = 200;
  let added = 0;
  let skipped = 0;
  for (let i = 0; i < missing.length; i += CHUNK) {
    const slice = missing.slice(i, i + CHUNK);
    const scores = brokerTs(slice, password);
    for (let j = 0; j < slice.length; j += 1) {
      const key = slice[j];
      const raw = scores[j];
      if (!raw || raw === '') {
        // Listed by SCAN but the hash lost its brokerTs: a partial write, or the
        // record expired between the two calls. Do not invent a score.
        process.stdout.write(`  skip ${key}: record vanished or has no brokerTs\n`);
        skipped += 1;
        continue;
      }
      const score = Number(raw);
      if (!Number.isFinite(score) || score <= 0) {
        // A record with no usable timestamp cannot be placed on a timeline.
        // Skipping is honest; scoring it "now" would put a week-old event at the
        // top of the All events list.
        process.stdout.write(`  skip ${key}: unusable brokerTs ${raw}\n`);
        skipped += 1;
        continue;
      }
      if (DRY) {
        process.stdout.write(`  would add ${key} @ ${score}\n`);
      } else {
        cli(password, 'ZADD', INDEX, String(score), key);
      }
      added += 1;
    }
  }

  process.stdout.write(`\n${DRY ? 'would add' : 'added'} ${added}, skipped ${skipped}\n`);
  if (!DRY) {
    const after = cli(password, 'ZCARD', INDEX).trim();
    process.stdout.write(`${INDEX} now holds ${after} member(s)\n`);
    process.stdout.write(
      'reload the All events tab to see them. The pruning cutoff is the worker\'s\n'
      + 'event TTL, so anything older than that will be removed again on the next\n'
      + 'prune as expected.\n',
    );
  }
}

main();
