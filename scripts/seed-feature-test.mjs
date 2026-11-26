/**
 * Generate a fixed, verifiable batch of events through the real ingress.
 *
 *   FW_INGEST_TOKEN=… node scripts/seed-feature-test.mjs [--url http://host:30900/events]
 *
 * The batch is shaped so every number it prints can be checked independently in
 * Valkey afterwards, and so a single miscount is visible rather than averaged away.
 * It also exercises CORRELATION: a CLEAR/HARMLESS arriving on the same
 * device+summary+eventKey as an earlier CRITICAL must retire that record rather than
 * create a second one.
 *
 *   45  A  CRITICAL, one condition            => 1 record, count 45, OPEN
 *   20  B  CRITICAL, one condition            => 1 record, count 20
 *    5  B  CLEAR on the same condition        => the SAME record, now CLOSED
 *   10  C  CRITICAL, 10 distinct conditions   => 10 records, count 1, OPEN
 *   10  D  CRITICAL, 10 distinct conditions   => 10 records, count 1
 *   10  D  HARMLESS on those same conditions  => the SAME 10 records, now CLOSED
 *
 *   ------
 *  100 POSTs, 100 arrivals, 22 records
 *
 *   open   = 11  (A + C)
 *   closed = 11  (B + D)
 *
 * Two things this proves that a CRITICAL-only batch cannot:
 *
 *   1. RETIREMENT CORRELATES. B's CLEAR and D's HARMLESS carry the same
 *      device+summary+eventKey as their CRITICAL, so they land on the same record key
 *      and close it. A retirement that failed to correlate would show as 11 extra
 *      orphan records (22 -> 33), not as a subtle difference.
 *   2. A RETIREMENT DOES NOT BUMP `count`. The fold's retirement branch returns 3
 *      without touching the counter, so B stays at 20 after its 5 CLEARs — 25 would
 *      mean the counter was incremented on a close.
 *
 * D's arrivals have EVT_Identifier BLANK on purpose: with an identifier policy
 * configured the key becomes sha256(device + summary + eventKey), so the CRITICAL and
 * the HARMLESS agree only if the policy is applied consistently to both. That is the
 * correlation case a supplied identifier would have hidden.
 */
const URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'http://192.168.50.4:30900/events';
const TOKEN = process.env.FW_INGEST_TOKEN;

if (!TOKEN) {
  console.error('FW_INGEST_TOKEN is required (read it from the cluster secret)');
  process.exit(1);
}

const RUN = process.env.RUN_ID || String(Date.now());

/** Shared shape, so a retirement can reuse the exact fields its CRITICAL used. */
function condition(device, severity, over = {}) {
  return {
    EVT_Device: device,
    EVT_Severity: severity,
    EVT_Agent: 'seed-feature-test',
    EVT_AgentOwner: 'lab',
    ...over,
  };
}

const A = 'cond-a';
const B = 'cond-b';
const D = Array.from({ length: 10 }, (_, i) => `retire-${String(i).padStart(2, '0')}`);

const batch = [];

// ---- A: 45 CRITICAL on ONE condition. Stays open, count 45.
for (let i = 0; i < 45; i += 1) {
  batch.push(condition(A, 'CRITICAL', {
    EVT_EventKey: 'dup.test',
    EVT_Summary: `duplicate condition ${RUN}`,
    EVT_Details: `arrival ${i + 1} of 45 for the same condition`,
  }));
}

// ---- B: 20 CRITICAL, then 5 CLEAR, all on ONE condition.
//
// The CLEARs share device+summary+eventKey with the CRITICALs, so they retire the same
// record. `count` must stay 20 — a retirement is not an occurrence.
for (let i = 0; i < 20; i += 1) {
  batch.push(condition(B, 'CRITICAL', {
    EVT_EventKey: 'dup.withid',
    EVT_Summary: `identified condition ${RUN}`,
    // Reused every 5 sends: a supplied identifier does not split the record, the
    // condition does.
    EVT_Identifier: `id-block-${RUN}-${Math.floor(i / 5)}`,
  }));
}
for (let i = 0; i < 5; i += 1) {
  batch.push(condition(B, 'CLEAR', {
    EVT_EventKey: 'dup.withid',
    EVT_Summary: `identified condition ${RUN}`,
    EVT_Identifier: `clear-${RUN}-${i}`,
    EVT_Details: 'resolved by the source',
  }));
}

// ---- C: 10 CRITICAL on 10 DISTINCT conditions. All stay open.
for (let i = 0; i < 10; i += 1) {
  batch.push(condition(`uniq-${String(i).padStart(2, '0')}`, 'CRITICAL', {
    EVT_EventKey: 'unique.test',
    EVT_Summary: `unique condition ${i} ${RUN}`,
    EVT_Identifier: `uniq-id-${RUN}-${i}`,
  }));
}

// ---- D: 10 CRITICAL then 10 HARMLESS, on the SAME 10 conditions.
//
// HARMLESS retires exactly like CLEAR (both map to the internal CLOSE), so this proves
// the other producer spelling correlates too. The identifiers are BLANK, which is what
// puts the operator's identifier policy on the hook: CRITICAL and HARMLESS must hash to
// the same key or this block shows as 20 records instead of 10.
for (let i = 0; i < D.length; i += 1) {
  batch.push(condition(D[i], 'CRITICAL', {
    EVT_EventKey: 'hash.test',
    EVT_Summary: `blank identifier ${i} ${RUN}`,
  }));
}
for (let i = 0; i < D.length; i += 1) {
  batch.push(condition(D[i], 'HARMLESS', {
    EVT_EventKey: 'hash.test',
    EVT_Summary: `blank identifier ${i} ${RUN}`,
  }));
}

if (batch.length !== 100) throw new Error(`expected 100 events, built ${batch.length}`);

/** POST one event; returns the status code. */
async function post(evt) {
  const res = await fetch(URL, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'X-Ingest-Token': TOKEN },
    body: JSON.stringify(evt),
  });
  return res.status;
}

const codes = new Map();
for (const evt of batch) {
  let code;
  try {
    code = await post(evt);
  } catch (err) {
    code = `ERR ${err.message}`;
  }
  codes.set(code, (codes.get(code) ?? 0) + 1);
}

console.log(`POSTed ${batch.length} events to ${URL}`);
for (const [code, n] of [...codes].sort()) console.log(`  HTTP ${code}: ${n}`);
console.log();
console.log('Expect in Valkey (checked independently, not read back from here):');
console.log('  idx:history      = 100   one row per ARRIVAL');
console.log('  idx:event:all    = 22    one row per CONDITION');
console.log('  idx:event:open   = 11    A + C');
console.log('  idx:event:closed = 11    B + D  <- the CLEAR/HARMLESS correlated');
console.log(`  ${A} count       = 45    state=open`);
console.log(`  ${B} count       = 20    state=closed (a retirement does not count)`);
console.log(`  ${D[0]} count   = 1     state=closed, keyed by the identifier hash`);
console.log(`  run id           = ${RUN}`);
