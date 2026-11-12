#!/bin/sh
# Verify the duplicate contract end to end against the live cluster:
#
#   same message id, same content  -> DROPPED     (count unchanged, Last unchanged)
#   new id, SAME device+summary    -> FOLDED      (one record, count +1, Last moves)
#   new id, DIFFERENT summary      -> NEW RECORD  (a second row)
#
# Identity is the CONDITION now: sha256(device + 0x1f + summary). That collapses the
# two cases that used to need separate machinery — a redelivery is dropped by the
# (ts, id) watermark, and a re-report of the same condition lands on the same key and
# cannot create a second row.
set -eu
cd "$(dirname "$0")/.."

PW=$(grep -E '^VALKEY_PASSWORD=' .env | cut -d= -f2-)
TOKEN=$(grep -E '^FW_INGEST_TOKEN=' .env | cut -d= -f2- | tr -d '"' | tr -d "'")
NODE=${NODE_IP:-$(kubectl get nodes -o jsonpath='{range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address}{"\n"}{end}' | head -1)}
URL="http://${NODE}:30900/events"
TS=$(date +%s)
SUMMARY="dupcheck probe ${TS}"

keyof() {
  node --input-type=module -e "
    import { bareKeyFor } from './services/worker/src/keygen.js';
    process.stdout.write(bareKeyFor({ source: 'selftest', payload: { msg: process.argv[1] } }));
  " "$1"
}

KEY=$(keyof "$SUMMARY")

rc() { kubectl -n fire-watch-lab exec valkey-0 -c valkey -- \
         valkey-cli -p 6379 --no-auth-warning -a "$PW" --raw "$@"; }

post() {
  curl -sS -o /dev/null -w '%{http_code}' -X POST "$URL" \
    -H "X-Ingest-Token: $TOKEN" -H 'content-type: application/json' -d "$1"
}

echo "record key: $KEY"

code=$(post "{\"id\":\"orig-${TS}\",\"source\":\"selftest\",\"type\":\"dupcheck\",\"severity\":\"CRITICAL\",\"payload\":{\"msg\":\"${SUMMARY}\"}}")
echo "original ingested:      $code"
sleep 2
echo "  count=$(rc hget "event:${KEY}" count)  last=$(rc hget "event:${KEY}" lastSeen)"

before_count=$(rc hget "event:${KEY}" count)
before_last=$(rc hget "event:${KEY}" lastSeen)

# Same id, same content: the fold's (ts, id) watermark must drop it.
code=$(post "{\"id\":\"orig-${TS}\",\"source\":\"selftest\",\"type\":\"dupcheck\",\"severity\":\"CRITICAL\",\"payload\":{\"msg\":\"${SUMMARY}\"}}")
sleep 2
after_count=$(rc hget "event:${KEY}" count)
after_last=$(rc hget "event:${KEY}" lastSeen)
echo "same-id resend:         $code"
echo "  count ${before_count} -> ${after_count}   last ${before_last} -> ${after_last}"

# New id, SAME device+summary: a different message, the same CONDITION. It lands on
# the same key, so it is counted but no second record exists.
code=$(post "{\"id\":\"newid-${TS}\",\"source\":\"selftest\",\"type\":\"dupcheck\",\"severity\":\"CRITICAL\",\"payload\":{\"msg\":\"${SUMMARY}\"}}")
sleep 2
final_count=$(rc hget "event:${KEY}" count)
final_last=$(rc hget "event:${KEY}" lastSeen)
echo "same-condition resend:  $code"
echo "  count ${after_count} -> ${final_count}   last ${after_last} -> ${final_last}"

# New id AND a different summary: a genuinely different condition, therefore a SECOND
# record. Without this the test would pass on a key that ignores the summary.
OTHER=$(keyof "${SUMMARY} (other)")
code=$(post "{\"id\":\"other-${TS}\",\"source\":\"selftest\",\"type\":\"dupcheck\",\"severity\":\"CRITICAL\",\"payload\":{\"msg\":\"${SUMMARY} (other)\"}}")
sleep 2
other_count=$(rc hget "event:${OTHER}" count)
echo "different summary:      $code"
echo "  second record count=${other_count:-<absent>}"

echo "---"
fail=0
[ "$after_count" = "$before_count" ] || { echo "FAIL: a same-id duplicate changed Count"; fail=1; }
[ "$after_last" = "$before_last" ]   || { echo "FAIL: a same-id duplicate moved Last occurrence"; fail=1; }
[ "$final_count" -gt "$after_count" ] || { echo "FAIL: a re-report of the same condition was not counted"; fail=1; }
[ "$final_last" -gt "$after_last" ]   || { echo "FAIL: a re-report did not move Last occurrence"; fail=1; }
[ "$other_count" = "1" ] || { echo "FAIL: a different summary did not create its own record"; fail=1; }
[ "$fail" = 0 ] && echo "PASS: redelivery dropped, repeat folded onto one record, new condition is a new record"

rc del "event:${KEY}" "event:${OTHER}" >/dev/null
rc zrem idx:event:open idx:event:all "$KEY" "$OTHER" >/dev/null 2>&1 || true
exit "$fail"
