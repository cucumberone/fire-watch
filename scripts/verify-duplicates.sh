#!/bin/sh
# Verify the duplicate contract end to end against the live cluster:
#
#   same event id       -> DROPPED  (count unchanged, Last unchanged)
#   new id, same alert  -> COUNTED  (count +1, Last moves)
#
# A duplicate is identified by EVENT ID. That is the only stable identity: every
# broker delivery carries a fresh timestamp, so a re-sent id always looks newer
# and a timestamp-based check cannot catch it.
#
# Requires the worker to be running the build that gates on `SET ... NX`.
set -eu
cd "$(dirname "$0")/.."

PW=$(grep -E '^DRAGONFLY_PASSWORD=' .env | cut -d= -f2-)
TOKEN=$(grep -E '^FW_INGEST_TOKEN=' .env | cut -d= -f2- | tr -d '"' | tr -d "'")
NODE=$(kubectl get nodes -o jsonpath='{range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address}{"\n"}{end}' | head -1)
URL="http://${NODE}:30900/events"
TS=$(date +%s)
TYPE="dupcheck-${TS}"
KEY="selftest:${TYPE}"

rc() { kubectl -n fire-watch-lab exec dragonfly-0 -c dragonfly -- \
         redis-cli -p 6379 --no-auth-warning -a "$PW" --raw "$@"; }

post() {
  curl -sS -o /dev/null -w '%{http_code}' -X POST "$URL" \
    -H "X-Ingest-Token: $TOKEN" -H 'content-type: application/json' -d "$1"
}

echo "key: $KEY"

code=$(post "{\"id\":\"orig-${TS}\",\"source\":\"selftest\",\"type\":\"${TYPE}\",\"severity\":\"CRITICAL\",\"payload\":{\"msg\":\"original\"}}")
echo "original ingested:      $code"
sleep 2
echo "  count=$(rc hget "fw:alert:${KEY}" count)  last=$(rc hget "fw:alert:${KEY}" last_seen)"

before_count=$(rc hget "fw:alert:${KEY}" count)
before_last=$(rc hget "fw:alert:${KEY}" last_seen)
code=$(post "{\"id\":\"orig-${TS}\",\"source\":\"selftest\",\"type\":\"${TYPE}\",\"severity\":\"CRITICAL\",\"payload\":{\"msg\":\"same id, fresh delivery\"}}")
sleep 2
after_count=$(rc hget "fw:alert:${KEY}" count)
after_last=$(rc hget "fw:alert:${KEY}" last_seen)
echo "same-id resend:         $code"
echo "  count ${before_count} -> ${after_count}   last ${before_last} -> ${after_last}"

code=$(post "{\"id\":\"newid-${TS}\",\"source\":\"selftest\",\"type\":\"${TYPE}\",\"severity\":\"CRITICAL\",\"payload\":{\"msg\":\"same condition, new event id\"}}")
sleep 2
final_count=$(rc hget "fw:alert:${KEY}" count)
final_last=$(rc hget "fw:alert:${KEY}" last_seen)
echo "new-id resend:          $code"
echo "  count ${after_count} -> ${final_count}   last ${after_last} -> ${final_last}"

echo "---"
fail=0
[ "$after_count" = "$before_count" ] || { echo "FAIL: a same-id duplicate changed Count"; fail=1; }
[ "$after_last" = "$before_last" ]   || { echo "FAIL: a same-id duplicate moved Last occurrence"; fail=1; }
[ "$final_count" -gt "$after_count" ] || { echo "FAIL: a new-id event was not counted"; fail=1; }
[ "$final_last" -gt "$after_last" ]   || { echo "FAIL: a new-id event did not move Last occurrence"; fail=1; }
[ "$fail" = 0 ] && echo "PASS: duplicate dropped, distinct event counted, Last occurrence follows the newest event"

rc del "fw:alert:${KEY}" "fw:alert:open:${KEY}" >/dev/null
rc zrem fw:idx:alert:open "$KEY" >/dev/null 2>&1 || true
exit "$fail"
