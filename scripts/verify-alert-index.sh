#!/bin/sh
# Verify the worker maintains idx:event:open automatically.
#
# The record key is DERIVED, not authored: sha256(device + 0x1f + summary)[0:32].
# So this script computes it with the real keygen module rather than inventing one —
# an invented key would test nothing, because the fold would create a record at a key
# no query reads.
set -eu
cd "$(dirname "$0")/.."

PW=$(grep -E '^VALKEY_PASSWORD=' .env | cut -d= -f2-)
TOKEN=$(grep -E '^FW_INGEST_TOKEN=' .env | cut -d= -f2- | tr -d '"' | tr -d "'")
NODE_IP=${NODE_IP:-192.168.50.2}
TS=$(date +%s)
SUMMARY="index maintenance probe ${TS}"

KEY=$(node --input-type=module -e "
  import { bareKeyFor } from './services/worker/src/keygen.js';
  process.stdout.write(bareKeyFor({ source: 'selftest', payload: { msg: process.argv[1] } }));
" "$SUMMARY")
echo "probe record key: $KEY"

rc() { kubectl -n fire-watch-lab exec valkey-0 -c valkey -- \
         valkey-cli -p 6379 --no-auth-warning -a "$PW" --raw "$@"; }

BEFORE=$(rc zcard idx:event:open)
echo "index before: $BEFORE"

CODE=$(curl -sS -o /dev/null -w '%{http_code}' -X POST \
  "http://${NODE_IP}:30900/events" \
  -H "X-Ingest-Token: $TOKEN" -H 'content-type: application/json' \
  -d "{\"id\":\"selftest-${TS}\",\"source\":\"selftest\",\"type\":\"indexcheck\",\"severity\":\"CRITICAL\",\"payload\":{\"msg\":\"${SUMMARY}\"}}")
echo "ingest: $CODE"

sleep 3

AFTER=$(rc zcard idx:event:open)
SCORE=$(rc zscore idx:event:open "$KEY")
STATE=$(rc hget "event:${KEY}" state)

echo "index after:  $AFTER"
echo "probe score:  ${SCORE:-<absent>}"
echo "record state: ${STATE:-<absent>}"

if [ -n "${SCORE:-}" ] && [ "$STATE" = "open" ]; then
  echo "PASS: the new worker indexed the record without manual help"
else
  echo "FAIL: record was not added to the open index"
  exit 1
fi

# Clean up so repeated runs do not accumulate probe rows on the board.
rc del "event:${KEY}" >/dev/null
rc zrem idx:event:open idx:event:all "$KEY" >/dev/null 2>&1 || true
