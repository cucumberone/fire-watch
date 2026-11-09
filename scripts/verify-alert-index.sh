#!/bin/sh
# Verify the worker maintains fw:idx:alert:open automatically.
set -eu
cd "$(dirname "$0")/.."

PW=$(grep -E '^DRAGONFLY_PASSWORD=' .env | cut -d= -f2-)
TOKEN=$(grep -E '^FW_INGEST_TOKEN=' .env | cut -d= -f2- | tr -d '"' | tr -d "'")
TS=$(date +%s)
TYPE="verify-${TS}"
KEY="selftest:${TYPE}"

echo "probe key: $KEY"

BEFORE=$(kubectl -n fire-watch-lab exec dragonfly-0 -c dragonfly -- \
  redis-cli -p 6379 --no-auth-warning -a "$PW" zcard fw:idx:alert:open)
echo "index before: $BEFORE"

CODE=$(curl -sS -o /dev/null -w '%{http_code}' -X POST \
  "http://192.168.50.2:30900/events" \
  -H "X-Ingest-Token: $TOKEN" -H 'content-type: application/json' \
  -d "{\"id\":\"selftest-${TS}\",\"source\":\"selftest\",\"type\":\"${TYPE}\",\"severity\":\"CRITICAL\",\"payload\":{\"msg\":\"index maintenance probe\"}}")
echo "ingest: $CODE"

sleep 3

AFTER=$(kubectl -n fire-watch-lab exec dragonfly-0 -c dragonfly -- \
  redis-cli -p 6379 --no-auth-warning -a "$PW" zcard fw:idx:alert:open)
SCORE=$(kubectl -n fire-watch-lab exec dragonfly-0 -c dragonfly -- \
  redis-cli -p 6379 --no-auth-warning -a "$PW" --raw zscore fw:idx:alert:open "$KEY")

echo "index after:  $AFTER"
echo "probe score:  ${SCORE:-<absent>}"

if [ -n "${SCORE:-}" ]; then
  echo "PASS: the new worker indexed the alert without manual help"
else
  echo "FAIL: alert was not added to the index"
  exit 1
fi
