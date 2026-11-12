#!/bin/sh
# Verify the operator Close path end to end:
#
#   open a condition -> close it via the gateway's `op: close` -> the record is
#   state=closed, gone from idx:event:open, present in idx:event:closed.
#
# The index movement is the part worth testing: a record closed in the hash but left
# in idx:event:open is a phantom row that reappears on every page load.
set -eu
cd "$(dirname "$0")/.."

PW=$(grep -E '^VALKEY_PASSWORD=' .env | cut -d= -f2-)
TOKEN=$(grep -E '^FW_INGEST_TOKEN=' .env | cut -d= -f2- | tr -d '"' | tr -d "'")
NODE=${NODE_IP:-$(kubectl get nodes -o jsonpath='{range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address}{"\n"}{end}' | head -1)}
TS=$(date +%s)
SUMMARY="close probe ${TS}"

KEY=$(node --input-type=module -e "
  import { bareKeyFor } from './services/worker/src/keygen.js';
  process.stdout.write(bareKeyFor({ source: 'selftest', payload: { msg: process.argv[1] } }));
" "$SUMMARY")

rc() { kubectl -n fire-watch-lab exec valkey-0 -c valkey -- \
         valkey-cli -p 6379 --no-auth-warning -a "$PW" --raw "$@"; }

echo "record key: $KEY"

# 1. open it through the normal ingest path
curl -sS -o /dev/null -w 'ingest:  %{http_code}\n' -X POST "http://${NODE}:30900/events" \
  -H "X-Ingest-Token: $TOKEN" -H 'content-type: application/json' \
  -d "{\"id\":\"closecheck-${TS}\",\"source\":\"selftest\",\"type\":\"closecheck\",\"severity\":\"CRITICAL\",\"payload\":{\"msg\":\"${SUMMARY}\"}}"
sleep 2
echo "  state=$(rc hget "event:${KEY}" state)  inOpenIndex=$(rc zscore idx:event:open "$KEY")"

# 2. close it as an operator would, through the gateway
command -v node >/dev/null && node --input-type=module -e "
import WebSocket from 'ws';
const ws = new WebSocket(process.argv[1]);
ws.on('open', () => {
  ws.send(JSON.stringify({ op: 'subscribe' }));
  setTimeout(() => ws.send(JSON.stringify({ op: 'close', key: process.argv[2] })), 200);
});
ws.on('message', (m) => {
  const f = JSON.parse(String(m));
  if (f.op === 'alert' && f.applied === 3) { console.log('gateway confirmed close'); ws.close(); }
});
ws.on('close', () => process.exit(0));
setTimeout(() => process.exit(1), 8000);
" "ws://${NODE}:30300/" "$KEY" 2>/dev/null || echo "(ws client unavailable; skipping gateway step)"

sleep 1
state=$(rc hget "event:${KEY}" state)
openIdx=$(rc zscore idx:event:open "$KEY")
closedIdx=$(rc zscore idx:event:closed "$KEY")
echo "  state=${state}  openScore=${openIdx:-<removed>}  closedScore=${closedIdx:-<absent>}"

fail=0
[ "$state" = "closed" ] || { echo "FAIL: state is not closed"; fail=1; }
[ -z "$openIdx" ] || { echo "FAIL: record still in idx:event:open (phantom row)"; fail=1; }
[ -n "$closedIdx" ] || { echo "FAIL: record missing from idx:event:closed — the Closed tab cannot list it"; fail=1; }
[ "$fail" = 0 ] && echo "PASS: operator close retires the record and moves it between indexes"

rc del "event:${KEY}" >/dev/null
rc zrem idx:event:open idx:event:closed idx:event:all "$KEY" >/dev/null 2>&1 || true
exit "$fail"
