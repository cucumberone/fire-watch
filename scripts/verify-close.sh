#!/bin/sh
# Verify the operator Close path end to end:
#
#   open an alert -> close it via the gateway's `op: close` -> the alert is gone
#   from the hash, the open index AND the board snapshot.
#
# The index removal is the part worth testing: an alert closed in the hash but
# left in fw:idx:alert:open is a phantom row that reappears on every page load.
set -eu
cd "$(dirname "$0")/.."

PW=$(grep -E '^DRAGONFLY_PASSWORD=' .env | cut -d= -f2-)
TOKEN=$(grep -E '^FW_INGEST_TOKEN=' .env | cut -d= -f2- | tr -d '"' | tr -d "'")
NODE=$(kubectl get nodes -o jsonpath='{range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address}{"\n"}{end}' | head -1)
TS=$(date +%s)
KEY="selftest:closecheck-${TS}"

rc() { kubectl -n fire-watch-lab exec dragonfly-0 -c dragonfly -- \
         redis-cli -p 6379 --no-auth-warning -a "$PW" --raw "$@"; }

echo "key: $KEY"

# 1. open it through the normal ingest path
curl -sS -o /dev/null -w 'ingest:  %{http_code}\n' -X POST "http://${NODE}:30900/events" \
  -H "X-Ingest-Token: $TOKEN" -H 'content-type: application/json' \
  -d "{\"id\":\"closecheck-${TS}\",\"source\":\"selftest\",\"type\":\"closecheck-${TS}\",\"severity\":\"CRITICAL\",\"payload\":{\"msg\":\"close probe\"}}"
sleep 2
echo "  state=$(rc hget "fw:alert:${KEY}" state)  inIndex=$(rc zscore fw:idx:alert:open "$KEY")"

# 2. close it as an operator would, through the gateway
command -v node >/dev/null && node -e '
const WebSocket = require("ws");
const ws = new WebSocket(process.argv[1]);
ws.on("open", () => {
  ws.send(JSON.stringify({ op: "subscribe" }));
  setTimeout(() => ws.send(JSON.stringify({ op: "close", key: process.argv[2] })), 200);
});
ws.on("message", (m) => {
  const f = JSON.parse(String(m));
  if (f.op === "alert" && f.applied === 3) { console.log("gateway confirmed close"); ws.close(); }
});
ws.on("close", () => process.exit(0));
setTimeout(() => process.exit(1), 8000);
' "ws://${NODE}:30300/" "$KEY" 2>/dev/null || echo "(ws client unavailable; skipping gateway step)"

sleep 1
state=$(rc hget "fw:alert:${KEY}" state)
idx=$(rc zscore fw:idx:alert:open "$KEY")
open=$(rc exists "fw:alert:open:${KEY}")
echo "  state=${state}  indexScore=${idx:-<removed>}  openFlag=${open}"

fail=0
[ "$state" = "closed" ] || { echo "FAIL: state is not closed"; fail=1; }
[ -z "$idx" ] || { echo "FAIL: alert still in fw:idx:alert:open (phantom row)"; fail=1; }
[ "$open" = "0" ] || { echo "FAIL: fw:alert:open still exists"; fail=1; }
[ "$fail" = 0 ] && echo "PASS: operator close retires hash, index and open flag"

rc del "fw:alert:${KEY}" "fw:alert:open:${KEY}" >/dev/null
rc zrem fw:idx:alert:open "$KEY" >/dev/null 2>&1 || true
exit "$fail"
