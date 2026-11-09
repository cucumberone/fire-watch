# Frontend

React single-page app served as static assets, talking to the WebSocket gateway.

## 1. Topology

| Item | Value |
|---|---|
| Kind | `Deployment`, 3 replicas |
| Image | `image-registry.openshift-image-registry.svc:5000/fire-watch/frontend:<sha>` (nginx-unprivileged + built assets) |
| Port | 8080 (nginx listening unprivileged; OpenShift-friendly) |
| Exposure | OpenShift `Route`, edge TLS |
| State | none — 100% static, any pod can serve any request |
| Anti-affinity | **required**, `kubernetes.io/hostname` |
| PDB | `minAvailable: 2` |

## 2. Why a separate Route

The frontend is served over HTTPS by a Route; the gateway is a separate Route with WebSocket support. Keeping them apart means:

- the gateway's 1h route timeout does not apply to asset requests
- a gateway roll-out never invalidates a cached JS bundle
- each can be scaled and rolled back independently

## 3. Runtime configuration

The bundle is built once and configured at runtime, because build-time env vars would force a rebuild per environment:

```text
/usr/share/nginx/html/config.json   (mounted from ConfigMap)
{
  "wsUrl": "wss://ws.fire-watch.example.com",
  "apiUrl": "",
  "backfillDefaultMinutes": 15
}
```

nginx serves `config.json` with `Cache-Control: no-store`; `index.html` also `no-store`; hashed assets get `immutable, max-age=31536000`.

## 4. WebSocket client requirements

| Requirement | Detail |
|---|---|
| Reconnect | jittered exponential backoff, 0.5s → 30s cap, infinite retries |
| Resume | on reconnect, send `subscribe` with `since` = last received event broker timestamp minus 5s overlap |
| De-duplication (transport) | keyed on event `id` (overlap + at-least-once delivery can duplicate) |
| De-duplication (occurrence) | render `count` from `op: alert` frames; never increment it per received event — the worker coalesces ([event-lifecycle.md](event-lifecycle.md) §5.4) |
| Ordering | sort by broker timestamp, not arrival order and not producer `ts` — cross-worker delivery can invert ([event-lifecycle.md](event-lifecycle.md) §2) |
| `applied` code | **must** be honoured: `0` stale / `1` new occurrence / `2` coalesced / `3` closed. Treating `2` as new causes entry churn; treating `0` as a close causes flicker ([event-lifecycle.md](event-lifecycle.md) §9) |
| Alert state | render from `op: alert` frames and `fw:alert:*` on reconnect; never derive "is it open" or the count from the event list |
| `msgc` decoding | field may be gzip + base64. Decode with the native `DecompressionStream`; on failure render `[undecodable payload]` and keep the rest of the alert usable ([payload-and-compression.md](payload-and-compression.md) §5) |
| `msg` decoding | always plain — leave it alone (it is below the compression threshold by design) |
| Severity display | `FATAL` > `CRITICAL` > `WARNING`; `HARMLESS` / `CLOSE` are retirement events and get no alert badge of their own |
| Buffering | render at most `N` rows (default 500) in the live table; older windows come from backfill |
| Backpressure | if the socket's `bufferedAmount` exceeds a threshold, drop render cadence rather than the socket |
| Visibility | close the socket when the tab is hidden > 5 min, reconnect on focus with backfill |

## 5. nginx configuration notes

```nginx
# unprivileged image: no user directive, listen 8080
server {
  listen 8080;
  root /usr/share/nginx/html;

  location = /config.json      { add_header Cache-Control "no-store"; }
  location = /index.html       { add_header Cache-Control "no-store"; }
  location /assets/            { add_header Cache-Control "immutable, max-age=31536000"; }
  location /                   { try_files $uri $uri/ /index.html; }
  location = /healthz          { access_log off; return 200 "ok\n"; }
}
```

Never `listen 80` in the image: OpenShift runs with an arbitrary UID and cannot bind < 1024 without a capability grant that the SCC will refuse.

## 6. Probes

| Probe | Path |
|---|---|
| readiness | `GET /healthz` — static 200 |
| liveness | `GET /healthz` |

The probes are intentionally trivial: the frontend has no dependencies, and a dependency-aware probe would restart pods during a gateway outage, which is exactly when the UI must stay up to show the reconnect state.

## 7. CSP and security headers

```nginx
add_header Content-Security-Policy "default-src 'self'; connect-src 'self' wss://ws.fire-watch.example.com; img-src 'self' data:; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'" always;
add_header X-Content-Type-Options "nosniff" always;
add_header Referrer-Policy "no-referrer" always;
```

`connect-src` must list the gateway origin explicitly, otherwise the browser silently blocks the WebSocket.

## 8. Sizing

| Aspect | Value |
|---|---|
| Requests | 20m CPU, 64Mi RAM |
| Limits | 200m CPU, 128Mi RAM |
| HPA | none — static serving is cheap; 3 replicas is the H/A requirement |

## 9. Verification checklist

```bash
kubectl -n fire-watch get route frontend -o jsonpath='{.spec.host}{"\n"}'
curl -sI "https://$(kubectl -n fire-watch get route frontend -o jsonpath='{.spec.host}')/" | head -20
curl -s  "https://$(kubectl -n fire-watch get route frontend -o jsonpath='{.spec.host}')/config.json"

# delete a pod; the app must keep loading from the other two
kubectl -n fire-watch delete pod -l app.kubernetes.io/name=frontend --field-selector status.phase=Running
```
