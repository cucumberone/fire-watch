# HTTP Ingress

External nginx speaks HTTP; the ingest queue is a Valkey stream. This component validates a request and appends it to the stream.

```
producer ──HTTP──> external nginx ──HTTP──> [http-ingress] ──XADD + WAIT 1──> fw:stream:ingest
```

It is a **producer**, not a consumer. It is separate from `worker` because the two have opposite failure semantics: the ingress must answer `5xx` when it cannot append durably, while a worker must never answer anything at all.

Implemented in [services/http-ingress](../services/http-ingress/).

The request body contract — the `EVT_*` fields, their required-ness, and how each
severity maps onto the board — is specified separately in
[event-schema.md](event-schema.md) and defined by
[`schemas/event-ingest.schema.json`](../schemas/event-ingest.schema.json). This page
covers what the ingress does with a request once it is valid.

## 1. Why a separate component and not a plugin

| Alternative | Why not |
|---|---|
| nginx talking to Valkey directly (`redis2`/OpenResty modules) | no schema validation, no Sentinel discovery, no `WAIT`, and the Valkey password would live on the external host |
| Workers exposing HTTP themselves | mixes the "answer 5xx" and "never answer" failure semantics in one process, and couples ingest capacity to fold capacity |
| **A tiny producer** | full control over validation, batching, `WAIT` and status codes |

The last option is ~300 lines and gives exact control over the one thing that matters: **when we are allowed to answer 202**.

## 2. The 202 contract

This is the whole design.

| Status | Meaning | nginx must |
|---|---|---|
| `202` | appended to `fw:stream:ingest` **and replicated** to `FW_WAIT_REPLICAS` replicas | discard its buffer |
| `400` | malformed body / invalid event | not retry — fix the producer |
| `401` | bad or missing `X-Ingest-Token` | not retry |
| `404` | wrong path | not retry |
| `405` | not POST | not retry |
| `413` | body over `FW_MAX_BODY_BYTES` | not retry |
| `503` | `store_unavailable`: Valkey unreachable, `NOREPLICAS`, or `WAIT` short | **retry** |

There is no `429`: the old confirm-channel pool is gone.

A `202` is only sent after both steps in §3 succeeded.

**`503` must be retryable in nginx.** Without that, the no-data-loss requirement is broken at the ingress boundary rather than inside the cluster. A 503 after `EXEC` but with a short `WAIT` can mean the batch is in the stream *and* will be sent again; that duplicate is harmless (§10, [event-lifecycle.md](event-lifecycle.md) §7).

The accepted body shapes are in §11.

## 3. Durability: XADD + WAIT

1. `MULTI` / `XADD fw:stream:ingest * evt <envelope> src http [cid <x-request-id>]` × N / `EXEC` — the whole batch is one transaction, so a request is all-or-nothing and an nginx retry can never leave half a batch behind. The master refuses the `XADD` with `NOREPLICAS` when `min-replicas-to-write 1` is not met ([valkey.md](valkey.md) §3). A per-command error inside `MULTI` is surfaced in the `EXEC` reply and treated as a failure.
2. `WAIT <FW_WAIT_REPLICAS> <FW_CONFIRM_TIMEOUT_MS>` (default `1`, `10000`) must return at least `FW_WAIT_REPLICAS`; otherwise the request fails with `NOT_REPLICATED` → 503.

Answering 202 after step 1 alone would be a lie: replication is asynchronous, so a master that crashes in the replication gap takes the entry with it, and the replica Sentinel promotes never saw it — while nginx had already discarded its copy. `WAIT 1` closes that gap: a 202'd event is in the memory of at least two pods, and each of them writes it to its own AOF within a second (`appendfsync everysec`, [persistence-and-durability.md](persistence-and-durability.md) §3).

The stream is never trimmed and the master runs `maxmemory-policy noeviction`, so an entry stays until a worker has folded it and removed it with `XACK`+`XDEL` ([workers.md](workers.md) §6).

## 4. Exposure: NodePort, not a Route

The traffic comes from **outside** the cluster, which changes the calculus:

| Option | Verdict |
|---|---|
| **NodePort 30900** | chosen — the external nginx already has an upstream pool; a NodePort keeps it unchanged and has no proxy timeout |
| Route | works, but adds the router as a hop and inherits its 30s timeout. A large batch waiting on `WAIT` during a failover could exceed it |
| LoadBalancer | needs a cloud provider or MetalLB; more moving parts for the same result |

Nothing else is exposed. Valkey and Sentinel stay ClusterIP-only.

The Service is `NodePort` with a **fixed** port so the external nginx config is a constant.

## 5. Authentication

`X-Ingest-Token` must equal `FW_INGEST_TOKEN`, which comes from `Secret/fire-watch-ingress`. Comparison is constant-time-ish (length + XOR accumulate) — not a perfect side-channel defence, but it costs nothing and removes the obvious timing oracle.

**Empty `FW_INGEST_TOKEN` disables the check.** That is allowed for the lab and must never be used in production: an unauthenticated ingest endpoint inside a shared namespace means anything that can reach the Service can inject events.

## 6. Finding the master, readiness

The ingress uses the same Sentinel client as the worker ([`services/http-ingress/src/valkey.js`](../services/http-ingress/src/valkey.js)): it asks the Sentinels in `SENTINELS` for the master of `VALKEY_MASTER_NAME`, verifies `ROLE` is `master`, and re-resolves on `READONLY` / connection loss. It never targets a pod IP.

`/healthz` (readiness) is a Valkey `PING`, probed every 5 s. An ingress that cannot reach Valkey is taken out of the Service, so nginx sends to a pod that can accept rather than collecting 503s. `/livez` is process-only.

During a failover the in-flight `WAIT` can return short or the `EXEC` fail; the request answers 503 and nginx retries — on this pod after the client re-resolves, or on another.

The appender is [`services/http-ingress/src/stream.js`](../services/http-ingress/src/stream.js) (`StreamWriter`); the field list per entry is `streamFields()` in [`normalise.js`](../services/http-ingress/src/normalise.js).

## 7. Configuration

| Variable | Default | Meaning |
|---|---|---|
| `FW_PORT` | `8080` | listen port |
| `FW_EVENT_PATH` | `/events` | the only path that accepts events |
| `FW_INGEST_TOKEN` | *(empty = off)* | shared secret from nginx |
| `FW_MAX_BODY_BYTES` | `2097152` | request size cap |
| `FW_MAX_BATCH` | `500` | events per request |
| `FW_CONFIRM_TIMEOUT_MS` | `10000` | `WAIT` timeout before answering 503 |
| `FW_WAIT_REPLICAS` | `1` | replicas that must acknowledge before 202. Must not exceed `min-replicas-to-write`; `0` in the lab ([kustomize.md](kustomize.md), `stream-lab.yaml`) |
| `FW_STREAM_KEY` | `fw:stream:ingest` | the stream the workers consume |
| `SENTINELS` | — | `host:port` list of the Sentinels |
| `VALKEY_MASTER_NAME` | — | Sentinel monitor name |
| `VALKEY_PASSWORD` | — | from `Secret/fire-watch-valkey` |

The gateway's operator close appends to the same stream with `src operator` ([event-lifecycle.md](event-lifecycle.md) §8.2), so the worker path is identical for both producers.

## 8. nginx side

```nginx
upstream fire_watch_ingress {
    server 192.168.50.2:30900 max_fails=2 fail_timeout=5s;
    server 192.168.50.3:30900 max_fails=2 fail_timeout=5s;
    server 192.168.50.4:30900 max_fails=2 fail_timeout=5s;
}

location /events {
    proxy_pass         http://fire_watch_ingress;
    proxy_set_header   X-Ingest-Token $fire_watch_token;
    proxy_set_header   X-Request-Id $request_id;
    proxy_connect_timeout 2s;
    proxy_read_timeout    15s;   # must exceed FW_CONFIRM_TIMEOUT_MS

    # THE IMPORTANT PART: a 503 means "not accepted, retry".
    proxy_next_upstream error timeout http_502 http_503;
    proxy_next_upstream_tries 3;
}
```

`proxy_read_timeout` must be longer than `FW_CONFIRM_TIMEOUT_MS`, or nginx gives up before the ingress has decided — and then nginx does not know whether the event was accepted.

## 9. Verification

```bash
# the happy path returns 202 only after XADD + WAIT 1
curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST "http://<node-ip>:30900/events" \
  -H "X-Ingest-Token: $TOKEN" -H 'content-type: application/json' \
  -d '{"EVT_Device":"probe","EVT_EventKey":"test","EVT_Severity":"CRITICAL",
       "EVT_Summary":"hello","EVT_Agent":"curl","EVT_AgentOwner":"AM-1"}'
# expect: 202

# the stream backlog drains back to 0 once the workers have folded it
kubectl -n fire-watch exec valkey-0 -c valkey -- sh -c \
  'valkey-cli -a "$VALKEY_PASSWORD" --no-auth-warning XLEN fw:stream:ingest'

# a bad token is refused
curl -sS -o /dev/null -w '%{http_code}\n' -X POST "http://<node-ip>:30900/events" \
  -H 'content-type: application/json' -d '{}'          # expect: 401

# a required field missing is refused, and named
curl -sS -X POST "http://<node-ip>:30900/events" -H "X-Ingest-Token: $TOKEN" \
  -H 'content-type: application/json' -d '{"EVT_Device":"probe"}' | jq -r .error
# expect: 400 + "EVT_EventKey is required"

# an unknown severity is refused with the allowed list from the schema
curl -sS -X POST "http://<node-ip>:30900/events" -H "X-Ingest-Token: $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"EVT_Device":"p","EVT_EventKey":"t","EVT_Severity":"critical",
       "EVT_Summary":"s","EVT_Agent":"a","EVT_AgentOwner":"AM-1"}' | jq -r .error
# expect: 400 + "EVT_Severity must be one of CLEAR, HARMLESS, …"

# with no replica able to acknowledge, the ingress must answer 503, never 202
# (production: stop both replicas -> NOREPLICAS; see validation.md for the drill)
curl -sS -o /dev/null -w '%{http_code}\n' -X POST ... | grep -q 503 && echo 'correct: not accepted'
```

That last check is the one that matters: **if the ingress ever answers 202 while the write cannot reach a replica, the no-data-loss guarantee is void.**

## 10. What this component does not do

| Not done | Why |
|---|---|
| decompress or inspect `msgc` | compression happens at the worker ([payload-and-compression.md](payload-and-compression.md)); the ingress must not touch payload content |
| assign the arrival timestamp | the master does that at `XADD` (`*` → entry ID `<ms>-<seq>`), and it is the ordering authority ([event-lifecycle.md](event-lifecycle.md) §2) |
| deduplicate on `id` | the fold is idempotent per condition key; an ingress-side dedup would need shared state for no gain |
| retry internally | it returns 5xx and lets nginx retry. An internal retry queue would be a second buffer with its own durability problem |
| trim the stream | `MAXLEN` would delete entries no worker has processed; the stream is never trimmed ([workers.md](workers.md) §6) |
| terminate TLS | TLS belongs on the external nginx; in-cluster traffic is plaintext by design ([networking.md](networking.md) §7) |

## 11. Accepted body shapes

Four, because external producers vary. The canonical shape is the `EVT_*` contract
specified in [event-schema.md](event-schema.md) and enforced from
[`schemas/event-ingest.schema.json`](../schemas/event-ingest.schema.json):

```json
{ "EVT_Device": "core-sw-07", "EVT_EventKey": "net.link.down",
  "EVT_Severity": "CRITICAL", "EVT_Summary": "uplink flapping",
  "EVT_Details": "Gi0/24 lost carrier", "EVT_Agent": "Prometheus",
  "EVT_AgentOwner": "AM-000042", "EVT_Identifier": "abc",
  "EVT_Environment": "production", "EVT_URL": "https://…" }
```

```json
[ { ... }, { ... } ]
```

```json
{ "events": [ { ... }, { ... } ] }
```

The legacy internal envelope is still accepted for the internal probe scripts:

```json
{ "id": "abc", "source": "sensor-a", "type": "temp", "severity": "CRITICAL",
  "ts": "2026-11-09T00:00:00Z", "payload": { "msg": "hot", "msgc": "detail" } }
```

Batches are capped at `FW_MAX_BATCH` (default 500) and are **all-or-nothing**: one invalid event rejects the whole request with the failing index named, so a producer can fix it rather than guessing which of 500 failed.

### 11.1 Which path a body takes

Detection is by **any** key beginning with `EVT_`. That is deliberately not keyed on
`EVT_EventKey`: keying it on one field meant a request missing exactly that field
fell through to the lenient legacy path, which defaults everything and answers
`202` for something that should have been refused `400`.

| Body | Path | Validation |
|---|---|---|
| any `EVT_*` key present | upstream mapping | strict, schema-driven |
| otherwise | legacy envelope | lenient defaults |

The legacy path is lenient because a dumb shipper that only forwards text must
still work:

| Field | Missing behaviour | Why |
|---|---|---|
| `id` | generated | idempotency key; the ingress is the only place that can invent one |
| `source` | `"http-ingress"` | a default that is obvious in the UI |
| `type` | `"http.event"` | same |
| `severity` | **`WARNING`** | defaulting to `CRITICAL` would make every unknown shipper look like an outage |
| `ts` | now (ISO) | display only — ordering uses the stream entry timestamp ([event-lifecycle.md](event-lifecycle.md) §2) |
| `payload` | `{}`, or built from bare `msg`/`msgc` | matches the two-field contract |

The `EVT_*` path has **no defaults for required fields** — `EVT_Device`,
`EVT_EventKey`, `EVT_Severity`, `EVT_Summary`, `EVT_Agent` and `EVT_AgentOwner` are
refused with `400` when absent or blank, because a mapping that invents a device
name would put events on the board under the wrong alert key.

Validation is **strict** on what it does receive: severity must be one of the exact
uppercase values, `payload` must be an object, and string fields have length limits.
Rejecting is better than normalising a typo — a lowercase `critical` would otherwise
create a second, invisible alert key.
