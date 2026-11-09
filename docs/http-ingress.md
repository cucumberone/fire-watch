# HTTP Ingress

External nginx speaks HTTP; RabbitMQ speaks AMQP 0-9-1. This component converts one to the other.

```
producer ──HTTP──> external nginx ──HTTP──> [http-ingress] ──AMQP 0-9-1──> q.event.ingest
```

It is a **producer**, not a consumer. It exists because you cannot make an external nginx speak AMQP, and it is separate from `worker` because the two have opposite failure semantics: the ingress must answer `5xx` when it cannot publish, while a worker must never answer anything at all.

Implemented in [services/http-ingress](../services/http-ingress/).

## 1. Why a separate component and not a plugin

| Alternative | Why not |
|---|---|
| nginx `ngx_mqtt_module` / an AMQP nginx module | none maintained; AMQP 0-9-1 is not a good fit for an HTTP reverse proxy |
| Shovel plugin | moves messages *between brokers*, not HTTP into a broker |
| RabbitMQ's own HTTP API (`POST /api/exchanges/.../publish`) | works, but needs the management plugin as a data path, has no batching, and couples throughput to the management API. Also `mandatory=false` semantics quietly drop unroutable messages |
| **A tiny producer** | full control over confirms, batching and status codes |

The last option is ~300 lines and gives exact control over the one thing that matters: **when we are allowed to answer 202**.

## 2. The 202 contract

This is the whole design.

| Status | Meaning | nginx must |
|---|---|---|
| `202` | accepted and **confirmed durable** by the broker | discard its buffer |
| `400` | malformed body / invalid event | not retry — fix the producer |
| `401` | bad or missing `X-Ingest-Token` | not retry |
| `404` | wrong path | not retry |
| `405` | not POST | not retry |
| `413` | body over `FW_MAX_BODY_BYTES` | not retry |
| `429` | confirm channel pool saturated | **retry with backoff** |
| `503` | broker unreachable, or confirm did not arrive | **retry** |

A `202` is only sent after `waitForConfirms()` resolves. Answering 202 before the confirm would be a lie: the connection can die between `publish()` and the broker persisting, and the event would be gone while nginx had already discarded it. That is the same data-loss path that publisher confirms exist to prevent, and it is the rule producers follow in [rabbitmq.md](rabbitmq.md) §3.

**`503` must be retryable in nginx.** Without that, the no-data-loss requirement is broken at the ingress boundary rather than inside the cluster.

## 3. Accepted body shapes

Three, because external producers vary:

```json
{ "id": "abc", "source": "sensor-a", "type": "temp", "severity": "CRITICAL",
  "ts": "2026-11-09T00:00:00Z", "payload": { "msg": "hot", "msgc": "detail" } }
```

```json
[ { ... }, { ... } ]
```

```json
{ "events": [ { ... }, { ... } ] }
```

Batches are capped at `FW_MAX_BATCH` (default 500) and are **all-or-nothing**: one invalid event rejects the whole request with the failing index named, so a producer can fix it rather than guessing which of 500 failed.

### 3.1 What gets filled in

A dumb shipper that only forwards text still works:

| Field | Missing behaviour | Why |
|---|---|---|
| `id` | generated | idempotency key; the ingress is the only place that can invent one |
| `source` | `"http-ingress"` | a default that is obvious in the UI |
| `type` | `"http.event"` | same |
| `severity` | **`WARNING`** | defaulting to `CRITICAL` would make every unknown shipper look like an outage |
| `ts` | now (ISO) | display only — ordering uses the broker timestamp ([event-lifecycle.md](event-lifecycle.md) §2) |
| `payload` | `{}`, or built from bare `msg`/`msgc` | matches the two-field contract |

Validation is **strict** on what it does receive: severity must be one of the five exact uppercase values, `payload` must be an object, and `id`/`source`/`type` have length limits. Rejecting is better than normalising a typo — a lowercase `critical` would otherwise create a second, invisible alert key.

## 4. Exposure: NodePort, not a Route

The traffic comes from **outside** the cluster, which changes the calculus:

| Option | Verdict |
|---|---|
| **NodePort 30900** | chosen — the external nginx already has an upstream pool; a NodePort keeps it unchanged and has no proxy timeout |
| Route | works, but adds the router as a hop and inherits its 30s timeout. A large batch under confirm pressure could exceed it |
| LoadBalancer | needs a cloud provider or MetalLB; more moving parts for the same result |

Nothing else is exposed. RabbitMQ, Dragonfly and Sentinel stay ClusterIP-only.

The Service is `NodePort` with a **fixed** port so the external nginx config is a constant.

## 5. Authentication

`X-Ingest-Token` must equal `FW_INGEST_TOKEN`, which comes from `Secret/fire-watch-ingress`. Comparison is constant-time-ish (length + XOR accumulate) — not a perfect side-channel defence, but it costs nothing and removes the obvious timing oracle.

**Empty `FW_INGEST_TOKEN` disables the check.** That is allowed for the lab and must never be used in production: an unauthenticated ingest endpoint inside a shared namespace means anything that can reach the Service can inject events.

## 6. Channel pool

Confirms are per-channel and `waitForConfirms()` acknowledges the whole channel, so concurrent requests on one channel serialise. `FW_CHANNELS` (default 4) slots let concurrent HTTP requests overlap without unbounded channel growth.

A request that cannot get a slot within 5s gets **429**, not 202 — again, "I did not accept it".

Two amqplib details that are easy to get wrong and both were wrong in the first draft:

- `channel.publish()` returns `false` when the write buffer is full. You must wait for the `'drain'` event; ignoring it buffers unboundedly in the ingress and turns a broker stall into an OOM kill.
- `waitForConfirms()` takes **no timeout argument**. Bounding it needs a `Promise.race` against a timer, or a stalled broker hangs the request until nginx gives up.

## 7. Configuration

| Variable | Default | Meaning |
|---|---|---|
| `FW_PORT` | `8080` | listen port |
| `FW_EVENT_PATH` | `/events` | the only path that accepts events |
| `FW_INGEST_TOKEN` | *(empty = off)* | shared secret from nginx |
| `FW_MAX_BODY_BYTES` | `2097152` | request size cap |
| `FW_MAX_BATCH` | `500` | events per request |
| `FW_CONFIRM_TIMEOUT_MS` | `10000` | how long to wait for a confirm |
| `FW_CHANNELS` | `4` | confirm-channel pool size |
| `AMQP_HOST` / `AMQP_PORT` | — / `5672` | broker |
| `AMQP_USER` / `AMQP_PASS` | — | from `Secret/fire-watch-rabbitmq` |
| `AMQP_EXCHANGE` | `fx.events` | **same exchange as native producers** |
| `AMQP_ROUTING_KEY` | `event.ingest` | **same key**, so the topology is unchanged |

Publishing to `fx.events`/`event.ingest` means the queue, DLX and audit path are identical to a native AMQP producer. Nothing downstream can tell the difference — which is the point.

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
    proxy_next_upstream error timeout http_502 http_503 http_429;
    proxy_next_upstream_tries 3;
}
```

`proxy_read_timeout` must be longer than `FW_CONFIRM_TIMEOUT_MS`, or nginx gives up before the ingress has decided — and then nginx does not know whether the event was accepted.

## 9. Verification

```bash
# the happy path returns 202 only after a confirm
curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST "http://<node-ip>:30900/events" \
  -H "X-Ingest-Token: $TOKEN" -H 'content-type: application/json' \
  -d '{"id":"t1","source":"probe","type":"test","severity":"CRITICAL","payload":{"msg":"hello"}}'
# expect: 202

# the event really is on the quorum queue
kubectl -n fire-watch exec rabbitmq-0 -- rabbitmqctl list_queues name messages

# a bad token is refused
curl -sS -o /dev/null -w '%{http_code}\n' -X POST "http://<node-ip>:30900/events" \
  -H 'content-type: application/json' -d '{}'          # expect: 401

# an invalid severity is refused with the allowed list
curl -sS -X POST "http://<node-ip>:30900/events" -H "X-Ingest-Token: $TOKEN" \
  -d '{"severity":"critical"}' | jq .
# expect: 400 + "severity must be one of FATAL, CRITICAL, WARNING, HARMLESS, CLOSE"

# with the broker stopped, the ingress must answer 503, never 202
kubectl -n fire-watch scale statefulset/rabbitmq --replicas=0
curl -sS -o /dev/null -w '%{http_code}\n' -X POST ... | grep -q 503 && echo 'correct: not accepted'
kubectl -n fire-watch scale statefulset/rabbitmq --replicas=3
```

That last check is the one that matters: **if the ingress ever answers 202 while the broker is down, the no-data-loss guarantee is void.**

## 10. What this component does not do

| Not done | Why |
|---|---|
| decompress or inspect `msgc` | compression happens at the worker ([payload-and-compression.md](payload-and-compression.md)); the ingress must not touch payload content |
| assign the broker timestamp | RabbitMQ does that on arrival, and it is the ordering authority ([event-lifecycle.md](event-lifecycle.md) §3) |
| deduplicate on `id` | the worker's `SET` is idempotent; an ingress-side dedup would need shared state for no gain |
| retry internally | it returns 5xx and lets nginx retry. An internal retry queue would be a second WAL with its own durability problem |
| terminate TLS | TLS belongs on the external nginx; in-cluster traffic is plaintext by design ([networking.md](networking.md) §7) |
