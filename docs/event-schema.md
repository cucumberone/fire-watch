# Event Ingest Schema

The contract a producer must satisfy to publish events. This is the **only**
external interface in the system: everything after the HTTP ingress uses the
internal envelope (`id` / `source` / `type` / `severity` / `payload`) and never
sees `EVT_*` names.

- **Schema file:** [`schemas/event-ingest.schema.json`](../schemas/event-ingest.schema.json)
- **Mapping and validation:** [`services/http-ingress/src/normalise.js`](../services/http-ingress/src/normalise.js)

The JSON Schema is not documentation that happens to exist alongside the code. The
ingress **reads it at runtime**: required fields, the set of accepted properties
and the severity enum all come from the file. A test asserts that every property in
the schema has a corresponding mapping, so adding a field to the schema without
handling it fails the build rather than silently dropping data in production.

---

## 1. Sending events

```bash
curl -X POST http://<ingress>/events \
  -H "X-Ingest-Token: $FW_INGEST_TOKEN" \
  -H 'content-type: application/json' \
  -d '{
    "EVT_Device": "core-sw-07",
    "EVT_EventKey": "net.link.down",
    "EVT_Severity": "CRITICAL",
    "EVT_Summary": "uplink flapping on port Gi0/24",
    "EVT_Details": "Gi0/24 lost carrier, 14 flaps in the last hour",
    "EVT_Agent": "Prometheus",
    "EVT_AgentOwner": "AM-000042",
    "EVT_Environment": "production"
  }'
# 202 {"accepted":1}   — only after the broker confirms
```

Three shapes are accepted:

| Body | Meaning |
|---|---|
| `{ ... }` | one event |
| `[ { ... }, { ... } ]` | a batch |
| `{ "events": [ ... ] }` | a wrapped batch |

All-or-nothing: one invalid event in a batch rejects the whole request with `400`
and a message naming the index (`event[3]: EVT_Severity must be one of …`). A
partial accept would leave the producer unable to tell which events landed.

## 2. Fields

| Field | Required | Becomes | Notes |
|---|---|---|---|
| `EVT_Device` | **yes** | `source` | Half of the alert identity |
| `EVT_EventKey` | **yes** | `type` | The other half |
| `EVT_Severity` | **yes** | `severity` | Mapped, see §3 |
| `EVT_Summary` | **yes** | `payload.msg` | The single line in the alert table |
| `EVT_Agent` | **yes** | `payload.EVT_Agent` | The tool that shipped it |
| `EVT_AgentOwner` | **yes** | `payload.EVT_AgentOwner` | The group owning that tool |
| `EVT_Identifier` | no | `id` | Generated when blank |
| `EVT_Details` | no | `payload.msgc` | The expandable body; gzipped when large |
| `EVT_Environment` | no | `payload.EVT_Environment` | Shown in the UI |
| `EVT_URL` | no | `payload.EVT_URL` | Deep link, shown in the UI |

`additionalProperties` is `true` in the schema, so a producer may send extra
fields; they are preserved in the payload and visible in the inspector, but nothing
downstream interprets them.

### 2.1 The alert identity is `EVT_Device:EVT_EventKey`

An alert is the aggregate an operator watches, and this pair is what it is grouped
on. Two events with the same device and event key are **the same alert** — the
second one moves *Last occurrence* and increments *Count* rather than creating a
row.

`EVT_Agent` is deliberately **not** part of the identity: one device's problem
reported by both Prometheus and Zabbix is one problem, not two.

### 2.2 `EVT_Identifier` is not what deduplicates

It becomes the event id, which governs **redelivery** — the same message arriving
twice. It does **not** govern duplicates in the operator's sense: a producer that
re-reports the same condition with a fresh identifier every interval still lands on
one row, because deduplication is on **content** (device + event key + severity +
payload). See [event-lifecycle.md](event-lifecycle.md) §5.4.

That distinction is what keeps the board readable at 1 000 events/min: the
identifier identifies a message, the content identifies a condition.

## 3. Severity mapping

The producer's scale is richer than the board's. The internal scale is three levels
of urgency plus a retirement, because finer distinctions do not change what an
operator does.

| `EVT_Severity` | Internal | Effect |
|---|---|---|
| `CRITICAL` | `CRITICAL` | Opens / escalates |
| `MAJOR` | `CRITICAL` | Opens / escalates |
| `MINOR` | `WARNING` | Opens |
| `WARNING` | `WARNING` | Opens |
| `INDETERMINATE` | `WARNING` | Opens |
| `INFORMATION` | `HARMLESS` | **Retires** the alert |
| `CLEAR` | `CLOSE` | **Retires** the alert |
| `HARMLESS` | `CLOSE` | **Retires** the alert |

**`CLEAR` is the resolution signal.** The schema has no `CLOSE`, so this mapping is
what ends an episode — without it a resolved alarm would stay on the board forever.
An alert retired this way is identical to one retired by any other path; there is
no separate "closed by source" state.

`INFORMATION` retiring an alert is the existing internal rule for `HARMLESS` and is
worth calling out because it is not obvious: a purely informational event resolves
rather than raises. If information should instead be an open, low-severity alert,
change `INFORMATION` to map to `WARNING` in `SEVERITY_MAP`.

The producer's original severity is preserved as `payload.EVT_Severity`, so the
inspector can show that a `WARNING` row was reported as `MINOR`.

## 4. Validation rules

The schema file declares types, the severity enum and which fields are required. It
deliberately does **not** declare `maxLength`, because the JSON Schema would then
have to agree with the constants in `normalise.js` and the two would drift silently.
The length bounds live only in code, applied by the shared `text()` helper:

| Rule | Response |
|---|---|
| Required field missing or blank after trimming | `400` naming the field |
| `EVT_Severity` not in the enum | `400` listing the allowed values |
| `EVT_Identifier` over 128 chars | `400` |
| `EVT_Details` over 8192 chars | `400` |
| Any other field over 256 chars | `400` |
| A field that is neither string nor absent | coerced with `String()`, then trimmed and length-checked |
| Batch over `FW_MAX_BATCH` (default 500) | `413` |
| Body that is not an object, array or `{"events":[]}` | `400` |

`EVT_Details` has its own, much larger bound because it is the expandable body
rather than a label. Leaving it on the 256 default would have rejected any producer
that genuinely explains what happened — the failure would surface as an unexplained
`400` on exactly the events with the most useful content.

Detection of the schema is by **any** `EVT_*` field being present, not by
`EVT_EventKey` alone. Keying it on one field meant a request missing exactly that
field fell through to the legacy envelope path, which invented a default and
answered `202` for something that should have been rejected.

## 5. Changing the schema

1. Edit `schemas/event-ingest.schema.json`.
2. Update `fromUpstream()` in `services/http-ingress/src/normalise.js` if the field
   needs a mapping. `npm test` in `services/http-ingress` fails until you do —
   `every property in the schema is handled by the mapping` is the guard.
3. If the change affects what the UI shows, update `ENRICHED_FIELDS` in
   `services/frontend/src/lib.js`.
4. Update this page.

Required fields are enforced from the schema, so step 1 alone starts rejecting
requests that omit a newly required field. That is intentional: the contract is the
file, not a description of the file.

## 6. Verification

```bash
# the happy path
curl -sS -o /dev/null -w '%{http_code}\n' -X POST "http://<ingress>/events" \
  -H "X-Ingest-Token: $FW_INGEST_TOKEN" -H 'content-type: application/json' \
  -d '{"EVT_Device":"d1","EVT_EventKey":"k1","EVT_Severity":"CRITICAL",
       "EVT_Summary":"s","EVT_Agent":"a","EVT_AgentOwner":"AM-1"}'
# expect 202

# a missing required field is refused, naming it
curl -sS -X POST "http://<ingress>/events" -H "X-Ingest-Token: $FW_INGEST_TOKEN" \
  -H 'content-type: application/json' -d '{"EVT_Device":"d1"}'
# expect 400 + "EVT_EventKey is required"

# an unknown severity is refused with the allowed list
# expect 400 + "EVT_Severity must be one of CLEAR, HARMLESS, …"

# a CLEAR retires the alert
#   send CRITICAL, note the open row, then send CLEAR with the same device+key
#   expect the row to disappear from the board
```
