# Valkey

Primary datastore **and the ingest queue**. **1 master + 2 replicas in production, 1 pod in the lab**, manifests
only, no operator. It holds the records, their indexes, the `hist:` trail, the journal
and the ingest stream `fw:stream:ingest` that http-ingress appends to and workers
consume (§2.4).

This replaces Valkey. The reasons are collected in §1; the important ones are
that Valkey **implements AOF** (so the durability requirement is finally satisfiable
as written) and **has `min-replicas-to-write`** (so split-brain protection can be
enforced by the engine, not only by routing).

---

## 1. Why Valkey and not Valkey

| Requirement (AGENTS.md) | Valkey | Valkey |
|---|---|---|
| AOF, rotated at 32 MB | **not implemented** — snapshots only | `appendonly yes` + `auto-aof-rewrite-min-size 32mb` |
| "always find replicas before accepting writes" (split-brain protection) | no equivalent flag; had to be enforced by emptying a Service endpoint | `min-replicas-to-write 1` (§3.1, §5) |
| 3 replicas, no operator | yes | yes |
| RESP compatibility with Sentinel and every client | yes | yes |

The Valkey design worked, but two of its most important guarantees were
approximations:

- **Durability** came from an audit queue in the former message broker, because the datastore could not
  persist writes between snapshots. With AOF on every pod and writers that `WAIT 1`,
  that external copy was dropped together with the broker
  ([persistence-and-durability.md](persistence-and-durability.md) §3).
- **Split-brain protection** was a routing gate: `sentinel-bridge` withheld the
  Service endpoint when the master had lost its replicas. A client talking to a
  pod IP could still write. Valkey's `min-replicas-to-write` closes that hole in the
  engine itself.

---

## 2. One record per CONDITION

The most consequential change is not the server, it is the key shape.

| Before (Valkey) | Now (Valkey) |
|---|---|
| `event:<event-id>` — one key per DELIVERY | `event:<32 hex>` — one key per CONDITION |
| `event:<source>:<type>` — the rollup, separately keyed | the same hash holds the rollup |
| `event:<fingerprint>` — a third key to detect content duplicates | **gone**: the key IS the fingerprint |

The key is

```text
event:<sha256(EVT_Device + 0x1f + EVT_Summary)[0:32]>
```
computed in [`services/worker/src/keygen.js`](../services/worker/src/keygen.js). Two
arrivals with the same device and the same summary are **the same row**: the second
one moves `lastSeen`, increments `count` and does not create a record. That is the
requested behaviour — "update Last occurrence and drop the duplicate event" — and it
is why storage now grows with **conditions**, not deliveries.

### 2.1 Two duplicates, one rule

The two cases that used to need separate machinery collapse:

| Case | Before | Now |
|---|---|---|
| **Transport** duplicate: the same stream entry delivered twice | `SET fw:evt:{id} NX` rejected it (the old model) | the fold's `(brokerTs, id)` watermark returns `0` |
| **Content** duplicate: the same CONDITION re-reported with a new id | `SET event:{fp} NX` claimed it, then the copy was folded into the owner | it hashes to the same key, so there is no second key to create |

A content duplicate can no longer *be* stored as a separate record. That removes an
entire class of bug — the owner-id chasing, the fingerprint that expired before the
record, the "all events shows one row for three deliveries" complaint — because there
is nothing left to reconcile.

### 2.2 What is lost — and the one place it is kept

**Per-delivery history is not queryable from `event:<key>`.** The record keeps
`count`, `firstSeen`, `lastSeen` and `lastId` (the newest arrival's id), but not the
individual arrivals: the record is updated in place, so after 400 repeats it looks
like it did after the first apart from those three fields.

Per-delivery questions are answered by the `hist:` trail, written by the worker
before the fold ([history.md](history.md)):

| | `hist:` + `idx:history` |
|---|---|
| purpose | a read model for the UI: "how many times did this fire, and when" |
| retention | 3 days (`FW_HISTORY_TTL`) |
| queryable | `op: history`, bounded |
| on a total PVC loss | gone with the data — there is no copy outside Valkey ([persistence-and-durability.md](persistence-and-durability.md) §4) |

```text
hist:<deliveryId>   hash  ONE delivery, immutable, no state
idx:history         zset  <deliveryId> -> stream entry timestamp (brokerTs)
```

The id is a per-delivery UUID, and the CONDITION key travels inside the hash — so a
condition that fires 400 times keeps 400 rows rather than overwriting one, and a
redelivery is two rows rather than one.

Both expire on `FW_HISTORY_TTL`, and the index is pruned on that cutoff — not the
record's — so three days of members cannot outlive the hashes behind them.

### 2.3 Why the key is opaque

`event:<device>:<summary>` was rejected:

- ~520 bytes at the documented field limits (256 + 256 + separators), which the
  capacity model in [sizing.md](sizing.md) §2 does not budget for;
- requires escaping for `:` and whitespace, and every colon-splitting helper in the
  codebase would misparse it;
- glob-metacharacter collisions under `SCAN`, which the gateway and the reconcile
  both use.

Device and summary are stored as hash **fields** instead, so nothing is lost for
display or filtering. The trade-off is that `valkey-cli KEYS 'event:*'` tells you
nothing about what a row is; `HGETALL` does.

### 2.4 The ingest stream

| Key | Type | Written by | Removed by |
|---|---|---|---|
| `fw:stream:ingest` | stream, consumer group `workers` | http-ingress (`src http`), gateway operator close (`src operator`) — `XADD ... *` + `WAIT 1` | the worker, `XACK`+`XDEL` in one `MULTI` after the fold write |
| `fw:stream:dlq` | stream | the worker, for entries that can never succeed (`origId`, `reason` added) | an operator |

Entry fields: `evt` (the JSON envelope), `src`, optional `cid` (the request's
`x-request-id`). The entry ID `<ms>-<seq>` is assigned by the master and its ms part
is the event's `brokerTs` ([event-lifecycle.md](event-lifecycle.md) §2).

Rules:

- **Never trimmed.** No `MAXLEN`/`MINID`: a trim deletes unprocessed entries. The
  stream holds only in-flight entries, so `XLEN` is the backlog
  ([workers.md](workers.md) §6.4).
- **`noeviction` protects it.** An evicted stream entry is an event that was
  answered 202 and never processed (§4.4).
- **The group and its PEL are part of the key**, so they replicate and are in every
  pod's AOF; a failover or restart keeps track of what was delivered but not
  acknowledged.
- It is a normal key, so it inherits every guarantee in §3–§5: `min-replicas-to-write
  1`, `WAIT 1`, AOF on every pod, Sentinel failover.

---

## 3. Topology

| Item | Value |
|---|---|
| Image | `docker.io/valkey/valkey:8.1` — pin a digest before committing |
| Kind | `StatefulSet`, 3 replicas, `podManagementPolicy: OrderedReady` |
| Service (headless) | `valkey-headless` — `clusterIP: None`, stable per-pod DNS |
| Service (client) | `valkey` — ClusterIP, **selector managed by Sentinel**, see [redis-sentinel.md](redis-sentinel.md) |
| Port | 6379 client + replication |
| Data | `data` volumeClaimTemplate, **64Gi** ([sizing.md](sizing.md) §3.5) |
| Roles at bootstrap | `valkey-0` master, `-1` and `-2` replicas |

`OrderedReady` is required: replicas must not attempt replication before `valkey-0`
exists.

There is **no admin/metrics port**. Valkey has no HTTP endpoint at all, so metrics go
through `redis_exporter` (§7).

### 3.1 Writes: `min-replicas-to-write 1` + `WAIT 1`

| Setting | Production | Lab |
|---|---|---|
| `min-replicas-to-write` | **1** | 0 |
| `min-replicas-max-lag` | 10 | 10 |
| `FW_WRITE_GUARD_MIN_REPLICAS` (sentinel-bridge) | 1 — must equal `min-replicas-to-write` | 0 |
| `FW_WAIT_REPLICAS` (http-ingress, ws-gateway) | 1 | 0 |

- The master refuses every write with `NOREPLICAS` while it sees **no** replica
  that acknowledged within 10 s. An isolated master therefore cannot accept writes.
- Producers (`XADD` at http-ingress and at the operator close) then issue `WAIT 1`
  and only acknowledge once a replica has the write. That closes the "master dies in
  the replication gap" loss that asynchronous replication otherwise allows.
- **Why 1, not 2.** With 2, losing **either** replica stopped every write, so one
  Valkey pod down meant ingest down. With 1, any one Valkey pod can be lost and writes
  continue. The cost is a bounded partition window — an old master that keeps one
  replica on its side can accept writes until clients re-resolve — documented in
  [fault-tolerance.md](fault-tolerance.md) §4.1 and [redis-sentinel.md](redis-sentinel.md) §6.

`minAvailable: 2` on the Valkey PDB keeps a voluntary drain from ever taking the
master below one replica.

---

## 4. Persistence

### 4.1 Configuration

Every pod — master and both replicas — loads the same `valkey.conf`, so AOF is on
everywhere. A replica persists the replication stream to its own AOF on its own PVC,
and a promoted replica restarts from its own disk.

```text
dir /data
appendonly yes
appendfsync everysec
appenddirname appendonlydir
aof-use-rdb-preamble yes
auto-aof-rewrite-percentage 0
auto-aof-rewrite-min-size 32mb
aof-load-truncated yes
aof-timestamp-enabled yes
save ""
```

`appendfsync everysec` is the only sane choice: `always` costs a fsync per write
(fine at 16.7/s, not at 500/s), `no` gives the OS up to 30 s to lose the tail.

`aof-use-rdb-preamble yes` keeps the AOF base a compact RDB image rather than a long
command history — that is what makes an AOF-only restart fast.

`save ""` disables RDB snapshots. With AOF they add nothing, and each background save
forks a ~29 GiB process: a memory spike for no durability gain.

### 4.2 Rotation at 32 MB

The rule is **"take a 32 MB snapshot and roll the AOF log"**. In Valkey those are one
operation: `BGREWRITEAOF` builds a fresh AOF whose base is an RDB image of the live
keyspace, then switches to it atomically.

**The engine cannot express this rule, so the sidecar enforces it.** That conclusion
came from measurement on Valkey 8.1.11, not from reading the manual:

| Attempt | Result |
|---|---|
| `auto-aof-rewrite-percentage 0`, `min-size 1` | **Auto-rewrite is DISABLED.** `aof_rewrites` did not move while `aof_current_size` exceeded `aof_base_size`. `0` does not mean "trigger on size alone" — it means "off" |
| `auto-aof-rewrite-percentage 1` | Rewrites resume, but the trigger is `current >= base * (1 + pct/100)` — **relative to the base** |
| `auto-aof-rewrite-min-size 32mb` alone | Never fires in practice, because the base is the whole dataset |

The reason the last row matters: with `aof-use-rdb-preamble yes`, `aof_base_size` is an
RDB image of the **entire dataset** — ~33 GiB at the production working set
([sizing.md](sizing.md) §3.5). An integer percentage of that is hundreds of MB at
minimum, and `percentage 0` does not lower the bar, it turns the feature off.

So the configuration reads:

| Setting | Value | Why |
|---|---|---|
| `auto-aof-rewrite-min-size` | `32mb` | states the threshold in one place |
| `auto-aof-rewrite-percentage` | **`0`** | engine trigger OFF, so it cannot fight the sidecar |
| `FW_AOF_REWRITE_BYTES` (sidecar) | `33554432` | the enforced threshold: 32 MiB |
| `FW_AOF_CHECK_INTERVAL` (sidecar) | `60` | how often growth is measured |

### 4.2.1 The sidecar measures GROWTH, not size

```text
growth = aof_current_size - aof_base_size     -> rewrite when growth >= 32 MB
```

**Measuring absolute size is the trap.** `aof_current_size` already includes the base,
so a naive `current > 32mb` check is permanently true the moment the dataset passes
32 MB. The sidecar would then issue `BGREWRITEAOF` on every pass, each one forking a
full copy-on-write image of a multi-GiB dataset — an OOMKill waiting to happen, and a
disk that never settles. Verified against the running pod: with the growth test, a
below-threshold AOF reports `growth 0` repeatedly; a single write past the threshold
triggers exactly one rewrite and `growth` returns to 0.

The script is therefore a loop, not a timer:

1. read `aof_current_size`, `aof_base_size`, `aof_rewrite_in_progress`;
2. if the growth is 32 MB or more and nothing is already running, `BGREWRITEAOF`;
3. log the growth on every pass either way.

That last point is not noise. "The AOF never rotated" is otherwise discovered as a
slow restart months later, with nothing in any log to say when it stopped.

Skipping while `aof_rewrite_in_progress` is non-zero also keeps two replicas from
stacking rewrites on a busy master, and makes the script safe to run alongside an
operator's manual `BGREWRITEAOF`.

### 4.2.2 Why size rather than time

A clock-triggered rewrite cleans up at the boundary: a busy hour leaves a large AOF
sitting there until the next tick, and a quiet hour rewrites for nothing. Replay time
is a function of AOF size, so the size is what should trigger the rewrite. This is the
one design decision in this file that replaced an earlier, working-but-worse choice (an
hourly sidecar), and it was made because the requirement names a size.

### 4.3 RPO

| Event | Loss window |
|---|---|
| One pod restarts | 0 — the other pods hold the data; it resyncs |
| Master dies | 0 for acknowledged writes (`WAIT 1` put them on a replica) |
| All pods restart at once (power loss) | ≤ 1 s of writes per pod not yet fsynced (`everysec`); an acknowledged write is in two pods' AOFs |
| All three PVCs lost | everything — no copy outside Valkey ([persistence-and-durability.md](persistence-and-durability.md) §4) |

The AOF closes the "up to one hour" window that snapshots left.

### 4.4 What is still true from the Valkey design

- **`maxmemory-policy noeviction` is mandatory.** Any other policy silently deletes
  live events — and ingest stream entries — at `maxmemory`, breaking the 7-day
  retention promise with no signal. With `noeviction`, writes fail loudly: the
  ingress answers 503, workers leave entries pending, and the backlog becomes visible
  ([fault-tolerance.md](fault-tolerance.md) §3.9).
- **`maxmemory` must sit ABOVE the working set.** This inverts the usual "leave 20 %
  free for eviction" advice: hitting it stops ingest, so the sizing headroom in
  [sizing.md](sizing.md) §3.4 is what keeps it from happening.
- **`--requirepass` is mandatory.** Sentinel's `auth-pass` and every client
  authenticate; without it Sentinel's `PING` is rejected, the master is permanently
  "down", and failover loops without converging
  ([fault-tolerance.md](fault-tolerance.md) §3.6).

---

## 5. Split-brain protection

Three layers, and the engine one is new.

### Layer 1 — `min-replicas-to-write` (engine)

```text
min-replicas-to-write 1
min-replicas-max-lag 10
```

A master that cannot see at least one replica with an ack within 10 s **refuses writes**:

```
-NOREPLICAS Not enough good replicas to write.
```

That is the split-brain rule enforced by the thing doing the writing. The ingress
answers 503 (nginx retries); the worker leaves the entry pending and re-reads it after
`FW_RETRY_MS` ([workers.md](workers.md) §6).

Producers add `WAIT 1` on top (§3.1): the engine flag says "a replica is connected",
`WAIT` says "a replica has *this* write".

**This is why the lab sets it to `0`.** One pod has no replica; `1` would make every
write fail, and the lab would come up permanently broken in a way that looks like a
credential problem. The lab also sets `FW_WAIT_REPLICAS=0` (`stream-lab.yaml`).

### Layer 2 — the endpoint gate (routing)

`sentinel-bridge` polls every pod and reads `ROLE`:

```
master -> [ "master", <offset>, [ [ip, port, offset], ... ] ]   # replica count = length
slave  -> [ "slave", ip, port, state, offset ]
```

- If the pod Sentinel designates as master reports `master` with fewer than
  `FW_WRITE_GUARD_MIN_REPLICAS` (1, equal to `min-replicas-to-write`) connected replicas → **withhold the
  endpoint**.
- Withholding means `Endpoints/valkey` is set to zero addresses, clients see an empty
  endpoint list, and the pod gets label `fire-watch.io/writable=false` for humans and
  alerts.
- When the replica count recovers the endpoint is published again on the next cycle
  (`FW_RECONCILE_INTERVAL`, default 5 s). There is no separate recovery timer to get
  stuck.

The gate is still worth having alongside the engine flag because it also stops
**reads** of stale data from a minority master, which `min-replicas-to-write` does
not.

Accepted limitation, unchanged from the Valkey design: this is a **routing** gate.
A client that bypasses the Service and talks to a pod IP can still attempt a write —
and will be refused by the engine if the master sees no replica. Ingress, workers and gateways
are forbidden from using pod IPs, enforced by review, since K8s cannot enforce it.

### Layer 3 — Sentinel quorum

`quorum = 2` with majority promotion: 2 of 3 sentinels must agree to fail over. A
partitioned-off master cannot be promoted while the side holding 2 of 3 sentinels
elects a new master; the old master is demoted via `REPLICAOF` on rejoin and discards
its divergent tail. With `min-replicas-to-write 1` that tail can be non-empty if the
old master kept one replica on its side — the bounded window in
[fault-tolerance.md](fault-tolerance.md) §4.1. Full walkthrough in
[redis-sentinel.md](redis-sentinel.md) §6.

---

## 6. The fold script (Lua)

`event:<key>` is written by ONE server-side script,
[`k8s/base/20-event-fold-config.yaml`](../k8s/base/20-event-fold-config.yaml), invoked
with `EVAL`/`EVALSHA` so the read-modify-write is atomic across three workers.

### 6.1 Should key generation be in Lua?

**No.** The key is computed in application code, and the reasons are concrete:

1. **Cluster safety.** Valkey rejects a script that touches a key it was not given in
   `KEYS` — `ERR Script attempted to access a non local key`. A key minted inside Lua
   cannot be declared up front, so the script could only ever access keys the caller
   already computed.
2. **Reproducibility.** The frontend, the verification scripts and any repair script must
   derive the same key from the same event. A key that exists only inside a
   server-side script cannot be reproduced outside the server.
3. **It is pure.** Key generation is a hash of two input fields: no reads, no server
   state, nothing to make atomic. There is nothing for Lua to add.

### 6.2 What Lua is for

The dedup itself is a read-modify-write across five keys:

```text
1. ordering guard  (brokerTs, arrivalId) vs (watermark, watermarkId)
2. retirement      -> state=closed, move between the two indexes
3. occurrence      -> count += 1, lastSeen = ts, keep firstSeen
4. record fields   -> HSET of the producer's payload fields
5. indexes         -> ZADD all/open/source, ZREM closed
```

Doing that client-side is three round trips with two race windows, and three workers
racing on the same condition is the normal case, not the exception.

### 6.3 The rules that keep it safe

- **Every key is passed in `KEYS`.** The script never builds one. This is the same
  constraint as §6.1 and it is not optional in cluster mode.
- **No `SCAN`, no loops over sets.** A script blocks the whole server for its
  duration, so an O(N) script is an outage at N. This one is O(1) per call plus the
  field/value pairs the caller supplied.
- **Return codes are the contract**: `0` stale/redelivery, `1` occurrence, `3`
  closed. The worker forwards them as `applied`
  ([event-lifecycle.md](event-lifecycle.md) §5.2).

---

## 7. Monitoring

Valkey has **no HTTP metrics endpoint**, so an exporter is required. Two exist because
they answer different questions and one cannot be asked through the other:

| Exporter | Target | Answers |
|---|---|---|
| `redis-exporter` | Sentinel (`26379`) | is quorum intact, is the master flapping — `redis_sentinel_*` |
| `valkey-exporter` | `valkey:6379` (the master) | memory, eviction, replica count, AOF status — `redis_*`; plus `fw:stream:ingest` and `fw:stream:dlq` length and group pending (`REDIS_EXPORTER_CHECK_SINGLE_STREAMS`) |

A Sentinel-mode scrape never issues `INFO` against the data node, so it has no
`used_memory`, no `evicted_keys` and no `connected_slaves`. Rules built on those would
have been permanently silent — the same class of defect as the Valkey metric names
that did not exist ([monitoring.md](monitoring.md) §3).

Alerts are in [`k8s/base/60-monitoring.yaml`](../k8s/base/60-monitoring.yaml):

| Signal | Alert condition |
|---|---|
| `redis_connected_slaves` | `< 1` for 2 min — the write gate is refusing writes (`ValkeyWriteGateClosed`, critical) |
| `redis_connected_slaves` | `< 2` for 5 min — one replica down, writes still succeed (`ValkeyReplicaDown`, warning) |
| `redis_stream_length{stream="fw:stream:ingest"}` | `> 0` and not falling for 15 min — workers are not keeping up |
| `redis_stream_group_messages_pending{stream="fw:stream:ingest",group="workers"}` | `> 1000` for 10 min — entries are stuck |
| `redis_stream_length{stream="fw:stream:dlq"}` | `> 0` for 5 min — entries were dead-lettered |
| `redis_evicted_keys_total` | **`> 0` — page immediately**, retention is already broken |
| `redis_memory_used_bytes / redis_memory_max_bytes` | `> 0.98` for 5 min — writes are being rejected |
| `redis_aof_last_write_status` | `== 0` for 5 min — durability is broken |
| `redis_aof_last_bgrewrite_status` | `== 0` for 1 h — restart will get slow |
| container RSS / limit | `> 0.90` — OOMKill during a rewrite |
| pod label `fire-watch.io/writable=false` | `== 1` for 5 min — the bridge emptied the endpoint |

Deliberately **not** alerting on `used_memory / maxmemory` as a warning threshold:
`maxmemory` is sized to the working set by design, so that ratio sits near 1.0
permanently and would be pure noise. Eviction and rejection, not utilisation, are the
failure signals here.

---

## 8. Verification checklist

```bash
# who is the master, and does it see two replicas
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 -a "$VALKEY_PASSWORD" INFO replication
# role:master, connected_slaves:2, both master_link_status:up

kubectl -n fire-watch exec valkey-1 -- valkey-cli -p 6379 -a "$VALKEY_PASSWORD" INFO replication
# role:slave, master_link_status:up

# the write gate is ARMED (production: 1 — a lab reports 0 by design)
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 -a "$VALKEY_PASSWORD" CONFIG GET min-replicas-to-write

# AOF is on and healthy — on EVERY pod, not just the master
for i in 0 1 2; do
  kubectl -n fire-watch exec valkey-$i -- valkey-cli -p 6379 -a "$VALKEY_PASSWORD" INFO persistence \
    | grep -E 'aof_enabled|aof_last_write_status|aof_last_bgrewrite_status'
done
kubectl -n fire-watch exec valkey-0 -- ls -la /data/appendonlydir

# the ingest stream: backlog, group, pending, dead letters
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 -a "$VALKEY_PASSWORD" XLEN fw:stream:ingest
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 -a "$VALKEY_PASSWORD" XINFO GROUPS fw:stream:ingest
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 -a "$VALKEY_PASSWORD" XLEN fw:stream:dlq

# a record, end to end
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 -a "$VALKEY_PASSWORD" \
  HGETALL event:<32-hex-key>
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 -a "$VALKEY_PASSWORD" \
  ZCARD idx:event:all
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 -a "$VALKEY_PASSWORD" \
  MEMORY USAGE event:<32-hex-key>

kubectl -n fire-watch get pods -l app.kubernetes.io/name=valkey -L fire-watch.io/writable
```

Scenario rehearsal in [validation.md](validation.md).
