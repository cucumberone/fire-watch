# Sizing

Capacity model for the baseline: **1 000 events/minute sustained, 7-day retention, event size ≥ 2048 B**, with 500 events/s storm peaks.

## 1. Inputs

| Parameter | Value | Source |
|---|---|---|
| Events/minute — **sustained** | **1 000** (= 16.7/s) | given |
| Events/s — **storm peak** | **500** | given |
| Storm duration assumed | minutes to a few hours, not days | §2.3 |
| Event size — minimum | **2 048 B** | given (events vary; 2 KiB is the floor) |
| Event size — planning value | **2 048 B** | use the floor, never the average (§2.2) |
| Retention TTL | **7 days** = 604 800 s | given |
| Sources | 500 | assumption |
| Concurrent browser clients | 1 000 | assumption |
| AOF base + incremental tail | rewritten hourly | §3.5 |

Derived:

```text
sustained throughput = 1 000/min × 2 048 B   = 2.05 MB/min = 34 KB/s
storm throughput     = 500/s     × 2 048 B   = 1 MiB/s
events retained      = 1 000/min × 1 440 min/day × 7 = 10 080 000
```

**The number that matters is the 7-day total, not the instantaneous rate.** With a 7-day TTL, Valkey holds everything written in the last rolling week. Peak/s only matters for storm headroom (§2.3); the sustained/min figure determines the baseline working set.

### 1.1 What grows: CONDITIONS, not deliveries

Every figure below is per **stored record**, and a record is a **condition** — one
`(EVT_Device, EVT_Summary)` pair — not a delivery
([valkey.md](valkey.md) §2).

That changes the input completely, and it is the main capacity argument for the Valkey
redesign:

```text
records = distinct conditions in the 7-day window
        ≠ messages delivered
```

| Feed | Delivered | Distinct conditions | Records |
|---|---|---|---|
| Each condition reported once | 10.08 M | 10.08 M | 10.08 M |
| Standing fault re-reported every 30 s | 10.08 M | ~20 k | **~20 k** |
| Producer retry storm (same message 5×) | 50.4 M | 10.08 M | 10.08 M |

The previous design stored **every arrival** under its own key, so a re-reporting
producer multiplied the working set by its repeat rate: the old notes here carried a
"duplicate factor" warning and told you to multiply §2 by it. That warning is gone
because the multiplication can no longer happen — the repeat has no key of its own.

**The conservative planning assumption is therefore still "one record per delivered
event"** (§2.1 uses it), because a non-repeating feed really does produce that many
conditions. Any repetition makes the real number SMALLER, which is a safe direction to
be wrong in.

Measure it:

```bash
ZCARD idx:event:all     # records in the window
# and compare with the delivery rate from RabbitMQ
rabbitmqctl list_queues name message_stats.publish_details.rate
```

## 2. Working set

### 2.1 Per-event memory footprint

A stored event costs more than its value:

| Component | Bytes | Note |
|---|---|---|
| hash fields (`event:<key>`) | **2 048** | the payload JSON plus the ~10 bookkeeping fields (count, timestamps, state) |
| hash key overhead | ~90 | hashtable entry + key string (32 hex) + expiry metadata |
| zset member, in TWO indexes | ~300 | `idx:event:all` and `idx:source:<device>` — see [architecture.md](architecture.md) §6.1; each member is stored twice (hash table + skiplist) |
| zset member, state index | ~150 | exactly one of `idx:event:open` / `idx:event:closed` |
| **Total per record** | **≈ 2 590 B** | |

The bookkeeping fields are counted inside the 2 048 B because a hash stores its field
names and values together, and `count`, `firstSeen`, `lastSeen`, `state`, `severity`,
`lastId`, `watermark`, `watermarkId`, `source`, `type` and `msg` add roughly 150 B on
top of the payload.

```text
raw        = 10 080 000 × 2 590 B = 26.1 GB ≈ 24.3 GiB
+ 1.15 allocator/fragmentation     ≈ 28.0 GiB
```

**Each Valkey pod must hold ≈ 28 GiB** for a non-repeating feed (replication is
full-copy, so all three hold the same data).

Aggregate: `3 × 28 ≈ 84 GiB` of RAM. The number went up from 24.5 GiB in the Dragonfly-era model because the
record now carries its rollup, its state, its ack fields and THREE index members
instead of one. It is the pessimistic case: any repetition brings it back down (§1.1).

### 2.2 Why 2 048 B is the right planning number

The event size varies. Do **not** plan with the average:

- Valkey's memory is determined by total bytes actually stored, so a distribution with a long tail costs more than `count × mean`.
- Sizing to the floor is the only choice that never evicts. Sizing to an average guarantees eviction on a busy week, which silently breaks the 7-day retention promise.

| Planning size | Working set / pod | Aggregate RAM |
|---|---|---|
| **2 048 B (floor)** | **≈ 28.0 GiB** | **≈ 84 GiB** |
| 4 KiB | ≈ 54 GiB | ≈ 162 GiB |
| 8 KiB | ≈ 105 GiB | ≈ 315 GiB |

`message_bytes / messages` from `rabbitmqctl list_queues` gives the true mean; use a histogram if the tail matters.

### 2.3 Storm headroom

A storm at 500/s for one hour adds:

```text
500/s × 3 600 s = 1 800 000 events
1 800 000 × 2 590 B × 1.15 = 5.4 GiB per hour of storm
```

Because the TTL is 7 days, storm events do **not** disappear afterwards — they stay for a week. Storms therefore accumulate until they age out:

| Storm load | Extra working set | Total per pod |
|---|---|---|
| none (baseline 1 000/min) | — | 28.0 GiB |
| 1h storm, once per week | +5.4 GiB | **33.4 GiB** |
| 1h storm, daily | +38 GiB | 66 GiB |
| 3h storm, once per week | +16 GiB | 44 GiB |
| 500/s for 8 hours, once per week | +43 GiB | 71 GiB |

**This is the real sizing input.** Plan `maxmemory` for the worst 7-day rolling volume you expect, not for 1 000/min. The baseline config below assumes **one 1-hour storm per week** (≈33.4 GiB peak) and sets `maxmemory=40gb`.

If storms are daily, raise `maxmemory` + pod limit + node RAM to ~64 GiB per pod, shorten the TTL, or compress values (§2.4).

### 2.4 Compression is the cheapest lever

At 2 048 B the value dominates the footprint, so compressing it scales everything down. `payload.msgc` is the field that grows — plain `msg` is short and should stay uncompressed, since gzip's ~18 B framing makes short strings *larger* ([payload-and-compression.md](payload-and-compression.md) §2).

| Stored value size | Working set / pod (baseline) | With weekly 1h storm |
|---|---|---|
| 2 048 B (uncompressed) | 28.0 GiB | 33.4 GiB |
| ≈ 700 B (3x) | 11.0 GiB | 13.1 GiB |
| ≈ 400 B (5x) | 7.0 GiB | 8.4 GiB |

```text
worker:  gzip payload.msgc before SET (only if >= FW_COMPRESS_MIN)
gateway: pass the bytes through — never decompress
browser: DecompressionStream -> plain text
```

Trade-off: the stored `msgc` is no longer readable in `valkey-cli`, and it is no longer substring-searchable in Valkey. The audit queue stays plain, so history searches go through RabbitMQ instead ([payload-and-compression.md](payload-and-compression.md) §6). **Compression is the right move if storm headroom or node RAM gets expensive** — it is roughly a 3x hardware reduction.

## 3. Per-component resources

### 3.1 Resource table

| Component | Reps | CPU req | CPU lim | Mem req | Mem lim | PV |
|---|---|---|---|---|---|---|
| RabbitMQ | 3 | 250m | 1 | 512Mi | 1Gi | 96Gi |
| Valkey | 3 | 500m | 4 | 8Gi | **52Gi** | 64Gi |
| Sentinel | 3 | 20m | 200m | 64Mi | 128Mi | — |
| sentinel-bridge | 2 | 20m | 200m | 64Mi | 192Mi | — |
| redis-exporter | 1 | 10m | 100m | 32Mi | 96Mi | — |
| http-ingress | 3 | 100m | 500m | 128Mi | 384Mi | — |
| Workers | 3 | 100m | 500m | 256Mi | 512Mi | — |
| WS gateway | 3 | 200m | 1 | 256Mi | 768Mi | — |
| Frontend | 3 | 20m | 200m | 64Mi | 128Mi | — |
| Prometheus (optional) | 1 | 200m | 1 | 512Mi | 1Gi | 50Gi |
| **Sum** | **25 pods** | **≈ 3.8 CPU** | **≈ 23.7 CPU** | **≈ 28.4 GiB** | **≈ 154 GiB** | **530Gi** |

Valkey dominates both columns: `3 × 8Gi` of requests and `3 × 52Gi` of limits, i.e. 24 of the 28 GiB requested and 156 of the 166 GiB of limits. Everything else is noise by comparison, which is why the node-size decision in §3.3 is the only one that really matters.

PV total: `3 × 96 (RabbitMQ) + 3 × 64 (Valkey) + 50 (Prometheus) = 530 GiB`.

### 3.2 Cluster recommendation

**3 worker nodes, each 8 vCPU / 128 GiB RAM / ~200 GiB storage.**

Per node: one Valkey (500m req / 4 CPU lim, 8Gi req / 52Gi lim), one RabbitMQ
(250m / 1 CPU, 512Mi / 1Gi), one Sentinel, one worker, one gateway, one frontend.
**Requests** ≈ 1.1 CPU / 9.1 GiB per node; **limits** ≈ 6.9 CPU / 54.5 GiB per node.
Two Valkey pods (104 GiB of limits) fit on a 128 GiB node, which is what §3.3
requires.

### 3.3 The node-loss rule (get this right)

To **reschedule** an evicted Valkey pod, a surviving node must host **two** Valkey pods, because it already runs one:

```text
node RAM >= 2 × Valkey mem limit + other pods
         >= 2 × 52Gi + ~7Gi
         >= 111 GiB   ->  128 GiB nodes
```

This general rule is the single most consequential hardware decision at this scale:

| Node RAM | Node loss | Redundancy after reschedule |
|---|---|---|
| 64 GiB | evicted Valkey pod stays **Pending** | 3 → 2, permanently, until the node returns |
| **128 GiB** | pod reschedules onto a survivor | **3 → 3**, full redundancy restored |

Data safety holds in both cases (master + replica + audit WAL), but only 128 GiB nodes keep the R3 promise of 3 replicas on 3 nodes through a node loss. Any smaller node size is a deliberate trade of redundancy for cost, and must be recorded in [roadmap.md](roadmap.md) §3.2.

### 3.4 Valkey memory detail

```text
baseline working set          ≈ 28.0 GiB
+ one weekly 1h storm         ≈ 5.4 GiB
+ AOF rewrite copy-on-write   ≈ 4.0 GiB (forked rewrite of a 33 GiB image)
+ runtime headroom            ≈ 9.6 GiB
= pod limit                   52Gi

--maxmemory=40gb
```

Request is 8 GiB so the scheduler places it realistically; the limit is 52 GiB and is
what must fit on a node.

`maxmemory` must sit **above** the expected 7-day working set or writes start being
REFUSED with an OOM error (`noeviction`, [valkey.md](valkey.md) §4.1). This is the
opposite of the usual "leave 20 % free for eviction" advice, and it is only tolerable
because the audit WAL can refill anything the datastore loses
([persistence-and-durability.md](persistence-and-durability.md) §4).

The extra headroom over the Dragonfly figure is for the **AOF rewrite**: `BGREWRITEAOF`
forks, and the child's copy-on-write pages grow with write volume during the rewrite
([valkey.md](valkey.md) §4.2).

### 3.5 AOF I/O and storage

```text
AOF base (RDB preamble) ≈ working set with storm ≈ 33 GiB after a rewrite
grows between rewrites with every write, ≈ 2 KiB per event
rewritten hourly        -> the replayed file is a fresh compact image, not a week of history
PV                      = 64Gi  (~2x the compact size, so a rewrite can complete
                                while the previous file still exists)
rewrite write rate      ≈ 33 GiB / 3 600 s ≈ 9.4 MiB/s sustained while rewriting
```

- 9.4 MiB/s for one hour in every one is comfortable for a network PV and trivial for
  local NVMe.
- The PV must hold **two** copies during a rewrite: `BGREWRITEAOF` writes a new
  `appendonly` manifest and file set, then switches atomically and removes the old
  one. 64 GiB gives that headroom with room to spare.
- `aof-use-rdb-preamble yes` is what keeps the replayed file small: without it the
  base would be the full command history since the last rewrite
  ([valkey.md](valkey.md) §4.1).
- The hourly rewrite is what satisfies "make restart and reread fast": a restart
  replays at most one hour of incremental commands on top of a compact base.

### 3.6 RabbitMQ detail

At 1 000/min the daily volume is small enough that the audit WAL covers the **full 7-day retention**:

```text
audit volume = 1 000/min × 1 440 min × 2 048 B ≈ 2.95 GB/day ≈ 2.75 GiB/day
7 days       ≈ 19.3 GiB
```

| Volume | Size | Driver |
|---|---|---|
| `q.event.ingest` journal | ≈ 8 GiB | transient backlog; absorbs a 500/s storm (~1 GiB/h) |
| `q.audit.events` | 64 GiB cap | 7-day window ≈ 19.3 GiB, **3.3x slack** |
| `q.event.ingest.dlq` | 8 GiB cap | poison messages, should stay near zero |
| headroom | 16 GiB | compaction, memory spill, alarms |
| **Total** | **96 GiB** | |

Throughput: 16.7 msg/s sustained, 500 msg/s peak. RabbitMQ is far from its limits; disk I/O for the quorum journal is the only meaningful cost.

## 4. Scaling levers

| Pressure | Lever | Ceiling |
|---|---|---|
| Storm headroom exhausted (evictions) | raise `maxmemory` + pod limit + node RAM together, or compress values, or shorten TTL | one node's RAM |
| Queue backlog rising | add workers on queue depth | consumer-bound |
| WebSocket connections | add gateways on connection count | linear, stateless |
| Retention window | TTL is the only lever; RAM scales linearly | §2.2 |
| Write throughput | none needed — 500/s peak is a small fraction of one Valkey main thread | n/a |

## 5. What NOT to scale

| Component | Why fixed at 3 |
|---|---|
| RabbitMQ | quorum queues with 3 members |
| Valkey | 1 master + 2 replicas; more pods do not add redundancy value and complicate Sentinel ranking |
| Sentinel | 3 is the canonical odd quorum |
| Frontend | static; 3 exists purely for node-loss tolerance |

## 6. Verification

```bash
# true event size and both rates (sustained vs storm)
kubectl -n fire-watch exec rabbitmq-0 -- rabbitmqctl list_queues name messages message_bytes \
  message_stats.publish_details.rate

# actual per-record cost in Valkey — compare with the 2 590 B model
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 MEMORY USAGE event:<32-hex-key>
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 MEMORY USAGE idx:source:<device>

# working set vs maxmemory, and eviction count
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 INFO memory \
  | grep -E 'used_memory_human|maxmemory_human|evicted_keys'

# retained record count vs the model: at most 1 000/min x 10 080 min = 10 080 000,
# and lower in practice because a record is a CONDITION, not a delivery (§1.1)
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 ZCARD idx:event:all
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 DBSIZE

# AOF size and the last rewrite
kubectl -n fire-watch exec valkey-0 -- ls -lh /data/appendonlydir
kubectl -n fire-watch exec valkey-0 -- valkey-cli -p 6379 INFO persistence \
  | grep -E 'aof_last_bgrewrite_status|aof_base_size|aof_current_size'
```

Rules:

- `evicted_keys > 0` in steady state is an **incident**: the retention promise is
  already broken, which `noeviction` makes impossible unless the flag was overridden.
  More likely is an OOM error in the worker log — raise `maxmemory` and the pod limit
  together.
- `ZCARD idx:event:all` above the delivery-model ceiling means a storm's records are
  still inside their 7-day window (normal) — or the index is not being pruned
  ([architecture.md](architecture.md) §6.2). It can never exceed the number of
  *conditions*, so a value near the delivery count means the feed does not repeat
  itself.
- If `MEMORY USAGE event:<32-hex-key>` is far above the payload size, compression is
  the answer, not more RAM ([payload-and-compression.md](payload-and-compression.md)).
- If a node loss leaves a Valkey pod `Pending`, node RAM is below the §3.3 threshold.
