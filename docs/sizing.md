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
| Snapshot generations retained | 2 | §3.5 |

Derived:

```text
sustained throughput = 1 000/min × 2 048 B   = 2.05 MB/min = 34 KB/s
storm throughput     = 500/s     × 2 048 B   = 1 MiB/s
events retained      = 1 000/min × 1 440 min/day × 7 = 10 080 000
```

**The number that matters is the 7-day total, not the instantaneous rate.** With a 7-day TTL, Dragonfly holds everything written in the last rolling week. Peak/s only matters for storm headroom (§2.3); the sustained/min figure determines the baseline working set.

## 2. Working set

### 2.1 Per-event memory footprint

A stored event costs more than its value:

| Component | Bytes | Note |
|---|---|---|
| value (`fw:evt:{id}` string) | **2 048** | the event JSON itself |
| key string `fw:evt:<36-char uuid>` | 44 | 7 + 36 + null, rounded |
| dash-table slot (key metadata) | 24 | per-key overhead, amortised |
| zset member (UUID) | ~150 | [architecture.md](architecture.md) §6.1 — stored twice (hash + skiplist) |
| **Total per event** | **≈ 2 266 B** | |

```text
raw        = 10 080 000 × 2 266 B = 22.8 GB ≈ 21.3 GiB
+ 1.15 allocator/fragmentation     ≈ 24.5 GiB
```

**Each Dragonfly pod must hold ≈ 24.5 GiB** (replication is full-copy, so all three hold the same data).

Aggregate: `3 × 24.5 ≈ 73.5 GiB` of RAM.

### 2.2 Why 2 048 B is the right planning number

The event size varies. Do **not** plan with the average:

- Dragonfly's memory is determined by total bytes actually stored, so a distribution with a long tail costs more than `count × mean`.
- Sizing to the floor is the only choice that never evicts. Sizing to an average guarantees eviction on a busy week, which silently breaks the 7-day retention promise.

| Planning size | Working set / pod | Aggregate RAM |
|---|---|---|
| **2 048 B (floor)** | **≈ 24.5 GiB** | **≈ 73.5 GiB** |
| 4 KiB | ≈ 47 GiB | ≈ 141 GiB |
| 8 KiB | ≈ 92 GiB | ≈ 276 GiB |

`message_bytes / messages` from `rabbitmqctl list_queues` gives the true mean; use a histogram if the tail matters.

### 2.3 Storm headroom

A storm at 500/s for one hour adds:

```text
500/s × 3 600 s = 1 800 000 events
1 800 000 × 2 266 B × 1.15 = 4.7 GiB per hour of storm
```

Because the TTL is 7 days, storm events do **not** disappear afterwards — they stay for a week. Storms therefore accumulate until they age out:

| Storm load | Extra working set | Total per pod |
|---|---|---|
| none (baseline 1 000/min) | — | 24.5 GiB |
| 1h storm, once per week | +4.7 GiB | **29.2 GiB** |
| 1h storm, daily | +33 GiB | 57 GiB |
| 3h storm, once per week | +14 GiB | 38.5 GiB |
| 500/s for 8 hours, once per week | +38 GiB | 62 GiB |

**This is the real sizing input.** Plan `maxmemory` for the worst 7-day rolling volume you expect, not for 1 000/min. The baseline config below assumes **one 1-hour storm per week** (≈29.2 GiB peak) and sets `maxmemory=36gb`.

If storms are daily, raise `maxmemory` + pod limit + node RAM to ~64 GiB per pod, shorten the TTL, or compress values (§2.4).

### 2.4 Compression is the cheapest lever

At 2 048 B the value dominates the footprint, so compressing it scales everything down. `payload.msgc` is the field that grows — plain `msg` is short and should stay uncompressed, since gzip's ~18 B framing makes short strings *larger* ([payload-and-compression.md](payload-and-compression.md) §2).

| Stored value size | Working set / pod (baseline) | With weekly 1h storm |
|---|---|---|
| 2 048 B (uncompressed) | 24.5 GiB | 29.2 GiB |
| ≈ 700 B (3x) | 9.6 GiB | 11.4 GiB |
| ≈ 400 B (5x) | 6.1 GiB | 7.3 GiB |

```text
worker:  gzip payload.msgc before SET (only if >= FW_COMPRESS_MIN)
gateway: pass the bytes through — never decompress
browser: DecompressionStream -> plain text
```

Trade-off: the stored `msgc` is no longer readable in `redis-cli`, and it is no longer substring-searchable in Dragonfly. The audit queue stays plain, so history searches go through RabbitMQ instead ([payload-and-compression.md](payload-and-compression.md) §6). **Compression is the right move if storm headroom or node RAM gets expensive** — it is roughly a 3x hardware reduction.

## 3. Per-component resources

### 3.1 Resource table

| Component | Reps | CPU req | CPU lim | Mem req | Mem lim | PV |
|---|---|---|---|---|---|---|
| RabbitMQ | 3 | 250m | 1 | 512Mi | 1Gi | 96Gi |
| Dragonfly | 3 | 500m | 4 | 8Gi | **48Gi** | 64Gi |
| Sentinel | 3 | 20m | 200m | 64Mi | 128Mi | — |
| sentinel-bridge | 2 | 20m | 200m | 64Mi | 192Mi | — |
| redis-exporter | 1 | 10m | 100m | 32Mi | 96Mi | — |
| http-ingress | 3 | 100m | 500m | 128Mi | 384Mi | — |
| Workers | 3 | 100m | 500m | 256Mi | 512Mi | — |
| WS gateway | 3 | 200m | 1 | 256Mi | 768Mi | — |
| Frontend | 3 | 20m | 200m | 64Mi | 128Mi | — |
| Prometheus (optional) | 1 | 200m | 1 | 512Mi | 1Gi | 50Gi |
| **Sum** | **25 pods** | **≈ 3.8 CPU** | **≈ 23.7 CPU** | **≈ 28.4 GiB** | **≈ 154 GiB** | **530Gi** |

Dragonfly dominates both columns: `3 × 8Gi` of requests and `3 × 48Gi` of limits, i.e. 24 of the 28 GiB requested and 144 of the 154 GiB of limits. Everything else is noise by comparison, which is why the node-size decision in §3.3 is the only one that really matters.

PV total: `3 × 96 (RabbitMQ) + 3 × 64 (Dragonfly) + 50 (Prometheus) = 530 GiB`.

### 3.2 Cluster recommendation

**3 worker nodes, each 8 vCPU / 128 GiB RAM / ~200 GiB storage.**

Per node: one Dragonfly (500m req / 4 CPU lim, 8Gi req / 48Gi lim), one RabbitMQ (250m / 1 CPU, 512Mi / 1Gi), one Sentinel, one worker, one gateway, one frontend. **Requests** ≈ 1.1 CPU / 9.1 GiB per node; **limits** ≈ 6.9 CPU / 50.5 GiB per node. Two Dragonfly pods (98.5 GiB of limits) fit on a 128 GiB node, which is what §3.3 requires.

### 3.3 The node-loss rule (get this right)

To **reschedule** an evicted Dragonfly pod, a surviving node must host **two** Dragonfly pods, because it already runs one:

```text
node RAM >= 2 × Dragonfly mem limit + other pods
         >= 2 × 48Gi + ~7Gi
         >= 103 GiB   ->  128 GiB nodes
```

This general rule is the single most consequential hardware decision at this scale:

| Node RAM | Node loss | Redundancy after reschedule |
|---|---|---|
| 64 GiB | evicted Dragonfly pod stays **Pending** | 3 → 2, permanently, until the node returns |
| **128 GiB** | pod reschedules onto a survivor | **3 → 3**, full redundancy restored |

Data safety holds in both cases (master + replica + audit WAL), but only 128 GiB nodes keep the R3 promise of 3 replicas on 3 nodes through a node loss. Any smaller node size is a deliberate trade of redundancy for cost, and must be recorded in [roadmap.md](roadmap.md) §3.2.

### 3.4 Dragonfly memory detail

```text
baseline working set          ≈ 24.5 GiB
+ one weekly 1h storm         ≈ 4.7 GiB
+ snapshot serialization      ≈ 1.5 GiB (4 proactor threads)
+ runtime headroom            ≈ 7.3 GiB
= pod limit                   48Gi

--maxmemory=36gb
```

Request is 8 GiB so the scheduler places it realistically; the limit is 48 GiB and is what must fit on a node.

`maxmemory` must sit **above** the expected 7-day working set, or Dragonfly evicts live events and silently breaks the retention promise. This is the opposite of the usual "leave 20% free for eviction" advice, and it is only tolerable because the audit WAL can refill any accidental eviction ([persistence-and-durability.md](persistence-and-durability.md) §4).

### 3.5 Snapshot I/O and storage

```text
snapshot size ≈ working set with storm ≈ 29 GiB per generation
retain 2      → ~58 GiB logical
PV            = 64Gi   (~6 GiB headroom)
hourly write  = 29 GiB / 3 600 s ≈ 8.2 MiB/s sustained
```

- 8.2 MiB/s is comfortable for a network PV. Snapshots are not a performance concern at this scale, though local NVMe is still preferable.
- Retain 2 generations only. More buys nothing because `q.audit.events` covers the full 7-day window (§3.6).
- Hourly snapshots mean a restart never replays a long history, satisfying the "make restart and reread fast" requirement.

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
| Write throughput | none needed — 500/s peak uses a fraction of 4 proactor threads | n/a |

## 5. What NOT to scale

| Component | Why fixed at 3 |
|---|---|
| RabbitMQ | quorum queues with 3 members |
| Dragonfly | 1 master + 2 replicas; more pods do not add redundancy value and complicate Sentinel ranking |
| Sentinel | 3 is the canonical odd quorum |
| Frontend | static; 3 exists purely for node-loss tolerance |

## 6. Verification

```bash
# true event size and both rates (sustained vs storm)
kubectl -n fire-watch exec rabbitmq-0 -- rabbitmqctl list_queues name messages message_bytes \
  message_stats.publish_details.rate

# actual per-event cost in Dragonfly — compare with the 2 266 B model
kubectl -n fire-watch exec dragonfly-0 -- redis-cli -p 6379 MEMORY USAGE fw:evt:<known-id>
kubectl -n fire-watch exec dragonfly-0 -- redis-cli -p 6379 MEMORY USAGE fw:idx:src:<source>

# working set vs maxmemory, and eviction count
kubectl -n fire-watch exec dragonfly-0 -- redis-cli -p 6379 INFO memory \
  | grep -E 'used_memory_human|maxmemory_human|evicted_keys'

# retained key count vs the model: 1 000/min x 10 080 min = 10 080 000
kubectl -n fire-watch exec dragonfly-0 -- redis-cli -p 6379 DBSIZE

# snapshot size
kubectl -n fire-watch exec dragonfly-0 -- ls -lh /data
```

Rules:

- `evicted_keys > 0` in steady state is an **incident**: the retention promise is already broken. Raise storm headroom.
- `DBSIZE` materially above 10 080 000 means either a storm's events are still inside their 7-day window (normal) or the index is not being pruned ([architecture.md](architecture.md) §6.2).
- If `MEMORY USAGE fw:evt:<id>` is far above the value size, compression is the answer, not more RAM.
- If a node loss leaves a Dragonfly pod `Pending`, node RAM is below the §3.3 threshold.
