# Validation

Failure scenarios to rehearse before the deployment is considered done. Each scenario states the injection, the expected behaviour, and the check that proves it.

## 0. Preconditions

- 3 healthy worker nodes, each able to host every pod (check `allocatable` vs the sums in [sizing.md](sizing.md) §3; Valkey needs a 52Gi limit, and 128 GiB nodes are required to reschedule it after a node loss)
- all components at 3 replicas, all probes green
- a load generator publishing a steady, identifiable stream (1 000 events/min with monotonic ids, matching the sizing baseline)
- a storm injector able to sustain 500 evt/s for one hour, to validate headroom ([sizing.md](sizing.md) §2.3)
- a recorded baseline: `rabbitmqctl list_queues`, `ZCARD idx:event:all`, AOF file
  listing

## 1. Component pod loss

| # | Injection | Expected | Proof |
|---|---|---|---|
| 1.1 | `delete pod rabbitmq-1` | publishers continue, queue reports 2/3 members, no nack | `rabbitmqctl list_queues members`; publish rate unchanged |
| 1.2 | `delete pod rabbitmq-1 rabbitmq-2` | publish refused (`PRECONDITION_FAILED`), workers idle, **no acknowledged event lost** | publisher error log; `DBSIZE` after recovery matches baseline + unacked |
| 1.3 | `delete pod valkey-1` | master writes continue, `connected_slaves:1`, write guard **not** tripped (grace not exceeded) | `INFO replication`; `fw_valkey_write_guard_tripped == 0` |
| 1.4 | `delete pod valkey-1 valkey-2` | write guard trips after 30s, workers stop consuming (readiness false), queue depth grows | `get pods -L fire-watch.io/writable`; `rabbitmqctl list_queues messages_ready` rising |
| 1.5 | `delete pod -l app.kubernetes.io/name=worker` (one) | unacked message redelivered, processed once, no duplicate state | `ZCARD idx:source:<src>` unchanged vs distinct ids |
| 1.8 | Run at 1 000/min for 7 days without intervention | `evicted_keys == 0`, `DBSIZE` ≈ 10.08M, `used_memory < 0.9 × maxmemory`, PV < 60Gi used | [sizing.md](sizing.md) §6 rules; any eviction is an incident, not a tuning signal |
| 1.9 | Inject a 1-hour 500 evt/s storm, then continue at 1 000/min | no eviction during the storm; `used_memory` rises by ≈5.4 GiB and stays within `maxmemory=40gb` | `INFO memory` before/during/after; `evicted_keys` still 0 |
| 1.6 | `delete pod -l app.kubernetes.io/name=ws-gateway` (one) | clients reconnect to another pod, backfill fills the gap | client log shows reconnect + `backfill > 0` |
| 1.7 | `delete pod -l app.kubernetes.io/name=frontend` (one) | page keeps loading | `curl` via Route succeeds throughout |

## 2. Master failover

Injection: `kubectl delete pod valkey-0` (the master).

| Step | Expected | Proof |
|---|---|---|
| Sentinel detects down | within 5s (`down-after-milliseconds`) | sentinel log `+sdown`, then `+odown` with quorum 2 |
| New master elected | within ~10s | `SENTINEL get-master-addr-by-name fire-watch-valkey` returns a different pod IP |
| Service follows | within 5s of promotion | `get endpoints valkey` lists the new pod |
| Restarted `valkey-0` rejoins as replica | yes, as `REPLICAOF` is applied by the bridge, **not** from a stale manifest arg | `INFO replication` on the restarted pod shows `role:slave`, `master_link_status:up` |
| Data continuity | no record lost | `ZCARD idx:event:all` before vs after, plus replay of the gap window if the AOF was lost |
| Write availability | ≤ 15s of failed writes, all retried/nacked | worker log shows retries, queue returns to 0 ready |

Then repeat with `SENTINEL failover fire-watch-valkey` to test the controlled path, and verify the bridge does not flap roles afterwards (no repeated `+switch-master` in 10 min).

## 3. Node loss

Cordon and drain one node while load runs.

| Expected | Proof |
|---|---|
| PDBs block simultaneous eviction of a quorum (rabbitmq needs 2, valkey 2, sentinel 2) | drain waits rather than evicting a second member at once |
| All **non-Valkey** pods rescheduled onto the remaining 2 nodes | `get pods -o wide` shows new nodes |
| The Valkey pod reschedules onto a survivor (128 GiB nodes hold two 52Gi pods); on smaller nodes it stays **Pending** ([sizing.md](sizing.md) §3.3) | `get pods` shows either a new node assignment or `Pending` with `Insufficient memory` |
| Data safety holds while only 2 Valkey pods run | master keeps a replica, `connected_slaves:1` |
| No acknowledged event lost | `DBSIZE` + audit replay comparison |
| Valkey write guard may trip during the reschedule and must recover automatically | `fw_valkey_write_guard_tripped` returns to 0 within 60s of `connected_slaves >= 2` |

**Acceptance condition:** with 128 GiB nodes the evicted Valkey pod reschedules and full 3-replica redundancy returns. With 64 GiB nodes it stays `Pending` and the cluster runs at reduced redundancy (2 of 3 Valkey pods) and **cannot survive a second node loss** until the node returns ([sizing.md](sizing.md) §3.3).

Repeat with the **hard** variant: `kubectl delete node` (not drain) so the pods vanish without graceful shutdown. Expect the same end state, slower, and a possible full resync of the affected Valkey replica (watch `INFO replication` for `state=full_sync` then `stable_sync`).

## 4. Split brain

Simulate a partition by isolating one Valkey pod at the **CNI** layer — the cluster's network plugin, not a NetworkPolicy (which this deployment does not ship, [networking.md](networking.md) §5). Options, in order of preference:

1. `kubectl cordon` + `kubectl drain` the node hosting `valkey-0` and block its kubelet: heavy but faithful.
2. On a CNI with a chaos tool available, apply a temporary `NetworkPolicy` **in a dedicated test namespace only** — never in the shared production namespace.
3. `kubectl exec` into `valkey-1` and `valkey-2` and add an `iptables`/`nft` DROP rule for the master's IP (requires `NET_ADMIN`; not available under the default SCC, so usually only viable in the lab overlay).

```bash
# example of option 3, in the lab only
kubectl -n fire-watch-lab exec valkey-1 -- \
  sh -c 'iptables -I OUTPUT -d <master-pod-ip> -j DROP'
```

| Expected | Proof |
|---|---|
| Minority master trips the write guard at 30s | `fw_valkey_write_guard_tripped == 1`; label `writable=false` |
| Sentinel promotes the majority-side replica | `+switch-master`; new master accepts writes |
| Service points at the new master only | `get endpoints valkey` = 1 address, the new master |
| Workers never write to the old master | worker log: no successful `SET` after guard trip |
| Old master rejoins and is demoted | `INFO replication` on old master shows `role:slave`; its divergent tail is gone |
| No duplicate/stale data visible in the UI | index cardinality unchanged; spot-check a few ids |
| Events published during the partition survive | audit queue depth grew, then replay restored them | 
| **Writes refused by the ENGINE** | `min-replicas-to-write 2` makes the isolated master answer `NOREPLICAS`, so even a client using a direct pod IP cannot write | [valkey.md](valkey.md) §5 |
| **Documented gap** | a client with a direct pod IP can still READ stale data from the minority | accepted, see [fault-tolerance.md](fault-tolerance.md) §4 |

Restore the policy afterwards and confirm `connected_slaves:2` on the master.

## 5. Data-loss drills

| # | Injection | Expected |
|---|---|---|
| 5.1 | Wipe the Valkey PVC on all 3 pods (`delete pvc`), restart | AOF gone → empty master; `replay-gap` restores the **full 7 days** from `q.audit.events` (≈10.08M conditions at most, ≈19.3 GiB). `ZCARD idx:event:all` matches the pre-wipe value ([persistence-and-durability.md](persistence-and-durability.md) §4.4) |
| 5.6 | Kill the AOF rewrite mid-flight (`SIGKILL` the sidecar's redis-cli during `BGREWRITEAOF`) | the old AOF stays in place and is replayed; `aof_last_bgrewrite_status` reports an error; the next scheduled rewrite succeeds |
| 5.2 | Corrupt the tail of the current AOF (`truncate -s 100`), restart | `aof-load-truncated yes` loads the valid prefix; only the last second is lost; then re-replay the gap |
| 5.7 | `SIGKILL` valkey-0 immediately after an ack | at most 1 s of writes lost (`appendfsync everysec`); the audit queue still holds the originals and replay restores them |
| 5.3 | Kill a worker with `SIGKILL` mid-write, 50 times | zero messages in `q.event.ingest.dlq`, zero duplicate records |
| 5.4 | Fill the audit queue past `x-max-length-bytes` | worker audit publish fails, ingest nacks and requeues, no silent loss; alarm fires |
| 5.5 | Kill RabbitMQ during a publish burst without confirms (misconfigured client) | messages lost — this proves why confirms are mandatory; record the observation, then fix the client |

## 6. Event ordering and close semantics

These prove the severity model in [event-lifecycle.md](event-lifecycle.md) survives retries and multi-consumer reordering.

| # | Injection | Expected | Proof |
|---|---|---|---|
| 6.1 | Broker timestamps present on every message | every consumed message has `timestamp_in_ms` | `rabbitmqadmin get` shows the header; no message reaches the validator without it |
| 6.2 | Publish CRITICAL, then CLOSE, but make the CLOSE's Valkey write fail so it is requeued (`nack(requeue=true)`) | alert stays open until the CLOSE is successfully applied | `HGETALL event:<key>` watermark advances only on the applied attempt |
| 6.3 | **The headline case:** CRITICAL (t1) → CLOSE (t2) requeued → CRITICAL (t3 > t2) committed → CLOSE redelivered | the redelivered CLOSE returns `applied=0`; the t3 CRITICAL **stays open** | `event:<key>` severity is still CRITICAL; the Lua script returned `0` |
| 6.4 | Publish two events in the same millisecond, one CRITICAL and one CLOSE, repeatedly | identical final state on every run | tie-break on `(ts, id)`; run 20x, compare `HGETALL` |
| 6.5 | Publish out-of-order at the broker: CLOSE then CRITICAL with CLOSE's `timestamp_in_ms` **lower** | the CLOSE is stale and never retires the CRITICAL | alert state CRITICAL; `applied=0` on the CLOSE |
| 6.6 | Force the same condition to two different workers concurrently | one fold wins, no split state | exactly one call returns non-zero; final watermark is the max `(ts, id)` |
| 6.7 | Publish the same device+summary 100× with **100 different event ids** | `count == 100`, ONE record | `ZCARD idx:event:all` grows by exactly 1; `HMGET event:<key> count` is 100 |
| 6.8 | Publish a CRITICAL, then a WARNING for the same device+summary 10 s later | `severity` becomes WARNING, `count` 2, one record | same key; the row does not split |
| 6.11 | Publish two CRITICALs that differ only in `EVT_Summary` wording | **two** records | different key — the trade-off this design accepts ([valkey.md](valkey.md) §2.3) |
| 6.9 | CRITICAL → CLOSE → CRITICAL | new episode: `count` resets to 1, `firstSeen` is the third event | `applied` sequence is 1, 3, 1 |
| 6.10 | Run 1 000 evt/min from one permanently-faulted source for 24h | `count` stays bounded (≈288 at a 5-min window) and the alert table has **one** row for that key | `SCAN event:*` count is `#sources x #types`, not millions |
| 6.11 | Replay the audit queue with records deliberately shuffled | final alert state identical to the pre-wipe state | compare `SCAN event:*` before and after ([event-lifecycle.md](event-lifecycle.md) §7) |
| 6.12 | Republish a message through a queue move without `x-original-ts` | the worker re-stamps it with "now" and the ordering is wrong — **this must fail a review**, not a test | verify no code path republishes without copying the timestamp |
| 6.13 | Kill a worker between the Valkey write and the ack, repeatedly | no duplicate records, no duplicated alert transitions, `count` unchanged | `DBSIZE` stable; watermark unchanged on the redelivery |

### 6a. Payload compression

| # | Injection | Expected | Proof |
|---|---|---|---|
| 6a.1 | Publish an event with a 3 KiB `msgc` and a 90 B `msg` | `msgc` stored as `{"__gz": base64}`, `msg` stored plain string | `valkey-cli --raw HGET event:<key> payload`, field-level check ([payload-and-compression.md](payload-and-compression.md) §4) |
| 6a.2 | Publish an event with a 200 B `msgc` | **not** compressed — below `FW_COMPRESS_MIN` | value starts with `{`, `MEMORY USAGE` ≈ plain size |
| 6a.3 | Round-trip: decompress the stored value and compare with the original | byte-identical `msgc` | `gunzip` output diffed against the producer's string |
| 6a.4 | Inspect `q.audit.events` | the WAL copy is **plain**, never compressed | `rabbitmqadmin get` shows readable JSON |
| 6a.5 | Replay the audit queue after a PVC wipe | replayed `msgc` compresses to the same bytes as the live write | compare `MEMORY USAGE` and the raw value of one id before/after |
| 6a.6 | Corrupt one stored `msgc` (flip a byte after the gzip header) | the UI renders `[undecodable payload]` and the alert row **remains visible** with severity and timestamps intact | browser check; no blank row, no console-crashing error |
| 6a.7 | Store a `msgc` that expands beyond `FW_MSG_MAX_BYTES` | decoding truncates at the cap with a marker | output length is capped; no OOM in the browser tab |

## 7. Rolling updates

| # | Injection | Expected |
|---|---|---|
| 7.1 | `rollout restart statefulset/rabbitmq` | one node at a time, cluster never below 2, publishing continues |
| 7.2 | `rollout restart statefulset/valkey` | OrderedReady order, no two pods down at once; possibly one failover if the master restarts first |
| 7.3 | `rollout restart deploy/ws-gateway` | sockets closed gracefully (`op: shutdown`), clients reconnect with backfill, no errors surfaced to the user |
| 7.4 | Image rollback of any component | returns to previous behaviour. Note the two format boundaries: the AOF format changed across Valkey major versions (a newer AOF loaded by an older binary is **not** supported — pin the image and record the version), and a record written by the new worker has extra hash fields an old worker would ignore |

## 8. Security and hygiene

| Check | Expectation |
|---|---|
| `grep -rn "password\|secret\|token" k8s/` | only `Secret` references and `${VAR}` placeholders, no literals |
| `.gitignore` | contains `.env`, `*.pem`, `*.key`, `secrets/`, `kubeconfig*` |
| Pods run as non-root, no `privileged`, no hostPath | `oc adm policy scc-subject-review` clean |
| Route TLS | edge termination, HTTP redirects to HTTPS |
| No NetworkPolicy objects | `get networkpolicy` returns none — they are deliberately omitted in a shared namespace ([networking.md](networking.md) §5) |
| No external port exposure | all Services `ClusterIP`; exactly 2 Routes; no NodePort/LoadBalancer |
| No pod-IP usage in application config | `grep -rn 'valkey-[0-9]' <app-config>` returns only control-plane components |

## 9. Acceptance gate

The deployment is acceptable only when:

- [ ] every scenario in §1–§4 passes without manual intervention
- [ ] every check in [fault-tolerance.md](fault-tolerance.md) §7 passes: replica counts, **required** anti-affinity actually spread, PDBs present on all 7 workloads, no `x-delivery-limit`, Sentinel epochs agree and are non-zero, Sentinel can authenticate, `cluster_partition_handling = pause_minority`, `maxmemory-policy = noeviction`, exactly one bridge holds the lease, `valkey` has exactly one endpoint
- [ ] §5.1 passes: a PVC wipe is fully recoverable for the complete 7-day window from the audit queue ([persistence-and-durability.md](persistence-and-durability.md) §4.4)
- [ ] every scenario in §6 passes, in particular §6.3: **a redelivered CLOSE never retires a newer CRITICAL/FATAL** ([event-lifecycle.md](event-lifecycle.md) §4–5)
- [ ] every scenario in §6.7–6.11 passes: a repeat of one CONDITION folds into one
      record with a moving `lastSeen`, and a reworded summary is a new record
      ([valkey.md](valkey.md) §2)
- [ ] every scenario in §6a passes: `msgc` is compressed at rest, the WAL copy is plain, and a decode failure never blanks an alert ([payload-and-compression.md](payload-and-compression.md))
- [ ] `evicted_keys == 0` in steady state and `ZCARD idx:event:all` ≤ 10.08M at
      1 000/min × 7d ([sizing.md](sizing.md) §6)
- [ ] no event present in the audit queue is missing from Valkey after replay
- [ ] `min-replicas-to-write` is `2` in production and `0` in the lab, and the bridge's
      `FW_WRITE_GUARD_MIN_REPLICAS` matches it ([valkey.md](valkey.md) §5)
- [ ] the documented residual gap in §4 (direct-pod-IP READS of a stale master) is
      recorded in [roadmap.md](roadmap.md)
