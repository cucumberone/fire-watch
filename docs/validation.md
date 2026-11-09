# Validation

Failure scenarios to rehearse before the deployment is considered done. Each scenario states the injection, the expected behaviour, and the check that proves it.

## 0. Preconditions

- 3 healthy worker nodes, each able to host every pod (check `allocatable` vs the sums in [sizing.md](sizing.md) §3; Dragonfly needs a 48Gi limit, and 128 GiB nodes are required to reschedule it after a node loss)
- all components at 3 replicas, all probes green
- a load generator publishing a steady, identifiable stream (1 000 events/min with monotonic ids, matching the sizing baseline)
- a storm injector able to sustain 500 evt/s for one hour, to validate headroom ([sizing.md](sizing.md) §2.3)
- a recorded baseline: `rabbitmqctl list_queues`, `DBSIZE`, snapshot file list

## 1. Component pod loss

| # | Injection | Expected | Proof |
|---|---|---|---|
| 1.1 | `delete pod rabbitmq-1` | publishers continue, queue reports 2/3 members, no nack | `rabbitmqctl list_queues members`; publish rate unchanged |
| 1.2 | `delete pod rabbitmq-1 rabbitmq-2` | publish refused (`PRECONDITION_FAILED`), workers idle, **no acknowledged event lost** | publisher error log; `DBSIZE` after recovery matches baseline + unacked |
| 1.3 | `delete pod dragonfly-1` | master writes continue, `connected_slaves:1`, write guard **not** tripped (grace not exceeded) | `INFO replication`; `fw_dragonfly_write_guard_tripped == 0` |
| 1.4 | `delete pod dragonfly-1 dragonfly-2` | write guard trips after 30s, workers stop consuming (readiness false), queue depth grows | `get pods -L fire-watch.io/writable`; `rabbitmqctl list_queues messages_ready` rising |
| 1.5 | `delete pod -l app.kubernetes.io/name=worker` (one) | unacked message redelivered, processed once, no duplicate state | `ZCARD fw:idx:src:<src>` unchanged vs distinct ids |
| 1.8 | Run at 1 000/min for 7 days without intervention | `evicted_keys == 0`, `DBSIZE` ≈ 10.08M, `used_memory < 0.9 × maxmemory`, PV < 60Gi used | [sizing.md](sizing.md) §6 rules; any eviction is an incident, not a tuning signal |
| 1.9 | Inject a 1-hour 500 evt/s storm, then continue at 1 000/min | no eviction during the storm; `used_memory` rises by ≈4.7 GiB and stays within `maxmemory=36gb` | `INFO memory` before/during/after; `evicted_keys` still 0 |
| 1.6 | `delete pod -l app.kubernetes.io/name=ws-gateway` (one) | clients reconnect to another pod, backfill fills the gap | client log shows reconnect + `backfill > 0` |
| 1.7 | `delete pod -l app.kubernetes.io/name=frontend` (one) | page keeps loading | `curl` via Route succeeds throughout |

## 2. Master failover

Injection: `kubectl delete pod dragonfly-0` (the master).

| Step | Expected | Proof |
|---|---|---|
| Sentinel detects down | within 5s (`down-after-milliseconds`) | sentinel log `+sdown`, then `+odown` with quorum 2 |
| New master elected | within ~10s | `SENTINEL get-master-addr-by-name fire-watch-dragonfly` returns a different pod IP |
| Service follows | within 5s of promotion | `get endpoints dragonfly` lists the new pod |
| Restarted `dragonfly-0` rejoins as replica | yes, as `REPLICAOF` is applied by the bridge, **not** from a stale manifest arg | `INFO replication` on the restarted pod shows `role:slave`, `master_link_status:up` |
| Data continuity | no event lost | `DBSIZE` before vs after, plus replay of the gap window if a snapshot was loaded |
| Write availability | ≤ 15s of failed writes, all retried/nacked | worker log shows retries, queue returns to 0 ready |

Then repeat with `SENTINEL failover fire-watch-dragonfly` to test the controlled path, and verify the bridge does not flap roles afterwards (no repeated `+switch-master` in 10 min).

## 3. Node loss

Cordon and drain one node while load runs.

| Expected | Proof |
|---|---|
| PDBs block simultaneous eviction of a quorum (rabbitmq needs 2, dragonfly 2, sentinel 2) | drain waits rather than evicting a second member at once |
| All **non-Dragonfly** pods rescheduled onto the remaining 2 nodes | `get pods -o wide` shows new nodes |
| The Dragonfly pod reschedules onto a survivor (128 GiB nodes hold two 48Gi pods); on smaller nodes it stays **Pending** ([sizing.md](sizing.md) §3.3) | `get pods` shows either a new node assignment or `Pending` with `Insufficient memory` |
| Data safety holds while only 2 Dragonfly pods run | master keeps a replica, `connected_slaves:1` |
| No acknowledged event lost | `DBSIZE` + audit replay comparison |
| Dragonfly write guard may trip during the reschedule and must recover automatically | `fw_dragonfly_write_guard_tripped` returns to 0 within 60s of `connected_slaves >= 2` |

**Acceptance condition:** with 128 GiB nodes the evicted Dragonfly pod reschedules and full 3-replica redundancy returns. With 64 GiB nodes it stays `Pending` and the cluster runs at reduced redundancy (2 of 3 Dragonfly pods) and **cannot survive a second node loss** until the node returns ([sizing.md](sizing.md) §3.3).

Repeat with the **hard** variant: `kubectl delete node` (not drain) so the pods vanish without graceful shutdown. Expect the same end state, slower, and a possible full resync of the affected Dragonfly replica (watch `INFO replication` for `state=full_sync` then `stable_sync`).

## 4. Split brain

Simulate a partition by isolating one Dragonfly pod at the **CNI** layer — the cluster's network plugin, not a NetworkPolicy (which this deployment does not ship, [networking.md](networking.md) §5). Options, in order of preference:

1. `kubectl cordon` + `kubectl drain` the node hosting `dragonfly-0` and block its kubelet: heavy but faithful.
2. On a CNI with a chaos tool available, apply a temporary `NetworkPolicy` **in a dedicated test namespace only** — never in the shared production namespace.
3. `kubectl exec` into `dragonfly-1` and `dragonfly-2` and add an `iptables`/`nft` DROP rule for the master's IP (requires `NET_ADMIN`; not available under the default SCC, so usually only viable in the lab overlay).

```bash
# example of option 3, in the lab only
kubectl -n fire-watch-lab exec dragonfly-1 -- \
  sh -c 'iptables -I OUTPUT -d <master-pod-ip> -j DROP'
```

| Expected | Proof |
|---|---|
| Minority master trips the write guard at 30s | `fw_dragonfly_write_guard_tripped == 1`; label `writable=false` |
| Sentinel promotes the majority-side replica | `+switch-master`; new master accepts writes |
| Service points at the new master only | `get endpoints dragonfly` = 1 address, the new master |
| Workers never write to the old master | worker log: no successful `SET` after guard trip |
| Old master rejoins and is demoted | `INFO replication` on old master shows `role:slave`; its divergent tail is gone |
| No duplicate/stale data visible in the UI | index cardinality unchanged; spot-check a few ids |
| Events published during the partition survive | audit queue depth grew, then replay restored them | 
| **Documented gap** | a client with a direct pod IP could still have written to the minority | accepted, see [dragonfly.md](dragonfly.md) §3 |

Restore the policy afterwards and confirm `connected_slaves:2` on the master.

## 5. Data-loss drills

| # | Injection | Expected |
|---|---|---|
| 5.1 | Wipe Dragonfly PVC on all 3 pods (`delete pvc`), restart | newest `.dfs` gone → empty master; `replay-gap` restores the **full 7 days** from `q.audit.events` (≈10.08M events, ≈19.3 GiB). `DBSIZE` matches the pre-wipe value ([persistence-and-durability.md](persistence-and-durability.md) §4.4) |
| 5.6 | Restore from snapshot generation 2 after corrupting generation 1 | loads the previous generation (7d − 1h of history), then replay covers the last 24 h; verify `DBSIZE` matches the model |
| 5.2 | Corrupt the newest snapshot (`truncate -s 100` on the `.dfs`), restart | load fails, the previous generation loads; alert on snapshot load errors; then re-replay the gap |
| 5.3 | Kill a worker with `SIGKILL` mid-write, 50 times | zero messages in `q.event.ingest.dlq`, zero duplicate records |
| 5.4 | Fill the audit queue past `x-max-length-bytes` | worker audit publish fails, ingest nacks and requeues, no silent loss; alarm fires |
| 5.5 | Kill RabbitMQ during a publish burst without confirms (misconfigured client) | messages lost — this proves why confirms are mandatory; record the observation, then fix the client |

## 6. Event ordering and close semantics

These prove the severity model in [event-lifecycle.md](event-lifecycle.md) survives retries and multi-consumer reordering.

| # | Injection | Expected | Proof |
|---|---|---|---|
| 6.1 | Broker timestamps present on every message | every consumed message has `timestamp_in_ms` | `rabbitmqadmin get` shows the header; no message reaches the validator without it |
| 6.2 | Publish CRITICAL, then CLOSE, but make the CLOSE's Dragonfly write fail so it is requeued (`nack(requeue=true)`) | alert stays open until the CLOSE is successfully applied | `HGETALL fw:alert:<key>` watermark advances only on the applied attempt |
| 6.3 | **The headline case:** CRITICAL (t1) → CLOSE (t2) requeued → CRITICAL (t3 > t2) committed → CLOSE redelivered | the redelivered CLOSE returns `applied=0`; the t3 CRITICAL **stays open** | `fw:alert:<key>` severity is still CRITICAL; the Lua script returned `0` |
| 6.4 | Publish two events in the same millisecond, one CRITICAL and one CLOSE, repeatedly | identical final state on every run | tie-break on `(ts, id)`; run 20x, compare `HGETALL` |
| 6.5 | Publish out-of-order at the broker: CLOSE then CRITICAL with CLOSE's `timestamp_in_ms` **lower** | the CLOSE is stale and never retires the CRITICAL | alert state CRITICAL; `applied=0` on the CLOSE |
| 6.6 | Force the same `{source}:{type}` to two different workers concurrently | one fold wins, no split state | exactly one call returns non-zero; final watermark is the max `(ts, id)` |
| 6.7 | Publish the same CRITICAL 100× within 5 min, then 1× after 6 min | `count == 2`, not 101; `first_seen` is the first event, `last_seen` the last | `HMGET fw:alert:<key> count first_seen last_seen` ([event-lifecycle.md](event-lifecycle.md) §5.4) |
| 6.8 | Publish a CRITICAL, then a WARNING for the same key 10s later | `severity` becomes WARNING, `count` unchanged, one row | `applied=2`; the alert does not split into two rows |
| 6.9 | CRITICAL → CLOSE → CRITICAL | new episode: `count` resets to 1, `first_seen` is the third event | `applied` sequence is 1, 3, 1 |
| 6.10 | Run 1 000 evt/min from one permanently-faulted source for 24h | `count` stays bounded (≈288 at a 5-min window) and the alert table has **one** row for that key | `SCAN fw:alert:*` count is `#sources x #types`, not millions |
| 6.11 | Replay the audit queue with records deliberately shuffled | final alert state identical to the pre-wipe state | compare `SCAN fw:alert:*` before and after ([event-lifecycle.md](event-lifecycle.md) §7) |
| 6.12 | Republish a message through a queue move without `x-original-ts` | the worker re-stamps it with "now" and the ordering is wrong — **this must fail a review**, not a test | verify no code path republishes without copying the timestamp |
| 6.13 | Kill a worker between the Dragonfly write and the ack, repeatedly | no duplicate records, no duplicated alert transitions, `count` unchanged | `DBSIZE` stable; watermark unchanged on the redelivery |

### 6a. Payload compression

| # | Injection | Expected | Proof |
|---|---|---|---|
| 6a.1 | Publish an event with a 3 KiB `msgc` and a 90 B `msg` | `msgc` stored gzip (`0x1f 0x8b` prefix), `msg` stored plain | `redis-cli --no-raw GET fw:evt:<id>`, byte-level check ([payload-and-compression.md](payload-and-compression.md) §9) |
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
| 7.2 | `rollout restart statefulset/dragonfly` | OrderedReady order, no two pods down at once; possibly one failover if the master restarts first |
| 7.3 | `rollout restart deploy/ws-gateway` | sockets closed gracefully (`op: shutdown`), clients reconnect with backfill, no errors surfaced to the user |
| 7.4 | Image rollback of any component | returns to previous behaviour; verify no schema/format incompatibility in the `.dfs` (a newer snapshot loaded by an older binary is **not** supported — pin image and record the format version) |

## 8. Security and hygiene

| Check | Expectation |
|---|---|
| `grep -rn "password\|secret\|token" k8s/` | only `Secret` references and `${VAR}` placeholders, no literals |
| `.gitignore` | contains `.env`, `*.pem`, `*.key`, `secrets/`, `kubeconfig*` |
| Pods run as non-root, no `privileged`, no hostPath | `oc adm policy scc-subject-review` clean |
| Route TLS | edge termination, HTTP redirects to HTTPS |
| No NetworkPolicy objects | `get networkpolicy` returns none — they are deliberately omitted in a shared namespace ([networking.md](networking.md) §5) |
| No external port exposure | all Services `ClusterIP`; exactly 2 Routes; no NodePort/LoadBalancer |
| No pod-IP usage in application config | `grep -rn 'dragonfly-[0-9]' <app-config>` returns only control-plane components |

## 9. Acceptance gate

The deployment is acceptable only when:

- [ ] every scenario in §1–§4 passes without manual intervention
- [ ] every check in [fault-tolerance.md](fault-tolerance.md) §7 passes: replica counts, **required** anti-affinity actually spread, PDBs present on all 7 workloads, no `x-delivery-limit`, Sentinel epochs agree and are non-zero, Sentinel can authenticate, `cluster_partition_handling = pause_minority`, `maxmemory-policy = noeviction`, exactly one bridge holds the lease, `dragonfly` has exactly one endpoint
- [ ] §5.1 passes: a PVC wipe is fully recoverable for the complete 7-day window from the audit queue ([persistence-and-durability.md](persistence-and-durability.md) §4.4)
- [ ] every scenario in §6 passes, in particular §6.3: **a redelivered CLOSE never retires a newer CRITICAL/FATAL** ([event-lifecycle.md](event-lifecycle.md) §4–5)
- [ ] every scenario in §6.7–6.10 passes: repeated occurrences coalesce into one alert row with a moving `last_seen` ([event-lifecycle.md](event-lifecycle.md) §5.4)
- [ ] every scenario in §6a passes: `msgc` is compressed at rest, the WAL copy is plain, and a decode failure never blanks an alert ([payload-and-compression.md](payload-and-compression.md))
- [ ] `evicted_keys == 0` in steady state and `DBSIZE` ≈ 10.08M at 1 000/min × 7d ([sizing.md](sizing.md) §6)
- [ ] no event present in the audit queue is missing from Dragonfly after replay
- [ ] the documented gap in §4 (direct-pod-IP write) is recorded in [roadmap.md](roadmap.md)
