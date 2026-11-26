# Validation

Failure scenarios to rehearse before the deployment is considered done. Each scenario states the injection, the expected behaviour, and the check that proves it.

Helper used below:

```bash
vk() { kubectl -n fire-watch exec valkey-0 -c valkey -- sh -c "valkey-cli -a \"\$VALKEY_PASSWORD\" --no-auth-warning $*"; }
# run it against the CURRENT master (SENTINEL get-master-addr-by-name) when a failover has happened
```

## 0. Preconditions

- 3 healthy worker nodes, each able to host every pod (check `allocatable` vs the sums in [sizing.md](sizing.md) §3; Valkey needs a 52Gi limit, and 128 GiB nodes are required to reschedule it after a node loss)
- all components at 3 replicas, all probes green
- a load generator posting a steady, identifiable stream through nginx → http-ingress (1 000 events/min with monotonic ids, matching the sizing baseline), recording every response code
- a storm injector able to sustain 500 evt/s for one hour, to validate headroom ([sizing.md](sizing.md) §2.3)
- a recorded baseline: `ZCARD idx:event:all`, `XLEN fw:stream:ingest`, `XINFO GROUPS fw:stream:ingest`, `XLEN fw:stream:dlq`, AOF file listing on every Valkey pod

The invariant every drill checks: **every event answered 202 ends up folded into a record** — the generator's 202 count equals the sum of arrivals the records account for, and nothing answered 503 is counted until nginx's retry is answered 202.

## 1. Component pod loss

| # | Injection | Expected | Proof |
|---|---|---|---|
| 1.1 | `delete pod valkey-1` (one replica) | **writes continue**: master still sees 1 replica, `XADD` + `WAIT 1` succeed, ingress keeps answering 202; `ValkeyReplicaDown` warns | `INFO replication` shows `connected_slaves:1`; generator sees no 503 |
| 1.2 | `delete pod valkey-1 valkey-2` (both replicas) | master answers `NOREPLICAS`; ingress answers **503** (never 202); nginx buffers; workers leave entries pending; write guard empties the `valkey` endpoint; ingest resumes by itself when a replica returns | generator log: 503s, then 202s; `ValkeyWriteGateClosed` fires; no 202 during the window |
| 1.3 | `delete pod -l app.kubernetes.io/name=worker` (one), mid-batch | its pending entries are taken over by another worker with `XAUTOCLAIM` after ≤ 60 s idle (+ ≤ 30 s claim interval); each processed once into state | `XPENDING fw:stream:ingest workers` drains to 0; `XLEN fw:stream:dlq` stays 0; `count` on the affected records equals the number of 202'd arrivals |
| 1.4 | `delete pod -l app.kubernetes.io/name=http-ingress` (one) | in-flight requests get no 202 and are retried by nginx on another pod | generator: every event eventually 202; no event folded without a 202 |
| 1.5 | Scale workers to 0 for 10 min, then back to 3 | stream backlog grows (`XLEN`), nothing is lost, backlog drains after scale-up; `IngestStreamBacklogGrowing` does **not** fire for 10 min but would at 15 | `XLEN fw:stream:ingest` rises then returns to 0 |
| 1.6 | `delete pod -l app.kubernetes.io/name=ws-gateway` (one) | clients reconnect to another pod, backfill fills the gap | client log shows reconnect + `backfill > 0` |
| 1.7 | `delete pod -l app.kubernetes.io/name=frontend` (one) | page keeps loading | `curl` via Route succeeds throughout |
| 1.8 | Run at 1 000/min for 7 days without intervention | `evicted_keys == 0`, `DBSIZE` ≈ 10.08M at most, `used_memory < 0.9 × maxmemory`, PV < 60Gi used, `XLEN fw:stream:ingest` ≈ 0 | [sizing.md](sizing.md) §6 rules; any eviction is an incident, not a tuning signal |
| 1.9 | Inject a 1-hour 500 evt/s storm, then continue at 1 000/min | no eviction during the storm; `used_memory` rises by ≈5.4 GiB and stays within `maxmemory=40gb`; stream backlog stays small (workers keep up) | `INFO memory` before/during/after; `evicted_keys` still 0; `XLEN` |

## 2. Master failover

Injection: `kubectl delete pod valkey-0` (the master) while the generator runs.

| Step | Expected | Proof |
|---|---|---|
| Sentinel detects down | within 5s (`down-after-milliseconds`) | sentinel log `+sdown`, then `+odown` with quorum 2 |
| New master elected | within ~10s | `SENTINEL get-master-addr-by-name fire-watch-valkey` returns a different pod IP |
| Service follows | within 5s of promotion | `get endpoints valkey` lists the new pod |
| Ingress during the switch | **503 `store_unavailable`**, never a 202 it cannot keep; recovers to 202 once it re-resolves the master | generator log: a burst of 503 then 202; `fw_ingress_unavailable_total` rose |
| Stream continuity | every entry that was 202'd before the kill is on the new master (it waited for a replica) | `XINFO GROUPS fw:stream:ingest` on the new master: group `workers` present, pending entries carried over |
| Workers | writes fail during the switch (`retry`), entries stay pending and are re-read; no DLQ | worker log `write failed, will retry`; `XLEN fw:stream:dlq` 0 |
| Restarted `valkey-0` rejoins as replica | yes, as `REPLICAOF` is applied by the bridge, **not** from a stale manifest arg; it loads its own AOF first | `INFO replication` on the restarted pod shows `role:slave`, `master_link_status:up` |
| Data continuity | no 202'd event lost | generator 202 count vs folded arrivals; `ZCARD idx:event:all` before vs after |
| Write availability | ≤ 15s of 503s / failed writes | generator and worker logs |

Then repeat with `SENTINEL failover fire-watch-valkey` to test the controlled path, and verify the bridge does not flap roles afterwards (no repeated `+switch-master` in 10 min).

## 3. Node loss

Cordon and drain one node while load runs.

| Expected | Proof |
|---|---|
| PDBs block simultaneous eviction (valkey needs 2, sentinel 2, ingress 2, gateway 2, frontend 2) | drain waits rather than evicting a second member at once |
| All **non-Valkey** pods rescheduled onto the remaining 2 nodes | `get pods -o wide` shows new nodes |
| The Valkey pod reschedules onto a survivor (128 GiB nodes hold two 52Gi pods); on smaller nodes it stays **Pending** ([sizing.md](sizing.md) §3.3) | `get pods` shows either a new node assignment or `Pending` with `Insufficient memory` |
| Writes continue while only 2 Valkey pods run | master keeps a replica, `connected_slaves:1`, ingress keeps answering 202 (`min-replicas-to-write 1`) |
| No 202'd event lost | generator 202 count vs folded arrivals |
| A worker on the drained node finishes its batch or its entries are claimed | `XPENDING` returns to 0 |

**Acceptance condition:** with 128 GiB nodes the evicted Valkey pod reschedules and full 3-replica redundancy returns. With 64 GiB nodes it stays `Pending` and the cluster runs at reduced redundancy (2 of 3 Valkey pods): writes continue, but losing one more Valkey pod stops ingest (`NOREPLICAS` → 503) until the node returns ([sizing.md](sizing.md) §3.3).

Repeat with the **hard** variant: `kubectl delete node` (not drain) so the pods vanish without graceful shutdown. Expect the same end state, slower, and a possible full resync of the affected Valkey replica (watch `INFO replication` for `state=full_sync` then `stable_sync`).

## 4. Split brain

Simulate a partition by isolating Valkey pods at the **CNI** layer — the cluster's network plugin, not a NetworkPolicy (which this deployment does not ship, [networking.md](networking.md) §5). Options, in order of preference:

1. `kubectl cordon` + `kubectl drain` the node hosting `valkey-0` and block its kubelet: heavy but faithful.
2. On a CNI with a chaos tool available, apply a temporary `NetworkPolicy` **in a dedicated test namespace only** — never in the shared production namespace.
3. `kubectl exec` into `valkey-1` and `valkey-2` and add an `iptables`/`nft` DROP rule for the master's IP (requires `NET_ADMIN`; not available under the default SCC, so usually only viable in a test cluster).

### 4.1 Master isolated from both replicas

| Expected | Proof |
|---|---|
| **Isolated master refuses writes** | within `min-replicas-max-lag` (10 s) it answers `NOREPLICAS` to every write, even from a client using a direct pod IP ([valkey.md](valkey.md) §5) |
| Ingress answers 503, never 202, for requests that reached the old master | generator log |
| Write guard empties the endpoint | label `writable=false` on the old master |
| Sentinel promotes a majority-side replica | `+switch-master`; new master sees 1 replica and accepts writes |
| Service points at the new master only | `get endpoints valkey` = 1 address, the new master |
| Old master rejoins and is demoted | `INFO replication` on old master shows `role:slave`; nothing to discard (it refused writes) |
| No 202'd event lost | generator 202 count vs folded arrivals |
| **Documented gap** | a client with a direct pod IP can still READ stale data from the minority — accepted, see [fault-tolerance.md](fault-tolerance.md) §4 |

### 4.2 Master keeps one replica (the accepted window)

Isolate `valkey-2` **and two sentinels** from `valkey-0` + `valkey-1`, so the majority sentinels promote `valkey-2` while `valkey-0` still sees `valkey-1`.

| Expected | Proof |
|---|---|
| `valkey-2` promoted | `+switch-master` from the 2 sentinels |
| `valkey-0` still accepts writes from clients that have not re-resolved | direct `XADD` against `valkey-0` succeeds and `WAIT 1` returns 1 |
| Bridge moves `Endpoints/valkey` to `valkey-2`; ingress/workers/gateway follow on reconnect | `get endpoints valkey` |
| On heal, `valkey-0`'s writes from the window are discarded | compare entries written to `valkey-0` during the window with the new master |

This drill measures the window [fault-tolerance.md](fault-tolerance.md) §4.1 accepts; record its observed length. It must be bounded by client re-resolution, not by the partition's duration.

Restore networking afterwards and confirm `connected_slaves:2` on the master.

## 5. Data-loss drills

| # | Injection | Expected |
|---|---|---|
| 5.1 | Make both replicas unavailable (block them as in §4, or cordon their nodes and delete both pods so they stay `Pending`), then POST | every request answers **503** `store_unavailable`; zero 202s; after a replica returns, nginx's retries are answered 202 and folded ([persistence-and-durability.md](persistence-and-durability.md) §7) |
| 5.2 | `SIGKILL` the master immediately after a burst of 202s | every 202'd entry is on the promoted replica (`WAIT 1`); none lost |
| 5.3 | Restart all three Valkey pods at once (power-loss simulation) | each pod loads its own AOF; `XINFO GROUPS fw:stream:ingest` shows the group and its PEL; workers drain the backlog; at most ≤ 1 s per pod of non-fsynced writes missing — never a 202'd event, which was on two pods |
| 5.4 | Kill the AOF rewrite mid-flight (`SIGKILL` the sidecar's `valkey-cli` during `BGREWRITEAOF`) | the old AOF stays in place and is replayed; `aof_last_bgrewrite_status` reports an error; the next rewrite succeeds |
| 5.5 | Corrupt the tail of one pod's AOF (`truncate -s 100`), restart that pod | `aof-load-truncated yes` loads the valid prefix; the pod resyncs from the master |
| 5.6 | Kill a worker with `SIGKILL` mid-write, 50 times | zero entries in `fw:stream:dlq`, zero duplicate records, `count` matches 202'd arrivals |
| 5.7 | POST an entry whose `evt` is valid JSON but fails validation (bypass the ingress with a direct `XADD` in a test cluster) | moved to `fw:stream:dlq` with `origId` and `reason`; removed from the ingest stream; `IngestDeadLetteredEvents` fires |
| 5.8 | Wipe the Valkey PVC on all 3 pods | **data loss** — no copy exists outside Valkey ([persistence-and-durability.md](persistence-and-durability.md) §4). Run only to rehearse the volume-backup restore, if one exists |

## 6. Event ordering and close semantics

These prove the severity model in [event-lifecycle.md](event-lifecycle.md) survives retries and multi-consumer reordering.

| # | Injection | Expected | Proof |
|---|---|---|---|
| 6.1 | Arrival timestamps come from the stream | every record's `lastSeen` / `brokerTs` equals the ms part of an entry ID; producers cannot set it | `XRANGE` during a backlog vs `HGET event:<key> lastSeen` |
| 6.2 | POST CRITICAL, then CLOSE, but make the CLOSE's fold write fail so it stays pending | alert stays open until the CLOSE is successfully applied from the PEL | `HGETALL event:<key>` watermark advances only on the applied attempt |
| 6.3 | **The headline case:** CRITICAL (t1) → CLOSE (t2) left pending → CRITICAL (t3 > t2) committed by another worker → CLOSE re-read from the PEL | the retried CLOSE returns `applied=0`; the t3 CRITICAL **stays open** | `event:<key>` severity is still CRITICAL; the Lua script returned `0` |
| 6.4 | POST two events in the same millisecond, one CRITICAL and one CLOSE, repeatedly | identical final state on every run | tie-break on `(ts, id)`; run 20x, compare `HGETALL` |
| 6.5 | Out of order processing: CLOSE folded after a CRITICAL whose entry ID is **higher** | the CLOSE is stale and never retires the CRITICAL | alert state CRITICAL; `applied=0` on the CLOSE |
| 6.6 | Force the same condition to two different workers concurrently (claim race) | one fold wins, no split state | exactly one call returns non-zero; final watermark is the max `(ts, id)` |
| 6.7 | POST the same device+summary 100× with **100 different event ids** | `count == 100`, ONE record | `ZCARD idx:event:all` grows by exactly 1; `HMGET event:<key> count` is 100 |
| 6.8 | POST a CRITICAL, then a WARNING for the same device+summary 10 s later | `severity` becomes WARNING, `count` 2, one record | same key; the row does not split |
| 6.9 | CRITICAL → CLOSE → CRITICAL | new episode: `count` resets to 1, `firstSeen` is the third event | `applied` sequence is 1, 3, 1 |
| 6.10 | Run 1 000 evt/min from one permanently-faulted source for 24h | ONE row for that key, `count` grows per arrival | `SCAN event:*` count is the number of distinct conditions, not millions |
| 6.11 | POST two CRITICALs that differ only in `EVT_Summary` wording | **two** records | different key — the trade-off this design accepts ([valkey.md](valkey.md) §2.3) |
| 6.12 | Re-append an entry (new `XADD`) instead of leaving it pending | it gets a new, newer timestamp and counts as a new arrival — **this must fail a review**, not a test | verify no code path re-appends; retries use the PEL only |
| 6.13 | Kill a worker between the fold write and the `XACK`+`XDEL`, repeatedly | no duplicate records, no duplicated alert transitions, `count` unchanged by the redelivery; History shows one extra row per redelivery | `DBSIZE` stable; watermark unchanged on the redelivery |
| 6.14 | Make `WAIT` return short after a successful `EXEC` (e.g. pause the replica), let nginx retry | the batch is in the stream twice (two entry IDs); each arrival counted once — `count` +2, still one record | expected behaviour ([event-lifecycle.md](event-lifecycle.md) §7) |

### 6a. Payload compression

| # | Injection | Expected | Proof |
|---|---|---|---|
| 6a.1 | POST an event with a 3 KiB `msgc` and a 90 B `msg` | `msgc` stored as `{"__gz": base64}`, `msg` stored plain string | `valkey-cli --raw HGET event:<key> payload`, field-level check ([payload-and-compression.md](payload-and-compression.md) §4) |
| 6a.2 | POST an event with a 200 B `msgc` | **not** compressed — below `FW_COMPRESS_MIN` | value starts with `{`, `MEMORY USAGE` ≈ plain size |
| 6a.3 | Round-trip: decompress the stored value and compare with the original | byte-identical `msgc` | `gunzip` output diffed against the producer's string |
| 6a.4 | Inspect a stream entry (during a backlog) or a DLQ entry | the `evt` field is **plain** JSON, never compressed | `XRANGE` shows readable JSON |
| 6a.5 | Corrupt one stored `msgc` (flip a byte after the gzip header) | the UI renders `[undecodable payload]` and the alert row **remains visible** with severity and timestamps intact | browser check; no blank row, no console-crashing error |
| 6a.6 | Store a `msgc` that expands beyond `FW_MSG_MAX_BYTES` | decoding truncates at the cap with a marker | output length is capped; no OOM in the browser tab |

## 7. Rolling updates

| # | Injection | Expected |
|---|---|---|
| 7.1 | `rollout restart deploy/worker` | each pod drains its in-flight batch (≤ 20 s); anything left pending is claimed; `XLEN` returns to 0 |
| 7.2 | `rollout restart statefulset/valkey` | OrderedReady order, no two pods down at once; writes continue through a replica restart; possibly one failover (with a short burst of 503s) if the master restarts |
| 7.3 | `rollout restart deploy/ws-gateway` | sockets closed gracefully (`op: shutdown`), clients reconnect with backfill, no errors surfaced to the user |
| 7.4 | `rollout restart deploy/http-ingress` | in-flight requests finish their `XADD`/`WAIT` before exit; nginx retries anything cut off |
| 7.5 | Image rollback of any component | returns to previous behaviour. Note the format boundaries: the AOF format changed across Valkey major versions (a newer AOF loaded by an older binary is **not** supported — pin the image and record the version), and a record written by the new worker has extra hash fields an old worker would ignore |

## 8. Security and hygiene

| Check | Expectation |
|---|---|
| `grep -rn "password\|secret\|token" k8s/` | only `Secret` references and `${VAR}` placeholders, no literals |
| `.gitignore` | contains `.env`, `*.pem`, `*.key`, `secrets/`, `kubeconfig*` |
| Pods run as non-root, no `privileged`, no hostPath | `oc adm policy scc-subject-review` clean |
| Route TLS | edge termination, HTTP redirects to HTTPS |
| No NetworkPolicy objects | `get networkpolicy` returns none — they are deliberately omitted in a shared namespace ([networking.md](networking.md) §5) |
| No external port exposure | all Services `ClusterIP` except the http-ingress NodePort 30900; exactly 2 Routes; no LoadBalancer |
| Only two Secrets | `fire-watch-valkey` (`password`) and `fire-watch-ingress` (`token`) |
| No pod-IP usage in application config | `grep -rn 'valkey-[0-9]' <app-config>` returns only control-plane components |

## 9. Acceptance gate

The deployment is acceptable only when:

- [ ] every scenario in §1–§4.1 passes without manual intervention, and the §4.2 window is measured and recorded
- [ ] every check in [fault-tolerance.md](fault-tolerance.md) §7 passes: replica counts, **required** anti-affinity actually spread, PDBs present on all 7 workloads, the ingest group exists with nothing stuck pending, Sentinel epochs agree and are non-zero, Sentinel can authenticate, `maxmemory-policy = noeviction`, exactly one bridge holds the lease, `valkey` has exactly one endpoint
- [ ] §5.1 passes: the ingress **never** answers 202 while no replica can acknowledge ([http-ingress.md](http-ingress.md) §3)
- [ ] §1.3 and §5.6 pass: a killed worker's entries are claimed and folded exactly once into state
- [ ] every scenario in §6 passes, in particular §6.3: **a retried CLOSE never retires a newer CRITICAL/FATAL** ([event-lifecycle.md](event-lifecycle.md) §4–5)
- [ ] every scenario in §6.7–6.11 passes: a repeat of one CONDITION folds into one
      record with a moving `lastSeen`, and a reworded summary is a new record
      ([valkey.md](valkey.md) §2)
- [ ] every scenario in §6a passes: `msgc` is compressed at rest, the stream entry is plain, and a decode failure never blanks an alert ([payload-and-compression.md](payload-and-compression.md))
- [ ] `evicted_keys == 0` in steady state and `ZCARD idx:event:all` ≤ 10.08M at
      1 000/min × 7d ([sizing.md](sizing.md) §6)
- [ ] `XLEN fw:stream:ingest` returns to ≈ 0 after every drill and `XLEN fw:stream:dlq` is 0
- [ ] `min-replicas-to-write` is `1` in production and `0` in the lab, the bridge's
      `FW_WRITE_GUARD_MIN_REPLICAS` matches it, and `FW_WAIT_REPLICAS` is `1` / `0`
      ([valkey.md](valkey.md) §3.1)
- [ ] the documented residuals (direct-pod-IP READS of a stale master; the §4.2 write
      window; total PVC loss) are recorded in [roadmap.md](roadmap.md)
