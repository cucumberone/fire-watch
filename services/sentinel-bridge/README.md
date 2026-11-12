# sentinel-bridge

The control plane Valkey does not have.

Valkey gives you a primary-replica data plane and nothing else: it will not
detect that the master died, will not promote a replica, and will not tell
clients where the new master is. Redis Sentinel decides *who the master should
be*. This service makes Kubernetes agree with that decision.

Specified by [docs/redis-sentinel.md](../../docs/redis-sentinel.md) §4 and
[docs/valkey.md](../../docs/valkey.md) §3.

## What it does

Every `FW_RECONCILE_INTERVAL` (default 5s), if it holds the leader lease:

1. Asks Sentinel for the master address (`SENTINEL get-master-addr-by-name`).
2. Reads every Valkey pod's actual `ROLE`.
3. Computes a plan (pure function, [`src/plan.js`](src/plan.js)).
4. Applies it:
   - `REPLICAOF <master>` on any pod that is a master-but-not-the-master, or a replica of the wrong master.
   - `REPLICAOF NO ONE` if the designated master still thinks it is a replica.
   - Rewrites `Endpoints/valkey` so the Service points at exactly the master — or at **nothing** when the master is not safely writable.

## Three rules that matter

**Sentinel decides who the master is. Pods report what they are.**
Disagreements resolve in Sentinel's favour. That is what a quorum is for.

**Write access is published through the Service, never asked of Valkey.**
When the designated master has fewer than `FW_WRITE_GUARD_MIN_REPLICAS` connected
replicas, the endpoint list is emptied. Workers then fail loudly instead of
writing to a minority master. A degraded database cannot be trusted to report its
own degradation — and it cannot be asked to, because the state would live in the
database being judged.

**When in doubt, do nothing.**
No Sentinel answer, an unmappable master address, or an unreachable pod all
produce "leave the endpoints untouched" rather than a guess. A wrong endpoint is
a split brain; a stale endpoint is a stalled write that an operator can see.

## No state between cycles

Any replica can take over mid-flight. The leader lease (`coordination.k8s.io/v1`)
means only one acts at a time, but nothing in the reconcile logic depends on
having run before. This is why a replica-count check lives here rather than in a
per-pod sidecar: the bridge already reads `ROLE` on every pod, so the count is
free.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `FW_NAMESPACE` | *required* | namespace to reconcile |
| `SENTINELS` | *required* | `host:port,host:port,host:port` |
| `FW_VALKEY_MASTER` | *required* | Sentinel master name, e.g. `fire-watch-valkey` |
| `FW_VALKEY_SERVICE` | *required* | Service whose Endpoints it owns |
| `FW_VALKEY_STATEFULSET` | `valkey` | pod label selector value |
| `FW_VALKEY_PORT` | `6379` | data port |
| `VALKEY_PASSWORD` | *required* | from Secret |
| `FW_WRITE_GUARD_MIN_REPLICAS` | `2` | below this, withhold the endpoint |
| `FW_WRITABLE_LABEL` | `fire-watch.io/writable` | label set on each pod |
| `FW_RECONCILE_INTERVAL` | `5s` | loop period |
| `FW_LEASE_NAME` | `sentinel-bridge-leader` | leader election lease |
| `FW_LEASE_DURATION` | `15s` | failover window for the leader |
| `FW_HEALTH_PORT` | `8081` | `/livez`, `/readyz` |
| `FW_ENABLE_FAILOVER_REQUEST` | `false` | see below |
| `FW_FAILOVER_COOLDOWN` | `300s` | min gap between failover requests |
| `FW_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

### `FW_ENABLE_FAILOVER_REQUEST` is off by default

Sentinel is the component that decides a master is gone — it has a quorum and a
config epoch. If the bridge also requests failovers because *its own* probe
failed, a network problem on one bridge pod could start a promotion that Sentinel
would not have ordered. Leave it off unless you specifically want a second
detector.

## Health

| Endpoint | Meaning |
|---|---|
| `/livez` | the process is running |
| `/readyz` | at least one reconcile cycle completed |

A **standby is ready**: waiting for the lease is its job. Readiness does not mean
"leader" — gating readiness on leadership would take the standby out of the
Service for no reason.

## Tests

```bash
node --test src/plan.test.js
```

The tests cover the planner only, because the planner is where the correctness
lives. Cluster I/O is thin and deliberately dumb.

Cases covered:

- steady state → no actions
- no Sentinel answer → endpoints untouched
- promoted pod still reporting `slave` → endpoint withheld
- write guard short of replicas → endpoint withheld
- split brain, stray master → demoted to Sentinel's choice
- designated master that is a replica → `REPLICAOF NO ONE`
- `minReplicas: 0` (lab) → guard disabled
- unready master → not published
- unmappable master address → untouched

## Building

```bash
docker build -t sentinel-bridge:dev .
# or into the cluster's registry
oc -n fire-watch new-build --name=sentinel-bridge --binary --strategy=docker
oc -n fire-watch start-build sentinel-bridge --from-dir=. --follow
```

Pin a digest in `k8s/base/32-sentinel-bridge.yaml` before a real deploy.

## Known limits

- **Two replicas, one leader.** The standby does no useful work while waiting. It exists so a pod loss does not stop master discovery, not to double throughput.
- **`REPLICAOF` is not transactional.** A crash between demoting a stray master and promoting the designated one leaves both as replicas for one cycle. The next cycle fixes it; no writes are possible in the meantime because the endpoint is empty.
- **Pod IPs are used to read `ROLE`.** That is correct for a control-plane component — the ban on pod IPs ([docs/redis-sentinel.md](../../docs/redis-sentinel.md) §5) applies to workers and the gateway.
