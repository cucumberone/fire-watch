/**
 * Leader election via coordination.k8s.io/v1 Lease.
 *
 * Only the holder of the lease acts. A standby that cannot renew becomes the
 * leader within one lease duration. This is deliberately simple: acquire,
 * renew, and step down on failure. No leader-identity handoff, because a
 * reconcile cycle is idempotent and can be repeated by anyone.
 */
import k8s from '@kubernetes/client-node';

const { CoordinationV1Api } = k8s;

export class LeaderElector {
  constructor({ kc, namespace, leaseName, identity, leaseDurationMs, log }) {
    this.api = kc.makeApiClient(CoordinationV1Api);
    this.namespace = namespace;
    this.leaseName = leaseName;
    this.identity = identity;
    this.leaseDurationMs = leaseDurationMs;
    this.log = log;
    this.isLeader = false;
  }

  async _get() {
    try {
      // POSITIONAL signature in @kubernetes/client-node 0.22.x:
      //   readNamespacedLease(name, namespace, pretty?, options?)
      // The v1.x object form leaves `namespace` undefined here, and every lease
      // call then throws "Required parameter namespace was null or undefined" —
      // which surfaced as a permanent 503 on /readyz because ready is only set
      // after a successful reconcile.
      const res = await this.api.readNamespacedLease(
        this.leaseName,
        this.namespace,
      );
      return res;
    } catch (err) {
      if (err?.response?.statusCode === 404 || err?.statusCode === 404) return null;
      throw err;
    }
  }

  _expired(lease) {
    const holder = lease?.spec?.holderIdentity;
    const renew = lease?.spec?.renewTime;
    if (!holder) return true;
    if (!renew) return true;
    const age = Date.now() - new Date(renew).getTime();
    return age > this.leaseDurationMs;
  }

  /** Try to acquire or renew. Returns true when this replica holds the lease. */
  async tryAcquireOrRenew() {
    const now = new Date().toISOString();
    let lease = await this._get();

    if (!lease) {
      const body = {
        apiVersion: 'coordination.k8s.io/v1',
        kind: 'Lease',
        metadata: { name: this.leaseName, namespace: this.namespace },
        spec: {
          holderIdentity: this.identity,
          leaseDurationSeconds: Math.ceil(this.leaseDurationMs / 1000),
          acquireTime: now,
          renewTime: now,
          leaseTransitions: 0,
        },
      };
      try {
        await this.api.createNamespacedLease(this.namespace, body);
        this.isLeader = true;
        this.log.info('lease acquired (created)', { lease: this.leaseName });
        return true;
      } catch (err) {
        // Lost the create race to the other replica.
        this.log.debug('lease create race lost', { err: String(err) });
        this.isLeader = false;
        return false;
      }
    }

    const holder = lease.spec?.holderIdentity;
    if (holder === this.identity) {
      lease.spec.renewTime = now;
      await this.api.replaceNamespacedLease(this.leaseName, this.namespace, lease);
      if (!this.isLeader) this.log.info('lease renewed (still leader)', {});
      this.isLeader = true;
      return true;
    }

    if (this._expired(lease)) {
      lease.spec = {
        ...lease.spec,
        holderIdentity: this.identity,
        renewTime: now,
        acquireTime: now,
        leaseTransitions: (lease.spec?.leaseTransitions || 0) + 1,
      };
      try {
        await this.api.replaceNamespacedLease(this.leaseName, this.namespace, lease);
        this.isLeader = true;
        this.log.warn('lease taken over (previous holder expired)', { previous: holder });
        return true;
      } catch (err) {
        this.log.debug('lease takeover race lost', { err: String(err) });
        this.isLeader = false;
        return false;
      }
    }

    if (this.isLeader) {
      this.log.warn('leadership lost', { holder });
    }
    this.isLeader = false;
    return false;
  }

  /**
   * Release the lease on shutdown so the standby takes over immediately
   * instead of waiting for the lease to expire.
   */
  async release() {
    if (!this.isLeader) return;
    try {
      const lease = await this._get();
      if (lease?.spec?.holderIdentity === this.identity) {
        lease.spec.renewTime = new Date(Date.now() - this.leaseDurationMs * 2).toISOString();
        await this.api.replaceNamespacedLease(this.leaseName, this.namespace, lease);
        this.log.info('lease released');
      }
    } catch (err) {
      this.log.debug('could not release lease', { err: String(err) });
    }
    this.isLeader = false;
  }
}
