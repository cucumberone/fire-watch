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

/**
 * Kubernetes MicroTime, NOT RFC3339-with-millis.
 *
 * Lease spec.acquireTime / spec.renewTime are MicroTime, whose required format
 * is 2006-01-02T15:04:05.000000Z07:00 — EXACTLY six fractional digits. Plain
 * Date#toISOString() emits three (.123Z), and the API server rejects it:
 *
 *   400 BadRequest: Lease in version "v1" cannot be handled as a Lease:
 *   parsing time "...58.666Z" as "2006-01-02T15:04:05.000000Z07:00":
 *   cannot parse ".666Z" as ".000000"
 *
 * That 400 made every lease write fail, so leadership was never acquired, and
 * because the failure was logged as a lost create race it was invisible. JS has
 * only millisecond resolution, so pad with three zeros rather than pretending
 * to finer precision.
 */
function microTime(date = new Date()) {
  return date.toISOString().replace(/\.(\d{3})Z$/, '.$1000Z');
}

/** 409 means another replica really did create it first. */
function isConflict(err) {
  return err?.response?.statusCode === 409 || err?.statusCode === 409;
}

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
      // call then throws "Required parameter namespace was null or undefined".
      //
      // 0.22.x ALSO wraps the result: the promise resolves to
      //   { response: IncomingMessage, body: V1Lease }
      // Returning the envelope instead of `body` leaves `lease.spec` undefined,
      // which reads as "someone else holds it and it is expired", so every cycle
      // took the takeover path and replaced the Lease with an object that had no
      // metadata. The API server answered:
      //   400 the name of the object (… based on URL) was undeterminable:
      //       name must be provided
      // Accept either shape so a client upgrade cannot silently break this.
      const res = await this.api.readNamespacedLease(
        this.leaseName,
        this.namespace,
      );
      return res?.body ?? res ?? null;
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
    const now = microTime();
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
        this.isLeader = false;
        // Only a 409 is an actual race. Anything else (400 on a malformed
        // MicroTime, 403, network) is a real fault and must not be swallowed at
        // debug level — doing that is exactly how the timestamp bug hid.
        if (isConflict(err)) {
          this.log.debug('lease create race lost', { err: String(err) });
          return false;
        }
        this.log.error('lease create failed', {
          err: String(err),
          status: err?.response?.statusCode,
          body: err?.body,
        });
        throw err;
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
        this.isLeader = false;
        if (isConflict(err)) {
          this.log.debug('lease takeover race lost', { err: String(err) });
          return false;
        }
        this.log.error('lease takeover failed', {
          err: String(err),
          status: err?.response?.statusCode,
          body: err?.body,
        });
        throw err;
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
        lease.spec.renewTime = microTime(new Date(Date.now() - this.leaseDurationMs * 2));
        await this.api.replaceNamespacedLease(this.leaseName, this.namespace, lease);
        this.log.info('lease released');
      }
    } catch (err) {
      this.log.debug('could not release lease', { err: String(err) });
    }
    this.isLeader = false;
  }
}
