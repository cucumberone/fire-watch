/**
 * Event key generation — `event:<32 hex>` from EVT_Device + EVT_Summary.
 *
 * This is the identity of a CONDITION, and it is what dedup, the indexes and the
 * wire `key` field all use. Two arrivals with the same device and the same summary
 * are the same condition: the second one moves `lastSeen`, increments `count`, and
 * does not create a record (docs/event-lifecycle.md §5.4).
 *
 * WHY THIS LIVES IN CODE AND NOT IN THE LUA SCRIPT
 *
 *   1. Cluster safety. Valkey rejects a script that touches a key it was not given
 *      in KEYS. A key minted inside Lua cannot be declared, so the script could
 *      never access it.
 *   2. Reproducibility. The frontend, the verification scripts and any replay job
 *      must derive the same key from the same event.
 *   3. It is pure: a hash of two input fields, nothing to make atomic.
 *
 * See k8s/base/20-event-fold-config.yaml for the other half of that argument.
 */
import { createHash } from 'node:crypto';

/** Separator that cannot occur in a normalised device or summary. */
const SEP = '\u001f';

/** Key prefix. The record itself is a hash at `event:<hash>`. */
export const EVENT_PREFIX = 'event:';

/** 32 hex characters: the only shape a bare record key may have. */
const BARE_KEY_RE = /^[0-9a-f]{32}$/;

/**
 * Normalise one identity component.
 *
 * Whitespace is collapsed and the ends trimmed, because an upstream tool that pads
 * its summary differently on every report would otherwise mint a new key each time
 * — the exact duplicate storm this key exists to collapse. Case is PRESERVED:
 * "Disk Full" and "disk full" are treated as different summaries, because silently
 * merging them is a stronger claim than the data supports.
 */
function component(value) {
  return String(value ?? '').replace(/\s+/g, ' ').trim();
}

/**
 * The summary of an event.
 *
 * `payload.msg` is where the ingress puts `EVT_Summary` (services/http-ingress/src/
 * normalise.js). The `EVT_Summary` fallback covers a producer that posted the
 * upstream schema straight to a consumer that did not re-map it, and `summary` the
 * canonical-envelope spelling. Without the fallbacks an event with a summary in a
 * different field would collide with every other such event under the empty
 * string — one key for the whole fleet.
 */
export function summaryOf(evt) {
  const p = evt?.payload ?? {};
  return component(p.msg ?? p.EVT_Summary ?? p.summary ?? '');
}

/** The device of an event: `source` in the canonical envelope. */
export function deviceOf(evt) {
  return component(evt?.source ?? evt?.payload?.EVT_Device ?? '');
}

/**
 * `event:<sha256(device + 0x1f + summary)[0:32]>`.
 *
 * 32 hex chars = 128 bits. For 10 million live conditions the birthday collision
 * probability is about 3e-24, and a collision would merge two conditions into one
 * row — visible and correctable, never a crash.
 *
 * The key is OPAQUE on purpose: `event:<device>:<summary>` would be ~520 bytes at
 * the documented limits, would need escaping for `:` and whitespace, and every
 * colon-splitting helper in the codebase would misparse it. Device and summary are
 * stored as hash FIELDS, so nothing is lost for display.
 */
/**
 * The record key for an event.
 *
 * TWO SOURCES, in this order:
 *
 *   1. `evt.key` — the ingress computed it from the identifier keys an operator
 *      selected (services/http-ingress/src/schema.js `identifierHash`). It has
 *      already been validated as 32 hex before it reached the stream, and the
 *      gateway needs to derive the same key for its own writes, so it is carried on
 *      the envelope rather than recomputed here.
 *
 *   2. `device + 0x1f + summary` — the CONDITION key. The fallback, and still the
 *      normal case: an install that never configures identifier keys produces
 *      events with no `key` at all.
 *
 * WHY THE INGRESS COMPUTES IT AND NOT THIS FUNCTION
 *
 *   The hash has to be in the stream entry. The gateway derives it from the record
 *   it is closing (or an operator delete), the verification scripts read it back,
 *   and a replay job must produce the same key from the same payload — all three
 *   need it BEFORE a worker has seen anything. Computing it at the producer is the
 *   only place that satisfies all of them with one formula.
 *
 *   The shape is re-validated here rather than trusted, because a bad key on the
 *   envelope would mint a record nobody can address: `event:` + 32 hex is the whole
 *   contract every index member and every client already relies on.
 */
export function eventKeyFor(evt) {
  const fromEnvelope = evt?.key;
  if (typeof fromEnvelope === 'string' && BARE_KEY_RE.test(fromEnvelope)) {
    return `${EVENT_PREFIX}${fromEnvelope}`;
  }
  const canonical = `${deviceOf(evt)}${SEP}${summaryOf(evt)}`;
  const hash = createHash('sha256').update(canonical).digest('hex').slice(0, 32);
  return `${EVENT_PREFIX}${hash}`;
}

/** The bare key (no prefix) — what the zsets store as a member. */
export function bareKeyFor(evt) {
  return eventKeyFor(evt).slice(EVENT_PREFIX.length);
}

/**
 * Validate a key that arrived from a client.
 *
 * The gateway accepts keys from browsers (acks, closes, journal notes), so the
 * shape is checked rather than trusted: a 32-hex payload after the prefix, nothing
 * else. Legacy `source:type` keys are REJECTED rather than tolerated — a stale
 * browser sending the old shape would otherwise write an ack onto a key that no
 * fold ever created.
 */
export function isValidEventKey(key) {
  if (typeof key !== 'string') return false;
  if (!key.startsWith(EVENT_PREFIX)) return false;
  const bare = key.slice(EVENT_PREFIX.length);
  return BARE_KEY_RE.test(bare);
}
