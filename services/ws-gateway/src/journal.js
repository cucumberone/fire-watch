/**
 * Operator journal — validation and wire format.
 *
 * An operator attaches a note to an incident. The journal is keyed by the RECORD
 * KEY (`32 hex`), the same key the ack and the close use: with one hash per
 * condition there is no longer a separate "alert" object to attach to. See
 * services/worker/src/keygen.js.
 *
 * The storage is a LIST beside the record (`journal:<key>`), never fields on the
 * record hash. The worker's fold owns that hash and rewrites it atomically, so a
 * second writer HSET-ing notes into it would race the fold.
 *
 * Entries are APPEND-ONLY. A single editable text column was rejected: two
 * operators annotating the same incident would silently clobber each other,
 * last-write-wins, with no record of who said what. That is the same lost-update
 * race this codebase already had to fix for `count` and for the ack, and a journal
 * is a log — the whole value is that earlier entries survive later ones.
 *
 * Separated from the socket plumbing so the rules can be tested without a server,
 * a database or a browser.
 */

export const MAX_JOURNAL_LEN = 1024;
export const MAX_JOURNAL_BY_LEN = 128;
export const MAX_JOURNAL_QUERY_KEYS = 200;
export const MAX_JOURNAL_ENTRIES = 200;
export const DEFAULT_JOURNAL_LIMIT = 50;
export const ANONYMOUS_BY = 'anonymous';

/**
 * Strip control characters and cap the length.
 *
 * Truncation alone is not enough: a newline or a NUL in an operator's note would
 * break the row layout it is rendered into, and a note is the one field in the
 * system that is arbitrary human text. Collapsing runs of whitespace also keeps a
 * pasted terminal dump from becoming a wall of blank lines.
 */
export function cleanNote(value, maxLen = MAX_JOURNAL_LEN) {
  if (typeof value !== 'string') return '';
  const stripped = value
    // eslint-disable-next-line no-control-regex
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, '')
    // Tabs and newlines are allowed but normalised, so a note keeps intentional
    // paragraph breaks without letting a paste smuggle in vertical whitespace.
    .replace(/\r\n?/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
  return stripped.length > maxLen ? stripped.slice(0, maxLen) : stripped;
}

/**
 * Same rule as `isValidKey` in ack.js, kept local so this module stays standalone.
 *
 * STRICT: 32 lowercase hex characters. The legacy `source:type` shape is rejected
 * on purpose — a stale client would otherwise write a note under a key nothing
 * reads, and the note would vanish from the UI while sitting in the database.
 */
export function isValidJournalKey(key) {
  if (typeof key !== 'string') return false;
  const k = key.trim();
  return /^[0-9a-f]{32}$/.test(k);
}

/**
 * Validate an append frame.
 *
 * Returns `{ok, key, text, by, id}` or `{ok:false, code, message}`. The entry id is
 * generated HERE rather than accepted from the client: it is the ordering key and
 * the identity of the entry, so letting a caller choose it lets a caller overwrite
 * or reorder history. Two operators writing at the same millisecond must still get
 * distinct entries.
 *
 * @param {object} frame  the parsed `{"op":"journal"}` frame
 * @param {() => string} [newId]  injected for tests
 */
export function validateJournalAppend(frame, newId) {
  if (!frame || frame.op !== 'journal') {
    return { ok: false, code: 'bad_op', message: 'expected {"op":"journal"}' };
  }
  if (!isValidJournalKey(frame.key)) {
    return {
      ok: false,
      code: 'bad_key',
      message: 'key must be a 32-character lowercase hex record key',
    };
  }
  const text = cleanNote(frame.text);
  if (text.length === 0) {
    return { ok: false, code: 'empty_note', message: 'a journal entry cannot be empty' };
  }
  const by = cleanNote(frame.by, MAX_JOURNAL_BY_LEN) || ANONYMOUS_BY;
  const id = (newId || defaultEntryId)();
  return { ok: true, key: frame.key.trim(), text, by, id };
}

let counter = 0;
function defaultEntryId() {
  counter = (counter + 1) % 1_000_000;
  const rand = Math.random().toString(36).slice(2, 8);
  return `${Date.now().toString(36)}-${counter.toString(36)}-${rand}`;
}

/**
 * Validate a bulk read frame.
 *
 * Bounded exactly like `op: alerts`: one frame must not be able to ask a pod to
 * read an unbounded number of keys (websocket-server.md §4).
 */
export function validateJournalQuery(frame) {
  if (!frame || frame.op !== 'journals') {
    return { ok: false, code: 'bad_op', message: 'expected {"op":"journals"}' };
  }
  const raw = Array.isArray(frame.keys) ? frame.keys : [];
  const keys = [];
  const seen = new Set();
  for (const k of raw) {
    if (!isValidJournalKey(k)) continue;
    const clean = k.trim();
    if (seen.has(clean)) continue;
    seen.add(clean);
    keys.push(clean);
    if (keys.length >= MAX_JOURNAL_QUERY_KEYS) break;
  }
  const limit = Number.isInteger(frame.limit) && frame.limit > 0
    ? Math.min(frame.limit, MAX_JOURNAL_ENTRIES)
    : DEFAULT_JOURNAL_LIMIT;
  return { ok: true, keys, limit };
}

/**
 * The frame announcing a new entry.
 *
 * Carries the whole entry rather than a bare "something changed": a note is small,
 * and a client that only learned "there is new text" would have to make another
 * round trip to show it — which is the reload-to-see-it bug the Closed tab already
 * had once.
 */
export function journalEntryFrame(key, entry) {
  return JSON.stringify({ op: 'journal', key, entry });
}

/** The reply to a bulk read. One array of entries per key, newest first. */
export function journalListFrame(entries) {
  return JSON.stringify({ op: 'journals', journals: entries });
}
