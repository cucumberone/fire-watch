import test from 'node:test';
import assert from 'node:assert/strict';
import {
  MAX_JOURNAL_LEN,
  MAX_JOURNAL_ENTRIES,
  MAX_JOURNAL_QUERY_KEYS,
  ANONYMOUS_BY,
  cleanNote,
  isValidJournalKey,
  validateJournalAppend,
  validateJournalQuery,
  journalEntryFrame,
  journalListFrame,
} from './journal.js';

// -------------------------------------------------------------------- cleanNote
test('cleanNote strips control characters that would break the row layout', () => {
  // A note is the only arbitrary human text in the system, so it is the only place
  // a NUL or an escape sequence can enter. Left in, they corrupt whatever renders
  // the journal next.
  assert.equal(cleanNote('a\u0000b\u001fc'), 'abc');
  assert.equal(cleanNote('plain text'), 'plain text');
});

test('cleanNote keeps single newlines but collapses runs of them', () => {
  assert.equal(cleanNote('line one\nline two'), 'line one\nline two');
  assert.equal(cleanNote('a\n\n\n\n\nb'), 'a\n\nb');
  assert.equal(cleanNote('a\r\nb'), 'a\nb', 'CRLF normalised so a pasted Windows log is stable');
});

test('cleanNote trims and caps at the limit', () => {
  assert.equal(cleanNote('   padded   '), 'padded');
  const long = 'x'.repeat(MAX_JOURNAL_LEN + 500);
  assert.equal(cleanNote(long).length, MAX_JOURNAL_LEN);
  assert.equal(cleanNote(123), '', 'a non-string is not coerced into a note of "123"');
  assert.equal(cleanNote(null), '');
});

// ------------------------------------------------------------------------- keys
test('a journal key must be a source:type pair', () => {
  assert.equal(isValidJournalKey('core-sw-07:link.down'), true);
  // Without the separator it cannot address an alert, so an entry written under it
  // would be unreachable from any row.
  assert.equal(isValidJournalKey('core-sw-07'), false);
  assert.equal(isValidJournalKey(''), false);
  assert.equal(isValidJournalKey('   '), false);
  assert.equal(isValidJournalKey(null), false);
  assert.equal(isValidJournalKey('a:b\nc'), false);
});

// ----------------------------------------------------------------------- append
test('validateJournalAppend accepts a well-formed entry and mints the id', () => {
  const v = validateJournalAppend(
    { op: 'journal', key: 'core-sw-07:link.down', text: '  rebooted the SFP  ', by: 'alice' },
    () => 'entry-1',
  );
  assert.equal(v.ok, true);
  assert.equal(v.key, 'core-sw-07:link.down');
  assert.equal(v.text, 'rebooted the SFP');
  assert.equal(v.by, 'alice');
  assert.equal(v.id, 'entry-1');
});

test('validateJournalAppend rejects an empty or whitespace-only note', () => {
  // An empty entry would render as a blank line and push a real note down the list,
  // so it is refused rather than stored.
  for (const text of ['', '   ', '\n\n', '\u0000']) {
    const v = validateJournalAppend({ op: 'journal', key: 'a:b', text }, () => 'x');
    assert.equal(v.ok, false, `"${JSON.stringify(text)}" must be rejected`);
    assert.equal(v.code, 'empty_note');
  }
});

test('validateJournalAppend rejects a bad key and a bad op', () => {
  assert.equal(validateJournalAppend({ op: 'journal', key: 'nocolon', text: 'x' }).code, 'bad_key');
  assert.equal(validateJournalAppend({ op: 'close', key: 'a:b', text: 'x' }).code, 'bad_op');
  assert.equal(validateJournalAppend(null).code, 'bad_op');
});

test('validateJournalAppend defaults an absent author rather than refusing the note', () => {
  // The note is the valuable part; a missing `by` must not lose it.
  const v = validateJournalAppend({ op: 'journal', key: 'a:b', text: 'note' }, () => 'e');
  assert.equal(v.by, ANONYMOUS_BY);
});

test('the entry id is generated server-side, not taken from the client', () => {
  // It is the ordering key and the identity of the entry: a client that could
  // choose it could overwrite history.
  const a = validateJournalAppend({ op: 'journal', key: 'a:b', text: 'one', id: 'fixed' });
  const b = validateJournalAppend({ op: 'journal', key: 'a:b', text: 'two', id: 'fixed' });
  assert.equal(a.ok, true);
  assert.notEqual(a.id, 'fixed');
  assert.notEqual(a.id, b.id, 'two entries must never share an id');
});

test('two entries written in the same millisecond still get distinct ids', () => {
  const ids = new Set();
  for (let i = 0; i < 200; i += 1) ids.add(validateJournalAppend({ op: 'journal', key: 'a:b', text: 'x' }).id);
  assert.equal(ids.size, 200);
});

// ------------------------------------------------------------------------ query
test('validateJournalQuery dedupes, drops invalid keys and caps the count', () => {
  const v = validateJournalQuery({
    op: 'journals',
    keys: ['a:b', 'a:b', 'nocolon', '', null, 'c:d'],
  });
  assert.deepEqual(v.keys, ['a:b', 'c:d']);
  assert.equal(v.ok, true);
});

test('validateJournalQuery bounds how many keys one frame may ask for', () => {
  const keys = Array.from({ length: MAX_JOURNAL_QUERY_KEYS + 50 }, (_, i) => `s${i}:t`);
  const v = validateJournalQuery({ op: 'journals', keys });
  assert.equal(v.keys.length, MAX_JOURNAL_QUERY_KEYS);
});

test('validateJournalQuery caps the per-key limit', () => {
  const v = validateJournalQuery({ op: 'journals', keys: ['a:b'], limit: 100000 });
  assert.equal(v.limit, MAX_JOURNAL_ENTRIES);
  const d = validateJournalQuery({ op: 'journals', keys: ['a:b'] });
  assert.ok(d.limit > 0 && d.limit <= MAX_JOURNAL_ENTRIES);
});

test('validateJournalQuery rejects a wrong op', () => {
  assert.equal(validateJournalQuery({ op: 'journal' }).code, 'bad_op');
});

// ------------------------------------------------------------------------ frames
test('journalEntryFrame carries the whole entry, not just a notice', () => {
  // A frame that only said "there is new text" would force a round trip to show it
  // — the reload-to-see-it bug the Closed tab already had.
  const f = JSON.parse(journalEntryFrame('a:b', { id: 'e1', by: 'alice', at: 1, text: 'hi' }));
  assert.equal(f.op, 'journal');
  assert.equal(f.key, 'a:b');
  assert.deepEqual(f.entry, { id: 'e1', by: 'alice', at: 1, text: 'hi' });
});

test('journalListFrame is a distinct op from the append echo', () => {
  // `journal` = one new entry, `journals` = a bulk read. Collapsing them would make
  // a client unable to tell "append this" from "replace the list".
  const f = JSON.parse(journalListFrame([{ key: 'a:b', entries: [] }]));
  assert.equal(f.op, 'journals');
  assert.deepEqual(f.journals, [{ key: 'a:b', entries: [] }]);
});
