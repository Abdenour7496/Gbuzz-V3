import { test } from 'node:test';
import assert from 'node:assert/strict';
import { bech32 } from '@scure/base';
import { parseSecretKey, signHttpAuth, requireChannel, askKnowledge, searchKnowledge, listUnanswered, factHistory, publicKeyHex, KnowledgeError } from '../src/client.mjs';

const HEX = '7f'.repeat(32);
const CHANNEL = '11111111-1111-4111-8111-111111111111';

test('accepts hex and nsec keys and rejects garbage', () => {
  const nsec = bech32.encode('nsec', bech32.toWords(Buffer.from(HEX, 'hex')));
  assert.deepEqual(parseSecretKey(nsec), parseSecretKey(HEX));
  assert.throws(() => parseSecretKey(''), KnowledgeError);
  assert.throws(() => parseSecretKey('npub1xyz'), KnowledgeError);
});

test('NIP-98 proof binds url, method and body hash', () => {
  const token = signHttpAuth(parseSecretKey(HEX), { url: 'http://h/api/ask', method: 'POST', body: '{"a":1}', now: 1, nonce: 'n' });
  const event = JSON.parse(Buffer.from(token.slice(6), 'base64').toString());
  assert.equal(event.kind, 27235);
  assert.equal(event.content, '');
  assert.deepEqual(event.tags.slice(0, 2), [['u', 'http://h/api/ask'], ['method', 'POST']]);
  assert.equal(event.pubkey, publicKeyHex(parseSecretKey(HEX)));
});

test('channel must be a UUID', () => {
  assert.equal(requireChannel('#' + CHANNEL.toUpperCase()), CHANNEL);
  assert.throws(() => requireChannel('Kowledge Brain'), /channel UUID/);
});

test('ask maps citations and reports grounding', async () => {
  let seen;
  const fetchImpl = async (url, init) => { seen = { url, init };
    return new Response(JSON.stringify({ answer: 'A', citations: [{ title: 'T', document_id: 'd', source_uri: 'buzz://x', lifecycle_state: 'approved' }] })); };
  const r = await askKnowledge({ channelId: CHANNEL, question: 'q' }, { secret: parseSecretKey(HEX), fetchImpl, origin: 'http://k:5011' });
  assert.equal(seen.url, 'http://k:5011/api/ask');
  assert.equal(JSON.parse(seen.init.body).channel_id, CHANNEL);
  assert.match(seen.init.headers.Authorization, /^Nostr /);
  assert.equal(r.grounded, true);
  assert.equal(r.citations[0].state, 'approved');
});

test('authorization failures become actionable messages', async () => {
  const fetchImpl = async () => new Response(JSON.stringify({ detail: 'Agent requires active human sponsor in channel' }), { status: 403 });
  await assert.rejects(searchKnowledge({ channelId: CHANNEL, query: 'q' }, { secret: parseSecretKey(HEX), fetchImpl }),
    e => e.status === 403 && /sponsor/.test(e.message));
});

test('unreachable service is reported, not thrown raw', async () => {
  const fetchImpl = async () => { throw new TypeError('fetch failed'); };
  await assert.rejects(askKnowledge({ channelId: CHANNEL, question: 'q' }, { secret: parseSecretKey(HEX), fetchImpl }), /unreachable/);
});

test('unanswered lists open questions without asker identities', async () => {
  let seen;
  const fetchImpl = async (url, init) => { seen = { url, body: JSON.parse(init.body) };
    return new Response(JSON.stringify([{ id: 'q1', question: 'Who owns X?', asked_count: 3, distinct_askers: 2, last_asked_at: 't', askers: ['secret'] }])); };
  const r = await listUnanswered({ channelId: CHANNEL, limit: 500 }, { secret: parseSecretKey(HEX), fetchImpl, origin: 'http://k:5011' });
  assert.equal(seen.url, 'http://k:5011/api/workspace/unanswered');
  assert.deepEqual(seen.body, { channel_id: CHANNEL, status: 'open', limit: 100 });
  assert.deepEqual(r.questions, [{ id: 'q1', question: 'Who owns X?', asked: 3, people: 2, last_asked: 't' }]);
});

test('fact history posts a scoped request and summarises the timeline', async () => {
  let seen;
  const fetchImpl = async (url, init) => { seen = { url, body: JSON.parse(init.body) };
    return new Response(JSON.stringify({ subject: 'Release process', as_of: 'now', known_at: null, facts: [
      { subject: 'Release process', predicate: 'OWNED_BY', object: 'Alice', fact: 'Alice owns it', valid_at: '2026-03-01T00:00:00+00:00',
        invalid_at: '2026-10-01T00:00:00+00:00', status: 'superseded', is_true: false, superseded_by: { object: 'Bob' },
        source: { title: 'Handbook', document_id: 'd1' }, evidence_node_ids: ['n'] }] })); };
  const r = await factHistory({ channelId: CHANNEL, subject: ' Release process ', view: 'true_at', asOf: '2026-05-01', limit: 999 },
    { secret: parseSecretKey(HEX), fetchImpl, origin: 'http://k:5011' });
  assert.equal(seen.url, 'http://k:5011/api/facts/history');
  assert.deepEqual(seen.body, { channel_id: CHANNEL, subject: 'Release process', predicate: null, view: 'true_at',
    as_of: '2026-05-01', known_at: null, limit: 200 });
  assert.deepEqual(r.facts[0], { subject: 'Release process', predicate: 'OWNED_BY', value: 'Alice', fact: 'Alice owns it',
    from: '2026-03-01T00:00:00+00:00', until: '2026-10-01T00:00:00+00:00', status: 'superseded', true_now: false,
    superseded_by: 'Bob', source: { title: 'Handbook', document_id: 'd1' } });
  await assert.rejects(factHistory({ channelId: CHANNEL, subject: 'x', view: 'all' }, { secret: parseSecretKey(HEX), fetchImpl }), /view/);
});
