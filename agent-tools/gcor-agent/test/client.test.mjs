import { test } from 'node:test';
import assert from 'node:assert/strict';
import { bech32 } from '@scure/base';
import { parseSecretKey, signHttpAuth, requireChannel, askKnowledge, searchKnowledge, listUnanswered, publicKeyHex, KnowledgeError } from '../src/client.mjs';

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
