// Signed GCOR knowledge client for Buzz agents.
//
// Every request is a NIP-98 (kind 27235) proof made with the calling agent's own
// Nostr key (BUZZ_PRIVATE_KEY, injected by Buzz's managed-agent runtime). The
// knowledge API derives channel, access level and agent identity from that proof
// and live Buzz membership; nothing the caller sends can widen its scope.
import { schnorr } from '@noble/curves/secp256k1';
import { sha256 } from '@noble/hashes/sha256';
import { bytesToHex, hexToBytes } from '@noble/hashes/utils';
import { bech32 } from '@scure/base';
import { randomUUID } from 'node:crypto';

export const DEFAULT_ORIGIN = 'http://127.0.0.1:5011';
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export class KnowledgeError extends Error {
  constructor(message, status) { super(message); this.status = status; }
}

export function parseSecretKey(value) {
  const raw = (value || '').trim();
  if (!raw) throw new KnowledgeError('BUZZ_PRIVATE_KEY is not set. Run this from a Buzz-managed agent session.');
  let bytes;
  if (raw.startsWith('nsec1')) {
    const { prefix, words } = bech32.decode(raw, 1000);
    if (prefix !== 'nsec') throw new KnowledgeError('BUZZ_PRIVATE_KEY is not an nsec key');
    bytes = Uint8Array.from(bech32.fromWords(words));
  } else if (/^[0-9a-f]{64}$/i.test(raw)) {
    bytes = hexToBytes(raw.toLowerCase());
  } else {
    throw new KnowledgeError('BUZZ_PRIVATE_KEY must be nsec1… or 64 hex characters');
  }
  if (bytes.length !== 32) throw new KnowledgeError('BUZZ_PRIVATE_KEY has the wrong length');
  return bytes;
}

export function publicKeyHex(secret) {
  return bytesToHex(schnorr.getPublicKey(secret));
}

// NIP-01 event id = sha256 of the canonical serialisation; Python verifies with
// json.dumps(..., ensure_ascii=False, separators=(',',':')), which matches
// JSON.stringify for the ASCII-only fields used here.
export function signHttpAuth(secret, { url, method, body, now = Math.floor(Date.now() / 1000), nonce = randomUUID() }) {
  const pubkey = publicKeyHex(secret);
  const payload = bytesToHex(sha256(new TextEncoder().encode(body)));
  const tags = [['u', url], ['method', method], ['payload', payload], ['nonce', nonce]];
  const serialized = JSON.stringify([0, pubkey, now, 27235, tags, '']);
  const id = sha256(new TextEncoder().encode(serialized));
  const sig = schnorr.sign(id, secret);
  const event = { id: bytesToHex(id), pubkey, created_at: now, kind: 27235, tags, content: '', sig: bytesToHex(sig) };
  return 'Nostr ' + Buffer.from(JSON.stringify(event), 'utf8').toString('base64');
}

export function requireChannel(channelId) {
  const value = (channelId || '').trim().replace(/^#/, '');
  if (!UUID_RE.test(value)) {
    throw new KnowledgeError('channel_id must be the Buzz channel UUID shown in your prompt as "Channel: <name> (#<uuid>)"');
  }
  return value.toLowerCase();
}

const STATUS_HINTS = {
  401: 'The knowledge service rejected the signature. Check the host clock and that GCOR_KNOWLEDGE_URL matches the service origin.',
  403: 'Not permitted: this agent must be an active member of the channel and its human sponsor must also be an active member.',
  413: 'Request too large.',
  422: 'The request was rejected as invalid.',
  429: 'Rate limited; retry shortly.',
};

export async function signedPost(path, data, { origin = process.env.GCOR_KNOWLEDGE_URL || DEFAULT_ORIGIN,
  secret = parseSecretKey(process.env.BUZZ_PRIVATE_KEY), fetchImpl = fetch, timeoutMs = 90000 } = {}) {
  const base = new URL(origin);
  const url = new URL(path, base);
  if (url.origin !== base.origin) throw new KnowledgeError('Request must stay on the knowledge origin');
  const body = JSON.stringify(data);
  const authorization = signHttpAuth(secret, { url: url.href, method: 'POST', body });
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let response;
  try {
    response = await fetchImpl(url.href, { method: 'POST', body, signal: controller.signal,
      headers: { 'Content-Type': 'application/json', Authorization: authorization } });
  } catch (error) {
    throw new KnowledgeError(`Knowledge service unreachable at ${base.origin} (${error.name === 'AbortError' ? 'timeout' : error.message})`);
  } finally { clearTimeout(timer); }
  const text = await response.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch { /* non-JSON error page */ }
  if (!response.ok) {
    const detail = json && typeof json.detail === 'string' ? json.detail : '';
    throw new KnowledgeError(`${STATUS_HINTS[response.status] || `Knowledge request failed (${response.status}).`}${detail ? ' ' + detail : ''}`, response.status);
  }
  return json;
}

function clip(text, n) {
  const value = String(text ?? '');
  return value.length > n ? value.slice(0, n - 1) + '…' : value;
}

export async function askKnowledge({ channelId, question, maxCitations = 4 }, options) {
  const channel = requireChannel(channelId);
  if (!question || !question.trim()) throw new KnowledgeError('question is required');
  const result = await signedPost('/api/ask', { channel_id: channel, query: question.trim(),
    max_citations: maxCitations, approved_only: true }, options);
  const citations = (result?.citations || []).map((c, i) => ({ ref: i + 1, title: c.title,
    document_id: c.document_id, source: c.source_uri || null, state: c.lifecycle_state || null }));
  return { answer: result?.answer ?? '', grounded: citations.length > 0, citations };
}

export async function searchKnowledge({ channelId, query, topK = 6 }, options) {
  const channel = requireChannel(channelId);
  if (!query || !query.trim()) throw new KnowledgeError('query is required');
  const result = await signedPost('/api/retrieve', { channel_id: channel, query: query.trim(),
    top_k: Math.max(1, Math.min(20, Number(topK) || 6)), hops: 1 }, options);
  const chunks = (result?.chunks || []).map(c => ({ title: c.title || c.document_title,
    document_id: c.document_id, score: c.score, excerpt: clip(c.content, 600) }));
  const related = (result?.graph_nodes || []).slice(0, 10).map(n => ({ type: n.node_type,
    label: n.label, excerpt: clip(n.content, 300) }));
  return { results: chunks, related };
}

export async function listUnanswered({ channelId, limit = 20 }, options) {
  const channel = requireChannel(channelId);
  const rows = await signedPost('/api/workspace/unanswered', { channel_id: channel, status: 'open',
    limit: Math.max(1, Math.min(100, Number(limit) || 20)) }, options);
  return { questions: (Array.isArray(rows) ? rows : []).map(q => ({ id: q.id, question: q.question,
    asked: q.asked_count, people: q.distinct_askers, last_asked: q.last_asked_at })) };
}

// How facts about a subject changed over time, from approved knowledge in this channel.
// view: 'timeline' (every value with its validity window) or 'true_at' (only what was true at asOf).
export async function factHistory({ channelId, subject, predicate, view = 'timeline', asOf, knownAt, limit = 50 }, options) {
  const channel = requireChannel(channelId);
  if (!subject || !subject.trim()) throw new KnowledgeError('subject is required');
  if (view !== 'timeline' && view !== 'true_at') throw new KnowledgeError('view must be timeline or true_at');
  const result = await signedPost('/api/facts/history', { channel_id: channel, subject: subject.trim(),
    predicate: predicate || null, view, as_of: asOf || null, known_at: knownAt || null,
    limit: Math.max(1, Math.min(200, Number(limit) || 50)) }, options);
  const facts = (result?.facts || []).map(f => ({ subject: f.subject, predicate: f.predicate, value: f.object,
    fact: f.fact, from: f.valid_at, until: f.invalid_at, status: f.status, true_now: Boolean(f.is_true),
    superseded_by: f.superseded_by ? f.superseded_by.object : null,
    source: f.source ? { title: f.source.title, document_id: f.source.document_id } : null }));
  return { subject: result?.subject ?? subject, as_of: result?.as_of ?? null, known_at: result?.known_at ?? null, facts };
}
