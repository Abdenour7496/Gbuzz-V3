#!/usr/bin/env node
// gcor-agent: the knowledge brain for Buzz agents.
//   gcor-agent ask    --channel <uuid> "question"     grounded answer from approved knowledge
//   gcor-agent search --channel <uuid> "query"        ranked approved excerpts + related graph nodes
//   gcor-agent unanswered --channel <uuid>             open questions the knowledge base cannot answer yet
//   gcor-agent whoami                                  show the agent identity this process signs with
//   gcor-agent mcp                                     run as a stdio MCP server
import { askKnowledge, searchKnowledge, listUnanswered, parseSecretKey, publicKeyHex, KnowledgeError, DEFAULT_ORIGIN } from './client.mjs';

const VERSION = '1.0.0';
const HELP = `gcor-agent ${VERSION} — signed access to the GCOR knowledge brain

Usage:
  gcor-agent ask    --channel <uuid> "<question>"
  gcor-agent search --channel <uuid> [--top-k N] "<query>"
  gcor-agent unanswered --channel <uuid>
  gcor-agent whoami
  gcor-agent mcp

Signs every request with BUZZ_PRIVATE_KEY (the agent's own Buzz key).
Service: GCOR_KNOWLEDGE_URL (default ${DEFAULT_ORIGIN}).
Contribute new knowledge in Buzz by replying to the supporting messages with:
  !knowledge propose <Title> | <finding or decision>
or turn a whole thread into a reviewed proposal by replying inside it with:
  !knowledge synthesize thread`;

function parseArgs(argv) {
  const out = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--channel' || a === '-c') out.channel = argv[++i];
    else if (a === '--top-k') out.topK = Number(argv[++i]);
    else if (a === '--json') out.json = true;
    else if (a === '-h' || a === '--help') out.help = true;
    else out._.push(a);
  }
  return out;
}

function renderAsk(r) {
  if (!r.grounded) return `No approved knowledge covers this yet.\n${r.answer ? '\n' + r.answer + '\n' : ''}\nInvestigate, then propose what you learn with: !knowledge propose <Title> | <finding>`;
  return `${r.answer}\n\nSources:\n` + r.citations.map(c => `[${c.ref}] ${c.title} (${c.document_id})${c.source ? ' ' + c.source : ''}`).join('\n');
}

function renderSearch(r) {
  if (!r.results.length) return 'No approved knowledge matched.';
  const lines = r.results.map((c, i) => `${i + 1}. ${c.title} (${c.document_id}) score=${Number(c.score ?? 0).toFixed(3)}\n   ${c.excerpt.replace(/\s+/g, ' ')}`);
  if (r.related.length) lines.push('\nRelated:', ...r.related.map(n => `- [${n.type}] ${n.label}`));
  return lines.join('\n');
}

function renderUnanswered(r) {
  if (!r.questions.length) return 'No open unanswered questions in this channel.';
  return r.questions.map((q, i) => `${i + 1}. ${q.question}  (asked ${q.asked}x by ${q.people})`).join('\n');
}

async function runMcp() {
  const { McpServer } = await import('@modelcontextprotocol/sdk/server/mcp.js');
  const { StdioServerTransport } = await import('@modelcontextprotocol/sdk/server/stdio.js');
  const { z } = await import('zod');
  // Fail fast with a clear message rather than on the first tool call.
  parseSecretKey(process.env.BUZZ_PRIVATE_KEY);
  const server = new McpServer({ name: 'gcor-knowledge', version: VERSION });
  const channel = z.string().describe('Buzz channel UUID from your prompt ("Channel: <name> (#<uuid>)")');
  const wrap = fn => async args => {
    try { return { content: [{ type: 'text', text: JSON.stringify(await fn(args), null, 2) }] }; }
    catch (e) { return { isError: true, content: [{ type: 'text', text: e instanceof KnowledgeError ? e.message : String(e) }] }; }
  };
  server.tool('ask_knowledge',
    'Ask the team knowledge brain. Call this BEFORE investigating a question: it answers from human-approved knowledge for this channel, with citations. If grounded is false, nothing approved covers it yet.',
    { channel_id: channel, question: z.string().min(1).max(4000) },
    wrap(a => askKnowledge({ channelId: a.channel_id, question: a.question })));
  server.tool('search_knowledge',
    'Search approved knowledge for this channel and return ranked excerpts plus related graph concepts. Use for exploration when ask_knowledge is too narrow.',
    { channel_id: channel, query: z.string().min(1).max(4000), top_k: z.number().int().min(1).max(20).optional() },
    wrap(a => searchKnowledge({ channelId: a.channel_id, query: a.query, topK: a.top_k })));
  server.tool('unanswered_questions',
    'List open questions people and agents in this channel asked that approved knowledge could not answer. Pick ones you can research, then propose the finding in Buzz with "!knowledge propose <Title> | <finding>" replying to the supporting messages, so a human can review it.',
    { channel_id: channel, limit: z.number().int().min(1).max(100).optional() },
    wrap(a => listUnanswered({ channelId: a.channel_id, limit: a.limit })));
  await server.connect(new StdioServerTransport());
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const [command, ...rest] = args._;
  if (!command || args.help) { console.log(HELP); return; }
  if (command === 'mcp') return runMcp();
  if (command === 'whoami') {
    console.log(publicKeyHex(parseSecretKey(process.env.BUZZ_PRIVATE_KEY)));
    return;
  }
  const text = rest.join(' ');
  let result;
  if (command === 'ask') result = await askKnowledge({ channelId: args.channel, question: text });
  else if (command === 'search') result = await searchKnowledge({ channelId: args.channel, query: text, topK: args.topK });
  else if (command === 'unanswered') result = await listUnanswered({ channelId: args.channel });
  else { console.error(`Unknown command: ${command}\n\n${HELP}`); process.exitCode = 2; return; }
  const render = { ask: renderAsk, search: renderSearch, unanswered: renderUnanswered }[command];
  console.log(args.json ? JSON.stringify(result, null, 2) : render(result));
}

main().catch(e => { console.error(e instanceof KnowledgeError ? e.message : (e?.stack || String(e))); process.exitCode = 1; });
