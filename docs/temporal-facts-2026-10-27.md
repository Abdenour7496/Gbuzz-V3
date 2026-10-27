# Bi-temporal facts in Postgres (2026-10-27)

Knowledge that tracks time, without Graphiti or FalkorDB. Branch `feat/temporal-facts-20261027`, based on
`feat/knowledge-loop-sprint-20261026` (PR #17). Not deployed; the lab is untouched.

## What it does
A **fact** is a `(subject, predicate, object)` assertion taken from one approved source revision, for example
`Release process OWNED_BY Alice`. Every fact has two timelines:

| Timeline | Columns | Answers |
|---|---|---|
| Valid time | `valid_at`, `invalid_at` | When was it true in the world? |
| Record time | `recorded_at`, `expired_at` | What did the knowledge base believe, and when? |

Rows in `gcor.facts` are **versions**. A change closes the current version and inserts the next one, so history is
never rewritten. A trigger rejects any other update, and no runtime role can delete rows.

Contradictions are settled by fixed rules (no model involved):
- **Single-valued predicates** (`OWNED_BY`, `STATUS`, `VERSION`, `DECISION`, ...): values are ordered by `valid_at`.
  Each value is *superseded* by the next different one. Two values with the same start: the most recently
  approved source wins and the other is *contradicted*. Restating the current value is *corroborating*.
  An exclusion constraint makes overlapping current values impossible, even for a superuser.
- **Multi-valued predicates** (`USES`, `DEPENDS_ON`, `MEMBER_OF`): values coexist; only stated end dates apply.
- An unstated start defaults to when the source was approved. Late knowledge about the past
  ("Carol owned it from January") slots in by valid time.
- Retracting a source (back to draft, revised, evidence lapsed, deleted, or restricted to named readers) closes
  its facts. The previous value becomes current again, as a new version.

The vocabulary lives in `gcor.fact_predicates` (13 seeded predicates). Operators can add rows; a predicate the
model invents is dropped.

## Where it shows up
- `/api/retrieve` and `/api/ask` return an additive `facts` list. It holds the timelines of the matched documents'
  subjects, plus facts whose text matches the query. Each item has its window, `is_true`, and `superseded_by`.
  Answers, citations and chunks are unchanged: facts are not fed to the generation prompt yet.
- `POST /api/facts/history` takes `subject`, an optional `predicate`, and `view`:
  - `view=timeline` returns every value with its window.
  - `view=true_at` with `as_of` returns what was true at that moment.
  - `known_at` returns what was believed at that moment.

  It is open to signed members and agents (read path, `retrieve` operation) and to stack-secret callers.
- Agent tools: `fact_history` in `mcp-postgres-gcor` and in `gcor-agent` (MCP), plus
  `gcor-agent facts --channel <uuid> [--as-of T] "<subject>"`.

## Safety
- The runtime role can only `SELECT` facts, through the source document's row-level security. Every fact, and any
  superseding fact it names, must come from a source that passes the same scope predicates as retrieval:
  channel, access level, agent, approval, evidence currency, named readers, and current revision.
- Only the relationship projector writes facts. It can insert only for approved, evidence-current sources in their
  own channel, citing that source's own chunks (`gcor.relationship_fact_source_valid`).
- Reader-restricted documents are never folded into channel-wide timelines, so one cannot leak through
  "superseded on date X".
- Fact work runs in a savepoint. A failure is logged and rolled back; the entity/claim projection still commits.

## Fixed along the way (pre-existing relationship-projector bugs)
1. **The projector could never claim or publish.** Its claim and publish queries lock source rows
   (`SELECT ... FOR UPDATE` / `FOR SHARE`). That requires UPDATE privilege, which 0015 never granted, so
   both failed with "permission denied for table documents". CI never ran the publish path.
   `relationship-projector/migrations/0016_relationship_publication_locks.sql` grants one column each, plus UPDATE
   policies with `WITH CHECK (false)`: the projector can lock sources but still cannot modify them (verified).
2. **Un-approval was never detected.** The projector role only sees approved documents, so the stale check's
   `JOIN` dropped exactly the rows it was looking for. It is now a `LEFT JOIN` that treats an invisible source as stale.

## Turning it on
1. Deploy. `gcor-migrate` applies `migrations/0018_temporal_facts.sql`, which is additive (it needs `btree_gist`,
   included in the pgvector image). The proxy detects the table at startup.
2. Run the relationship-projection profile (`docker-compose.relationships.yml`). Its migrate step now applies every
   file in `relationship-projector/migrations/` in order. `RELATIONSHIP_TEMPORAL_FACTS` defaults to `true` in that
   overlay; the code default is `false`.
3. Facts appear as approved documents are (re)projected.

Kill switches: `RETRIEVAL_TEMPORAL_FACTS=false` on the proxy (empty `facts`, history returns 503), and
`RELATIONSHIP_TEMPORAL_FACTS=false` on the projector. With the projector switch off, the prompt and behaviour are
byte-identical to before (pinned by a unit test). Rollback means flipping these switches; the table can stay.

## Verification (cloud replica: Postgres 16 + pgvector, moto S3, mock model, parser worker)
- Unit tests: proxy 219 (12 new), relationship projector 8 + 21 new, event projector 18, MCP 5 (1 new),
  gcor-agent 8 (1 new); the committed bundle matches its source. pyflakes clean.
- Integration: all 13 CI scripts pass, including two new ones:
  - `temporal_facts_projection.py` runs the projector's own code and restricted role against real Postgres, with a
    scripted model. It covers supersede, record-time history, late knowledge, corroboration, retraction and
    reinstatement, delete-and-repair, reader restriction, failure isolation, feature off, the exclusion
    constraint, immutability, forgery attempts, and runtime RLS.
  - `temporal_facts_retrieval.py` uses signed Buzz identities. It covers channel isolation, draft and
    restricted sources hidden, `true_at`/`known_at`, the additive retrieval key, and the kill switch.
- Upgrade path: on a database built with the current migrations and holding data, the new proxy runs normally
  before 0018 (empty `facts`, history 503). After applying 0018 in place, and running every relationship
  migration twice for idempotency, the full suite passes.
- All 12 CI Compose combinations still render.

## Known limits and next steps
- Extraction quality with `qwen2.5:1.5b` is unmeasured. Add fact cases to the evaluation set before relying on it.
- Facts are not used in answer generation yet. Feeding current facts (with their windows) to the prompt is the
  next step, behind the evaluation set.
- Multi-valued predicates have no automatic contradiction handling. A model-proposed, human-approved invalidation
  queue would fit the existing approval flow.
