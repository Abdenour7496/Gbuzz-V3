# Knowledge-loop sprint (2026-10-26)

Builds on Gbuzz-V3 `main` (`b9adfdf`) and implements plan items 1, 2, 3 and 6 from the 26 Oct
improvement plan. Nothing here has been deployed to the lab.

## Changes

| # | Change | Default | Files |
| --- | --- | --- | --- |
| 6 | **Identifier leg.** Codes and ticket numbers in a question (`INC-6818`, `inc 6818`, `KB00042`, `CVE-2024-1234`) become a case-insensitive, separator-tolerant pattern. It is matched against title and chunk text inside the caller's authorised scope and fused by reciprocal rank. | on (`RETRIEVAL_IDENTIFIER_WEIGHT=0.5`; 0 disables) | `proxy/main.py` |
| 2 | **Unanswered-question queue.** Questions that find no authorised evidence are recorded for the asking channel (migration 0017, row-level security by channel and access level). They are de-duplicated with ask counts; askers are never revealed. Exposed through the workspace API and UI, and through a new agent tool (`unanswered_questions` / `gcor-agent unanswered`). A submission can declare the question it answers, and is linked when it is ingested. Members link answers; owners and admins dismiss or reopen. | on (`UNANSWERED_QUEUE_ENABLED`) | `migrations/0017_unanswered_questions.sql`, `proxy/unanswered.py`, `proxy/enterprise_workflows.py`, `proxy/access_policy.py`, `proxy/workspace/*`, `agent-tools/gcor-agent` |
| 3 | **Thread-aware synthesis.** `!knowledge synthesize thread`, sent as a reply inside a thread, gathers the root and all replies (relay `thread_metadata`, falling back to NIP-10 tags). The proposal is titled from the thread. All synthesis excerpts now name their speaker and time, so drafts can attribute decisions. Prompt version moves to v2: in-flight v1 synthesis jobs report "stale" and must be resubmitted. | new command; existing forms unchanged | `proxy/buzz_synthesis.py`, `proxy/buzz_chat.py` |
| 1 | **Real evaluation-set drafting.** `scripts/draft-evaluation-set.py` drafts owner-reviewable cases from approved lab knowledge through the owner's signed API, with verbatim answer phrases, correction, cross-channel and placeholder cases. Every case starts as `REVIEW_REQUIRED`. Runbook: `docs/evaluation-runbook.md`. | tool | `scripts/draft-evaluation-set.py`, `docs/evaluation-runbook.md` |

Correction to the 26 Oct plan: raw chat messages are never approved evidence for members, so
indexing whole threads as documents would not change answers. Thread context was added where chat
becomes knowledge instead: in synthesis.

## Measurements (isolated replica, 100k chunks, substitute embedding model)

| | Previous release (`b9adfdf`) | This sprint |
| --- | --- | --- |
| Standard set (246 answerable): required source in citations | 98.8% | 98.8% |
| Standard set: required source ranked first | 56.5% | **66.7%** |
| Identifier variants (108: `inc6818`, `INC 6818`, `inc-6818`): in citations | 51.9% | **100%** |
| Identifier variants: ranked first | 11.1% | **95.4%** |
| Leaks / bad citations | 0 / 0 | 0 / 0 |
| No-answer questions abstaining | 6/6 | 6/6 (and recorded as open questions) |
| Asks/s at 4 / 16 concurrent | 7.67 / 7.77 | 7.16 / 7.59 |
| p50 at 4 concurrent | 0.54 s | 0.58 s |

The throughput cost (2–7%) comes from the identifier leg, which runs a regex over the channel's
chunks when the question contains a code. Row-level security prevents a trigram index from helping
here, for the same leakproofness reason as migration 0016.

## Verification

- Unit tests: all proxy suites pass, including new `test_unanswered`, `test_draft_evaluation`, and
  additions to `test_retrieval_quality` and `test_buzz_synthesis`. The projector,
  graphiti-projector, relationship-projector, MCP and recovery-controller suites pass, and gcor-agent
  passes 7/7.
- Integration scripts against the replica all pass: smoke, rls_scope, enterprise_boundaries,
  nostr_membership, workspace_lifecycle, buzz_chat_lifecycle, governance_faults, relationship_rls,
  attachment_lifecycle, plus the new unanswered_queue and thread_synthesis (both added to CI).
- All 12 Compose combinations render.
- The workspace UI loads in Chromium with no console errors; the new view switches correctly.

## Rollout notes

- Migration 0017 is additive: one table with an RLS policy. Older images ignore it.
- Rebuild the knowledge image: `proxy/Dockerfile` now copies `unanswered.py` and the new scripts.
- Re-install the agent tools to get the new `unanswered_questions` tool.
- Tell users about `!knowledge synthesize thread`, and that pending syntheses started before the
  upgrade must be resubmitted.
