# Building and running the real evaluation set

Every tuning number so far comes from a synthetic corpus. This runbook turns the lab's own approved
knowledge into an owner-reviewed evaluation set, then uses it to calibrate retrieval and generation.
It takes about an hour of owner time.

All commands run on the lab host. Nothing is sent off the host: drafting and judging use the local
Ollama.

## 1. Identity registry (once)

Create `eval-identities.json`. Each private key stays in an environment variable and is never stored
in the file:

```json
{"owner": {"mode": "interactive", "private_key_env": "GBUZZ_EVAL_OWNER_KEY"}}
```

The owner identity must be a member of every channel you want covered. Use an evaluation account,
not your daily key.

## 2. Draft the set

```powershell
$env:GBUZZ_EVAL_OWNER_KEY = '<hex private key of the evaluation account>'
python scripts/draft-evaluation-set.py eval-draft.jsonl --identities eval-identities.json --principal owner `
    --url http://127.0.0.1:5011 --model qwen2.5:7b --per-channel 12
```

For each sampled approved document, the drafting model proposes one question the document answers,
plus the exact phrase that answers it. A draft whose phrase is not verbatim in the document is
dropped.

The tool also adds correction cases (superseded document → replacement) and cross-channel cases. It
adds placeholders for:
- abstention
- prompt injection
- revocation
- concurrency
- model capacity

It prints a coverage report showing which categories and formats are still below the policy minimum
(100 cases, 10 per category, all eight formats).

## 3. Owner review (the important part)

Open `eval-draft.jsonl` and, for every case:
- Read the question. Rewrite it the way a colleague would actually ask, or delete the case if it is
  unfair or trivial.
- Check `required_document_ids` and `expected_answer_contains`.
- Replace each `REPLACE_WITH_OWNER_QUESTION` placeholder with a real question:
  - Abstention: something this channel genuinely does not know.
  - Prompt injection: add a test document containing instructions first.
- Set `data_classification` to `non-sensitive-test` only if the case's content is suitable for test
  logs. Otherwise delete the case.

Formats the lab has no documents for (for example `pptx` or `image-ocr`): upload a small
non-sensitive sample of each through the workspace, approve it, then add a case for it.

Validate:

```powershell
python scripts/evaluate-knowledge.py eval-reviewed.jsonl --identities eval-identities.json --validate-only
```

## 4. Run and calibrate

Run the gate as documented in `scripts/evaluate-knowledge.py`; it records provenance. Then:
- **Retrieval weights:** run with `RETRIEVAL_VECTOR_WEIGHT` at 0.35, 0.5 and 0.65, and
  `RETRIEVAL_MIN_VECTOR_SCORE` at 0.40, 0.45 and 0.50. Keep the pair with the best recall and 100%
  abstention.
- **Reranker:** compare runs with `RERANKER_URL` unset and set. Keep it only if recall at rank 1
  improves and `gcor_rerank_duration_seconds` p95 stays under about 1 s.
- **Generation:**
  ```
  python scripts/judge-answers.py eval-reviewed.jsonl --identities eval-identities.json --judge-model qwen2.5:7b --output judge.json
  ```
  Repeat for `GENERATION_MODEL` qwen2.5:1.5b, 3b and 7b. Compare sentence faithfulness, abstention
  and latency.

The judge output is advisory: it ranks answers for owner review. The owner's signed review remains
the release gate (`scripts/check-knowledge-quality.py`).

## 5. Keep it alive

- Re-run after each release.
- Add a case whenever the unanswered-question queue or feedback reveals a real miss.
- Store the reviewed set with the encrypted backups. It is test data derived from your knowledge.
