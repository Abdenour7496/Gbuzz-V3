FROM python:3.12-slim

WORKDIR /app
COPY proxy/requirements.lock .
RUN pip install --no-cache-dir --require-hashes -r requirements.lock
COPY proxy/main.py .
COPY proxy/request_limits.py .
COPY proxy/egress.py .
COPY proxy/governance_outbox.py .
COPY proxy/governance_service.py .
COPY proxy/access_policy.py .
COPY proxy/db_scope.py .
COPY proxy/nostr_auth.py .
COPY proxy/enterprise_workflows.py .
COPY proxy/document_parsing.py .
COPY proxy/parser_client.py .
COPY proxy/graph_retrieval.py .
COPY proxy/reranker.py .
COPY proxy/unanswered.py .
COPY proxy/temporal_facts.py .
COPY proxy/audit_pack.py .
COPY proxy/buzz_chat.py .
COPY proxy/buzz_synthesis.py .
COPY proxy/buzz_cards.py .
COPY proxy/buzz_wiki.py .
COPY scripts/setup-buzz-knowledge.py ./scripts/setup-buzz-knowledge.py
COPY scripts/check-buzz-knowledge.py ./scripts/check-buzz-knowledge.py
COPY proxy/workspace ./workspace
COPY clients/buzz-knowledge.mjs ./workspace/buzz-knowledge.mjs
COPY scripts/evaluate-knowledge.py ./scripts/evaluate-knowledge.py
COPY scripts/review-enterprise-evaluation.py ./scripts/review-enterprise-evaluation.py
COPY scripts/evaluate-service-identity-matrix.py ./scripts/evaluate-service-identity-matrix.py
COPY config/enterprise-assurance-slos.v1.json ./config/enterprise-assurance-slos.v1.json
COPY scripts/check-knowledge-quality.py ./scripts/check-knowledge-quality.py
COPY scripts/judge-answers.py ./scripts/judge-answers.py
COPY scripts/draft-evaluation-set.py ./scripts/draft-evaluation-set.py
COPY schemas ./schemas
COPY tests ./tests

EXPOSE 5001
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5001"]
