"""Read side of bi-temporal facts (gcor.facts, migration 0018).

Retrieval responses gain an additive ``facts`` list: the time-aware facts recorded for the documents
that matched (and for facts whose text matches the query), each with its validity window, whether it
is true now, and what superseded it. ``/api/facts/history`` answers "what was true on date W" and
"what did we believe on date K".

Safety properties:
  * Read-only. The runtime role has SELECT on gcor.facts and nothing else; rows are additionally
    filtered by the source document's row-level security.
  * Every fact, and every superseding fact it names, must come from a source document that passes
    the same scope predicates as retrieval (channel, access level, agent, approval, evidence
    currency, named readers) and must be that document's current revision.
  * Never fails retrieval: any error yields an empty list and is counted.
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID

from fastapi import HTTPException
from prometheus_client import Counter
from pydantic import BaseModel, Field

from access_policy import current_principal

logger = logging.getLogger("gcor.temporal_facts")

RETRIEVAL_TEMPORAL_FACTS = os.getenv("RETRIEVAL_TEMPORAL_FACTS", "true").strip().lower() in {"1", "true", "yes", "on"}
MAX_RETRIEVAL_FACTS = max(1, min(200, int(os.getenv("RETRIEVAL_MAX_FACTS", "40"))))
MAX_QUERY_FACT_GROUPS = 10

FACT_READS = Counter("gcor_temporal_fact_reads_total", "Temporal fact lookups", ["kind", "outcome"])


@dataclass(frozen=True)
class Scope:
    """The effective retrieval scope after any signed principal has overridden the request."""
    access_level: str
    channel_id: str | None
    channel_name: str | None
    agent_id: str | None
    approved_only: bool
    default_state: str
    reader: str | None


# Same document predicates as run_retrieval_query, plus "this fact is from the current revision".
# $1 access_level, $2 agent_id, $3 channel_id, $4 channel_name, $5 approved_only, $6 default_state, $7 reader
SOURCE_VISIBLE = """
    {d}.access_level = $1 AND {d}.content_sha256 = {f}.source_revision
    AND ($2::text IS NULL OR {d}.agent_id IS NULL OR {d}.agent_id = $2)
    AND ($3::text IS NULL OR {d}.channel_id = $3)
    AND ($4::text IS NULL OR {d}.metadata->>'channel_name' = $4)
    AND ($5::bool IS FALSE OR COALESCE({d}.metadata->>'knowledge_state', $6::text) = 'approved')
    AND ($5::bool IS FALSE OR ((NOT ({d}.metadata ? 'buzz_evidence_ids') AND NOT ({d}.metadata ? 'wiki_dependencies'))
                               OR gcor.knowledge_evidence_current({d}.id)))
    AND ($7::text IS NULL OR NOT ({d}.metadata ? 'knowledge_readers') OR {d}.metadata->'knowledge_readers' = 'null'::jsonb
         OR {d}.metadata->'knowledge_readers' @> jsonb_build_array($7::text))
"""
SCOPE_PARAMS = 7


def _visible(fact: str, document: str = "d") -> str:
    return SOURCE_VISIBLE.format(f=fact, d=document)


FACT_COLUMNS = """
    f.id, f.fact_id, f.subject, f.predicate, f.cardinality, f.object, f.fact, f.valid_at, f.valid_at_source,
    f.invalid_at, f.status, f.invalidated_by, f.corroborates, f.recorded_at, f.expired_at, f.expiry_reason,
    f.source_document_id, d.title AS source_title, f.confidence, f.evidence_node_ids,
    nxt.object AS superseded_by_object, nxt.fact AS superseded_by_fact
"""


def _timeline_select(where: str, order: str, limit_param: str) -> str:
    # Superseding facts are named only when the caller may see their source too.
    return f"""
        SELECT {FACT_COLUMNS}
        FROM gcor.facts f
        JOIN gcor.documents d ON d.id = f.source_document_id
        LEFT JOIN LATERAL (
            SELECT n.object, n.fact FROM gcor.facts n JOIN gcor.documents nd ON nd.id = n.source_document_id
            WHERE n.fact_id = f.invalidated_by AND n.expired_at IS NULL AND {_visible('n', 'nd')}
            LIMIT 1
        ) nxt ON true
        WHERE f.access_level = $1 AND ($2::text IS NULL OR f.agent_id IS NULL OR f.agent_id = $2)
          AND {_visible('f')} AND {where}
        ORDER BY {order}
        LIMIT {limit_param}
    """


def serialize(row: Any, moment: datetime) -> dict[str, Any]:
    valid_at, invalid_at = row["valid_at"], row["invalid_at"]
    in_window = (valid_at is None or valid_at <= moment) and (invalid_at is None or invalid_at > moment)
    superseded_by = None
    if row["invalidated_by"] is not None and row["superseded_by_object"] is not None:
        superseded_by = {"fact_id": str(row["invalidated_by"]), "object": row["superseded_by_object"], "fact": row["superseded_by_fact"]}
    return {
        "fact_id": str(row["fact_id"]),
        "version_id": str(row["id"]),
        "subject": row["subject"],
        "predicate": row["predicate"],
        "object": row["object"],
        "fact": row["fact"],
        "valid_at": valid_at.isoformat() if valid_at else None,
        "valid_at_source": row["valid_at_source"],
        "invalid_at": invalid_at.isoformat() if invalid_at else None,
        "status": row["status"],
        "true_at": moment.isoformat(),
        "is_true": row["status"] in {"active", "superseded"} and in_window,
        "superseded_by": superseded_by,
        "corroborates": str(row["corroborates"]) if row["corroborates"] else None,
        "recorded_at": row["recorded_at"].isoformat(),
        "expired_at": row["expired_at"].isoformat() if row["expired_at"] else None,
        "source": {"document_id": str(row["source_document_id"]), "title": row["source_title"]},
        "confidence": row["confidence"],
        "evidence_node_ids": [str(node) for node in row["evidence_node_ids"]],
    }


def available(request: Any) -> bool:
    state = getattr(getattr(request, "app", None), "state", None)
    return RETRIEVAL_TEMPORAL_FACTS and bool(getattr(state, "temporal_facts_available", False))


def _scope_args(scope: Scope) -> list[Any]:
    return [scope.access_level, scope.agent_id, scope.channel_id, scope.channel_name,
            scope.approved_only, scope.default_state, scope.reader]


async def for_retrieval(request: Any, matches: list[Any], query: str, scope: Scope) -> list[dict[str, Any]]:
    """Timelines of every fact group touched by the matched documents or by the query text.
    Returns [] when unavailable or on any error; never raises."""
    if not available(request):
        return []
    document_ids = list({UUID(str(row["document_id"])) for row in matches})
    try:
        now = datetime.now(timezone.utc)
        sql = f"""
            WITH search_terms AS (
                SELECT NULLIF(replace(websearch_to_tsquery('english', $8)::text, ' & ', ' | '), '')::tsquery AS any_query
            ), seeds AS (
                SELECT f.channel_id, f.access_level, COALESCE(f.agent_id, '') AS agent, f.subject_key, f.predicate
                FROM gcor.facts f JOIN gcor.documents d ON d.id = f.source_document_id
                WHERE f.expired_at IS NULL AND f.source_document_id = ANY($9::uuid[]) AND f.access_level = $1 AND {_visible('f')}
                UNION
                SELECT * FROM (
                    SELECT f.channel_id, f.access_level, COALESCE(f.agent_id, '') AS agent, f.subject_key, f.predicate
                    FROM gcor.facts f JOIN gcor.documents d ON d.id = f.source_document_id CROSS JOIN search_terms s
                    WHERE f.expired_at IS NULL AND s.any_query IS NOT NULL AND f.search_vector @@ s.any_query
                      AND f.access_level = $1 AND {_visible('f')}
                    ORDER BY ts_rank_cd(f.search_vector, s.any_query) DESC
                    LIMIT {MAX_QUERY_FACT_GROUPS}
                ) lexical
            )
            {_timeline_select(
                "f.expired_at IS NULL AND EXISTS (SELECT 1 FROM seeds g WHERE g.channel_id = f.channel_id"
                " AND g.access_level = f.access_level AND g.agent = COALESCE(f.agent_id, '')"
                " AND g.subject_key = f.subject_key AND g.predicate = f.predicate)",
                "f.subject_key, f.predicate, f.valid_at NULLS FIRST, f.recorded_at",
                "$10",
            )}
        """
        async with request.app.state.pool.acquire() as connection:
            rows = await connection.fetch(sql, *_scope_args(scope), query, document_ids, MAX_RETRIEVAL_FACTS, timeout=5)
        FACT_READS.labels("retrieval", "ok").inc()
        return [serialize(row, now) for row in rows]
    except Exception as error:  # facts are supplementary; retrieval has already succeeded
        FACT_READS.labels("retrieval", "error").inc()
        logger.warning("temporal facts unavailable for retrieval: %s", type(error).__name__)
        return []


# ---------------------------------------------------------------- history API


class FactHistoryRequest(BaseModel):
    subject: str = Field(min_length=1, max_length=160)
    predicate: str | None = Field(default=None, pattern=r"^[A-Za-z][A-Za-z0-9_ ]{1,47}$")
    channel_id: str | None = Field(default=None, max_length=200)
    channel_name: str | None = Field(default=None, max_length=200)
    access_level: str = "public"
    agent_id: str | None = None
    approved_only: bool = True
    # World time: report which values were true at this moment (default now).
    as_of: datetime | None = None
    # Record time: answer from what the knowledge base believed at this moment (default: current belief).
    known_at: datetime | None = None
    # timeline = every value with its validity window; true_at = only the values true at as_of.
    view: Literal["timeline", "true_at"] = "timeline"
    limit: int = Field(default=100, ge=1, le=500)


def subject_key(text: str) -> str:
    """Must match relationship-projector/temporal_facts.normalize_key."""
    return re.sub(r"\s+", " ", text.casefold()).strip().strip(".,;:!?\"'()[]").strip()


def scope_for(*, access_level: str, channel_id: str | None, channel_name: str | None,
              agent_id: str | None, approved_only: bool) -> Scope:
    """Apply the signed principal exactly as run_retrieval_query does: it overrides channel, access
    level and agent, forces approved-only reads, and rejects attempts to name another scope."""
    principal = current_principal.get()
    if principal is not None and not (principal.workload and not principal.operations):
        if channel_id is not None and channel_id != principal.channel_id:
            raise HTTPException(403, "Channel is outside the authenticated scope")
        if agent_id is not None and agent_id != principal.agent_id:
            raise HTTPException(403, "Requested scope is not permitted")
        access_level, channel_id, channel_name = principal.access_level, principal.channel_id, None
        agent_id, approved_only = principal.agent_id, True
    return Scope(access_level, channel_id, channel_name, agent_id, approved_only,
                 "proposed" if principal else "approved", principal.subject if principal else None)


def _utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


async def history(request: Any, payload: FactHistoryRequest) -> dict[str, Any]:
    if not available(request):
        raise HTTPException(503, "Temporal facts require migration 0018 and RETRIEVAL_TEMPORAL_FACTS; retrieval remains available")
    scope = scope_for(access_level=payload.access_level, channel_id=payload.channel_id, channel_name=payload.channel_name,
                      agent_id=payload.agent_id, approved_only=payload.approved_only)
    if not scope.channel_id and not scope.channel_name:
        raise HTTPException(422, "channel scope is required: provide channel_id or channel_name")
    moment = _utc(payload.as_of) or datetime.now(timezone.utc)
    known_at = _utc(payload.known_at)
    predicate = payload.predicate.strip().upper().replace(" ", "_") if payload.predicate else None
    args: list[Any] = _scope_args(scope) + [subject_key(payload.subject), predicate, payload.limit]
    where = f"f.subject_key = ${SCOPE_PARAMS + 1} AND (${SCOPE_PARAMS + 2}::text IS NULL OR f.predicate = ${SCOPE_PARAMS + 2})"
    if known_at is None:
        where += " AND f.expired_at IS NULL"
    else:
        args.append(known_at)
        where += f" AND f.recorded_at <= ${len(args)} AND (f.expired_at IS NULL OR f.expired_at > ${len(args)})"
    if payload.view == "true_at":
        args.append(moment)
        where += (f" AND f.status IN ('active','superseded') AND (f.valid_at IS NULL OR f.valid_at <= ${len(args)})"
                  f" AND (f.invalid_at IS NULL OR f.invalid_at > ${len(args)})")
    sql = _timeline_select(where, "f.predicate, f.valid_at NULLS FIRST, f.recorded_at", f"${SCOPE_PARAMS + 3}")
    try:
        async with request.app.state.pool.acquire() as connection:
            rows = await connection.fetch(sql, *args, timeout=10)
    except Exception as error:
        FACT_READS.labels("history", "error").inc()
        logger.warning("fact history failed: %s", type(error).__name__)
        raise HTTPException(503, "Fact history is temporarily unavailable")
    FACT_READS.labels("history", "ok").inc()
    return {"subject": payload.subject, "predicate": predicate, "as_of": moment.isoformat(),
            "known_at": known_at.isoformat() if known_at else None, "view": payload.view,
            "facts": [serialize(row, moment) for row in rows]}
