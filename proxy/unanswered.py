"""Unanswered-question queue.

When /api/ask or /api/ask/reply finds no authorised evidence, the question is recorded for the
asking channel, so people and agents can see what the knowledge base cannot answer yet and fill
the gap. Rows are scoped by row-level security to the channel and access level they were asked
in. Askers' identities are kept for de-duplication but only counts are returned to callers.
Recording never affects the answer: failures are counted and ignored.
"""
from __future__ import annotations

import logging
import os
import random
import re
from datetime import datetime
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from prometheus_client import Counter
from pydantic import BaseModel, Field

from access_policy import Principal, current_principal

logger = logging.getLogger("gcor.unanswered")

UNANSWERED_QUEUE_ENABLED = os.getenv("UNANSWERED_QUEUE_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
UNANSWERED_RETENTION_DAYS = max(1, int(os.getenv("UNANSWERED_RETENTION_DAYS", "90")))
MAX_ASKERS = 20

UNANSWERED = Counter("gcor_unanswered_questions_total", "Questions recorded because no authorised evidence was found", ["outcome"])


def question_key(question: str) -> str:
    text = re.sub(r"[^\w\s-]", " ", question.casefold())
    return re.sub(r"\s+", " ", text).strip()[:300]


def recordable(principal: Principal | None, question: str) -> bool:
    if not UNANSWERED_QUEUE_ENABLED or principal is None:
        return False
    if not principal.channel_id or principal.access_level not in {"public", "private"}:
        return False  # unscoped legacy and stack callers have no channel to report to
    return len(question_key(question)) >= 3


async def record(pool, question: str) -> None:
    """Upsert an unanswered question for the current principal's channel. Never raises."""
    principal = current_principal.get()
    question = question.strip()[:2000]
    if not recordable(principal, question):
        return
    try:
        await pool.execute(
            """INSERT INTO gcor.unanswered_questions (channel_id, access_level, question, question_key, askers)
               VALUES ($1, $2, $3, $4, ARRAY[$5::text])
               ON CONFLICT (channel_id, access_level, question_key) DO UPDATE SET
                   asked_count = gcor.unanswered_questions.asked_count + 1,
                   last_asked_at = now(),
                   askers = CASE WHEN $5 = ANY(gcor.unanswered_questions.askers)
                                   OR cardinality(gcor.unanswered_questions.askers) >= $6
                                 THEN gcor.unanswered_questions.askers
                                 ELSE gcor.unanswered_questions.askers || $5::text END,
                   -- An answer that is approved yet still not found has not closed the gap: reopen.
                   -- An answer still awaiting review keeps the question marked answered.
                   status = CASE WHEN gcor.unanswered_questions.status = 'answered' AND EXISTS (
                                     SELECT 1 FROM gcor.documents d
                                     WHERE d.id = gcor.unanswered_questions.answer_document_id
                                       AND d.metadata->>'knowledge_state' = 'approved')
                                 THEN 'open' ELSE gcor.unanswered_questions.status END""",
            principal.channel_id, principal.access_level, question, question_key(question), principal.subject, MAX_ASKERS,
        )
        UNANSWERED.labels("recorded").inc()
        if random.random() < 0.02:
            await pool.execute(
                """DELETE FROM gcor.unanswered_questions
                   WHERE channel_id = $1 AND status <> 'open' AND resolved_at < now() - make_interval(days => $2)""",
                principal.channel_id, UNANSWERED_RETENTION_DAYS,
            )
    except Exception as error:  # the answer has already been decided; never fail it here
        UNANSWERED.labels("error").inc()
        logger.warning("could not record unanswered question: %s", type(error).__name__)


# ---------------------------------------------------------------- workspace API


async def require_schema(request: Request):
    if not getattr(request.app.state, "unanswered_available", False):
        raise HTTPException(503, "The unanswered-question queue requires migration 0017; retrieval remains available")


router = APIRouter(prefix="/api/workspace/unanswered", dependencies=[Depends(require_schema)])


def identity(admin: bool = False, contributor: bool = False) -> Principal:
    p = current_principal.get()
    if p is None or not p.channel_id or p.workload:
        raise HTTPException(401, "A signed Buzz identity is required")
    if admin and (p.agent_id or p.role not in {"owner", "admin"}):
        raise HTTPException(403, "Channel owner or admin required")
    if contributor and (p.agent_id or p.role not in {"owner", "admin", "member"}):
        raise HTTPException(403, "Channel contributor required")
    return p


class QueueRequest(BaseModel):
    channel_id: UUID
    status: Literal["open", "answered", "dismissed", "all"] = "open"
    limit: int = Field(default=50, ge=1, le=200)


class ResolveRequest(BaseModel):
    channel_id: UUID
    question_id: UUID
    action: Literal["answered", "dismissed", "reopen"]
    document_id: UUID | None = None
    note: str = Field(default="", max_length=4000)
    expected_last_asked_at: datetime | None = None


@router.post("")
async def queue(payload: QueueRequest, request: Request):
    p = identity()
    if str(payload.channel_id) != p.channel_id:
        raise HTTPException(403, "Channel is outside the authenticated scope")
    rows = await request.app.state.pool.fetch(
        """SELECT q.id, q.question, q.asked_count, cardinality(q.askers) AS distinct_askers,
                  q.first_asked_at, q.last_asked_at, q.status, q.resolved_at, q.resolution_note,
                  q.answer_document_id, d.title AS answer_title, d.metadata->>'knowledge_state' AS answer_state
           FROM gcor.unanswered_questions q
           LEFT JOIN gcor.documents d ON d.id = q.answer_document_id
           WHERE q.channel_id = $1 AND q.access_level = $2 AND ($3 = 'all' OR q.status = $3)
           ORDER BY (q.status = 'open') DESC, q.asked_count DESC, q.last_asked_at DESC
           LIMIT $4""",
        p.channel_id, p.access_level, payload.status, payload.limit,
    )
    return [dict(row) for row in rows]


@router.post("/resolve")
async def resolve(payload: ResolveRequest, request: Request):
    # Linking an answer is contributor work; dismissing or reopening is a stewardship decision.
    p = identity(admin=payload.action != "answered", contributor=payload.action == "answered")
    if str(payload.channel_id) != p.channel_id:
        raise HTTPException(403, "Channel is outside the authenticated scope")
    if payload.action == "answered":
        if payload.document_id is None:
            raise HTTPException(422, "Select the knowledge document that answers the question")
        found = await request.app.state.pool.fetchval(
            "SELECT 1 FROM gcor.documents WHERE id = $1 AND channel_id = $2 AND access_level = $3",
            payload.document_id, p.channel_id, p.access_level,
        )
        if not found:
            raise HTTPException(404, "Knowledge document not found")
    status = {"answered": "answered", "dismissed": "dismissed", "reopen": "open"}[payload.action]
    row = await request.app.state.pool.fetchrow(
        """UPDATE gcor.unanswered_questions SET status = $4,
               answer_document_id = CASE WHEN $4 = 'answered' THEN $5 ELSE answer_document_id END,
               resolved_by = CASE WHEN $4 = 'open' THEN NULL ELSE $6 END,
               resolved_at = CASE WHEN $4 = 'open' THEN NULL ELSE now() END,
               resolution_note = NULLIF($7, '')
           WHERE id = $1 AND channel_id = $2 AND access_level = $3
             AND ($8::timestamptz IS NULL OR last_asked_at = $8)
           RETURNING id, status, answer_document_id, resolved_at""",
        payload.question_id, p.channel_id, p.access_level, status, payload.document_id, p.subject, payload.note,
        payload.expected_last_asked_at,
    )
    if row is None:
        raise HTTPException(409, "Question not found or changed since it was loaded")
    return dict(row)


async def link_answer(connection, question_id: str, channel_id: str, document_id: str, actor: str) -> None:
    """Called by the ingestion worker when a submission declared which question it answers."""
    await connection.execute(
        """UPDATE gcor.unanswered_questions SET status = 'answered', answer_document_id = $3::uuid,
               resolved_by = $4, resolved_at = now()
           WHERE id = $1::uuid AND channel_id = $2 AND status = 'open'""",
        question_id, channel_id, document_id, actor,
    )
