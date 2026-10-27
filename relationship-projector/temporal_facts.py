"""Bi-temporal facts for the PostgreSQL-native relationship projector.

Facts are (subject, predicate, object) assertions extracted from one approved source revision. Each
carries valid time (when it was true in the world) and record time (when the knowledge base held
that version). Rows in gcor.facts are versions: a change closes the current version and inserts the
next one, so history is never rewritten (see migrations/0018_temporal_facts.sql).

Contradiction handling is deterministic, not model-driven:
  * cardinality 'one' predicates (OWNED_BY, STATUS, ...) hold one value at a time. Ordered by
    valid_at, each value is superseded by the next different one; assertions with the same start
    are resolved in favour of the most recently approved source (the others are 'contradicted');
    restating the same value is 'corroborating' and leaves the timeline unchanged.
  * cardinality 'many' predicates (USES, DEPENDS_ON, ...) coexist; only stated end dates apply.

This module only ever runs inside the projector's own transaction, behind a savepoint, so a fact
failure can never block the existing entity/claim projection.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable
from uuid import UUID, uuid5

NAMESPACE = UUID("4f0c7a52-0b8e-4d0e-9a51-7f2b9d3c6e10")
MAX_FACTS = 25
MAX_SUBJECT = 160
MAX_OBJECT = 400
MAX_FACT_TEXT = 1000
MIN_TIME = datetime(1900, 1, 1, tzinfo=timezone.utc)
MAX_TIME = datetime(2200, 1, 1, tzinfo=timezone.utc)
PREDICATE = re.compile(r"^[A-Z][A-Z0-9_]{1,47}$")
PRINTABLE = re.compile(r"^[^\x00-\x1f\x7f]+$")
FACT_KEYS = {"subject", "predicate", "object", "evidence_ordinals", "confidence"}
OPTIONAL_FACT_KEYS = {"fact", "valid_from", "valid_until"}
DATE_ONLY = re.compile(r"^\d{4}(-\d{2}(-\d{2})?)?$")
NO_OVERLAP_CONSTRAINT = "gcor.facts_single_value_no_overlap"

GroupKey = tuple[str, str, str, str, str]  # channel_id, access_level, agent_id ('' = none), subject_key, predicate


def normalize_key(text: str) -> str:
    """Case- and whitespace-insensitive identity; trailing punctuation does not create a new subject."""
    return re.sub(r"\s+", " ", text.casefold()).strip().strip(".,;:!?\"'()[]").strip()


def normalize_predicate(value: Any) -> str:
    return re.sub(r"[\s\-]+", "_", str(value).strip()).upper()


def parse_time(value: Any) -> datetime | None:
    """ISO year, month, date or timestamp (naive = UTC), bounded to a sane range. None/'' = not stated."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if not isinstance(value, str):
        raise ValueError("time must be an ISO string")
    text = value.strip()
    if DATE_ONLY.fullmatch(text):
        parts = [int(part) for part in text.split("-")] + [1, 1]
        parsed = datetime(parts[0], parts[1], parts[2], tzinfo=timezone.utc)
    else:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
    if not MIN_TIME <= parsed <= MAX_TIME:
        raise ValueError("time out of range")
    return parsed


def _bounded_text(value: Any, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(value)).strip()
    if not text or len(text) > limit or not PRINTABLE.fullmatch(text):
        raise ValueError("text must be printable and bounded")
    return text


def validate_facts(raw: Any, available_ordinals: set[int], vocabulary: dict[str, str]) -> tuple[list[dict[str, Any]], int]:
    """Return (valid facts, dropped count). Lenient per record: a bad fact is dropped, never fatal."""
    if raw is None:
        return [], 0
    if not isinstance(raw, list):
        return [], 1
    facts: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    dropped = max(0, len(raw) - MAX_FACTS)
    for item in raw[:MAX_FACTS]:
        try:
            if not isinstance(item, dict) or not FACT_KEYS <= set(item) or set(item) - FACT_KEYS - OPTIONAL_FACT_KEYS:
                raise ValueError("invalid fact record")
            subject = _bounded_text(item["subject"], MAX_SUBJECT)
            obj = _bounded_text(item["object"], MAX_OBJECT)
            predicate = normalize_predicate(item["predicate"])
            if not PREDICATE.fullmatch(predicate) or predicate not in vocabulary:
                raise ValueError("predicate outside the vocabulary")
            confidence = float(item["confidence"])
            if not 0 <= confidence <= 1:
                raise ValueError("confidence must be between zero and one")
            ordinals = item["evidence_ordinals"]
            if not isinstance(ordinals, list) or not ordinals or not all(
                isinstance(value, int) and not isinstance(value, bool) and value in available_ordinals for value in ordinals
            ):
                raise ValueError("every fact requires existing chunk evidence")
            valid_from = parse_time(item.get("valid_from"))
            valid_until = parse_time(item.get("valid_until"))
            if valid_from and valid_until and valid_until <= valid_from:
                raise ValueError("valid_until must be after valid_from")
            text = item.get("fact")
            fact_text = _bounded_text(text, MAX_FACT_TEXT) if text not in (None, "") else (
                f"{subject} {predicate.lower().replace('_', ' ')} {obj}"[:MAX_FACT_TEXT]
            )
            subject_key, object_key = normalize_key(subject), normalize_key(obj)
            if not subject_key or not object_key:
                raise ValueError("empty identity")
            identity = (subject_key, predicate, object_key)
            if identity in seen:
                raise ValueError("duplicate fact")
            seen.add(identity)
            facts.append({
                "subject": subject, "subject_key": subject_key, "predicate": predicate,
                "cardinality": vocabulary[predicate], "object": obj, "object_key": object_key,
                "fact": fact_text, "stated_valid_at": valid_from, "stated_invalid_at": valid_until,
                "evidence_ordinals": sorted(set(ordinals)), "confidence": confidence,
            })
        except (KeyError, TypeError, ValueError, OverflowError):
            dropped += 1
    return facts, dropped


def fact_identity(document_id: str, revision: str, subject_key: str, predicate: str, object_key: str) -> UUID:
    return uuid5(NAMESPACE, f"{document_id}:{revision}:{subject_key}:{predicate}:{object_key}")


def effective_start(stated_valid_at: datetime | None, stated_invalid_at: datetime | None, source_time: datetime) -> tuple[datetime | None, str]:
    """When a fact starts. Unstated starts default to when the source became knowledge, unless the
    source says it had already ended by then (then the start is unknown)."""
    if stated_valid_at is not None:
        return stated_valid_at, "stated"
    if stated_invalid_at is not None and stated_invalid_at <= source_time:
        return None, "unknown"
    return source_time, "document"


def source_time_of(created_at: datetime, approved_at: str | None) -> datetime:
    """Approval time when recorded and parseable, otherwise creation time (mirrors retrieval's governance_time)."""
    if approved_at:
        try:
            parsed = datetime.fromisoformat(str(approved_at).replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return created_at


@dataclass(frozen=True)
class Assertion:
    fact_id: UUID
    object_key: str
    valid_at: datetime | None          # None = unknown start (sorts first)
    stated_invalid_at: datetime | None
    source_time: datetime


@dataclass(frozen=True)
class Derived:
    valid_at: datetime | None
    invalid_at: datetime | None
    status: str
    invalidated_by: UUID | None = None
    corroborates: UUID | None = None


def _start_key(item: Assertion) -> tuple[int, datetime]:
    return (0, MIN_TIME) if item.valid_at is None else (1, item.valid_at)


def _recency(item: Assertion) -> tuple[datetime, str]:
    return item.source_time, str(item.fact_id)


def reconcile(assertions: Iterable[Assertion], cardinality: str) -> dict[UUID, Derived]:
    """Deterministic timeline for one (channel, access, agent, subject, predicate) group."""
    items = list(assertions)
    if cardinality == "many":
        return {item.fact_id: Derived(item.valid_at, item.stated_invalid_at, "active") for item in items}
    result: dict[UUID, Derived] = {}
    by_start: dict[tuple[int, datetime], list[Assertion]] = {}
    for item in items:
        by_start.setdefault(_start_key(item), []).append(item)
    winners: list[Assertion] = []
    for start in sorted(by_start):
        tied = sorted(by_start[start], key=_recency)
        winner = tied[-1]
        winners.append(winner)
        for other in tied[:-1]:
            if other.object_key == winner.object_key:
                result[other.fact_id] = Derived(other.valid_at, other.stated_invalid_at, "corroborating", corroborates=winner.fact_id)
            else:
                result[other.fact_id] = Derived(other.valid_at, other.stated_invalid_at, "contradicted", invalidated_by=winner.fact_id)
    if not winners:
        return result
    representative = winners[0]
    for following in winners[1:]:
        end = representative.stated_invalid_at
        if following.object_key == representative.object_key and end is None and following.stated_invalid_at is None:
            result[following.fact_id] = Derived(following.valid_at, None, "corroborating", corroborates=representative.fact_id)
            continue
        if end is not None and following.valid_at is not None and end <= following.valid_at:
            result[representative.fact_id] = Derived(representative.valid_at, end, "active")
        else:
            result[representative.fact_id] = Derived(
                representative.valid_at, following.valid_at, "superseded", invalidated_by=following.fact_id,
            )
        representative = following
    result[representative.fact_id] = Derived(representative.valid_at, representative.stated_invalid_at, "active")
    return result


def extraction_instructions(vocabulary: dict[str, str], descriptions: dict[str, str]) -> str:
    listed = "; ".join(f"{name} ({descriptions.get(name, '')})" for name in sorted(vocabulary))
    return (
        " Also return facts: explicit statements about a subject's value, each with subject, predicate, "
        "object, valid_from, valid_until, evidence_ordinals, confidence. predicate must be one of: "
        f"{listed}. valid_from and valid_until are ISO dates (YYYY-MM-DD) only when the evidence states "
        "them, otherwise null."
    )


# ------------------------------------------------------------------ database layer


async def available(connection) -> bool:
    return bool(await connection.fetchval(
        "SELECT to_regclass('gcor.facts') IS NOT NULL AND to_regclass('gcor.fact_predicates') IS NOT NULL"
        " AND has_table_privilege('gcor.facts', 'INSERT')"
    ))


async def load_vocabulary(connection) -> tuple[dict[str, str], dict[str, str]]:
    rows = await connection.fetch("SELECT predicate, cardinality, description FROM gcor.fact_predicates ORDER BY predicate")
    return {row["predicate"]: row["cardinality"] for row in rows}, {row["predicate"]: row["description"] for row in rows}


def _group(row: Any) -> GroupKey:
    return (row["channel_id"], row["access_level"], row["agent_id"] or "", row["subject_key"], row["predicate"])


async def retire_document_facts(connection, document_id: UUID, revision: str | None = None) -> set[GroupKey]:
    """Close the current versions sourced from a document (optionally only one revision)."""
    rows = await connection.fetch(
        """UPDATE gcor.facts SET expired_at = now(), expiry_reason = 'source_retired'
           WHERE source_document_id = $1 AND expired_at IS NULL AND ($2::text IS NULL OR source_revision = $2)
           RETURNING channel_id, access_level, agent_id, subject_key, predicate""",
        document_id, revision,
    )
    return {_group(row) for row in rows}


async def insert_assertions(connection, source: Any, facts: list[dict[str, Any]], chunk_nodes: dict[int, UUID],
                            source_time: datetime, projector: str, model: str, run_id: UUID) -> set[GroupKey]:
    """Insert new assertions as provisional 'active' versions; reconcile_groups settles the timeline."""
    groups: set[GroupKey] = set()
    for fact in facts:
        fact_id = fact_identity(str(source["id"]), source["content_sha256"], fact["subject_key"], fact["predicate"], fact["object_key"])
        valid_at, valid_source = effective_start(fact["stated_valid_at"], fact["stated_invalid_at"], source_time)
        await connection.execute(
            """INSERT INTO gcor.facts(fact_id,channel_id,access_level,agent_id,subject,subject_key,predicate,cardinality,
                   object,object_key,fact,stated_valid_at,stated_invalid_at,valid_at,valid_at_source,invalid_at,status,
                   source_document_id,source_revision,source_time,evidence_node_ids,confidence,projector,model,run_id)
               VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$13,'active',$16,$17,$18,$19,$20,$21,$22,$23)""",
            fact_id, source["channel_id"], source["access_level"], source["agent_id"], fact["subject"], fact["subject_key"],
            fact["predicate"], fact["cardinality"], fact["object"], fact["object_key"], fact["fact"],
            fact["stated_valid_at"], fact["stated_invalid_at"], valid_at, valid_source,
            source["id"], source["content_sha256"], source_time,
            [chunk_nodes[ordinal] for ordinal in fact["evidence_ordinals"]], fact["confidence"], projector, model, run_id,
        )
        groups.add((source["channel_id"], source["access_level"], source["agent_id"] or "", fact["subject_key"], fact["predicate"]))
    return groups


COPY_COLUMNS = (
    "fact_id,channel_id,access_level,agent_id,subject,subject_key,predicate,object,object_key,fact,"
    "stated_valid_at,stated_invalid_at,valid_at_source,source_document_id,source_revision,source_time,"
    "evidence_node_ids,confidence,projector,model,run_id"
)


# A source may feed shared timelines only while it is the approved, evidence-current revision and is
# readable by the whole channel (reader-restricted knowledge would leak through a timeline).
ELIGIBLE_SOURCE = (
    "d.content_sha256 = f.source_revision AND d.metadata->>'knowledge_state' = 'approved'"
    " AND (NOT (d.metadata ? 'knowledge_readers') OR d.metadata->'knowledge_readers' = 'null'::jsonb)"
    " AND gcor.knowledge_evidence_current(d.id)"
)


def restricted(readers: Any) -> bool:
    """True when a source's knowledge_readers limits it to named readers."""
    return readers not in (None, "null")


async def reconcile_groups(connection, groups: set[GroupKey], vocabulary: dict[str, str]) -> int:
    """Recompute each group's timeline and write new versions only where the derived state changed."""
    changed = 0
    for group in sorted(groups):
        channel_id, access_level, agent_id, subject_key, predicate = group
        await connection.execute("SELECT pg_advisory_xact_lock(hashtextextended($1, 93467124))", "\x1f".join(group))
        rows = await connection.fetch(
            f"""SELECT f.id, f.fact_id, f.object_key, f.valid_at, f.invalid_at, f.stated_invalid_at, f.source_time,
                      f.status, f.invalidated_by, f.corroborates, f.cardinality,
                      (d.id IS NOT NULL) AS source_current
               FROM gcor.facts f
               LEFT JOIN gcor.documents d ON d.id = f.source_document_id AND {ELIGIBLE_SOURCE}
               WHERE f.expired_at IS NULL AND f.channel_id = $1 AND f.access_level = $2
                 AND COALESCE(f.agent_id, '') = $3 AND f.subject_key = $4 AND f.predicate = $5
               ORDER BY f.fact_id
               FOR UPDATE OF f""",
            channel_id, access_level, agent_id, subject_key, predicate,
        )
        retired = [row["id"] for row in rows if not row["source_current"]]
        if retired:
            await connection.execute(
                "UPDATE gcor.facts SET expired_at = now(), expiry_reason = 'source_retired' WHERE id = ANY($1::uuid[])", retired,
            )
        live = [row for row in rows if row["source_current"]]
        cardinality = vocabulary.get(predicate, "many")
        derived = reconcile(
            [Assertion(row["fact_id"], row["object_key"], row["valid_at"] if row["valid_at"] is not None else None,
                       row["stated_invalid_at"], row["source_time"]) for row in live],
            cardinality,
        )
        for row in live:
            target = derived[row["fact_id"]]
            current = (row["valid_at"], row["invalid_at"], row["status"], row["invalidated_by"], row["corroborates"], row["cardinality"])
            wanted = (target.valid_at, target.invalid_at, target.status, target.invalidated_by, target.corroborates, cardinality)
            if current == wanted:
                continue
            await connection.execute(
                "UPDATE gcor.facts SET expired_at = now(), expiry_reason = 'reconciled' WHERE id = $1", row["id"],
            )
            await connection.execute(
                f"""INSERT INTO gcor.facts({COPY_COLUMNS},cardinality,valid_at,invalid_at,status,invalidated_by,corroborates)
                    SELECT {COPY_COLUMNS},$2,$3,$4,$5,$6,$7 FROM gcor.facts WHERE id = $1""",
                row["id"], cardinality, target.valid_at, target.invalid_at, target.status, target.invalidated_by, target.corroborates,
            )
            changed += 1
    return changed


async def dangling_groups(connection, limit: int = 50) -> set[GroupKey]:
    """Groups needing re-settlement outside the normal projection flow: a current version points at
    a fact that no longer has a current version (its source was deleted), or a current version's
    source is no longer eligible (unapproved, revised, evidence lapsed or reader-restricted)."""
    rows = await connection.fetch(
        f"""SELECT f.channel_id, f.access_level, f.agent_id, f.subject_key, f.predicate
            FROM gcor.facts f
            WHERE f.expired_at IS NULL AND COALESCE(f.invalidated_by, f.corroborates) IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM gcor.facts t WHERE t.fact_id = COALESCE(f.invalidated_by, f.corroborates) AND t.expired_at IS NULL)
            UNION
            SELECT f.channel_id, f.access_level, f.agent_id, f.subject_key, f.predicate
            FROM gcor.facts f
            WHERE f.expired_at IS NULL
              AND NOT EXISTS (SELECT 1 FROM gcor.documents d WHERE d.id = f.source_document_id AND {ELIGIBLE_SOURCE})
            LIMIT $1""",
        limit,
    )
    return {_group(row) for row in rows}


async def check_timeline(connection) -> None:
    """Force the deferred no-overlap constraint now, so a violation fails the fact savepoint
    instead of the whole projection at commit."""
    await connection.execute(f"SET CONSTRAINTS {NO_OVERLAP_CONSTRAINT} IMMEDIATE")
