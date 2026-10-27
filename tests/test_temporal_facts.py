import importlib.util
import re
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import main
import temporal_facts
from access_policy import READ_PATHS, WORKLOAD_OPERATIONS, Principal, current_principal

ROOT = Path(__file__).resolve().parents[1]
PROJECTOR_SOURCE = ROOT / "relationship-projector" / "temporal_facts.py"
projector_facts = None
if PROJECTOR_SOURCE.exists():  # absent inside the proxy image, where the release gate runs this file
    spec = importlib.util.spec_from_file_location("projector_temporal_facts", PROJECTOR_SOURCE)
    projector_facts = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = projector_facts
    spec.loader.exec_module(projector_facts)


def request_with(pool=None, available=True):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(pool=pool, temporal_facts_available=available)))


def row(**overrides):
    now = datetime(2026, 10, 1, tzinfo=timezone.utc)
    base = {"id": uuid4(), "fact_id": uuid4(), "subject": "Release process", "predicate": "OWNED_BY", "cardinality": "one",
            "object": "Alice", "fact": "Alice owns releases", "valid_at": now - timedelta(days=200), "valid_at_source": "document",
            "invalid_at": now, "status": "superseded", "invalidated_by": uuid4(), "corroborates": None,
            "recorded_at": now, "expired_at": None, "expiry_reason": None, "source_document_id": uuid4(),
            "source_title": "March handbook", "confidence": 0.9, "evidence_node_ids": [uuid4()],
            "superseded_by_object": "Bob", "superseded_by_fact": "Bob owns releases"}
    base.update(overrides)
    return base


class ScopeParityTest(unittest.IsolatedAsyncioTestCase):
    """scope_for must apply a signed principal exactly as run_retrieval_query does."""

    async def test_principal_overrides_and_forces_approved(self):
        token = current_principal.set(Principal("member", "chan-a", "private", agent_id=None))
        try:
            scope = temporal_facts.scope_for(access_level="public", channel_id=None, channel_name="ignored",
                                             agent_id=None, approved_only=False)
        finally:
            current_principal.reset(token)
        self.assertEqual((scope.access_level, scope.channel_id, scope.channel_name, scope.approved_only, scope.reader),
                         ("private", "chan-a", None, True, "member"))
        self.assertEqual(scope.default_state, "proposed")

    async def test_other_channel_or_agent_is_rejected_like_retrieval(self):
        token = current_principal.set(Principal("member", "chan-a", "private"))
        try:
            for kwargs in ({"channel_id": "chan-b", "agent_id": None}, {"channel_id": None, "agent_id": "someone"}):
                with self.subTest(kwargs=kwargs):
                    with self.assertRaises(main.HTTPException) as facts_error:
                        temporal_facts.scope_for(access_level="public", channel_name=None, approved_only=True, **kwargs)
                    with patch.object(main, "embed", AsyncMock()):
                        with self.assertRaises(main.HTTPException) as retrieval_error:
                            await main.run_retrieval_query(None, query="q", top_k=1, hops=0, access_level="public",
                                                           min_confidence=0, vector_weight=1, lexical_weight=0, **kwargs)
                    self.assertEqual(facts_error.exception.status_code, retrieval_error.exception.status_code)
        finally:
            current_principal.reset(token)

    async def test_unscoped_stack_caller_keeps_requested_scope(self):
        scope = temporal_facts.scope_for(access_level="public", channel_id="c", channel_name=None, agent_id=None, approved_only=False)
        self.assertEqual((scope.channel_id, scope.approved_only, scope.default_state, scope.reader), ("c", False, "approved", None))


class ReadPathTest(unittest.IsolatedAsyncioTestCase):
    async def test_unavailable_returns_nothing_without_touching_the_database(self):
        for request in (None, request_with(available=False)):
            self.assertEqual(await temporal_facts.for_retrieval(request, [], "q", None), [])
        with patch.object(temporal_facts, "RETRIEVAL_TEMPORAL_FACTS", False):
            self.assertFalse(temporal_facts.available(request_with()))

    async def test_database_errors_never_fail_retrieval(self):
        class Broken:
            def acquire(self):
                raise RuntimeError("database down")
        scope = temporal_facts.Scope("public", "c", None, None, True, "approved", None)
        self.assertEqual(await temporal_facts.for_retrieval(request_with(Broken()), [{"document_id": str(uuid4())}], "q", scope), [])

    def test_serialize_reports_truth_at_the_requested_moment(self):
        item = row()
        before = temporal_facts.serialize(item, datetime(2026, 5, 1, tzinfo=timezone.utc))
        after = temporal_facts.serialize(item, datetime(2026, 11, 1, tzinfo=timezone.utc))
        self.assertTrue(before["is_true"])
        self.assertFalse(after["is_true"])
        self.assertEqual(before["superseded_by"]["object"], "Bob")
        self.assertFalse(temporal_facts.serialize(row(status="contradicted"), datetime(2026, 5, 1, tzinfo=timezone.utc))["is_true"])

    def test_invisible_superseder_is_not_named(self):
        self.assertIsNone(temporal_facts.serialize(row(superseded_by_object=None, superseded_by_fact=None),
                                                   datetime.now(timezone.utc))["superseded_by"])

    def test_queries_reapply_every_source_predicate(self):
        sql = temporal_facts._timeline_select("true", "f.valid_at", "$10")
        for fragment in ("d.content_sha256 = f.source_revision", "nd.content_sha256 = n.source_revision",
                         "knowledge_readers", "gcor.knowledge_evidence_current(d.id)", "gcor.knowledge_evidence_current(nd.id)",
                         "d.channel_id = $3", "d.access_level = $1", "f.access_level = $1"):
            self.assertIn(fragment, sql)
        self.assertNotIn("{", sql)

    @unittest.skipIf(projector_facts is None, "relationship-projector source not present (release image)")
    def test_subject_normalization_matches_the_projector(self):
        for text in ("Release process", "  release   PROCESS. ", "Ünïcode Team!", "Payments (EU)"):
            self.assertEqual(temporal_facts.subject_key(text), projector_facts.normalize_key(text))

    def test_history_is_a_read_path_for_signed_identities(self):
        self.assertIn("/api/facts/history", READ_PATHS)
        self.assertEqual(WORKLOAD_OPERATIONS["/api/facts/history"], "retrieve")


class HistoryEndpointTest(unittest.IsolatedAsyncioTestCase):
    async def test_requires_channel_scope_and_availability(self):
        payload = temporal_facts.FactHistoryRequest(subject="Release process")
        with self.assertRaises(main.HTTPException) as unavailable:
            await temporal_facts.history(request_with(available=False), payload)
        self.assertEqual(unavailable.exception.status_code, 503)
        with self.assertRaises(main.HTTPException) as unscoped:
            await temporal_facts.history(request_with(), payload)
        self.assertEqual(unscoped.exception.status_code, 422)

    async def test_parameters_are_numbered_for_each_view(self):
        captured = []

        class Connection:
            async def fetch(self, sql, *args, **kwargs):
                captured.append((sql, args)); return []

        class Acquire:
            async def __aenter__(self): return Connection()
            async def __aexit__(self, *exc): return False

        pool = SimpleNamespace(acquire=lambda: Acquire())
        moment = datetime(2026, 2, 1, tzinfo=timezone.utc)
        for view, known in (("timeline", None), ("true_at", None), ("timeline", moment), ("true_at", moment)):
            captured.clear()
            payload = temporal_facts.FactHistoryRequest(subject="Release process", predicate="owned by", channel_id="c",
                                                        view=view, as_of=moment, known_at=known)
            result = await temporal_facts.history(request_with(pool), payload)
            sql, args = captured[0]
            highest = max(int(n) for n in re.findall(r"\$(\d+)", sql))
            self.assertEqual(highest, len(args), (view, known))
            self.assertEqual(result["predicate"], "OWNED_BY")
            self.assertEqual(args[7], "release process")
            self.assertEqual(" AND f.expired_at IS NULL" in sql, known is None)


if __name__ == "__main__":
    unittest.main()
