import asyncio
import importlib
import json
import os
import unittest
from unittest import mock

import httpx

os.environ.setdefault("INGEST_WEBHOOK_SECRET", "test-only")

import main  # noqa: E402
import reranker  # noqa: E402


def rows(*ids):
    return [{"document_id": doc, "content": f"text {i}", "title": f"t{i}", "n": i} for i, doc in enumerate(ids)]


class RerankParsing(unittest.TestCase):
    def test_accepts_llama_cpp_jina_and_tei_shapes(self):
        self.assertEqual(reranker.parse_scores({"results": [{"index": 1, "relevance_score": 0.2}, {"index": 0, "relevance_score": 0.9}]}, 2), [0.9, 0.2])
        self.assertEqual(reranker.parse_scores([{"index": 0, "score": -1.5}, {"index": 1, "score": 3}], 2), [-1.5, 3.0])

    def test_rejects_incomplete_or_malformed_replies(self):
        for payload in ({"results": [{"index": 0, "relevance_score": 1}]}, {"results": [{"index": 5, "score": 1}, {"index": 0, "score": 1}]},
                        {"results": [{"index": 0, "score": "high"}, {"index": 1, "score": 1}]}, {"results": [{"index": True, "score": 1}, {"index": 1, "score": 1}]},
                        {"error": "x"}, "nope", None):
            self.assertIsNone(reranker.parse_scores(payload, 2), payload)


class Reordering(unittest.TestCase):
    def test_reorders_by_relevance_and_keeps_fused_order_on_ties(self):
        data = rows("a", "b", "c")
        self.assertEqual([r["document_id"] for r in reranker.reorder(data, [0.1, 0.5, 0.5])], ["b", "c", "a"])

    def test_no_scores_keeps_order(self):
        data = rows("a", "b")
        self.assertEqual(reranker.reorder(data, None), data)
        self.assertEqual(reranker.reorder(data, [1.0]), data)

    def test_reorder_never_adds_rows(self):
        data = rows("a", "b", "c")
        self.assertCountEqual(reranker.reorder(data, [3, 2, 1]), data)

    def test_cap_per_document(self):
        data = rows("a", "a", "a", "b", "c", "b")
        self.assertEqual([r["document_id"] for r in reranker.cap_per_document(data, 4, 1)], ["a", "b", "c"])
        self.assertEqual([r["document_id"] for r in reranker.cap_per_document(data, 4, 2)], ["a", "a", "b", "c"])
        self.assertEqual(reranker.cap_per_document(data, 2, 0), data[:2])


class FailOpen(unittest.TestCase):
    def setUp(self):
        reranker._skip_until = 0.0

    def call(self, handler, documents=("x", "y")):
        transport = httpx.MockTransport(handler)
        real = httpx.AsyncClient
        with mock.patch.object(reranker, "RERANKER_URL", "http://reranker:8080/v1/rerank"), \
             mock.patch.object(reranker.httpx, "AsyncClient", lambda **kw: real(transport=transport, **{k: v for k, v in kw.items() if k != "trust_env"})):
            return asyncio.run(reranker.scores("q", list(documents)))

    def test_success(self):
        def handler(request):
            body = json.loads(request.content)
            self.assertEqual(body["query"], "q")
            self.assertEqual(body["documents"], ["x", "y"])
            return httpx.Response(200, json={"results": [{"index": 0, "relevance_score": 0.1}, {"index": 1, "relevance_score": 0.7}]})
        self.assertEqual(self.call(handler), [0.1, 0.7])

    def test_http_error_timeout_and_garbage_fail_open(self):
        self.assertIsNone(self.call(lambda r: httpx.Response(500)))
        reranker._skip_until = 0.0
        self.assertIsNone(self.call(lambda r: httpx.Response(200, text="not json")))
        reranker._skip_until = 0.0
        def boom(request):
            raise httpx.ReadTimeout("slow", request=request)
        self.assertIsNone(self.call(boom))

    def test_failure_starts_cooldown_then_recovers(self):
        calls = []
        def handler(request):
            calls.append(1)
            return httpx.Response(500) if len(calls) == 1 else httpx.Response(200, json=[{"index": 0, "score": 1}, {"index": 1, "score": 2}])
        self.assertIsNone(self.call(handler))
        self.assertIsNone(self.call(handler))  # skipped during cooldown: no second request
        self.assertEqual(len(calls), 1)
        reranker._skip_until = 0.0  # cooldown elapsed
        self.assertEqual(self.call(handler), [1.0, 2.0])

    def test_disabled_without_url(self):
        with mock.patch.object(reranker, "RERANKER_URL", ""):
            self.assertFalse(reranker.enabled())
            self.assertIsNone(asyncio.run(reranker.scores("q", ["x"])))


class PoolSize(unittest.TestCase):
    def test_unchanged_when_features_off(self):
        with mock.patch.object(reranker, "RERANKER_URL", ""), mock.patch.object(main, "RETRIEVAL_MAX_CHUNKS_PER_DOCUMENT", 0):
            for k in (1, 6, 20):
                self.assertEqual(main.result_pool_size(k), k)

    def test_widened_and_bounded_when_enabled(self):
        with mock.patch.object(reranker, "RERANKER_URL", "http://r"), mock.patch.object(reranker, "RERANK_CANDIDATES", 24):
            self.assertEqual(main.result_pool_size(6), 24)
            self.assertEqual(main.result_pool_size(2), 8)  # never more than the 4*top_k candidates per leg
            self.assertEqual(main.result_pool_size(30), 30)
        with mock.patch.object(reranker, "RERANKER_URL", ""), mock.patch.object(main, "RETRIEVAL_MAX_CHUNKS_PER_DOCUMENT", 1):
            self.assertEqual(main.result_pool_size(6), 18)


class HnswSettings(unittest.TestCase):
    def settings(self, version, mode="auto", ef=0, fail=False):
        class Conn:
            async def fetchval(self, sql):
                return version
            async def close(self):
                pass
        async def connect(**kw):
            if fail:
                raise OSError("down")
            self.assertNotIn("min_size", kw)
            return Conn()
        with mock.patch.object(main, "HNSW_ITERATIVE_SCAN", mode), mock.patch.object(main, "HNSW_EF_SEARCH", ef), \
             mock.patch.object(main.asyncpg, "connect", connect):
            return asyncio.run(main.hnsw_server_settings({"min_size": 1, "max_size": 10, "dsn": "x"}))

    def test_version_parsing(self):
        self.assertEqual(main.parse_version("0.8.1"), (0, 8, 1))
        self.assertEqual(main.parse_version("0.6.2"), (0, 6, 2))
        self.assertEqual(main.parse_version(None), ())

    def test_auto_enables_only_on_supported_pgvector(self):
        self.assertEqual(self.settings("0.8.1"), {"hnsw.iterative_scan": "relaxed_order"})
        self.assertEqual(self.settings("0.6.2"), {})
        self.assertEqual(self.settings(None), {})

    def test_off_and_explicit_modes(self):
        self.assertEqual(self.settings("0.8.1", mode="off"), {})
        self.assertEqual(self.settings("0.8.0", mode="strict_order"), {"hnsw.iterative_scan": "strict_order"})
        self.assertEqual(self.settings("0.7.4", mode="strict_order"), {})
        self.assertEqual(self.settings("0.6.2", ef=100), {"hnsw.ef_search": "100"})

    def test_probe_failure_leaves_defaults(self):
        self.assertEqual(self.settings("0.8.1", fail=True), {})


class IdentifierPattern(unittest.TestCase):
    def test_codes_are_found_with_any_separator_or_case(self):
        for query, expected in (("What was the root cause of INC-6818?", "INC[-_ ]?6818"),
                                ("root cause inc 6818", "inc[-_ ]?6818"),
                                ("INC6818 outage", "INC[-_ ]?6818"),
                                ("ISO 27001 controls", "ISO[-_ ]?27001")):
            self.assertIn(expected, main.identifier_pattern(query), query)
        both = main.identifier_pattern("see KB-00042 and cve-2024-1234")
        self.assertIn("KB[-_ ]?00042", both)
        self.assertIn("cve[-_ ]?2024[-_ ]?1234", both)

    def test_plain_language_has_no_identifier(self):
        for query in ("travel per diem Sydney", "version 2 of the policy", "v2.3 release", "who owns the Q4 budget", ""):
            self.assertIsNone(main.identifier_pattern(query), query)

    def test_pattern_is_bounded_and_alphanumeric_only(self):
        query = " ".join(f"AB-{1000 + i}" for i in range(20)) + " DROP-1; select 1"
        pattern = main.identifier_pattern(query)
        self.assertEqual(pattern.count("[-_ ]?"), 5)  # at most five identifiers
        body = pattern.split("(")[2].split(")")[0]
        self.assertRegex(body, r"^[A-Za-z0-9|\[\]\-_ ?]+$")

    def test_disabled_by_weight(self):
        with mock.patch.object(main, "RETRIEVAL_IDENTIFIER_WEIGHT", 0.0):
            self.assertIsNone(main.identifier_pattern("INC-6818"))


class FusionSql(unittest.TestCase):
    def test_ties_use_fractional_ranks_and_pool_limit(self):
        source = open(main.__file__).read()  # works in the repository and inside the release image
        self.assertIn("count(*) OVER (PARTITION BY s.lexical_score)", source)
        self.assertIn("count(*) OVER (PARTITION BY s.vector_score)", source)
        self.assertIn("LIMIT $17", source)
        self.assertIn("identifier_candidates", source)
        self.assertIn("~* $18", source)


if __name__ == "__main__":
    unittest.main()
