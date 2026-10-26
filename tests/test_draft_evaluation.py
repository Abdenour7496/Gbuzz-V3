import importlib.util
import json
import random
import unittest
from pathlib import Path

root = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("draft", root / "scripts" / "draft-evaluation-set.py")
draft = importlib.util.module_from_spec(spec)
spec.loader.exec_module(draft)

TEXT = "Postmortem INC-6818. The search cluster was unavailable for 336 minutes. Root cause: a bad schema migration."


def detail(doc_id, metadata=None):
    return {"document": {"id": doc_id, "metadata": json.dumps(metadata or {})}, "chunks": [{"ordinal": 0, "content": TEXT}]}


class ParseDraft(unittest.TestCase):
    def test_accepts_only_verbatim_answers(self):
        ok = draft.parse_draft('{"question": "What caused the search outage?", "answer": "a bad schema migration."}', TEXT)
        self.assertEqual(ok, {"question": "What caused the search outage?", "answer": "a bad schema migration"})
        self.assertIsNone(draft.parse_draft('{"question": "What caused the search outage?", "answer": "a network fault"}', TEXT))
        self.assertIsNone(draft.parse_draft("not json", TEXT))
        self.assertIsNone(draft.parse_draft('{"question": "Why?", "answer": "a bad schema migration"}', TEXT))

    def test_prompt_marks_text_untrusted(self):
        self.assertIn("untrusted", draft.draft_prompt("T", TEXT))


class Formats(unittest.TestCase):
    def test_maps_media_types(self):
        self.assertEqual(draft.document_format({"media_type": "application/pdf"}), "pdf")
        self.assertEqual(draft.document_format({"media_type": "image/png"}), "image-ocr")
        self.assertEqual(draft.document_format({"buzz_evidence_ids": ["x"]}), "buzz-message")
        self.assertEqual(draft.document_format({}), "text")


class Build(unittest.TestCase):
    def test_builds_reviewable_cases_from_api_results(self):
        channels = [{"id": "c1", "visibility": "private"}, {"id": "c2", "visibility": "public"}]
        documents = {"c1": [{"id": "d1", "title": "INC-6818", "state": "approved"},
                            {"id": "old", "title": "Old policy", "state": "superseded"},
                            {"id": "new", "title": "New policy", "state": "approved"}],
                     "c2": []}
        details = {"d1": detail("d1", {"media_type": "application/pdf"}), "new": detail("new"),
                   "old": detail("old", {"superseded_by_document_id": "new"})}
        drafter = lambda prompt: '{"question": "What caused the search outage?", "answer": "a bad schema migration"}'
        cases = draft.build(channels, documents, details, drafter, "owner", 5, random.Random(1))
        by_category = {}
        for case in cases:
            by_category.setdefault(case["category"], []).append(case)
            self.assertEqual(case["data_classification"], draft.REVIEW)  # nothing runs before owner review
            self.assertEqual(case["principal_id"], "owner")
        self.assertEqual(len({c["id"] for c in cases}), len(cases))
        drafted = by_category["factual-support"] + by_category["citation"]
        self.assertEqual(len(drafted), 2)
        self.assertIn("pdf", {c["format"] for c in drafted})
        self.assertTrue(all(c["expected_answer_contains"] == ["a bad schema migration"] for c in drafted))
        conflict = by_category["correction-conflict"][0]
        self.assertEqual((conflict["required_document_ids"], conflict["forbidden_document_ids"]), (["new"], ["old"]))
        cross = by_category["cross-channel"]
        self.assertTrue(cross and all(c["channel_id"] == "c2" and c["access_level"] == "public" for c in cross))
        self.assertTrue(all(c["forbidden_document_ids"] for c in cross))
        self.assertTrue(all(c.get("expect_no_answer") for c in by_category["abstention"]))
        report = draft.coverage(cases, {"minimum_cases": 100, "minimum_cases_per_category": 10})
        self.assertEqual(report["drafted_questions"], 2 + len(cross))
        self.assertIn("factual-support", report["categories_below_minimum"])
        self.assertGreater(report["cases_needed_for_minimum"], 0)

    def test_without_model_documents_are_bound_but_questions_left_to_owner(self):
        channels = [{"id": "c1"}]
        cases = draft.build(channels, {"c1": [{"id": "d1", "title": "T", "state": "approved"}]}, {"d1": detail("d1")}, None, "owner", 5, random.Random(1))
        first = cases[0]
        self.assertTrue(first["query"].startswith(draft.REVIEW))
        self.assertEqual(first["required_document_ids"], ["d1"])

    def test_model_must_be_local(self):
        with self.assertRaises(SystemExit):
            draft.ollama("https://api.example.com", "m")


if __name__ == "__main__":
    unittest.main()
