import asyncio
import os
import unittest
from unittest import mock

os.environ.setdefault("INGEST_WEBHOOK_SECRET", "test-only")

import unanswered  # noqa: E402
from access_policy import Principal, current_principal  # noqa: E402


class Pool:
    def __init__(self, fail=False):
        self.calls, self.fail = [], fail

    async def execute(self, sql, *args):
        if self.fail:
            raise OSError("database unavailable")
        self.calls.append((sql, args))


def member(channel="11111111-1111-4111-8111-111111111111", access="private"):
    return Principal("a" * 64, channel, access, role="member")


class QuestionKey(unittest.TestCase):
    def test_normalises_case_punctuation_and_space(self):
        self.assertEqual(unanswered.question_key("  Who APPROVES the INC-6818 fix?? "), "who approves the inc-6818 fix")
        self.assertEqual(unanswered.question_key("Who approves the inc-6818 fix"), unanswered.question_key("who approves the INC-6818 fix!"))
        self.assertEqual(len(unanswered.question_key("x " * 500)), 300)


class Recordable(unittest.TestCase):
    def test_requires_channel_scoped_identity(self):
        self.assertTrue(unanswered.recordable(member(), "Who owns the budget?"))
        self.assertFalse(unanswered.recordable(None, "Who owns the budget?"))
        self.assertFalse(unanswered.recordable(Principal("ingestion-worker", "", "", role="service", workload=True), "Who owns it?"))
        self.assertFalse(unanswered.recordable(member(access="secret"), "Who owns it?"))
        self.assertFalse(unanswered.recordable(member(), "??"))
        with mock.patch.object(unanswered, "UNANSWERED_QUEUE_ENABLED", False):
            self.assertFalse(unanswered.recordable(member(), "Who owns the budget?"))


class Record(unittest.TestCase):
    def run_record(self, pool, principal, question):
        token = current_principal.set(principal)
        try:
            with mock.patch.object(unanswered.random, "random", return_value=0.5):
                asyncio.run(unanswered.record(pool, question))
        finally:
            current_principal.reset(token)

    def test_upserts_scoped_row(self):
        pool = Pool()
        self.run_record(pool, member(), "  Who owns the Q4 budget? ")
        sql, args = pool.calls[0]
        self.assertIn("ON CONFLICT (channel_id, access_level, question_key)", sql)
        self.assertEqual(args[:5], ("11111111-1111-4111-8111-111111111111", "private", "Who owns the Q4 budget?", "who owns the q4 budget", "a" * 64))

    def test_never_raises_and_counts_errors(self):
        before = unanswered.UNANSWERED.labels("error")._value.get()
        self.run_record(Pool(fail=True), member(), "Who owns the budget?")
        self.assertEqual(unanswered.UNANSWERED.labels("error")._value.get(), before + 1)

    def test_skips_unscoped_callers(self):
        pool = Pool()
        self.run_record(pool, None, "Who owns the budget?")
        self.assertEqual(pool.calls, [])


if __name__ == "__main__":
    unittest.main()
