"""Run only in the isolated integration stack, with its API publisher stopped."""
import asyncio
import hashlib
import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import UUID, uuid4

import asyncpg
import main
from tests.integration.admin_db import admin_pool
from governance_outbox import publish_artifact, publish_one


class GovernanceFaultTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.pool = await asyncpg.create_pool(host=main.POSTGRES_HOST, port=main.POSTGRES_PORT, user=main.POSTGRES_USER,
                                             password=main.POSTGRES_PASSWORD, database=main.POSTGRES_DB,
                                             min_size=1, max_size=5, server_settings={'gcor.workload':'ingestion-worker'})
        self.admin = await admin_pool()
        self.s3 = main.object_store_client()
        self.doc = uuid4()
        digest = hashlib.sha256(self.doc.bytes).hexdigest()
        await self.pool.execute(
            """INSERT INTO gcor.documents(id,content_sha256,identity_sha256,title,object_key,metadata)
               VALUES ($1,$2,$2,'Governance fault test','test', $3::jsonb)""", self.doc, digest,
            json.dumps({"bucket": main.MINIO_BUCKET, "knowledge_state": "proposed"}))
        self.entry = await self.pool.fetchval(
            """INSERT INTO gcor.knowledge_entries(session_id,participant_id,source_document_id,
               source_record_id,external_id,entry_type,content,occurred_at)
               SELECT session_id,participant_id,$1,source_record_id,$2,entry_type,content,occurred_at
               FROM gcor.knowledge_entries LIMIT 1 RETURNING id""", self.doc, str(self.doc))
        assert self.entry, "Run smoke.py first to create a canonical session entry"
        await self.pool.execute("INSERT INTO gcor.graphiti_projection(entry_id,graphiti_episode_id,group_id,status,attempts) VALUES ($1,$1,'fault-test','failed',3)", self.entry)
        self.request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(pool=self.pool, governance_available=True)))

    async def asyncTearDown(self):
        await self.pool.execute("DELETE FROM gcor.governance_outbox WHERE document_id=$1", self.doc)
        await self.pool.execute("DELETE FROM gcor.knowledge_entries WHERE id=$1", self.entry)
        await self.pool.execute("DELETE FROM gcor.documents WHERE id=$1", self.doc)
        self.s3.close()
        await self.admin.close()
        await self.pool.close()

    async def approve(self, key=None, **fields):
        return await main.approve_knowledge(main.ApproveKnowledgeRequest(target_document_id=str(self.doc), **fields),
                                           self.request, main.STACK_API_SECRET, idempotency_key=key)

    async def assert_rolled_back(self):
        self.assertEqual("proposed", await self.pool.fetchval("SELECT metadata->>'knowledge_state' FROM gcor.documents WHERE id=$1", self.doc))
        self.assertEqual("failed", await self.pool.fetchval("SELECT status FROM gcor.graphiti_projection WHERE entry_id=$1", self.entry))
        self.assertEqual(0, await self.pool.fetchval("SELECT count(*) FROM gcor.governance_outbox WHERE document_id=$1", self.doc))

    async def test_graph_queue_failure_rolls_back_every_write(self):
        original = main.queue_graphiti_lifecycle
        async def fail_after_queue(*args, **kwargs):
            await original(*args, **kwargs)
            raise RuntimeError("injected after graph update")
        with patch.object(main, "queue_graphiti_lifecycle", fail_after_queue), self.assertRaises(RuntimeError):
            await self.approve()
        await self.assert_rolled_back()

    async def test_outbox_insert_failure_rolls_back_document_and_graph(self):
        await self.admin.execute("ALTER TABLE gcor.governance_outbox ADD CONSTRAINT fault_test_actor CHECK (payload->>'actor' IS DISTINCT FROM 'reject-for-test')")
        try:
            with self.assertRaises(asyncpg.CheckViolationError):
                await self.approve(approved_by="reject-for-test")
            await self.assert_rolled_back()
        finally:
            await self.admin.execute("ALTER TABLE gcor.governance_outbox DROP CONSTRAINT fault_test_actor")

    async def test_concurrent_retries_produce_one_event(self):
        key = str(uuid4())
        responses = await asyncio.gather(*(self.approve(key) for _ in range(8)))
        self.assertTrue(all(response == responses[0] for response in responses))
        self.assertEqual(1, await self.pool.fetchval("SELECT count(*) FROM gcor.governance_outbox WHERE document_id=$1", self.doc))
        self.assertEqual("pending", await self.pool.fetchval("SELECT status FROM gcor.graphiti_projection WHERE entry_id=$1", self.entry))
        with self.assertRaises(main.HTTPException) as error:
            await self.approve(key, note="different request")
        self.assertEqual(409, error.exception.status_code)

    async def test_storage_outage_then_recovery_keeps_event_durable(self):
        response = await self.approve()
        event_id = UUID(response["governance_event_id"])
        unavailable = Mock()
        unavailable.put_object.side_effect = RuntimeError("injected storage outage")
        await publish_one(self.pool, unavailable)
        row = await self.pool.fetchrow("SELECT * FROM gcor.governance_outbox WHERE event_id=$1", event_id)
        self.assertIsNone(row["published_at"])
        self.assertEqual(1, row["attempts"])
        self.assertEqual("approved", await self.pool.fetchval("SELECT metadata->>'knowledge_state' FROM gcor.documents WHERE id=$1", self.doc))
        await self.pool.execute("UPDATE gcor.governance_outbox SET next_attempt_at=now() WHERE event_id=$1", event_id)
        await publish_one(self.pool, self.s3)
        self.assertIsNotNone(await self.pool.fetchval("SELECT published_at FROM gcor.governance_outbox WHERE event_id=$1", event_id))

    async def test_crash_after_object_write_does_not_create_second_version(self):
        response = await self.approve()
        event_id = UUID(response["governance_event_id"])
        row = await self.pool.fetchrow("SELECT * FROM gcor.governance_outbox WHERE event_id=$1", event_id)
        await asyncio.to_thread(publish_artifact, self.s3, row)
        # This models process death after the object write, before the DB acknowledgment.
        await asyncio.gather(publish_one(self.pool, self.s3), publish_one(self.pool, self.s3))
        versions = await asyncio.to_thread(self.s3.list_object_versions, Bucket=row["bucket"], Prefix=row["object_key"])
        self.assertEqual(1, len(versions.get("Versions", [])))
        self.assertIsNotNone(await self.pool.fetchval("SELECT published_at FROM gcor.governance_outbox WHERE event_id=$1", event_id))

    async def test_missing_superseding_document_rolls_back(self):
        with self.assertRaises(main.HTTPException) as error:
            await main.transition_knowledge(main.TransitionKnowledgeRequest(
                target_document_id=str(self.doc), transition="superseded", superseded_by_document_id=str(uuid4())),
                self.request, main.STACK_API_SECRET)
        self.assertEqual(404, error.exception.status_code)
        await self.assert_rolled_back()

    async def test_supersession_edge_and_audit_commit_together(self):
        target = uuid4()
        digest = hashlib.sha256(target.bytes).hexdigest()
        await self.pool.execute(
            "INSERT INTO gcor.documents(id,content_sha256,identity_sha256,title,object_key) VALUES ($1,$2,$2,'Target','test')", target, digest)
        try:
            for doc in (self.doc, self.doc, target):
                await self.pool.execute("INSERT INTO gcor.nodes(document_id,node_type,label) VALUES ($1,'Document','test')", doc)
            result = await main.transition_knowledge(main.TransitionKnowledgeRequest(
                target_document_id=str(self.doc), transition="superseded", superseded_by_document_id=str(target)),
                self.request, main.STACK_API_SECRET)
            self.assertEqual("superseded", result["knowledge_state"])
            self.assertEqual(1, await self.pool.fetchval(
                "SELECT count(*) FROM gcor.edges e JOIN gcor.nodes n ON n.id=e.source_id WHERE n.document_id=$1 AND e.relation='RELATES_TO'", self.doc))
            self.assertEqual(1, await self.pool.fetchval("SELECT count(*) FROM gcor.governance_outbox WHERE document_id=$1", self.doc))
        finally:
            await self.pool.execute("DELETE FROM gcor.documents WHERE id=$1", target)


if __name__ == "__main__":
    unittest.main(verbosity=2)
