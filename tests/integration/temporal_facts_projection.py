"""Bi-temporal facts end to end on real PostgreSQL, using the relationship projector's own code and
restricted database role with a scripted model. Disposable integration database only.

Runs in the relationship-projector image (see docker-compose.integration.yml):
    python /checks/temporal_facts_projection.py
"""
import asyncio
import json
import os
import secrets
from datetime import datetime, timezone
from uuid import uuid4

import asyncpg
import httpx

os.environ["RELATIONSHIP_TEMPORAL_FACTS"] = "true"
import main as projector  # relationship-projector/main.py
import temporal_facts

assert os.getenv("POSTGRES_DB", "integration") == "integration"
MODEL = "integration-model"
NEXT_RESPONSE: dict = {}


def at(text):
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


async def connect(user, password, **kwargs):
    return await (asyncpg.create_pool if kwargs.pop("pool", False) else asyncpg.connect)(
        host=os.getenv("POSTGRES_HOST", "postgres"), database=os.getenv("POSTGRES_DB", "integration"),
        user=user, password=password, **kwargs,
    )


def scripted_model(request: httpx.Request) -> httpx.Response:
    body = json.loads(request.content)
    assert body["model"] == MODEL
    assert ("entities, claims, links, facts." in body["prompt"]) == NEXT_RESPONSE.pop("_expect_fact_prompt")
    return httpx.Response(200, json={"response": json.dumps(NEXT_RESPONSE)})


async def make_document(admin, channel, title, approved_at, texts, state="approved"):
    document = uuid4()
    await admin.execute(
        """INSERT INTO gcor.documents(id,content_sha256,identity_sha256,title,object_key,access_level,metadata)
           VALUES($1,$2,$3,$4,'facts-test','private',$5::jsonb)""",
        document, secrets.token_hex(32), secrets.token_hex(32), title,
        json.dumps({"channel_id": channel, "knowledge_state": state, "knowledge_approved_at": approved_at}),
    )
    nodes = []
    for ordinal, text in enumerate(texts):
        node = await admin.fetchval(
            "INSERT INTO gcor.nodes(document_id,node_type,label,content,access_level) VALUES($1,'Chunk',$2,$3,'private') RETURNING id",
            document, f"{title} {ordinal}", text,
        )
        await admin.execute(
            "INSERT INTO gcor.chunks(document_id,node_id,ordinal,content,embedding) VALUES($1,$2,$3,$4,$5::vector)",
            document, node, ordinal, text, "[" + ",".join(["0"] * 7 + ["1"]) + "]",
        )
        nodes.append(node)
    return document, nodes


async def project(admin, pool, client, document, owner, response, expect_fact_prompt=True):
    await admin.execute(
        """INSERT INTO gcor.relationship_projection(document_id,source_revision,channel_id,status,attempts,model,lease_owner,lease_expires_at)
           SELECT id,content_sha256,metadata->>'channel_id','processing',1,$2,$3,now()+interval '5 minutes' FROM gcor.documents WHERE id=$1
           ON CONFLICT(document_id) DO UPDATE SET source_revision=EXCLUDED.source_revision,status='processing',
             lease_owner=EXCLUDED.lease_owner,lease_expires_at=EXCLUDED.lease_expires_at""",
        document, MODEL, owner,
    )
    row = await admin.fetchrow(
        """SELECT d.id,d.content_sha256,d.access_level,d.agent_id,d.metadata->>'channel_id' AS channel_id,
                  d.created_at,d.metadata->>'knowledge_approved_at' AS approved_at,d.metadata->'knowledge_readers' AS readers
           FROM gcor.documents d WHERE d.id=$1""",
        document,
    )
    NEXT_RESPONSE.clear()
    NEXT_RESPONSE.update(response, _expect_fact_prompt=expect_fact_prompt)
    await projector.project_document(pool, client, row, MODEL, owner)
    assert await admin.fetchval("SELECT status FROM gcor.relationship_projection WHERE document_id=$1", document) == "projected"


def owned_by(name, ordinal=0, **extra):
    return {"subject": "Release process", "predicate": "OWNED_BY", "object": name,
            "evidence_ordinals": [ordinal], "confidence": 0.9, **extra}


def entities(name):
    return {"entities": [{"key": name, "text": f"{name} is a person", "evidence_ordinals": [0], "confidence": 0.9}],
            "claims": [], "links": []}


async def timeline(admin, channel):
    rows = await admin.fetch(
        """SELECT fact_id, object, valid_at, invalid_at, status, invalidated_by, corroborates FROM gcor.facts
           WHERE channel_id=$1 AND predicate='OWNED_BY' AND expired_at IS NULL ORDER BY valid_at NULLS FIRST""",
        channel,
    )
    return {row["object"]: row for row in rows}


async def true_at(admin, channel, moment):
    return await admin.fetchval(
        """SELECT object FROM gcor.facts WHERE channel_id=$1 AND predicate='OWNED_BY' AND expired_at IS NULL
             AND status IN ('active','superseded') AND tstzrange(valid_at, invalid_at) @> $2::timestamptz""",
        channel, moment,
    )


async def expect(error, coroutine, message):
    try:
        await coroutine
    except error:
        return
    raise AssertionError(message)


async def main():
    admin = await connect("integration", "integration")
    runtime = await connect("gcor_runtime", "runtime-secret")
    pool = await connect("relationship_integration", "relationship-secret", pool=True, min_size=1, max_size=2)
    client = httpx.AsyncClient(transport=httpx.MockTransport(scripted_model))
    channel, other_channel = str(uuid4()), str(uuid4())
    owner = uuid4()
    try:
        async with pool.acquire() as connection:
            async with connection.transaction():
                await projector.set_workload(connection)
                projector.FACTS_STATE["available"] = await temporal_facts.available(connection)
        assert projector.FACTS_STATE["available"], "projector role cannot write gcor.facts"

        # 1. March: Alice owns releases. Entities still project; an invalid fact is dropped, not fatal.
        doc_a, nodes_a = await make_document(admin, channel, "March handbook", "2026-03-01T00:00:00+00:00",
                                             ["Alice owns the release process.", "Payments uses Postgres."])
        await project(admin, pool, client, doc_a, owner, {**entities("Alice"), "facts": [
            owned_by("Alice"),
            {"subject": "Payments", "predicate": "USES", "object": "Postgres", "evidence_ordinals": [1], "confidence": 0.8},
            owned_by("Mallory", ordinal=99),
        ]})
        assert await admin.fetchval("SELECT node_count FROM gcor.relationship_projection WHERE document_id=$1", doc_a) == 1
        line = await timeline(admin, channel)
        assert set(line) == {"Alice"} and line["Alice"]["status"] == "active", line
        assert line["Alice"]["valid_at"] == at("2026-03-01") and line["Alice"]["invalid_at"] is None
        believed_in_march = await admin.fetchval("SELECT now()")
        await asyncio.sleep(0.05)

        # 2. October: Bob takes over. Alice is closed at Bob's start and points to Bob; history is kept.
        doc_b, _ = await make_document(admin, channel, "October update", "2026-10-01T00:00:00+00:00",
                                       ["Bob now owns the release process."])
        await project(admin, pool, client, doc_b, owner, {**entities("Bob"), "facts": [owned_by("Bob")]})
        line = await timeline(admin, channel)
        assert line["Alice"]["status"] == "superseded" and line["Alice"]["invalid_at"] == at("2026-10-01"), line
        assert line["Alice"]["invalidated_by"] == line["Bob"]["fact_id"] and line["Bob"]["status"] == "active"
        assert await admin.fetchval("SELECT count(*) FROM gcor.facts WHERE fact_id=$1", line["Alice"]["fact_id"]) == 2
        believed = await admin.fetchrow(
            """SELECT status, invalid_at FROM gcor.facts WHERE fact_id=$1
                 AND recorded_at <= $2 AND (expired_at IS NULL OR expired_at > $2)""",
            line["Alice"]["fact_id"], believed_in_march,
        )
        assert believed["status"] == "active" and believed["invalid_at"] is None, "record-time history lost"

        # 3. Late knowledge about the past: Carol owned it from January. She slots in before Alice.
        doc_c, _ = await make_document(admin, channel, "Archive note", "2026-10-15T00:00:00+00:00",
                                       ["Carol owned the release process from January 2026."])
        await project(admin, pool, client, doc_c, owner, {**entities("Carol"), "facts": [owned_by("Carol", valid_from="2026-01-01")]})
        line = await timeline(admin, channel)
        assert line["Carol"]["status"] == "superseded" and line["Carol"]["invalid_at"] == at("2026-03-01"), line
        assert await true_at(admin, channel, at("2026-02-01")) == "Carol"
        assert await true_at(admin, channel, at("2026-05-01")) == "Alice"
        assert await true_at(admin, channel, datetime.now(timezone.utc)) == "Bob"

        # 4. Restating a current value corroborates instead of creating a new period.
        doc_d, _ = await make_document(admin, channel, "Team page", "2026-10-20T00:00:00+00:00", ["Bob owns releases."])
        await project(admin, pool, client, doc_d, owner, {**entities("Bob page"), "facts": [owned_by("Bob")]})
        line_d = await admin.fetchrow(
            "SELECT status, corroborates FROM gcor.facts WHERE source_document_id=$1 AND expired_at IS NULL", doc_d)
        assert line_d["status"] == "corroborating" and line_d["corroborates"] == line["Bob"]["fact_id"]

        # 5. Retracting Bob's source (back to draft) reinstates Alice as current, via new versions.
        await admin.execute("UPDATE gcor.documents SET metadata=metadata||'{\"knowledge_state\":\"draft\"}' WHERE id=$1", doc_b)
        await projector.invalidate_stale(pool)
        line = await timeline(admin, channel)
        assert "Bob" in line and line["Bob"]["status"] == "active", "the corroborating page should now carry Bob"
        assert await admin.fetchval("SELECT count(*) FROM gcor.facts WHERE source_document_id=$1 AND expired_at IS NULL", doc_b) == 0
        assert await admin.fetchval(
            "SELECT expiry_reason FROM gcor.facts WHERE source_document_id=$1 ORDER BY recorded_at DESC LIMIT 1", doc_b) == "source_retired"
        assert line["Alice"]["invalid_at"] == at("2026-10-20"), line

        # 6. Deleting a source cascades; timelines that pointed at it are repaired.
        await admin.execute("DELETE FROM gcor.relationship_projection WHERE document_id=$1", doc_a)
        await admin.execute("DELETE FROM gcor.documents WHERE id=$1", doc_a)
        await projector.repair_dangling_facts(pool)
        line = await timeline(admin, channel)
        assert "Alice" not in line and line["Carol"]["invalid_at"] == at("2026-10-20"), line
        assert line["Carol"]["invalidated_by"] == line["Bob"]["fact_id"]

        # 7. A fact failure is isolated: the entity projection still commits, with no partial facts.
        original = temporal_facts.reconcile_groups

        async def broken(*args, **kwargs):
            raise RuntimeError("simulated reconcile failure")

        temporal_facts.reconcile_groups = broken
        try:
            doc_e, _ = await make_document(admin, channel, "Isolation", "2026-10-21T00:00:00+00:00", ["Erin owns releases."])
            await project(admin, pool, client, doc_e, owner, {**entities("Erin"), "facts": [owned_by("Erin")]})
        finally:
            temporal_facts.reconcile_groups = original
        assert await admin.fetchval("SELECT count(*) FROM gcor.facts WHERE source_document_id=$1", doc_e) == 0
        assert await admin.fetchval("SELECT node_count FROM gcor.relationship_projection WHERE document_id=$1", doc_e) == 1

        # 8. Feature off: the original prompt and three-key response still work, and no facts are written.
        projector.FACTS_STATE["available"] = False
        doc_f, _ = await make_document(admin, channel, "Facts off", "2026-10-22T00:00:00+00:00", ["Frank owns releases."])
        await project(admin, pool, client, doc_f, owner, entities("Frank"), expect_fact_prompt=False)
        assert await admin.fetchval("SELECT count(*) FROM gcor.facts WHERE source_document_id=$1", doc_f) == 0
        projector.FACTS_STATE["available"] = True

        # 8b. Reader-restricted knowledge never feeds the shared timeline: not at projection time, and a
        # source that becomes restricted later is retired by the repair pass.
        doc_r, _ = await make_document(admin, channel, "Restricted", "2026-10-23T00:00:00+00:00", ["Rita owns releases."])
        await admin.execute("UPDATE gcor.documents SET metadata=metadata||'{\"knowledge_readers\":[\"someone\"]}' WHERE id=$1", doc_r)
        await project(admin, pool, client, doc_r, owner, entities("Rita"), expect_fact_prompt=False)
        assert await admin.fetchval("SELECT count(*) FROM gcor.facts WHERE source_document_id=$1", doc_r) == 0
        await admin.execute("UPDATE gcor.documents SET metadata=metadata||'{\"knowledge_readers\":[\"someone\"]}' WHERE id=$1", doc_d)
        await projector.repair_dangling_facts(pool)
        assert await admin.fetchval("SELECT count(*) FROM gcor.facts WHERE source_document_id=$1 AND expired_at IS NULL", doc_d) == 0
        assert "Bob" not in await timeline(admin, channel)
        await admin.execute("UPDATE gcor.documents SET metadata=metadata-'knowledge_readers' WHERE id=$1", doc_d)
        await admin.execute("UPDATE gcor.relationship_projection SET status='stale' WHERE document_id=$1", doc_d)
        await project(admin, pool, client, doc_d, owner, {**entities("Bob page"), "facts": [owned_by("Bob")]})
        assert (await timeline(admin, channel))["Bob"]["status"] == "active"

        # 9. The single-value timeline cannot overlap, even for a superuser writing directly.
        bob = (await timeline(admin, channel))["Bob"]
        overlap = admin.execute(
            """INSERT INTO gcor.facts(fact_id,channel_id,access_level,subject,subject_key,predicate,cardinality,object,object_key,fact,
                   valid_at,valid_at_source,status,source_document_id,source_revision,source_time,evidence_node_ids,confidence,projector)
               SELECT gen_random_uuid(),channel_id,access_level,subject,subject_key,predicate,cardinality,'Zed','zed','Zed owns it',
                   valid_at + interval '1 day','stated','active',source_document_id,source_revision,source_time,evidence_node_ids,1,projector
               FROM gcor.facts WHERE fact_id=$1 AND expired_at IS NULL""",
            bob["fact_id"],
        )
        await expect(asyncpg.ExclusionViolationError, overlap, "overlapping single-valued facts were accepted")

        # 10. History is immutable and append-only for the projector.
        await expect(asyncpg.CheckViolationError, _projector_update(pool, bob["fact_id"]), "projector rewrote a fact version")
        await expect(asyncpg.InsufficientPrivilegeError, _projector_sql(pool, "DELETE FROM gcor.facts"), "projector deleted facts")
        await expect(asyncpg.InsufficientPrivilegeError, _projector_forge(pool, doc_b, nodes_a), "projector forged a fact for a draft source")
        await expect(asyncpg.InsufficientPrivilegeError, _projector_forge(pool, doc_d, nodes_a), "projector cited another document's evidence")

        # 11. The runtime role reads facts only through the source document's channel scope, and never writes.
        await runtime.execute("SELECT set_config('gcor.channel_id',$1,false), set_config('gcor.access_level','private',false)", channel)
        assert await runtime.fetchval("SELECT count(*) FROM gcor.facts WHERE channel_id=$1", channel) > 0
        await runtime.execute("SELECT set_config('gcor.channel_id',$1,false)", other_channel)
        assert await runtime.fetchval("SELECT count(*) FROM gcor.facts") == 0, "facts leaked across channels"
        await runtime.execute("RESET gcor.channel_id")
        assert await runtime.fetchval("SELECT count(*) FROM gcor.facts") == 0, "unscoped runtime read facts"
        await expect(asyncpg.InsufficientPrivilegeError, runtime.execute("DELETE FROM gcor.facts"), "runtime deleted facts")
        await expect(asyncpg.InsufficientPrivilegeError, runtime.execute("UPDATE gcor.facts SET object='x'"), "runtime updated facts")
        print("PASS: temporal facts supersede, slot in late knowledge, corroborate, retract, repair, isolate failures,"
              " stay immutable and channel-scoped")
    finally:
        await client.aclose()
        await pool.close()
        await runtime.close()
        await admin.close()


async def _projector_update(pool, fact_id):
    async with pool.acquire() as connection:
        async with connection.transaction():
            await projector.set_workload(connection)
            await connection.execute("UPDATE gcor.facts SET object='Mallory' WHERE fact_id=$1 AND expired_at IS NULL", fact_id)


async def _projector_sql(pool, sql):
    async with pool.acquire() as connection:
        async with connection.transaction():
            await projector.set_workload(connection)
            await connection.execute(sql)


async def _projector_forge(pool, document, evidence):
    async with pool.acquire() as connection:
        async with connection.transaction():
            await projector.set_workload(connection)
            source = await connection.fetchrow(
                "SELECT content_sha256, metadata->>'channel_id' AS channel_id FROM gcor.documents WHERE id=$1", document)
            revision = source["content_sha256"] if source else "0" * 64
            channel = source["channel_id"] if source else "unknown"
            await connection.execute(
                """INSERT INTO gcor.facts(fact_id,channel_id,access_level,subject,subject_key,predicate,cardinality,object,object_key,fact,
                       valid_at,valid_at_source,status,source_document_id,source_revision,source_time,evidence_node_ids,confidence,projector)
                   VALUES(gen_random_uuid(),$1,'private','Forged','forged','STATUS','one','x','x','forged',now(),'stated','active',
                          $2,$3,now(),$4,1,'postgres_relationship_v1')""",
                channel, document, revision, evidence,
            )


if __name__ == "__main__":
    asyncio.run(main())
