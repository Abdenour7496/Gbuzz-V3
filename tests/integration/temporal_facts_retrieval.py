"""Temporal facts on the read side: signed Buzz identities, row-level security, source predicates,
record-time history and the kill switch. Disposable integration database only."""
import asyncio
import json
import os
import secrets
from datetime import datetime, timezone
from uuid import uuid4

import httpx
from coincurve import PrivateKey

assert os.environ['POSTGRES_DB'] == 'integration'
os.environ['GCOR_ACCESS_MODE'] = 'buzz'
os.environ['GCOR_PUBLIC_ORIGIN'] = 'http://test'
import main
import temporal_facts
from tests.integration.admin_db import admin_pool
from tests.test_enterprise_access import signed_event
from access_policy import Principal, current_principal


def at(text):
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


async def document(admin, channel, title, text, state='approved', readers=None):
    doc, revision = uuid4(), secrets.token_hex(32)
    metadata = {'channel_id': str(channel), 'knowledge_state': state}
    if readers is not None:
        metadata['knowledge_readers'] = readers
    await admin.execute(
        """INSERT INTO gcor.documents(id,content_sha256,identity_sha256,title,object_key,access_level,metadata)
           VALUES($1,$2,$3,$4,'facts-read','private',$5::jsonb)""",
        doc, revision, secrets.token_hex(32), title, json.dumps(metadata))
    node = await admin.fetchval(
        "INSERT INTO gcor.nodes(document_id,node_type,label,content,access_level) VALUES($1,'Chunk',$2,$3,'private') RETURNING id",
        doc, title, text)
    await admin.execute(
        "INSERT INTO gcor.chunks(document_id,node_id,ordinal,content,embedding) VALUES($1,$2,0,$3,'[1,0,0,0,0,0,0,0]')",
        doc, node, text)
    return doc, revision, node


async def fact(admin, source, channel, subject, predicate, obj, *, valid_at, invalid_at=None, status='active',
               invalidated_by=None, fact_id=None, recorded_at=None, expired_at=None):
    doc, revision, node = source
    fact_id = fact_id or uuid4()
    await admin.execute(
        """INSERT INTO gcor.facts(fact_id,channel_id,access_level,subject,subject_key,predicate,cardinality,object,object_key,fact,
               valid_at,valid_at_source,invalid_at,status,invalidated_by,recorded_at,expired_at,expiry_reason,
               source_document_id,source_revision,source_time,evidence_node_ids,confidence,projector)
           VALUES($1,$2,'private',$3,lower($3),$4,'one',$5,lower($5),$3||' '||$4||' '||$5,$6,'stated',$7,$8,$9,
                  COALESCE($10,now()),$11,CASE WHEN $11::timestamptz IS NULL THEN NULL ELSE 'reconciled' END,
                  $12,$13,now(),ARRAY[$14::uuid],0.9,'postgres_relationship_v1')""",
        fact_id, str(channel), subject, predicate, obj, valid_at, invalid_at, status, invalidated_by,
        recorded_at, expired_at, doc, revision, node)
    return fact_id


async def run():
    async with main.lifespan(main.app):
        assert main.app.state.temporal_facts_available
        admin = await admin_pool()
        # Minimal Buzz relay tables, as in the other identity tests (no-ops when they already exist).
        await admin.execute('''
            CREATE TABLE IF NOT EXISTS public.channels(id uuid PRIMARY KEY,community_id uuid,name text DEFAULT 'Test',visibility text,deleted_at timestamptz,archived_at timestamptz);
            CREATE TABLE IF NOT EXISTS public.users(community_id uuid,pubkey bytea,deactivated_at timestamptz,agent_type text,agent_owner_pubkey bytea);
            CREATE TABLE IF NOT EXISTS public.channel_members(community_id uuid,channel_id uuid,pubkey bytea,role text DEFAULT 'member',removed_at timestamptz);
        ''')
        community, channel, other_channel = uuid4(), uuid4(), uuid4()
        member, outsider = PrivateKey(), PrivateKey()
        for ch in (channel, other_channel):
            await admin.execute("INSERT INTO public.channels(id,community_id,visibility) VALUES($1,$2,'private')", ch, community)
        for key, ch in ((member, channel), (outsider, other_channel)):
            pub = key.public_key_xonly.format()
            await admin.execute('INSERT INTO public.users(community_id,pubkey) VALUES($1,$2)', community, pub)
            await admin.execute("INSERT INTO public.channel_members(community_id,channel_id,pubkey,role) VALUES($1,$2,$3,'member')", community, ch, pub)

        marker = uuid4().hex[:8]
        subject = f'Release process {marker}'
        handbook = await document(admin, channel, f'Handbook {marker}', f'Alice owns the release process {marker}.')
        update = await document(admin, channel, f'October update {marker}', f'Bob now owns the release process {marker}.')
        draft = await document(admin, channel, f'Draft {marker}', f'Dave owns the release process {marker}.', state='draft')
        restricted = await document(admin, channel, f'Restricted {marker}', f'Payments {marker} is on version 2.', readers=['someone-else'])
        elsewhere = await document(admin, other_channel, f'Elsewhere {marker}', f'Yolanda owns the release process {marker}.')

        bob = uuid4()
        alice = uuid4()
        # Alice was believed current from September until Bob's update was reconciled in October.
        await fact(admin, handbook, channel, subject, 'OWNED_BY', 'Alice', valid_at=at('2026-03-01'), fact_id=alice,
                   recorded_at=at('2026-09-01'), expired_at=at('2026-10-01'))
        await fact(admin, handbook, channel, subject, 'OWNED_BY', 'Alice', valid_at=at('2026-03-01'), invalid_at=at('2026-10-01'),
                   status='superseded', invalidated_by=bob, fact_id=alice, recorded_at=at('2026-10-01'))
        await fact(admin, update, channel, subject, 'OWNED_BY', 'Bob', valid_at=at('2026-10-01'), fact_id=bob, recorded_at=at('2026-10-01'))
        # A draft source's fact must stay hidden even before the projector retires it.
        await fact(admin, draft, channel, subject, 'OWNED_BY', 'Dave', valid_at=at('2026-10-05'), status='contradicted', invalidated_by=bob)
        await fact(admin, elsewhere, other_channel, subject, 'OWNED_BY', 'Yolanda', valid_at=at('2026-01-01'))
        v2 = uuid4()
        await fact(admin, handbook, channel, f'Payments {marker}', 'VERSION', '1', valid_at=at('2026-01-01'),
                   invalid_at=at('2026-06-01'), status='superseded', invalidated_by=v2)
        await fact(admin, restricted, channel, f'Payments {marker}', 'VERSION', '2', valid_at=at('2026-06-01'), fact_id=v2)

        async with httpx.AsyncClient(transport=httpx.ASGITransport(main.app), base_url='http://test') as client:
            async def call(path, payload, signer, ch=channel):
                body = json.dumps({'channel_id': str(ch), **payload}).encode()
                return await client.post(path, content=body, headers={'Content-Type': 'application/json',
                    'Authorization': 'Nostr ' + signed_event(signer, body, url='http://test' + path).decode()})

            # Timeline: only approved, current, in-scope sources; supersession is named.
            response = await call('/api/facts/history', {'subject': subject.upper() + ' '}, member)
            assert response.status_code == 200, response.text
            facts = response.json()['facts']
            assert [f['object'] for f in facts] == ['Alice', 'Bob'], facts
            assert facts[0]['status'] == 'superseded' and facts[0]['superseded_by']['object'] == 'Bob'
            assert not facts[0]['is_true'] and facts[1]['is_true']

            # World time: who owned it in May.
            may = await call('/api/facts/history', {'subject': subject, 'view': 'true_at', 'as_of': '2026-05-01T00:00:00Z'}, member)
            assert [f['object'] for f in may.json()['facts']] == ['Alice'], may.text

            # Record time: in mid-September the knowledge base still believed Alice was current.
            believed = await call('/api/facts/history', {'subject': subject, 'known_at': '2026-09-15T00:00:00Z'}, member)
            believed_facts = believed.json()['facts']
            assert [(f['object'], f['status']) for f in believed_facts] == [('Alice', 'active')], believed.text

            # A superseder from a reader-restricted source is neither returned nor named.
            payments = (await call('/api/facts/history', {'subject': f'payments {marker}'}, member)).json()['facts']
            assert [f['object'] for f in payments] == ['1'] and payments[0]['superseded_by'] is None, payments

            # Channel isolation: another channel's member sees only their own facts and cannot name ours.
            theirs = (await call('/api/facts/history', {'subject': subject}, outsider, other_channel)).json()['facts']
            assert [f['object'] for f in theirs] == ['Yolanda'], theirs
            assert (await call('/api/facts/history', {'subject': subject}, outsider)).status_code in (401, 403)

            # Retrieval responses carry the timelines of the matched documents, additively.
            asked = await call('/api/ask', {'query': f'who owns the release process {marker}'}, member)
            assert asked.status_code == 200, asked.text
            body = asked.json()
            assert body['chunks'] and {'answer', 'citations', 'chunks', 'graph_nodes', 'reflection'} <= set(body)
            assert {f['object'] for f in body['facts'] if f['subject'] == subject} == {'Alice', 'Bob'}, body['facts']
            retrieved = await call('/api/retrieve', {'query': f'release process {marker}', 'top_k': 5, 'hops': 0}, member)
            assert retrieved.status_code == 200, retrieved.text
            assert {f['object'] for f in retrieved.json()['facts'] if f['subject'] == subject} == {'Alice', 'Bob'}

            # Kill switch: retrieval unchanged, facts empty, history unavailable.
            temporal_facts.RETRIEVAL_TEMPORAL_FACTS = False
            try:
                off = await call('/api/ask', {'query': f'who owns the release process {marker}'}, member)
                assert off.status_code == 200 and off.json()['facts'] == [] and off.json()['chunks']
                assert (await call('/api/facts/history', {'subject': subject}, member)).status_code == 503
            finally:
                temporal_facts.RETRIEVAL_TEMPORAL_FACTS = True

        # Row-level security alone keeps another channel's facts out of the runtime role's reach.
        token = current_principal.set(Principal(outsider.public_key_xonly.format().hex(), str(other_channel), 'private', role='member'))
        try:
            assert await main.app.state.pool.fetchval('SELECT count(*) FROM gcor.facts WHERE channel_id=$1', str(channel)) == 0
        finally:
            current_principal.reset(token)
        await admin.close()
    print('PASS: fact timelines are channel-scoped, approval- and reader-filtered, answer as-of and known-at, '
          'appear additively in retrieval and switch off cleanly')


asyncio.run(run())
