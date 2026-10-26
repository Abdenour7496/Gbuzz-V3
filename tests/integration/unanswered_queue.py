"""Unanswered questions become channel-scoped work, and submissions can close them. Disposable DB only."""
import asyncio
import json
import os
from datetime import datetime, timezone, timedelta
from uuid import UUID, uuid4

import httpx
from coincurve import PrivateKey

assert os.environ['POSTGRES_DB'] == 'integration'
os.environ['GCOR_ACCESS_MODE'] = 'buzz'
os.environ['GCOR_PUBLIC_ORIGIN'] = 'http://test'
import main
from tests.integration.admin_db import admin_pool
from tests.test_enterprise_access import signed_event
from enterprise_workflows import process_one
from access_policy import Principal, current_principal


async def drain(admin, job_id):
    for _ in range(25):
        token = current_principal.set(Principal('ingestion-worker', '', '', role='service', workload=True, operations=frozenset({'ingest'})))
        try:
            await process_one(main.app)
        finally:
            current_principal.reset(token)
        job = await admin.fetchrow('SELECT status,result,error FROM gcor.ingestion_jobs WHERE id=$1', job_id)
        if job['status'] == 'completed':
            return json.loads(job['result'])
        assert job['status'] != 'failed', dict(job)
        await asyncio.sleep(1)
    raise AssertionError('job did not complete')


async def run():
    async with main.lifespan(main.app):
        assert main.app.state.unanswered_available
        admin = await admin_pool()
        community = uuid4()
        channel, other_channel = uuid4(), uuid4()
        member, colleague, outsider = PrivateKey(), PrivateKey(), PrivateKey()
        for ch in (channel, other_channel):
            await admin.execute("INSERT INTO public.channels(id,community_id,visibility) VALUES($1,$2,'private')", ch, community)
        for key, ch in ((member, channel), (colleague, channel), (outsider, other_channel)):
            pub = key.public_key_xonly.format()
            await admin.execute('INSERT INTO public.users(community_id,pubkey) VALUES($1,$2)', community, pub)
            await admin.execute("INSERT INTO public.channel_members(community_id,channel_id,pubkey,role) VALUES($1,$2,$3,'member')", community, ch, pub)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(main.app), base_url='http://test') as client:
            async def call(path, payload, signer, ch=channel):
                body = json.dumps({'channel_id': str(ch), **payload}).encode()
                return await client.post(path, content=body, headers={'Content-Type': 'application/json',
                    'Authorization': 'Nostr ' + signed_event(signer, body, url='http://test' + path).decode()})

            marker = uuid4().hex[:10]
            question = f'Who approves the zephyr{marker} vendor contract?'
            first = await call('/api/ask', {'query': question}, member)
            assert first.status_code == 200 and not first.json()['chunks'], first.text
            assert (await call('/api/ask', {'query': question.upper() + '  '}, colleague)).status_code == 200
            assert (await call('/api/ask', {'query': question}, member)).status_code == 200
            row = await admin.fetchrow('SELECT * FROM gcor.unanswered_questions WHERE channel_id=$1', str(channel))
            assert row['asked_count'] == 3 and len(row['askers']) == 2 and row['status'] == 'open', dict(row)

            listed = await call('/api/workspace/unanswered', {}, colleague)
            assert listed.status_code == 200, listed.text
            items = listed.json()
            assert [i['question'] for i in items] == [question] and items[0]['distinct_askers'] == 2
            assert 'askers' not in items[0]  # identities are never returned
            # Another channel neither sees nor can address this question.
            assert (await call('/api/workspace/unanswered', {}, outsider, other_channel)).json() == []
            assert (await call('/api/workspace/unanswered', {}, outsider)).status_code in (401, 403)
            assert (await call('/api/workspace/unanswered/resolve', {'question_id': str(row['id']), 'action': 'dismissed'},
                               outsider, other_channel)).status_code in (403, 409)

            # A member answers by submitting knowledge that declares the question it answers.
            submitted = await call('/api/workspace/submit', {'request_id': str(uuid4()), 'title': f'Zephyr{marker} contracts',
                                   'text': f'The zephyr{marker} vendor contract is approved by the finance director.',
                                   'answers_question_id': str(row['id'])}, member)
            assert submitted.status_code == 202, submitted.text
            result = await drain(admin, UUID(submitted.json()['id']))
            linked = await admin.fetchrow('SELECT status,answer_document_id,resolved_by FROM gcor.unanswered_questions WHERE id=$1', row['id'])
            assert linked['status'] == 'answered' and str(linked['answer_document_id']) == result['document_id'], dict(linked)
            shown = (await call('/api/workspace/unanswered', {'status': 'answered'}, colleague)).json()
            assert shown[0]['answer_state'] == 'proposed'

            # Asked again while the answer awaits review: stays answered (approved-only retrieval still abstains).
            await call('/api/ask', {'query': question}, colleague)
            assert await admin.fetchval('SELECT status FROM gcor.unanswered_questions WHERE id=$1', row['id']) == 'answered'

            # Members cannot dismiss; admins can, and can reopen.
            assert (await call('/api/workspace/unanswered/resolve', {'question_id': str(row['id']), 'action': 'dismissed'}, member)).status_code == 403
            await admin.execute("UPDATE public.channel_members SET role='admin' WHERE channel_id=$1 AND pubkey=$2", channel, member.public_key_xonly.format())
            reopened = await call('/api/workspace/unanswered/resolve', {'question_id': str(row['id']), 'action': 'reopen'}, member)
            assert reopened.status_code == 200 and reopened.json()['status'] == 'open', reopened.text
            dismissed = await call('/api/workspace/unanswered/resolve', {'question_id': str(row['id']), 'action': 'dismissed', 'note': 'out of scope'}, member)
            assert dismissed.status_code == 200 and dismissed.json()['status'] == 'dismissed'
            bad_link = await call('/api/workspace/unanswered/resolve', {'question_id': str(row['id']), 'action': 'answered', 'document_id': str(uuid4())}, member)
            assert bad_link.status_code == 404

            # Once the answer is approved and found, the question is not recorded again.
            detail = (await call('/api/workspace/detail', {'document_id': result['document_id']}, member)).json()['document']
            approved = await call('/api/workspace/review', {'document_id': result['document_id'], 'request_id': str(uuid4()),
                                  'expected_updated_at': detail['updated_at'], 'state': 'approved',
                                  'owner_pubkey': member.public_key_xonly.format().hex(),
                                  'review_due': (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()}, member)
            assert approved.status_code == 200, approved.text
            before = await admin.fetchval('SELECT asked_count FROM gcor.unanswered_questions WHERE id=$1', row['id'])
            found = await call('/api/ask', {'query': f'zephyr{marker} vendor contract'}, colleague)
            assert found.json()['chunks'], found.text
            assert await admin.fetchval('SELECT asked_count FROM gcor.unanswered_questions WHERE id=$1', row['id']) == before

            # Runtime role cannot read another channel's rows directly (RLS).
            pool = main.app.state.pool
            token = current_principal.set(Principal(outsider.public_key_xonly.format().hex(), str(other_channel), 'private', role='member'))
            try:
                assert await pool.fetchval('SELECT count(*) FROM gcor.unanswered_questions WHERE channel_id=$1', str(channel)) == 0
            finally:
                current_principal.reset(token)
        await admin.close()
    print('PASS: unanswered questions recorded per channel, de-duplicated, identities withheld, linked by submissions, admin-only dismissal, RLS scoped')


asyncio.run(run())
