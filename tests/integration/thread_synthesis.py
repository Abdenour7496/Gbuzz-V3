"""!knowledge synthesize thread gathers a thread's root and replies, and nothing else. Isolated DB only."""
import asyncio
import json
import os
from datetime import datetime, timezone
from unittest.mock import patch
from uuid import uuid4

from coincurve import PrivateKey

assert os.environ['POSTGRES_DB'] == 'integration'
os.environ['GCOR_INGESTION_WORKER'] = 'false'
import main
from tests.integration.admin_db import admin_pool
from buzz_chat import execute, sign_event


async def run():
    async with main.lifespan(main.app):
        admin = await admin_pool()
        await admin.execute('''
            CREATE TABLE IF NOT EXISTS public.channels(id uuid PRIMARY KEY,community_id uuid,name text,visibility text,deleted_at timestamptz,archived_at timestamptz);
            CREATE TABLE IF NOT EXISTS public.users(community_id uuid,pubkey bytea,deactivated_at timestamptz);
            ALTER TABLE public.users ADD COLUMN IF NOT EXISTS agent_type text;
            ALTER TABLE public.users ADD COLUMN IF NOT EXISTS agent_owner_pubkey bytea;
            CREATE TABLE IF NOT EXISTS public.channel_members(community_id uuid,channel_id uuid,pubkey bytea,role text,removed_at timestamptz);
            CREATE TABLE IF NOT EXISTS public.events(id bytea PRIMARY KEY,pubkey bytea,kind integer,tags jsonb,content text,sig bytea,created_at timestamptz,received_at timestamptz DEFAULT now(),channel_id uuid,deleted_at timestamptz);
        ''')
        channel, community = uuid4(), uuid4()
        alice, bob = PrivateKey(), PrivateKey()
        await admin.execute("INSERT INTO public.channels(id,community_id,name,visibility) VALUES($1,$2,'Thread test','private')", channel, community)
        for key in (alice, bob):
            pub = key.public_key_xonly.format()
            await admin.execute('INSERT INTO public.users(community_id,pubkey) VALUES($1,$2)', community, pub)
            await admin.execute("INSERT INTO public.channel_members(community_id,channel_id,pubkey,role) VALUES($1,$2,$3,'member')", community, channel, pub)
        clock = [int(datetime.now(timezone.utc).timestamp()) - 100]

        async def message(key, content, reply_to=None):
            clock[0] += 1
            tags = [['h', str(channel)]] + ([['e', reply_to, '', 'reply']] if reply_to else [])
            e = sign_event(key, 9, tags, content, created_at=clock[0])
            dt = datetime.fromtimestamp(e['created_at'], timezone.utc)
            await admin.execute('INSERT INTO public.events(id,pubkey,kind,tags,content,sig,created_at,channel_id) VALUES($1,$2,9,$3::jsonb,$4,$5,$6,$7)',
                                bytes.fromhex(e['id']), bytes.fromhex(e['pubkey']), json.dumps(e['tags']), content, bytes.fromhex(e['sig']), dt, channel)
            return dict(event_id=e['id'], author=e['pubkey'], kind=9, tags=e['tags'], content=content, signature=e['sig'], created_at=dt, channel_id=channel)

        with patch.object(main, 'GENERATION_MODEL', ''):
            unrelated = await message(bob, 'Unrelated: the cafeteria menu changes on Mondays and Thursdays.')
            root = await message(alice, 'Should we rotate the backup encryption key every 90 days or every 180 days?')
            r1 = await message(bob, 'Every 90 days; the auditors flagged 180 days as too long last year.', root['event_id'])
            other_root = await message(bob, 'Separate topic: who owns the VPN renewal?')
            await message(alice, 'Finance owns the VPN renewal.', other_root['event_id'])
            r2 = await message(alice, 'Agreed. Decision: rotate the backup key every 90 days, owner is the platform team.', root['event_id'])
            command = await message(bob, '!knowledge synthesize thread', root['event_id'])
            reply = await execute(main.app, command)
            assert 'Proposal saved:' in str(reply), reply
            doc = await admin.fetchrow("SELECT title,metadata FROM gcor.documents WHERE metadata->>'buzz_command_id'=$1", command['event_id'])
            assert doc, 'no proposal recorded'
            metadata = json.loads(doc['metadata'])
            evidence = metadata['buzz_evidence_ids']
            assert evidence[:3] == [root['event_id'], r1['event_id'], r2['event_id']], evidence
            assert unrelated['event_id'] not in evidence and other_root['event_id'] not in evidence
            assert doc['title'].startswith('Thread findings: Should we rotate the backup encryption key'), doc['title']
            assert metadata['knowledge_state'] == 'proposed' and metadata['synthesis_prompt_version'] == 'discussion-evidence-v2'

            # With the relay's thread_metadata, nested replies (which tag their parent, not the root) are included.
            await admin.execute('CREATE TABLE IF NOT EXISTS public.thread_metadata(event_id bytea, channel_id uuid, parent_event_id bytea, root_event_id bytea)')
            nested = await message(bob, 'Nested: the platform team will automate the 90-day rotation.', r1['event_id'])
            for child, parent in ((r1, root), (r2, root), (nested, r1)):
                await admin.execute('INSERT INTO public.thread_metadata(event_id,channel_id,parent_event_id,root_event_id) VALUES($1,$2,$3,$4)',
                                    bytes.fromhex(child['event_id']), channel, bytes.fromhex(parent['event_id']), bytes.fromhex(root['event_id']))
            second = await message(alice, '!knowledge synthesize thread', nested['event_id'])
            assert 'Proposal saved:' in str(await execute(main.app, second))
            evidence = json.loads(await admin.fetchval("SELECT metadata FROM gcor.documents WHERE metadata->>'buzz_command_id'=$1", second['event_id']))['buzz_evidence_ids']
            assert evidence[:4] == [root['event_id'], r1['event_id'], r2['event_id'], nested['event_id']], evidence

            # Outside a thread the command explains what to do instead of guessing.
            lone = await message(alice, '!knowledge synthesize thread')
            try:
                await execute(main.app, lone)
                raise AssertionError('thread synthesis without a thread')
            except __import__('fastapi').HTTPException as error:
                assert error.status_code == 422 and 'reply inside the thread' in error.detail
        await admin.close()
    print('PASS: thread synthesis collects root and replies only, titles the proposal from the thread, and needs a thread')


asyncio.run(run())
