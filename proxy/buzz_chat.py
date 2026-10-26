"""Chat-native knowledge agent. Commands originate only from stored signed Buzz events."""
import asyncio
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID, uuid5, NAMESPACE_URL
from urllib.parse import urlsplit

from coincurve import PrivateKey, PublicKeyXOnly
from fastapi import HTTPException
from websockets.asyncio.client import connect

from access_policy import Principal, current_principal
from governance_outbox import json_object
import enterprise_workflows as workflow
from buzz_cards import ChatReply, card_tags
from buzz_synthesis import SynthesisPending

HELP = """Knowledge commands (send as ordinary Buzz messages):
!knowledge propose Title | finding or decision
!knowledge synthesize (latest 50 discussion messages, or the message you reply to)
!knowledge synthesize thread (reply inside a thread: its root and all replies, with speakers)
!knowledge synthesize EVENT_ID [EVENT_ID ...] (up to 200 messages)
!knowledge synthesize session START_EVENT_ID END_EVENT_ID
!knowledge status COMMAND_EVENT_ID
!knowledge cancel COMMAND_EVENT_ID
!knowledge list
!knowledge index (browse topics)
!knowledge topic Title | reusable explanation or procedure
!knowledge revise DOCUMENT_ID REVISION | replacement text
!knowledge changes DOCUMENT_ID
!knowledge related DOCUMENT_ID
!knowledge history DOCUMENT_ID
!knowledge lint (check knowledge maintenance)
!knowledge save ANSWER_EVENT_ID (propose a cited answer as knowledge)
!knowledge show DOCUMENT_ID [PAGE]
!knowledge approve DOCUMENT_ID REVISION
!knowledge reject DOCUMENT_ID REVISION
!knowledge archive DOCUMENT_ID REVISION
!knowledge ask your question
!knowledge feedback DOCUMENT_ID incorrect|outdated|missing|helpful your note
Show displays the exact revision required for review. Only human channel owners/admins can approve. Proposals and AI drafts are not trusted answers until approved. Replies are visible to the channel; restricted documents stay out of chat replies."""


def sign_event(key, kind, tags, content, created_at=None):
    event = dict(pubkey=key.public_key_xonly.format().hex(), created_at=int(time.time()) if created_at is None else created_at,
                 kind=kind, tags=tags, content=content)
    digest = hashlib.sha256(json.dumps([0,event['pubkey'],event['created_at'],kind,tags,content],
                                     ensure_ascii=False,separators=(',',':')).encode()).digest()
    return event | {'id':digest.hex(),'sig':key.sign_schnorr(digest).hex()}


def verified(row):
    tags = json.loads(row['tags']) if isinstance(row['tags'],str) else row['tags']
    event = dict(id=row['event_id'],pubkey=row['author'],kind=row['kind'],tags=tags,
                 content=row['content'],created_at=int(row['created_at'].timestamp()),sig=row['signature'])
    if event['kind'] not in (9,40002) or not isinstance(tags,list): raise ValueError('Unsupported message')
    if [t for t in tags if t and t[0]=='h'] != [['h',str(row['channel_id'])]]: raise ValueError('Channel mismatch')
    digest=hashlib.sha256(json.dumps([0,event['pubkey'],event['created_at'],event['kind'],tags,event['content']],
                                   ensure_ascii=False,separators=(',',':')).encode()).digest()
    if digest.hex()!=event['id'] or not PublicKeyXOnly(bytes.fromhex(event['pubkey'])).verify(bytes.fromhex(event['sig']),digest):
        raise ValueError('Invalid message signature')
    return event


async def principal(pool, channel, author):
    row=await pool.fetchrow('''SELECT c.visibility::text AS visibility,m.role::text AS role,u.agent_type,u.agent_owner_pubkey
        FROM public.channels c JOIN public.channel_members m ON m.channel_id=c.id AND m.community_id=c.community_id
        JOIN public.users u ON u.community_id=c.community_id AND u.pubkey=m.pubkey
        WHERE c.id=$1 AND m.pubkey=$2 AND m.removed_at IS NULL AND u.deactivated_at IS NULL
        AND c.deleted_at IS NULL AND c.archived_at IS NULL''',UUID(str(channel)),bytes.fromhex(author))
    if not row: raise PermissionError('Active membership required')
    agent=bool(row.get('agent_type')) or row['role']=='bot'
    if agent:
        sponsor=row.get('agent_owner_pubkey')
        if not sponsor or not await pool.fetchval('''SELECT 1 FROM public.channel_members m JOIN public.users u
            ON u.community_id=m.community_id AND u.pubkey=m.pubkey WHERE m.channel_id=$1 AND m.pubkey=$2
            AND m.removed_at IS NULL AND u.deactivated_at IS NULL AND u.agent_type IS NULL
            AND m.role::text IN ('owner','admin','member')''',UUID(str(channel)),sponsor):
            raise PermissionError('Agent requires active human sponsor in channel')
    return Principal(author,str(channel),'public' if row['visibility']=='public' else 'private',
                     agent_id=author if agent else None,role=row['role'])


async def source_events(pool, channel, ids, *, max_events=6):
    if not 1<=len(ids)<=max_events or len(set(ids))!=len(ids) or any(not re.fullmatch('[0-9a-f]{64}',i) for i in ids):
        raise HTTPException(422,f'Supply 1 to {max_events} distinct message event IDs')
    rows=await pool.fetch('''SELECT encode(id,'hex') AS event_id,encode(pubkey,'hex') AS author,kind,tags,content,
        created_at,channel_id,encode(sig,'hex') AS signature FROM public.events
        WHERE channel_id=$1 AND encode(id,'hex')=ANY($2::text[]) AND deleted_at IS NULL AND kind IN (9,40002)''',UUID(channel),ids)
    if len(rows)!=len(ids): raise HTTPException(404,'Supporting messages are unavailable in this channel')
    ordered={r['event_id']:r for r in rows}
    for row in rows:
        verified(row)
        if command_text(row['content']).startswith('!knowledge') or any(t and t[0]=='gcor' for t in json_object_tags(row['tags'])):
            raise HTTPException(422,'Use original discussion messages, not knowledge commands or generated replies')
    return [ordered[i] for i in ids]


def json_object_tags(value):
    return json.loads(value) if isinstance(value,str) else value


async def chat_document(pool, doc_id, p):
    row=await workflow.document(pool,UUID(doc_id),p)
    if json_object(row['metadata']).get('knowledge_readers') is not None:
        raise HTTPException(404,'Document is not available for channel-wide replies')
    return row


def command_text(content):
    text=content.strip()
    if text.startswith('`') and text.endswith('`') and not text.startswith('``'):
        text=text[1:-1].strip()
    return text


async def execute(app,row):
    import main
    event=verified(row);p=await principal(app.state.pool,row['channel_id'],event['pubkey'])
    text=command_text(event['content'])
    if len(text)>20000: raise HTTPException(422,'Command is too long')
    parts=text.split(maxsplit=2)
    if not parts or parts[0]!='!knowledge': raise HTTPException(422,'Unknown command')
    action=parts[1].lower() if len(parts)>1 else 'help';args=parts[2] if len(parts)>2 else ''
    token=current_principal.set(p);request=SimpleNamespace(app=app)
    try:
        if action=='help': return ChatReply(HELP,kind='help',title='Knowledge workspace')
        if action in {'status','cancel'}:
            from buzz_synthesis import control
            return await control(app.state.pool,p,action,args)
        if action in {'topic','revise','save','index','related','history','changes','lint'}:
            from buzz_wiki import handle
            return await handle(action,args,app,request,p,event,row)
        if action=='ask':
            # A channel reply must not disclose even the requester's privately restricted documents.
            current_principal.set(Principal('channel-broadcast',p.channel_id,p.access_level))
            result=await main.ask(main.AskRequest(query=args,channel_id=p.channel_id,access_level=p.access_level,approved_only=True),request)
            sources='\n'.join(f'[{i}] {c["title"]}: {c.get("source_uri") or c["document_id"]}' for i,c in enumerate(result['citations'],1))
            dependencies=[]
            for citation in result['citations']:
                doc=await chat_document(app.state.pool,str(citation['document_id']),p)
                dep={'id':str(doc['id']),'revision':doc['updated_at'].isoformat()}
                if dep not in dependencies:dependencies.append(dep)
            answer=str(result['answer'])[:6500]+'\n'+sources[:3500]
            await app.state.pool.execute('UPDATE gcor.buzz_knowledge_commands SET draft=$2::jsonb WHERE event_id=$1',event['id'],
                json.dumps({'question':args,'answer':answer,'dependencies':dependencies}))
            return ChatReply(answer,kind='answer',title='Answer from approved knowledge',saveable=bool(dependencies))
        if action=='list':
            current_principal.set(Principal('channel-broadcast',p.channel_id,p.access_level,role=p.role))
            rows=await workflow.documents(workflow.Channel(channel_id=p.channel_id),request)
            return '\n'.join(f'{r["id"]} | {r["state"]} | {r["title"]}' for r in rows[:25]) or 'No channel knowledge yet. Use !knowledge propose or synthesize.'
        if action in {'propose','synthesize'}:
            if p.role not in {'owner','admin','member','bot'}:raise HTTPException(403,'Contributor required')
            evidence=[];provenance={}
            if action=='propose':
                title,sep,content=args.partition('|')
                if not sep or not title.strip() or not content.strip():raise HTTPException(422,'Use propose Title | finding')
                # Reply tags bind a manual proposal to its supporting discussion.
                ids=list(dict.fromkeys(t[1] for t in event['tags'] if len(t)>1 and t[0]=='e'))
                evidence=await source_events(app.state.pool,p.channel_id,ids) if ids else []
            else:
                from buzz_synthesis import synthesize
                evidence,content,provenance=await synthesize(app,p,event,row,args)
                title='Discussion findings '+event['id'][:8]
                if args.strip()=='thread' and evidence:
                    opening=re.sub(r'\s+',' ',evidence[0]['content']).strip()
                    title=('Thread findings: '+opening[:90]+('…' if len(opening)>90 else ''))[:300]
            refs=[r['event_id'] for r in evidence]
            content=content.strip()+'\n\nSupporting Buzz events:\n'+'\n'.join('buzz://event/'+i for i in refs+[event['id']])
            await principal(app.state.pool,p.channel_id,p.subject)
            result=await main.ingest_payload(request,content=content.encode(),media_type='text/plain',title=title.strip()[:300],
                access_level=p.access_level,agent_id=None,source_uri='buzz://knowledge/'+event['id'],channel_name=None,
                channel_id=p.channel_id,event_id=event['id'],event_kind='knowledge.propose',event_timestamp=row['created_at'].isoformat(),
                author_pubkey=p.subject,file_url=None,file_name='discussion.txt',preserve_existing=True,
                metadata={'knowledge_state':'proposed','knowledge_owner':p.subject,'buzz_command_id':event['id'],
                          'buzz_evidence_ids':refs+[event['id']],'knowledge_origin':'agent_synthesis' if action=='synthesize' else 'chat_proposal',
                          'contributor_is_agent':bool(p.agent_id),**provenance})
            if result.get('quarantined'):raise HTTPException(422,'Proposal quarantined; inspect ingestion diagnostics')
            if action=='synthesize':
                await app.state.pool.execute("UPDATE gcor.buzz_synthesis_jobs SET status='published',updated_at=now() WHERE event_id=$1",event['id'])
            revision=await app.state.pool.fetchval('SELECT updated_at FROM gcor.documents WHERE id=$1',UUID(str(result['document_id'])))
            return ChatReply(f'Proposal saved: {result["document_id"]}. Human review required.\n'+content[:4500]+f'\nInspect: !knowledge show {result["document_id"]}\nApprove: !knowledge approve {result["document_id"]} {revision.isoformat()}',
                kind='proposal',title=title.strip(),document_id=result['document_id'],revision=revision.isoformat(),state='proposed',sources=refs)
        if action=='show':
            fields=args.split()
            if not 1<=len(fields)<=2:raise HTTPException(422,'Use show DOCUMENT_ID [PAGE]')
            page=int(fields[1]) if len(fields)==2 else 1
            if page<1:raise HTTPException(422,'Page must be positive')
            doc=await chat_document(app.state.pool,fields[0],p)
            count=await app.state.pool.fetchval('SELECT count(*) FROM gcor.chunks WHERE document_id=$1',doc['id'])
            pages=max(1,(count+2)//3)
            if page>pages:raise HTTPException(422,'Page is beyond the document')
            chunks=await app.state.pool.fetch('SELECT content FROM gcor.chunks WHERE document_id=$1 ORDER BY ordinal LIMIT 3 OFFSET $2',doc['id'],(page-1)*3)
            navigation=f'\nPage {page} of {pages}.'+(f' Next: !knowledge show {doc["id"]} {page+1}' if page<pages else '')
            meta=json_object(doc['metadata'])
            return ChatReply(f'{doc["title"]}\nState: {meta.get("knowledge_state","proposed")}\nRevision: {doc["updated_at"].isoformat()}\n'+ '\n'.join(c['content'] for c in chunks)+navigation+f'\nReview changes: !knowledge changes {doc["id"]}\nRelated: !knowledge related {doc["id"]}\nHistory: !knowledge history {doc["id"]}',
                kind='document',title=doc['title'],document_id=doc['id'],revision=doc['updated_at'].isoformat(),state=meta.get('knowledge_state','proposed'),sources=meta.get('buzz_evidence_ids',[]))
        if action in {'approve','reject','archive'}:
            if p.agent_id or p.role not in {'owner','admin'}:raise HTTPException(403,'Human channel owner/admin required')
            review_args,_,rationale=args.partition('|')
            doc_id,revision=review_args.split()
            doc=await chat_document(app.state.pool,doc_id,p);meta=json_object(doc['metadata'])
            if action=='approve':
                if meta.get('synthesis_batches',0)>1 and not rationale.strip():
                    raise HTTPException(422,'Review every page and resolve cross-section conflicts, then approve DOCUMENT_ID REVISION | review rationale')
                ids=meta.get('buzz_evidence_ids',[])
                if ids:
                    count=await app.state.pool.fetchval('SELECT count(*) FROM public.events WHERE channel_id=$1 AND encode(id,\'hex\')=ANY($2::text[]) AND deleted_at IS NULL',UUID(p.channel_id),ids)
                    if count!=len(ids):raise HTTPException(409,'Evidence changed or was removed; create a fresh proposal')
            result=await workflow.review(workflow.Review(channel_id=p.channel_id,document_id=doc_id,
                request_id=uuid5(NAMESPACE_URL,event['id']),expected_updated_at=datetime.fromisoformat(revision),
                state={'approve':'approved','reject':'rejected','archive':'archived'}[action],owner_pubkey=p.subject,
                review_due=row['created_at']+timedelta(days=90),note='Buzz review event '+event['id']+(' | '+rationale.strip() if rationale.strip() else '')),request)
            return f'Knowledge {action} recorded for {doc_id}. Review history includes your verified Buzz identity.'
        if action=='feedback':
            doc_id,category,note=args.split(maxsplit=2);await chat_document(app.state.pool,doc_id,p)
            await workflow.feedback(workflow.Feedback(channel_id=p.channel_id,document_id=doc_id,
                request_id=uuid5(NAMESPACE_URL,event['id']),category=category,note=note),request)
            return 'Feedback recorded for '+doc_id
        raise HTTPException(422,'Unknown action. Send !knowledge help')
    finally: current_principal.reset(token)


async def publish(key,event):
    url=os.getenv('BUZZ_KNOWLEDGE_RELAY_URL','ws://relay:3000')
    auth_url=os.getenv('BUZZ_KNOWLEDGE_AUTH_URL',url)
    async with asyncio.timeout(20):
        # Preserve the public authority for Buzz's community routing while dialing Docker DNS.
        target=urlsplit(url)
        async with connect(auth_url,host=target.hostname,port=target.port or 80,proxy=None,max_size=200000) as socket:
            await socket.send(json.dumps(['EVENT',event]))
            async for frame in socket:
                message=json.loads(frame)
                if message[0]=='AUTH':
                    auth=sign_event(key,22242,[['relay',auth_url],['challenge',message[1]]],'')
                    await socket.send(json.dumps(['AUTH',auth]))
                elif message[0]=='OK':
                    if message[1]==event['id'] and message[2]:return
                    if message[1]==event['id'] and not message[2] and not message[3].startswith('auth-required'):
                        raise RuntimeError('Relay rejected knowledge reply')
                    if message[1]!=event['id'] and message[2]:await socket.send(json.dumps(['EVENT',event]))
                    if message[1]!=event['id'] and not message[2]:raise RuntimeError('Relay rejected agent authentication')
    raise RuntimeError('No relay acknowledgement')


async def cycle(app,key):
    pool=app.state.pool;bot=key.public_key_xonly.format().hex()
    # One worker owns command execution and delivery; PostgreSQL releases this lock on disconnect.
    async with pool.acquire() as lock:
        if not await lock.fetchval("SELECT pg_try_advisory_lock(93467122)"):return
        try:
            await invalidate_removed_evidence(app)
            await pool.execute('''INSERT INTO gcor.buzz_knowledge_commands(event_id,channel_id)
                SELECT encode(e.id,'hex'),e.channel_id FROM public.events e JOIN gcor.buzz_knowledge_channels c ON c.channel_id=e.channel_id
                WHERE c.enabled AND e.received_at>=c.enabled_at AND e.deleted_at IS NULL AND e.kind IN (9,40002)
                AND (btrim(e.content, E' \\t\\r\\n`')='!knowledge' OR btrim(e.content, E' \\t\\r\\n`') LIKE '!knowledge %') AND encode(e.pubkey,'hex')<>$1
                AND NOT EXISTS(SELECT 1 FROM gcor.buzz_knowledge_commands old WHERE old.event_id=encode(e.id,'hex'))
                ORDER BY e.received_at LIMIT 1000 ON CONFLICT DO NOTHING''',bot)
            rows=await pool.fetch('''SELECT q.*,e.kind,e.tags,e.content,e.created_at AS event_time,
                encode(e.pubkey,'hex') AS author,encode(e.sig,'hex') AS signature
                FROM gcor.buzz_knowledge_commands q JOIN public.events e ON encode(e.id,'hex')=q.event_id AND e.channel_id=q.channel_id
                JOIN gcor.buzz_knowledge_channels c ON c.channel_id=q.channel_id
                WHERE c.enabled AND q.delivered_at IS NULL AND q.next_attempt_at<=now() AND e.deleted_at IS NULL
                AND (q.response IS NOT NULL OR q.attempts<3) ORDER BY (btrim(e.content, E' \\t\\r\\n`') LIKE '!knowledge cancel %') DESC,q.next_attempt_at,q.created_at LIMIT 10''')
            for stored in rows:
                row=dict(stored);row['created_at']=row.pop('event_time')
                try:
                    await principal(pool,row['channel_id'],row['author'])
                    await principal(pool,row['channel_id'],bot)
                    if row['response'] is not None and time.time()-json_object(row['response'])['created_at']>120:
                        await pool.execute("UPDATE gcor.buzz_knowledge_commands SET delivered_at=now(),error='StaleReplySuppressed' WHERE event_id=$1",row['event_id'])
                        continue
                    if row['response'] is None:
                        try:
                            async with asyncio.timeout(240):text=await execute(app,row)
                        except (HTTPException,ValueError) as error:
                            from buzz_synthesis import fail_job
                            await fail_job(pool,row)
                            text='Knowledge action unavailable: '+(str(error.detail) if isinstance(error,HTTPException) else 'Invalid command or revision. Send !knowledge help')
                        response=sign_event(key,9,[['h',str(row['channel_id'])],['e',row['event_id'],'','reply'],['gcor','knowledge-reply']]+card_tags(text,row['channel_id']),text[:12000])
                        await pool.execute('UPDATE gcor.buzz_knowledge_commands SET response=$2::jsonb WHERE event_id=$1',row['event_id'],json.dumps(response))
                    else:response=json_object(row['response'])
                    sender=await principal(pool,row['channel_id'],row['author'])
                    await principal(pool,row['channel_id'],bot)
                    if not await reply_evidence_current(pool,row['event_id'],sender):
                        await pool.execute("UPDATE gcor.buzz_knowledge_commands SET delivered_at=now(),error='StaleEvidenceSuppressed' WHERE event_id=$1",row['event_id'])
                        continue
                    await publish(key,response)
                    await pool.execute('UPDATE gcor.buzz_knowledge_commands SET delivered_at=now(),error=NULL WHERE event_id=$1',row['event_id'])
                except SynthesisPending:
                    await pool.execute("UPDATE gcor.buzz_knowledge_commands SET error=NULL,next_attempt_at=now()+interval '2 seconds' WHERE event_id=$1",row['event_id'])
                except Exception as error:
                    await pool.execute("UPDATE gcor.buzz_knowledge_commands SET attempts=attempts+1,error=$2,next_attempt_at=now()+interval '30 seconds' WHERE event_id=$1",row['event_id'],type(error).__name__)
                    if isinstance(error,PermissionError) or row['attempts']>=2:
                        from buzz_synthesis import fail_job
                        await fail_job(pool,row)
                    if isinstance(error,PermissionError):
                        await pool.execute('UPDATE gcor.buzz_knowledge_commands SET delivered_at=now() WHERE event_id=$1',row['event_id'])
                finally:
                    __import__('pathlib').Path('/tmp/buzz-chat-heartbeat').touch()
        finally:await lock.execute('SELECT pg_advisory_unlock(93467122)')


async def reply_evidence_current(pool,event_id,p):
    """A persisted answer must not outlive its evidence during delivery retries."""
    draft=json_object(await pool.fetchval('SELECT draft FROM gcor.buzz_knowledge_commands WHERE event_id=$1',event_id))
    dependencies=draft.get('dependencies',[]) if draft else []
    token=current_principal.set(Principal('channel-broadcast',p.channel_id,p.access_level))
    try:
        for dependency in dependencies:
            row=await pool.fetchrow("SELECT updated_at,metadata,gcor.knowledge_evidence_current(id) AS current FROM gcor.documents WHERE id=$1",UUID(dependency['id']))
            if not row or not row['current'] or row['updated_at'].isoformat()!=dependency['revision']:
                return False
            meta=json_object(row['metadata'])
            if meta.get('knowledge_state')!='approved' or meta.get('knowledge_readers') is not None:
                return False
        return True
    finally:current_principal.reset(token)


async def invalidate_removed_evidence(app):
    channels=await app.state.pool.fetch('SELECT c.id,c.visibility::text AS visibility FROM public.channels c JOIN gcor.buzz_knowledge_channels k ON k.channel_id=c.id')
    for channel in channels:
        token=current_principal.set(Principal('system:buzz-evidence',str(channel['id']),
            'public' if channel['visibility']=='public' else 'private',role='service'))
        try: await _invalidate_channel_evidence(app)
        finally: current_principal.reset(token)


async def _invalidate_channel_evidence(app):
    """Withdraw approval when an original supporting event disappears or is deleted."""
    import main
    from governance_service import commit_governance
    # Readers already exclude these rows through gcor.knowledge_evidence_current; this
    # pass records the withdrawal durably (governance event, outbox, Graphiti lifecycle).
    rows=await app.state.pool.fetch('''SELECT d.id,d.metadata,d.updated_at,d.access_level FROM gcor.documents d
        WHERE d.metadata->>'knowledge_state'='approved'
          AND (d.metadata ? 'buzz_evidence_ids' OR d.metadata ? 'wiki_dependencies')
          AND NOT gcor.knowledge_evidence_current(d.id) LIMIT 50''')
    for row in rows:
        meta=json_object(row['metadata']);owner=meta.get('knowledge_owner','')
        p=Principal(owner,meta['channel_id'],row['access_level'],role='admin')
        try:
            await commit_governance(SimpleNamespace(app=app),row['id'],
                {'knowledge_state':'proposed','knowledge_evidence_invalidated':True},'proposed','system:buzz-evidence',
                'Supporting message or knowledge source changed; fresh evidence and review required',
                'buzz-invalidated:'+str(row['id'])+':'+row['updated_at'].isoformat(),'removed-evidence','review',
                main.MINIO_BUCKET,main.queue_graphiti_lifecycle,expected_updated_at=row['updated_at'],principal=p)
        except HTTPException as error:
            if error.status_code!=409:raise


async def run():
    import main
    key=PrivateKey(bytes.fromhex(os.environ['BUZZ_KNOWLEDGE_PRIVATE_KEY']))
    async with main.lifespan(main.app):
        while True:
            try:
                await cycle(main.app,key)
                await main.app.state.pool.execute("INSERT INTO gcor.worker_heartbeats(worker) VALUES('buzz-chat') ON CONFLICT(worker) DO UPDATE SET seen_at=now()")
                __import__('pathlib').Path('/tmp/buzz-chat-heartbeat').touch()
            except Exception:logging.exception('Knowledge chat cycle failed')
            await asyncio.sleep(2)


if __name__=='__main__':asyncio.run(run())
