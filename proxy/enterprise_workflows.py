"""Buzz-scoped stewardship and durable ingestion. No relay mutations."""
import asyncio
import os
import hashlib
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel, Field
from typing import Literal

from access_policy import current_principal
from governance_outbox import json_object
from governance_service import commit_governance

async def require_schema(request: Request):
    if not getattr(request.app.state,'workflows_available',False):
        raise HTTPException(503,'Workspace requires migration 0008; existing retrieval remains available')

router=APIRouter(prefix='/api/workspace',dependencies=[Depends(require_schema)])


def identity(admin=False, contributor=False):
    p=current_principal.get()
    if p is None: raise HTTPException(401,'A signed Buzz identity is required')
    if admin and (p.agent_id or p.role not in {'owner','admin'}): raise HTTPException(403,'Channel owner or admin required')
    if contributor and (p.agent_id or p.role not in {'owner','admin','member'}): raise HTTPException(403,'Channel contributor required')
    return p


class Channel(BaseModel):
    channel_id: UUID


class Document(Channel):
    document_id: UUID


class Submission(Channel):
    request_id: UUID
    title: str=Field(min_length=1,max_length=300)
    text: str=Field(min_length=1,max_length=500000)
    source_uri: str | None=Field(default=None,max_length=2000)
    answers_question_id: UUID | None=None  # unanswered question this submission answers


class Review(Document):
    request_id: UUID
    expected_updated_at: datetime
    state: Literal['proposed','approved','superseded','archived','rejected']
    owner_pubkey: str=Field(pattern=r'^[0-9a-f]{64}$')
    review_due: datetime
    note: str=Field(default='',max_length=4000)
    superseded_by: UUID | None=None
    readers: list[str] | None=Field(default=None,max_length=100)


class Feedback(Document):
    request_id: UUID
    category: Literal['incorrect','outdated','missing','helpful']
    note: str=Field(default='',max_length=4000)


class JobAction(Channel):
    job_id: UUID
    action: Literal['retry','cancel']


async def document(pool, doc_id, p):
    row=await pool.fetchrow("SELECT id,title,source_uri,metadata,updated_at FROM gcor.documents WHERE id=$1 AND channel_id=$2 AND access_level=$3",doc_id,p.channel_id,p.access_level)
    if row is None: raise HTTPException(404,'Knowledge document not found')
    readers=json_object(row['metadata']).get('knowledge_readers')
    if readers is not None and (not isinstance(readers,list) or p.subject not in readers):
        raise HTTPException(404,'Knowledge document not found')
    if p.role not in {'owner','admin','member'} and (json_object(row['metadata']).get('knowledge_state')!='approved'
            or not await pool.fetchval('SELECT gcor.knowledge_evidence_current($1)',row['id'])):
        raise HTTPException(404,'Knowledge document not found')
    return row


@router.post('/channels')
async def channels(request: Request):
    p=identity()
    rows=await request.app.state.pool.fetch('''SELECT c.id,c.name,m.role::text AS role,c.visibility::text AS visibility
        FROM public.channels c JOIN public.channel_members m ON m.channel_id=c.id AND m.community_id=c.community_id
        JOIN public.users u ON u.pubkey=m.pubkey AND u.community_id=c.community_id
        WHERE m.pubkey=$1 AND m.removed_at IS NULL AND u.deactivated_at IS NULL
          AND c.deleted_at IS NULL AND c.archived_at IS NULL ORDER BY c.name LIMIT 200''',bytes.fromhex(p.subject))
    return [dict(r) for r in rows]


@router.post('/documents')
async def documents(payload: Channel,request: Request):
    p=identity()
    rows=await request.app.state.pool.fetch('''SELECT id,title,source_uri,updated_at,
        metadata->>'knowledge_state' AS state,metadata->>'knowledge_owner' AS owner,
        metadata->>'knowledge_review_due' AS review_due
        FROM gcor.documents WHERE channel_id=$1 AND access_level=$2
        AND ($3 OR (metadata->>'knowledge_state'='approved' AND gcor.knowledge_evidence_current(id)))
        AND (NOT(metadata ? 'knowledge_readers') OR metadata->'knowledge_readers'='null'::jsonb OR metadata->'knowledge_readers' @> jsonb_build_array($4::text))
        ORDER BY updated_at DESC LIMIT 200''',p.channel_id,p.access_level,p.role in {'owner','admin','member'},p.subject)
    return [dict(r) for r in rows]


@router.post('/detail')
async def detail(payload: Document,request: Request):
    p=identity();pool=request.app.state.pool
    row=await document(pool,payload.document_id,p)
    chunks=await pool.fetch('SELECT ordinal,content FROM gcor.chunks WHERE document_id=$1 ORDER BY ordinal LIMIT 30',payload.document_id)
    history=await pool.fetch('SELECT event_id,payload,created_at,published_at FROM gcor.governance_outbox WHERE document_id=$1 ORDER BY created_at DESC LIMIT 30',payload.document_id)
    feedback=await pool.fetch('SELECT id,actor,category,note,created_at,resolved_at FROM gcor.knowledge_feedback WHERE document_id=$1 ORDER BY created_at DESC LIMIT 50',payload.document_id)
    return {'document':dict(row),'chunks':[dict(r) for r in chunks],
            'history':[dict(r)|{'payload':json_object(r['payload'])} for r in history],'feedback':[dict(r) for r in feedback]}


@router.post('/review')
async def review(payload: Review,request: Request):
    p=identity(admin=True);pool=request.app.state.pool
    reviewed=await document(pool,payload.document_id,p)
    if payload.state=='approved' and json_object(reviewed['metadata']).get('synthesis_batches',0)>1 and not payload.note.strip():
        raise HTTPException(422,'Multi-section synthesis requires a human review rationale resolving any conflicts')
    if payload.expected_updated_at.tzinfo is None or payload.review_due.tzinfo is None:
        raise HTTPException(422,'Review dates require a timezone')
    if payload.review_due<=datetime.now(timezone.utc): raise HTTPException(422,'Review date must be in the future')
    if payload.state=='superseded' and payload.superseded_by is None: raise HTTPException(422,'Select the replacement document')
    if not await pool.fetchval('''SELECT 1 FROM public.channel_members m JOIN public.users u
        ON u.community_id=m.community_id AND u.pubkey=m.pubkey
        WHERE m.channel_id=$1 AND m.pubkey=$2 AND m.removed_at IS NULL AND u.deactivated_at IS NULL''',payload.channel_id,bytes.fromhex(payload.owner_pubkey)):
        raise HTTPException(422,'Owner must be an active channel member')
    patch={'knowledge_state':payload.state,'knowledge_owner':payload.owner_pubkey,
           'knowledge_review_due':payload.review_due.isoformat(),'knowledge_transition_by':p.subject,
           'knowledge_transition_at':datetime.now(timezone.utc).isoformat()}
    if payload.readers is not None:
        import re
        if any(not re.fullmatch('[0-9a-f]{64}',reader) for reader in payload.readers):raise HTTPException(422,'Reader identities must be Nostr public keys')
        patch['knowledge_readers']=sorted(set(payload.readers+[p.subject,payload.owner_pubkey]))
    else:patch['knowledge_readers']=None
    if payload.state=='approved': patch.update(knowledge_approved_by=p.subject,knowledge_approved_at=patch['knowledge_transition_at'])
    if payload.superseded_by: patch['superseded_by_document_id']=str(payload.superseded_by)
    data=payload.model_dump(mode='json');fingerprint=hashlib.sha256(json.dumps(data,sort_keys=True).encode()).hexdigest()
    import main
    return await commit_governance(request,payload.document_id,patch,payload.state,p.subject,payload.note,
        f'workspace:{p.subject}:{payload.request_id}',fingerprint,'review',main.MINIO_BUCKET,main.queue_graphiti_lifecycle,
        superseded_by=payload.superseded_by,expected_updated_at=payload.expected_updated_at,principal=p)


@router.post('/feedback')
async def feedback(payload: Feedback,request: Request):
    p=identity();pool=request.app.state.pool
    await document(pool,payload.document_id,p)
    row=await pool.fetchrow('''INSERT INTO gcor.knowledge_feedback(document_id,channel_id,actor,request_id,category,note)
        VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(actor,request_id) DO UPDATE SET request_id=EXCLUDED.request_id
        RETURNING id,document_id,category,note''',payload.document_id,p.channel_id,p.subject,str(payload.request_id),payload.category,payload.note)
    if row['document_id']!=payload.document_id or row['category']!=payload.category or row['note']!=payload.note:
        raise HTTPException(409,'Request ID already used for other feedback')
    return {'id':row['id']}


@router.post('/submit',status_code=202)
async def submit(payload: Submission,request: Request):
    p=identity(contributor=True)
    data=payload.model_dump(mode='json')|{'access_level':p.access_level}
    if data.get('answers_question_id') is None: data.pop('answers_question_id',None)  # keeps earlier request digests stable
    digest=hashlib.sha256(json.dumps(data,sort_keys=True).encode()).hexdigest()
    async with request.app.state.pool.acquire() as connection:
        async with connection.transaction():
            await connection.execute("SELECT pg_advisory_xact_lock(hashtextextended($1,0))",'job-actor:'+p.subject)
            row=await connection.fetchrow('SELECT id,status,request_hash FROM gcor.ingestion_jobs WHERE actor=$1 AND request_id=$2',p.subject,str(payload.request_id))
            if row is None:
                pending=await connection.fetchval("SELECT count(*) FROM gcor.ingestion_jobs WHERE actor=$1 AND status IN ('pending','processing')",p.subject)
                if pending>=20:raise HTTPException(429,'At most 20 unfinished submissions per identity; wait or cancel pending work')
                row=await connection.fetchrow('''INSERT INTO gcor.ingestion_jobs(channel_id,actor,request_id,request_hash,payload)
                    VALUES($1,$2,$3,$4,$5::jsonb) RETURNING id,status,request_hash''',p.channel_id,p.subject,str(payload.request_id),digest,json.dumps(data))
    if row['request_hash']!=digest: raise HTTPException(409,'Request ID already used for another submission')
    return {'id':row['id'],'status':row['status']}


@router.post('/jobs')
async def jobs(payload: Channel,request: Request):
    p=identity()
    rows=await request.app.state.pool.fetch('''SELECT id,actor,status,attempts,error,result,created_at,updated_at
        FROM gcor.ingestion_jobs WHERE channel_id=$1 AND (actor=$2 OR $3) ORDER BY created_at DESC LIMIT 100''',p.channel_id,p.subject,p.role in {'owner','admin'})
    return [dict(r) for r in rows]


@router.post('/job-action')
async def job_action(payload: JobAction,request: Request):
    p=identity(contributor=True)
    row=await request.app.state.pool.fetchrow('''UPDATE gcor.ingestion_jobs
        SET status=$4,attempts=0,next_attempt_at=now(),error=NULL,updated_at=now()
        WHERE id=$1 AND channel_id=$2 AND (actor=$3 OR $5) AND status IN ('pending','failed') RETURNING id,status''',
        payload.job_id,p.channel_id,p.subject,'pending' if payload.action=='retry' else 'cancelled',p.role in {'owner','admin'})
    if row is None: raise HTTPException(409,'Job is unavailable or already running/completed')
    return dict(row)


# Transient dependency outages (object store, embedding model, database restarts) must not turn
# submissions into permanently failed jobs: retry with exponential backoff, capped, for up to
# JOB_MAX_ATTEMPTS attempts (defaults cover roughly two hours of outage). A short lease that the
# worker renews while it runs lets another worker reclaim a crashed job within about a minute.
JOB_MAX_ATTEMPTS=int(os.getenv('INGESTION_JOB_MAX_ATTEMPTS','12'))
JOB_BACKOFF_BASE_SECONDS=float(os.getenv('INGESTION_JOB_BACKOFF_SECONDS','15'))
JOB_BACKOFF_MAX_SECONDS=float(os.getenv('INGESTION_JOB_BACKOFF_MAX_SECONDS','900'))
JOB_LEASE_SECONDS=int(os.getenv('INGESTION_JOB_LEASE_SECONDS','60'))


def retry_delay(attempts):
    return min(JOB_BACKOFF_BASE_SECONDS*(2**max(attempts-1,0)),JOB_BACKOFF_MAX_SECONDS)


async def renew_lease(pool,job_id,lease):
    while True:
        await asyncio.sleep(max(JOB_LEASE_SECONDS/3,1))
        try:
            await pool.execute("UPDATE gcor.ingestion_jobs SET lease_until=now()+make_interval(secs=>$3) WHERE id=$1 AND lease_id=$2 AND status='processing'",job_id,lease,JOB_LEASE_SECONDS)
        except Exception:
            pass  # the next renewal retries; losing the lease only means another worker may resume the job


async def process_one(app):
    pool=app.state.pool;lease=uuid4()
    row=await pool.fetchrow('''WITH candidate AS (
        SELECT id FROM gcor.ingestion_jobs WHERE attempts<$2 AND
        ((status='pending' AND next_attempt_at<=now()) OR (status='processing' AND lease_until<now()))
        ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1)
        UPDATE gcor.ingestion_jobs j SET status='processing',attempts=attempts+1,lease_id=$1,
        lease_until=now()+make_interval(secs=>$3),updated_at=now() FROM candidate c WHERE j.id=c.id RETURNING j.*''',lease,JOB_MAX_ATTEMPTS,JOB_LEASE_SECONDS)
    if row is None: return False
    renewal=asyncio.create_task(renew_lease(pool,row['id'],lease))
    try:
        data=json_object(row['payload'])
        allowed=await pool.fetchval('''SELECT 1 FROM public.channel_members m JOIN public.users u ON u.pubkey=m.pubkey AND u.community_id=m.community_id
            JOIN public.channels c ON c.id=m.channel_id AND c.community_id=m.community_id
            WHERE m.channel_id=$1 AND m.pubkey=$2 AND m.removed_at IS NULL AND u.deactivated_at IS NULL
            AND m.role::text IN ('owner','admin','member') AND c.deleted_at IS NULL AND c.archived_at IS NULL
            AND CASE WHEN c.visibility::text='public' THEN 'public' ELSE 'private' END=$3''',UUID(row['channel_id']),bytes.fromhex(row['actor']),data['access_level'])
        if not allowed: raise PermissionError('Membership revoked')
        import main
        async with asyncio.timeout(300):
            result=await main.ingest_payload(SimpleNamespace(app=app),content=data['text'].encode(),media_type='text/plain',title=data['title'],
                access_level=data['access_level'],agent_id=None,source_uri=data.get('source_uri') or f'job:{row["id"]}',
                channel_name=None,channel_id=row['channel_id'],event_id=str(row['id']),event_kind='knowledge.propose',event_timestamp=None,
                author_pubkey=row['actor'],file_url=None,file_name='proposal.txt',metadata={'knowledge_state':'proposed','knowledge_owner':row['actor'],'ingestion_job_id':str(row['id'])},preserve_existing=True)
        if result.get('quarantined'): raise RuntimeError('Ingestion was quarantined')
        if data.get('answers_question_id') and result.get('document_id'):
            try:
                import unanswered
                await unanswered.link_answer(pool,data['answers_question_id'],row['channel_id'],result['document_id'],row['actor'])
            except Exception:
                pass  # linking is a convenience; the proposal itself was ingested
        await pool.execute("UPDATE gcor.ingestion_jobs SET status='completed',result=$3::jsonb,error=NULL,lease_until=NULL,updated_at=now() WHERE id=$1 AND lease_id=$2",row['id'],lease,json.dumps(result,default=str))
    except Exception as error:
        permanent=isinstance(error,PermissionError) or (isinstance(error,HTTPException) and error.status_code<500)
        state='failed' if permanent or row['attempts']>=JOB_MAX_ATTEMPTS else 'pending'
        await pool.execute("UPDATE gcor.ingestion_jobs SET status=$3,error=$4,next_attempt_at=now()+make_interval(secs=>$5),lease_until=NULL,updated_at=now() WHERE id=$1 AND lease_id=$2",row['id'],lease,state,type(error).__name__,retry_delay(row['attempts']))
    finally:
        renewal.cancel()
        await asyncio.gather(renewal,return_exceptions=True)
    return True


async def worker(app):
    while True:
        try:
            await app.state.pool.execute("INSERT INTO gcor.worker_heartbeats(worker) VALUES('ingestion') ON CONFLICT(worker) DO UPDATE SET seen_at=now()")
            await app.state.pool.execute("UPDATE gcor.ingestion_jobs SET status='failed',error='Worker lease expired after retry limit',updated_at=now() WHERE status='processing' AND lease_until<now() AND attempts>=$1",JOB_MAX_ATTEMPTS)
            if await process_one(app): continue
        except Exception:
            pass
        await asyncio.sleep(2)
