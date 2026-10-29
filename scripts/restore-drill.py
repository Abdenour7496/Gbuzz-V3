"""Copy snapshot-referenced GCOR objects to backup and isolated MinIO; verify bytes."""
import asyncio
import hashlib
import json
import os
from pathlib import Path

import asyncpg
import boto3
import main


async def run():
    secret=os.environ['DRILL_SECRET']
    db=await asyncpg.connect(host=os.environ['DRILL_POSTGRES_HOST'],user='drill',password=secret,database='drill')
    target=boto3.client('s3',endpoint_url='http://'+os.environ['DRILL_S3_HOST']+':9000',aws_access_key_id='drill-admin',aws_secret_access_key=secret)
    source=main.minio_client()
    rows=await db.fetch('SELECT bucket,original_key,markdown_key,record_key,content_sha256 FROM gcor.ingestion_records')
    objects={}
    for row in rows:
        for field in ('original_key','markdown_key','record_key'):
            if row[field]:objects[(row['bucket'],row[field])]=row['content_sha256'] if field=='original_key' else None
    for row in await db.fetch('SELECT bucket,object_key FROM gcor.governance_outbox WHERE published_at IS NOT NULL'):
        objects[(row['bucket'],row['object_key'])]=None
    # Objects held in gcor.objects (GCOR_OBJECT_STORE=postgres) were restored with the database
    # itself; their digests are enforced by gcor.blobs. Only S3-held objects are copied here.
    if await db.fetchval("SELECT to_regclass('gcor.objects') IS NOT NULL"):
        stored={(r['bucket'],r['object_key']) for r in await db.fetch('SELECT bucket,object_key FROM gcor.objects WHERE is_latest')}
        for pair in stored & objects.keys():
            del objects[pair]
    output=Path('/backup/objects');output.mkdir(exist_ok=True)
    index=[];total=0;created=set()
    for (bucket,key),expected in objects.items():
        response=source.get_object(Bucket=bucket,Key=key)
        total+=response['ContentLength']
        if total>5*1024**3:raise RuntimeError('Drill object budget exceeded (5 GiB)')
        file=output/hashlib.sha256((bucket+'\0'+key).encode()).hexdigest()
        digest=hashlib.sha256()
        with response['Body'] as stream,file.open('wb') as destination:
            while chunk:=stream.read(1024*1024):digest.update(chunk);destination.write(chunk)
        checksum=digest.hexdigest()
        if expected and checksum!=expected:raise RuntimeError('Original bytes do not match restored database provenance')
        if bucket not in created:target.create_bucket(Bucket=bucket);created.add(bucket)
        with file.open('rb') as body:target.put_object(Bucket=bucket,Key=key,Body=body)
        restored=hashlib.sha256()
        with target.get_object(Bucket=bucket,Key=key)['Body'] as stream:
            while chunk:=stream.read(1024*1024):restored.update(chunk)
        if restored.hexdigest()!=checksum:raise RuntimeError('Restored object checksum mismatch')
        index.append({'bucket':bucket,'key':key,'file':'objects/'+file.name,'sha256':checksum})
    Path('/backup/objects.json').write_text(json.dumps(index,indent=2))
    counts={name:await db.fetchval('SELECT count(*) FROM gcor.'+name) for name in ('documents','chunks','ingestion_records','governance_outbox')}
    Path('/backup/report.json').write_text(json.dumps({'passed':True,'scope':'PostgreSQL and snapshot-referenced GCOR ingestion objects; excludes other relay media and graph rebuild',
        'objects_verified':len(index),'bytes':total,'restored_counts':counts},indent=2))
    await db.close();source.close();target.close()
    print('Restored database and GCOR object checksum verification passed')


asyncio.run(run())
