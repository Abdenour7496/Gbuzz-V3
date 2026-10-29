"""Copy GCOR objects from S3 (MinIO) into PostgreSQL and prove the copy is complete.

Run once before switching a deployment to GCOR_OBJECT_STORE=postgres:

    docker compose run --rm --no-deps gcor-proxy python migrate_objects.py --dry-run
    docker compose run --rm --no-deps gcor-proxy python migrate_objects.py

Every object version is copied oldest first, so gcor.objects ends with the same history and the
same current version as the bucket. Each copy is verified by SHA-256, re-running is safe (versions
already copied are skipped), and the run ends by checking that every object GCOR's tables refer to
(ingestion records, published governance evidence, audit packs) resolves in PostgreSQL with the
digest S3 holds. The exit code is non-zero if anything is missing or differs; the bucket itself is
never modified, so rolling back is just leaving GCOR_OBJECT_STORE=s3.

Database access uses the schema owner (POSTGRES_ADMIN_USER / POSTGRES_ADMIN_PASSWORD) because the
reference check must see every channel; S3 access uses the proxy's MINIO_* settings.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
from typing import Any, Iterable

import asyncpg
import boto3
from botocore.exceptions import ClientError

REFERENCE_QUERIES = {
    "ingestion_original": "SELECT bucket, original_key AS key FROM gcor.ingestion_records",
    "ingestion_markdown": "SELECT bucket, markdown_key AS key FROM gcor.ingestion_records",
    "ingestion_record": "SELECT bucket, record_key AS key FROM gcor.ingestion_records",
    "governance_event": "SELECT bucket, object_key AS key FROM gcor.governance_outbox WHERE published_at IS NOT NULL",
    "audit_pack": "SELECT bucket, object_key AS key FROM gcor.audit_pack_exports",
}


def s3_client() -> Any:
    return boto3.client(
        "s3", endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"], region_name="us-east-1")


def admin_dsn() -> dict[str, Any]:
    if os.getenv("POSTGRES_ADMIN_DSN"):
        return {"dsn": os.environ["POSTGRES_ADMIN_DSN"]}
    return {"host": os.getenv("POSTGRES_HOST", "postgres"), "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "user": os.getenv("POSTGRES_ADMIN_USER") or os.environ["POSTGRES_USER"],
            "password": os.getenv("POSTGRES_ADMIN_PASSWORD") or os.environ["POSTGRES_PASSWORD"],
            "database": os.environ["POSTGRES_DB"]}


def object_versions(client: Any, bucket: str) -> list[dict[str, Any]]:
    """All versions of every key, oldest first per key; falls back to current objects only."""
    versions: list[dict[str, Any]] = []
    try:
        paginator = client.get_paginator("list_object_versions")
        for page in paginator.paginate(Bucket=bucket):
            for item in page.get("Versions", []):
                versions.append({"key": item["Key"], "version_id": item.get("VersionId"),
                                 "modified": item["LastModified"], "latest": item.get("IsLatest", False)})
    except ClientError as error:
        if str(error.response.get("Error", {}).get("Code")) not in {"NotImplemented", "501", "MethodNotAllowed"}:
            raise
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for item in page.get("Contents", []):
                versions.append({"key": item["Key"], "version_id": None,
                                 "modified": item["LastModified"], "latest": True})
    versions.sort(key=lambda item: (item["key"], item["modified"], not item["latest"]))
    # The current version must be written last for its key even if timestamps tie.
    ordered: list[dict[str, Any]] = []
    for key in dict.fromkeys(item["key"] for item in versions):
        group = [item for item in versions if item["key"] == key]
        group.sort(key=lambda item: (item["latest"], item["modified"]))
        ordered.extend(group)
    return ordered


def fetch(client: Any, bucket: str, key: str, version_id: str | None) -> tuple[bytes, str, dict[str, str]]:
    kwargs = {"Bucket": bucket, "Key": key}
    if version_id and version_id != "null":
        kwargs["VersionId"] = version_id
    response = client.get_object(**kwargs)
    try:
        body = response["Body"].read()
    finally:
        response["Body"].close()
    return body, response.get("ContentType") or "application/octet-stream", dict(response.get("Metadata") or {})


async def copy_version(connection: asyncpg.Connection, bucket: str, key: str, source_version: str,
                       body: bytes, content_type: str, metadata: dict[str, str]) -> str:
    digest = hashlib.sha256(body).hexdigest()
    async with connection.transaction():
        await connection.execute("SELECT pg_advisory_xact_lock(hashtextextended($1 || chr(31) || $2, 7019))", bucket, key)
        already = await connection.fetchval(
            """SELECT 1 FROM gcor.objects WHERE bucket=$1 AND object_key=$2
               AND metadata->>'migrated_from_version' = $3""", bucket, key, source_version)
        if already:
            return "skipped"
        await connection.execute(
            "INSERT INTO gcor.blobs (sha256, size, content) VALUES ($1,$2,$3) ON CONFLICT (sha256) DO NOTHING",
            digest, len(body), body)
        await connection.execute(
            "UPDATE gcor.objects SET is_latest=false WHERE bucket=$1 AND object_key=$2 AND is_latest", bucket, key)
        stored = {**{k.lower(): v for k, v in metadata.items()}, "migrated_from_version": source_version}
        await connection.execute(
            """INSERT INTO gcor.objects (bucket, object_key, sha256, content_type, metadata)
               VALUES ($1,$2,$3,$4,$5::jsonb)""", bucket, key, digest, content_type, json.dumps(stored))
        stored_digest = await connection.fetchval(
            """SELECT encode(sha256(b.content),'hex') FROM gcor.objects o JOIN gcor.blobs b USING (sha256)
               WHERE o.bucket=$1 AND o.object_key=$2 AND o.is_latest""", bucket, key)
        if stored_digest != digest:
            raise RuntimeError(f"Digest mismatch after copy: {bucket}/{key}")
    return "copied"


async def referenced_objects(connection: asyncpg.Connection) -> list[tuple[str, str, str]]:
    refs: list[tuple[str, str, str]] = []
    for label, query in REFERENCE_QUERIES.items():
        try:
            rows = await connection.fetch(query)
        except asyncpg.UndefinedTableError:
            continue
        refs.extend((label, row["bucket"], row["key"]) for row in rows if row["bucket"] and row["key"])
    return refs


async def run(buckets: Iterable[str] | None, dry_run: bool) -> dict[str, Any]:
    client = s3_client()
    connection = await asyncpg.connect(**admin_dsn())
    report: dict[str, Any] = {"dry_run": dry_run, "buckets": {}, "references": {}, "errors": []}
    try:
        if not await connection.fetchval("SELECT to_regclass('gcor.objects')"):
            raise SystemExit("Apply migration 0019_postgres_object_store.sql first")
        refs = await referenced_objects(connection)
        names = sorted(set(buckets or []) or ({bucket for _, bucket, _ in refs}
                                              | {os.getenv("MINIO_BUCKET", "buzz-gcor")}))
        # Completeness is judged for the buckets this run covers.
        refs = [ref for ref in refs if ref[1] in names]
        for bucket in names:
            counts = {"versions": 0, "copied": 0, "skipped": 0, "bytes": 0}
            try:
                versions = object_versions(client, bucket)
            except ClientError as error:
                report["errors"].append({"bucket": bucket, "error": str(error.response.get("Error", {}).get("Code"))})
                continue
            for item in versions:
                counts["versions"] += 1
                source_version = item["version_id"] or "current"
                if dry_run:
                    continue
                body, content_type, metadata = fetch(client, bucket, item["key"], item["version_id"])
                outcome = await copy_version(connection, bucket, item["key"], source_version, body, content_type, metadata)
                counts[outcome] += 1
                counts["bytes"] += len(body) if outcome == "copied" else 0
            report["buckets"][bucket] = counts
        # Completeness: every referenced object exists in PostgreSQL with the digest S3 holds now.
        missing, differing, verified = [], [], 0
        for label, bucket, key in refs:
            stored = await connection.fetchval(
                "SELECT sha256 FROM gcor.objects WHERE bucket=$1 AND object_key=$2 AND is_latest", bucket, key)
            try:
                body, _, _ = fetch(client, bucket, key, None)
            except ClientError:
                body = None
            if stored is None:
                missing.append({"kind": label, "bucket": bucket, "key": key, "in_s3": body is not None})
            elif body is not None and hashlib.sha256(body).hexdigest() != stored:
                differing.append({"kind": label, "bucket": bucket, "key": key})
            else:
                verified += 1
        report["references"] = {"total": len(refs), "verified": verified,
                                "missing": missing[:200], "missing_count": len(missing),
                                "differing": differing[:200], "differing_count": len(differing)}
    finally:
        await connection.close()
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bucket", action="append", help="Bucket to copy (repeatable). Default: every bucket GCOR references.")
    parser.add_argument("--dry-run", action="store_true", help="Count versions and check references without writing.")
    args = parser.parse_args(argv)
    report = asyncio.run(run(args.bucket, args.dry_run))
    print(json.dumps(report, indent=2, default=str))
    refs = report["references"]
    if report["errors"] or refs.get("differing_count"):
        return 1
    if not args.dry_run and refs.get("missing_count"):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
