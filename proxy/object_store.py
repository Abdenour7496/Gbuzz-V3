"""GCOR object storage in PostgreSQL (GCOR_OBJECT_STORE=postgres).

``PostgresObjectStore`` implements the small part of the boto3 S3 client API that GCOR uses
(put/get/head/list objects, bucket housekeeping) on top of ``gcor.blobs`` and ``gcor.objects``
(migration 0019). Call sites keep their S3 shape, so ingestion, recovery, the governance outbox and
audit packs work unchanged whichever backend is configured.

Every caller already runs these methods through ``asyncio.to_thread`` because boto3 is blocking.
The store therefore exposes blocking methods too, and runs its own asyncpg pool on a private event
loop thread. That keeps object I/O off the request loop and avoids a second database driver.

Errors are raised as botocore ``ClientError`` with the same codes S3 returns, so existing handlers
(missing object, conditional write already done) behave the same way.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import re
import threading
from datetime import timezone
from typing import Any

import asyncpg
from botocore.exceptions import ClientError

MAX_LIST_KEYS = 1000


def client_error(code: str, operation: str, status: int, message: str = "") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message or code},
         "ResponseMetadata": {"HTTPStatusCode": status}},
        operation,
    )


def body_bytes(body: Any) -> bytes:
    if body is None:
        return b""
    if isinstance(body, (bytes, bytearray, memoryview)):
        return bytes(body)
    if isinstance(body, str):
        return body.encode("utf-8")
    if hasattr(body, "read"):
        data = body.read()
        return data.encode("utf-8") if isinstance(data, str) else bytes(data)
    raise TypeError(f"Unsupported Body type: {type(body).__name__}")


def encode_token(key: str) -> str:
    return base64.urlsafe_b64encode(key.encode("utf-8")).decode("ascii")


def decode_token(token: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]+={0,2}", token or ""):
        raise client_error("InvalidArgument", "ListObjectsV2", 400, "Invalid continuation token")
    try:
        return base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8")
    except (ValueError, UnicodeDecodeError) as error:
        raise client_error("InvalidArgument", "ListObjectsV2", 400, "Invalid continuation token") from error


class PostgresObjectStore:
    """Blocking, thread-safe S3-shaped client backed by PostgreSQL."""

    def __init__(self, *, dsn: str | None = None, host: str | None = None, port: int = 5432,
                 user: str | None = None, password: str | None = None, database: str | None = None,
                 min_size: int = 1, max_size: int = 4, timeout: float = 30.0,
                 connect_timeout: float = 10.0) -> None:
        self._connect: dict[str, Any] = {"min_size": min_size, "max_size": max_size,
                                         "timeout": connect_timeout, "command_timeout": timeout}
        if dsn:
            self._connect["dsn"] = dsn
        else:
            self._connect.update({"host": host, "port": port, "user": user, "password": password,
                                  "database": database})
        self._timeout = timeout
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, name="gcor-object-store", daemon=True)
        self._thread.start()
        self._pool: asyncpg.Pool | None = None
        self._pool_lock: asyncio.Lock | None = None

    # Lifecycle -----------------------------------------------------------------------------
    def _run(self, coroutine, timeout: float | None = None):
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        return future.result(timeout or self._timeout + 5)

    async def _get_pool(self) -> asyncpg.Pool:
        # Runs only on the store's own loop, so a loop-bound lock is enough to create one pool.
        if self._pool is None:
            if self._pool_lock is None:
                self._pool_lock = asyncio.Lock()
            async with self._pool_lock:
                if self._pool is None:
                    self._pool = await asyncpg.create_pool(**self._connect)
        return self._pool

    def close(self) -> None:
        async def shutdown():
            if self._pool is not None:
                await self._pool.close()
        try:
            self._run(shutdown(), timeout=10)
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=5)

    # Buckets are namespaces: they need no creation and have no settings of their own -------
    def head_bucket(self, Bucket: str, **_: Any) -> dict:  # noqa: N803 - boto3 parameter names
        async def check():
            pool = await self._get_pool()
            await pool.fetchval("SELECT 1 FROM gcor.objects LIMIT 1")
        self._run(check())
        return {}

    def create_bucket(self, Bucket: str, **_: Any) -> dict:  # noqa: N803
        return {}

    def put_bucket_versioning(self, Bucket: str, **_: Any) -> dict:  # noqa: N803
        return {}  # Every object is versioned by construction.

    def list_buckets(self, **_: Any) -> dict:
        async def buckets():
            pool = await self._get_pool()
            rows = await pool.fetch("SELECT DISTINCT bucket FROM gcor.objects WHERE is_latest ORDER BY bucket")
            return [{"Name": row["bucket"]} for row in rows]
        return {"Buckets": self._run(buckets())}

    # Objects -------------------------------------------------------------------------------
    def put_object(self, Bucket: str, Key: str, Body: Any = None, ContentType: str | None = None,  # noqa: N803
                   Metadata: dict | None = None, IfNoneMatch: str | None = None, **_: Any) -> dict:
        data = body_bytes(Body)
        digest = hashlib.sha256(data).hexdigest()
        metadata = {str(key).lower(): str(value) for key, value in (Metadata or {}).items()}

        async def write():
            pool = await self._get_pool()
            async with pool.acquire() as connection:
                async with connection.transaction():
                    # Serialise writers of one key so exactly one version stays current.
                    await connection.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended($1 || chr(31) || $2, 7019))", Bucket, Key)
                    current = await connection.fetchrow(
                        "SELECT version_id FROM gcor.objects WHERE bucket=$1 AND object_key=$2 AND is_latest",
                        Bucket, Key)
                    if current is not None and IfNoneMatch == "*":
                        raise client_error("PreconditionFailed", "PutObject", 412,
                                           "At least one of the pre-conditions you specified did not hold")
                    await connection.execute(
                        "INSERT INTO gcor.blobs (sha256, size, content) VALUES ($1, $2, $3) ON CONFLICT (sha256) DO NOTHING",
                        digest, len(data), data)
                    if current is not None:
                        await connection.execute(
                            "UPDATE gcor.objects SET is_latest=false WHERE bucket=$1 AND object_key=$2 AND version_id=$3",
                            Bucket, Key, current["version_id"])
                    return await connection.fetchval(
                        """INSERT INTO gcor.objects (bucket, object_key, sha256, content_type, metadata)
                           VALUES ($1, $2, $3, $4, $5::jsonb) RETURNING version_id""",
                        Bucket, Key, digest, ContentType or "application/octet-stream", json.dumps(metadata))
        version_id = self._run(write())
        return {"ETag": f'"{digest}"', "VersionId": str(version_id), "ChecksumSHA256": digest}

    async def _fetch(self, bucket: str, key: str, version_id: str | None, with_body: bool):
        pool = await self._get_pool()
        content = ", b.content" if with_body else ""
        condition = "o.version_id = $3::uuid" if version_id else "o.is_latest"
        query = f"""SELECT o.version_id, o.sha256, o.content_type, o.metadata, o.created_at, b.size{content}
                    FROM gcor.objects o JOIN gcor.blobs b ON b.sha256 = o.sha256
                    WHERE o.bucket = $1 AND o.object_key = $2 AND {condition}"""
        args = [bucket, key] + ([version_id] if version_id else [])
        try:
            return await pool.fetchrow(query, *args)
        except asyncpg.DataError:
            return None

    def _describe(self, row) -> dict:
        metadata = row["metadata"]
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        return {
            "ContentLength": row["size"],
            "ContentType": row["content_type"],
            "ETag": f'"{row["sha256"]}"',
            "ChecksumSHA256": row["sha256"],
            "LastModified": row["created_at"].astimezone(timezone.utc),
            "Metadata": metadata,
            "VersionId": str(row["version_id"]),
        }

    def get_object(self, Bucket: str, Key: str, VersionId: str | None = None, **_: Any) -> dict:  # noqa: N803
        row = self._run(self._fetch(Bucket, Key, VersionId, True))
        if row is None:
            raise client_error("NoSuchKey", "GetObject", 404, "The specified key does not exist.")
        content = bytes(row["content"])
        if hashlib.sha256(content).hexdigest() != row["sha256"]:  # Defence in depth; the table checks too.
            raise client_error("InternalError", "GetObject", 500, "Stored object failed digest verification")
        return {**self._describe(row), "Body": io.BytesIO(content)}

    def head_object(self, Bucket: str, Key: str, VersionId: str | None = None, **_: Any) -> dict:  # noqa: N803
        row = self._run(self._fetch(Bucket, Key, VersionId, False))
        if row is None:
            raise client_error("404", "HeadObject", 404, "Not Found")
        return self._describe(row)

    def list_objects_v2(self, Bucket: str, Prefix: str = "", MaxKeys: int = MAX_LIST_KEYS,  # noqa: N803
                        ContinuationToken: str | None = None, StartAfter: str | None = None, **_: Any) -> dict:
        limit = max(1, min(int(MaxKeys or MAX_LIST_KEYS), MAX_LIST_KEYS))
        after = decode_token(ContinuationToken) if ContinuationToken else (StartAfter or "")

        async def page():
            pool = await self._get_pool()
            return await pool.fetch(
                """SELECT o.object_key, o.sha256, o.created_at, b.size
                   FROM gcor.objects o JOIN gcor.blobs b ON b.sha256 = o.sha256
                   WHERE o.bucket = $1 AND o.is_latest
                     AND left(o.object_key, char_length($2)) = $2 AND o.object_key COLLATE "C" > $3
                   ORDER BY o.object_key COLLATE "C" LIMIT $4""",
                Bucket, Prefix or "", after, limit + 1)
        rows = self._run(page())
        truncated = len(rows) > limit
        rows = rows[:limit]
        result: dict[str, Any] = {
            "Name": Bucket, "Prefix": Prefix or "", "KeyCount": len(rows), "MaxKeys": limit,
            "IsTruncated": truncated,
            "Contents": [{"Key": row["object_key"], "Size": row["size"], "ETag": f'"{row["sha256"]}"',
                          "LastModified": row["created_at"].astimezone(timezone.utc)} for row in rows],
        }
        if truncated:
            result["NextContinuationToken"] = encode_token(rows[-1]["object_key"])
        return result

    def list_object_versions(self, Bucket: str, Prefix: str = "", MaxKeys: int = MAX_LIST_KEYS, **_: Any) -> dict:  # noqa: N803
        """Every version under a prefix, newest first per key (single page; used by checks and tooling)."""
        limit = max(1, min(int(MaxKeys or MAX_LIST_KEYS), MAX_LIST_KEYS))

        async def versions():
            pool = await self._get_pool()
            return await pool.fetch(
                """SELECT o.object_key, o.version_id, o.is_latest, o.sha256, o.created_at, b.size
                   FROM gcor.objects o JOIN gcor.blobs b ON b.sha256 = o.sha256
                   WHERE o.bucket = $1 AND left(o.object_key, char_length($2)) = $2
                   ORDER BY o.object_key COLLATE "C", o.created_at DESC LIMIT $3""",
                Bucket, Prefix or "", limit + 1)
        rows = self._run(versions())
        return {
            "Name": Bucket, "Prefix": Prefix or "", "IsTruncated": len(rows) > limit,
            "Versions": [{"Key": row["object_key"], "VersionId": str(row["version_id"]),
                          "IsLatest": row["is_latest"], "Size": row["size"], "ETag": f'"{row["sha256"]}"',
                          "LastModified": row["created_at"].astimezone(timezone.utc)} for row in rows[:limit]],
        }
