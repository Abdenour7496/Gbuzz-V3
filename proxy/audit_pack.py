"""Channel-scoped, cryptographically sealed audit evidence exports."""

import asyncio
import hashlib
import io
import json
import os
import re
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlparse
from uuid import UUID, uuid4

import httpx
from botocore.exceptions import ClientError
from coincurve import PrivateKey, PublicKeyXOnly
from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field

from access_policy import current_principal


router = APIRouter()
HEX_64 = re.compile(r"^[0-9a-f]{64}$")
SAFE_FILE = re.compile(r"[^A-Za-z0-9._ -]+")
MAX_EVENTS = int(os.getenv("AUDIT_PACK_MAX_EVENTS", "5000"))
MAX_BYTES = int(os.getenv("AUDIT_PACK_MAX_BYTES", str(100 * 1024 * 1024)))
MAX_RANGE_DAYS = int(os.getenv("AUDIT_PACK_MAX_RANGE_DAYS", "366"))
MEDIA_BUCKET = os.getenv("BUZZ_MEDIA_BUCKET", "buzz-media")
# With GCOR_OBJECT_STORE=postgres GCOR holds no credential for the relay's media bucket, so it reads
# attachments through the relay's own content-addressed /media endpoint and verifies their hashes.
RELAY_MEDIA_URL = os.getenv("BUZZ_RELAY_HTTP_URL", "http://relay:3000").rstrip("/")


class AuditPackRequest(BaseModel):
    channel_id: str
    start_at: datetime
    end_at: datetime
    include_attachments: bool = True
    purpose: str = Field(min_length=3, max_length=500)
    approval_id: str | None = Field(default=None, pattern=r'^[0-9a-f]{64}$')


def _stack_secret(value: str | None) -> None:
    if current_principal.get() is not None:
        return
    expected = os.getenv("STACK_API_SECRET", "") or os.getenv("INGEST_WEBHOOK_SECRET", "")
    enforce = os.getenv("ENFORCE_STACK_API_SECRET", "true").strip().lower() in {"1", "true", "yes", "on"}
    if enforce and not expected:
        raise HTTPException(500, "STACK_API_SECRET is required when enforcement is enabled")
    if enforce and value != expected:
        raise HTTPException(401, "Invalid stack API secret")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise HTTPException(422, "start_at and end_at must include a timezone")
    return value.astimezone(timezone.utc)


def _authorize(payload: AuditPackRequest) -> tuple[str, str]:
    try:
        UUID(payload.channel_id)
    except ValueError as error:
        raise HTTPException(422, "channel_id must be a valid UUID") from error
    principal = current_principal.get()
    if principal is None:
        return "stack-service", "service"
    if principal.workload:
        if 'audit.export' not in principal.operations or principal.channel_id != payload.channel_id:
            raise HTTPException(403, 'Audit export workload is outside approved scope')
        if not payload.approval_id:
            raise HTTPException(403, 'Audit export workload requires an approved request context')
        return principal.subject, 'service'
    if principal.channel_id != payload.channel_id:
        raise HTTPException(403, "Channel is outside the authenticated scope")
    if principal.role not in {"owner", "admin"}:
        raise HTTPException(403, "Channel owner or admin role required for audit export")
    return principal.subject, principal.role


def canonical_event(row: Any) -> dict[str, Any]:
    created_at = row["created_at"]
    tags = json.loads(row["tags"]) if isinstance(row["tags"], str) else row["tags"]
    if not isinstance(tags, list):
        raise ValueError("Event tags are not a JSON array")
    event = {
        "id": bytes(row["id"]).hex(),
        "pubkey": bytes(row["pubkey"]).hex(),
        "created_at": int(created_at.timestamp()),
        "kind": int(row["kind"]),
        "tags": tags,
        "content": row["content"],
        "sig": bytes(row["sig"]).hex(),
    }
    canonical = json.dumps(
        [0, event["pubkey"], event["created_at"], event["kind"], event["tags"], event["content"]],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    digest = hashlib.sha256(canonical).digest()
    if digest.hex() != event["id"]:
        raise ValueError(f"Event {event['id']} failed identifier verification")
    if not PublicKeyXOnly(bytes.fromhex(event["pubkey"])).verify(bytes.fromhex(event["sig"]), digest):
        raise ValueError(f"Event {event['id']} failed signature verification")
    return event | {
        "received_at": row["received_at"].astimezone(timezone.utc).isoformat(),
        "deleted_at": row["deleted_at"].astimezone(timezone.utc).isoformat() if row["deleted_at"] else None,
        "verification": {"event_id": "valid", "schnorr_signature": "valid"},
    }


def attachment_refs(tags: Any) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    if not isinstance(tags, list):
        return refs
    for tag in tags:
        if not isinstance(tag, list) or not tag or tag[0] != "imeta":
            continue
        item: dict[str, str] = {}
        for part in tag[1:]:
            if isinstance(part, str) and " " in part:
                key, value = part.split(" ", 1)
                if key in {"url", "m", "x", "filename", "size"}:
                    item[key] = value
        parsed = urlparse(item.get("url", ""))
        key = PurePosixPath(parsed.path).name
        digest = item.get("x") or key.split(".", 1)[0]
        if parsed.path.startswith("/media/") and HEX_64.fullmatch(digest):
            item["object_key"] = key
            item["sha256"] = digest
            refs.append(item)
    return refs


def safe_name(value: str, fallback: str) -> str:
    cleaned = SAFE_FILE.sub("_", PurePosixPath(value).name).strip(" .")
    return (cleaned or fallback)[:180]


def json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, default=str) + "\n").encode("utf-8")


def seal_manifest(manifest: dict[str, Any], private_key_hex: str) -> dict[str, Any]:
    if not HEX_64.fullmatch(private_key_hex):
        raise RuntimeError("AUDIT_PACK_SIGNING_KEY must be a 32-byte lowercase hex key")
    payload = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    key = PrivateKey(bytes.fromhex(private_key_hex))
    return manifest | {
        "seal": {
            "algorithm": "BIP340-Schnorr-SHA256",
            "manifest_sha256": digest.hex(),
            "public_key": key.public_key_xonly.format().hex(),
            "signature": key.sign_schnorr(digest).hex(),
        }
    }


class PackWriter:
    def __init__(self) -> None:
        self.buffer = io.BytesIO()
        self.zip = zipfile.ZipFile(self.buffer, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6)
        self.files: list[dict[str, Any]] = []
        self.total_source_bytes = 0

    def add(self, path: str, data: bytes, *, evidence_type: str, source: str | None = None) -> None:
        self.total_source_bytes += len(data)
        if self.total_source_bytes > MAX_BYTES:
            raise HTTPException(413, f"Audit evidence exceeds {MAX_BYTES} bytes")
        self.zip.writestr(path, data)
        item = {"path": path, "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data), "type": evidence_type}
        if source:
            item["source"] = source
        self.files.append(item)

    def finish(self, manifest: dict[str, Any]) -> bytes:
        self.zip.writestr("manifest.json", json_bytes(manifest))
        self.zip.close()
        result = self.buffer.getvalue()
        if len(result) > MAX_BYTES:
            raise HTTPException(413, f"Compressed audit pack exceeds {MAX_BYTES} bytes")
        return result


async def s3_bytes(client: Any, bucket: str, key: str) -> bytes:
    def read() -> bytes:
        response = client.get_object(Bucket=bucket, Key=key)
        try:
            return response["Body"].read(MAX_BYTES + 1)
        finally:
            response["Body"].close()
    try:
        return await asyncio.to_thread(read)
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", "error"))
        raise HTTPException(409, f"Required evidence object is unavailable: {bucket}/{key} ({code})") from error


async def relay_media_bytes(object_key: str) -> bytes:
    if not re.fullmatch(r"[0-9a-f]{64}(\.[A-Za-z0-9]{1,16})?", object_key):
        raise HTTPException(409, "Attachment reference is not a content-addressed media key")
    url = f"{RELAY_MEDIA_URL}/media/{object_key}"
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=False, trust_env=False) as client:
            async with client.stream("GET", url) as response:
                response.raise_for_status()
                data = bytearray()
                async for chunk in response.aiter_bytes(64 * 1024):
                    data.extend(chunk)
                    if len(data) > MAX_BYTES:
                        raise HTTPException(413, f"Attachment exceeds {MAX_BYTES} bytes")
                return bytes(data)
    except httpx.HTTPError as error:
        raise HTTPException(409, f"Required attachment is unavailable from the relay: {object_key} ({type(error).__name__})") from error


def object_uri(request: Request, bucket: str, key: str) -> str:
    scheme = "gcor-postgres" if getattr(request.app.state, "object_store", "s3") == "postgres" else "s3"
    return f"{scheme}://{bucket}/{key}"


async def attachment_bytes(request: Request, object_key: str) -> tuple[bytes, str]:
    if getattr(request.app.state, "object_store", "s3") == "postgres":
        return await relay_media_bytes(object_key), f"{RELAY_MEDIA_URL}/media/{object_key}"
    return await s3_bytes(request.app.state.s3, MEDIA_BUCKET, object_key), f"s3://{MEDIA_BUCKET}/{object_key}"


def timeline(events: list[dict[str, Any]]) -> bytes:
    lines = ["# Audit chronology", ""]
    for event in events:
        stamp = datetime.fromtimestamp(event["created_at"], timezone.utc).isoformat()
        state = " [deleted]" if event["deleted_at"] else ""
        lines.extend([f"## {stamp}{state}", "", f"Event: `{event['id']}`", f"Author pubkey: `{event['pubkey']}`", "", event["content"] or "_(empty content)_", ""])
    return "\n".join(lines).encode("utf-8")


@router.post("/api/audit-packs")
async def create_audit_pack(
    payload: AuditPackRequest,
    request: Request,
    x_gcor_webhook_secret: str | None = Header(default=None),
):
    _stack_secret(x_gcor_webhook_secret)
    requested_by, requester_role = _authorize(payload)
    start_at, end_at = _utc(payload.start_at), _utc(payload.end_at)
    if end_at <= start_at:
        raise HTTPException(422, "end_at must be later than start_at")
    if end_at - start_at > timedelta(days=MAX_RANGE_DAYS):
        raise HTTPException(422, f"audit range cannot exceed {MAX_RANGE_DAYS} days")

    channel = await request.app.state.pool.fetchrow(
        "SELECT name,visibility::text AS visibility FROM public.channels WHERE id=$1::uuid AND deleted_at IS NULL",
        payload.channel_id,
    )
    if channel is None:
        raise HTTPException(404, "Channel not found")
    access_level = "public" if str(channel["visibility"]).casefold() == "public" else "private"

    rows = await request.app.state.pool.fetch(
        """SELECT id,pubkey,created_at,kind,tags,content,sig,received_at,deleted_at
           FROM public.events WHERE channel_id=$1::uuid AND created_at >= $2 AND created_at < $3
           ORDER BY created_at,id LIMIT $4""",
        payload.channel_id, start_at, end_at, MAX_EVENTS + 1,
    )
    if len(rows) > MAX_EVENTS:
        raise HTTPException(413, f"audit range exceeds {MAX_EVENTS} events")
    try:
        events = [canonical_event(row) for row in rows]
    except (TypeError, ValueError) as error:
        raise HTTPException(409, str(error)) from error
    event_ids = [event["id"] for event in events]

    records = []
    if event_ids:
        records = await request.app.state.pool.fetch(
            """SELECT id,document_id,content_sha256,bucket,original_key,markdown_key,record_key,
                      event_id,status,metadata,created_at
               FROM gcor.ingestion_records WHERE channel_id=$1 AND event_id=ANY($2::text[])
               ORDER BY created_at,id""",
            payload.channel_id, event_ids,
        )

    pack_id = uuid4()
    writer = PackWriter()
    for event in events:
        writer.add(f"events/{event['id']}.json", json_bytes(event), evidence_type="signed_nostr_event", source=f"buzz://event/{event['id']}")
    writer.add("chronology.md", timeline(events), evidence_type="human_readable_chronology")

    for row in records:
        record_id = str(row["id"])
        objects: dict[str, bytes] = {}
        for label, key in (("original", row["original_key"]), ("content", row["markdown_key"]), ("record", row["record_key"])):
            objects[label] = await s3_bytes(request.app.state.s3, row["bucket"], key)
        if hashlib.sha256(objects["original"]).hexdigest() != row["content_sha256"]:
            raise HTTPException(409, f"Archived original failed hash verification for record {record_id}")
        try:
            record_manifest = json.loads(objects["record"])
        except json.JSONDecodeError as error:
            raise HTTPException(409, f"Invalid record manifest for {record_id}") from error
        markdown_digest = record_manifest.get("objects", {}).get("markdown_sha256")
        if markdown_digest and hashlib.sha256(objects["content"]).hexdigest() != markdown_digest:
            raise HTTPException(409, f"Archived markdown failed hash verification for record {record_id}")
        writer.add(f"archive/{record_id}/original.bin", objects["original"], evidence_type="archived_original", source=object_uri(request, row['bucket'], row['original_key']))
        writer.add(f"archive/{record_id}/content.md", objects["content"], evidence_type="normalized_content", source=object_uri(request, row['bucket'], row['markdown_key']))
        writer.add(f"archive/{record_id}/record.json", objects["record"], evidence_type="ingestion_manifest", source=object_uri(request, row['bucket'], row['record_key']))

    attachment_count = 0
    if payload.include_attachments:
        for event in events:
            for index, ref in enumerate(attachment_refs(event["tags"]), 1):
                data, source = await attachment_bytes(request, ref["object_key"])
                if hashlib.sha256(data).hexdigest() != ref["sha256"]:
                    raise HTTPException(409, f"Attachment failed hash verification for event {event['id']}")
                name = safe_name(ref.get("filename", ""), ref["object_key"])
                writer.add(f"attachments/{event['id']}/{index:02d}-{name}", data, evidence_type="buzz_attachment", source=source)
                attachment_count += 1

    unsigned_manifest = {
        "schema_version": "1.0.0",
        "pack_id": str(pack_id),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "purpose": payload.purpose,
        "approval_id": payload.approval_id,
        "scope": {"channel_id": payload.channel_id, "channel_name": channel["name"], "start_at": start_at.isoformat(), "end_at": end_at.isoformat(), "access_level": access_level},
        "requester": {"subject": requested_by, "role": requester_role},
        "counts": {"events": len(events), "archive_records": len(records), "attachments": attachment_count},
        "files": writer.files,
        "verification": {"event_ids": "verified", "event_signatures": "verified", "archive_hashes": "verified", "attachment_hashes": "verified"},
    }
    signing_key = os.getenv("AUDIT_PACK_SIGNING_KEY", "")
    try:
        manifest = seal_manifest(unsigned_manifest, signing_key)
    except RuntimeError as error:
        raise HTTPException(503, str(error)) from error
    pack = writer.finish(manifest)
    pack_digest = hashlib.sha256(pack).hexdigest()
    bucket = records[0]["bucket"] if records else os.getenv("MINIO_BUCKET", "buzz-gcor")
    object_key = f"audit-packs/{start_at:%Y/%m/%d}/{pack_id}.zip"
    try:
        await asyncio.to_thread(
            request.app.state.s3.put_object,
            Bucket=bucket,
            Key=object_key,
            Body=pack,
            ContentType="application/zip",
            Metadata={"pack_id": str(pack_id), "sha256": pack_digest, "manifest_sha256": manifest["seal"]["manifest_sha256"]},
            IfNoneMatch="*",
        )
        await request.app.state.pool.execute(
            """INSERT INTO gcor.audit_pack_exports
               (id,channel_id,requested_by,requester_role,purpose,start_at,end_at,event_count,
                archive_record_count,attachment_count,bucket,object_key,pack_sha256,manifest_sha256,signing_pubkey)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)""",
            pack_id, payload.channel_id, requested_by, requester_role, payload.purpose,
            start_at, end_at, len(events), len(records), attachment_count, bucket, object_key,
            pack_digest, manifest["seal"]["manifest_sha256"], manifest["seal"]["public_key"],
        )
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", "error"))
        raise HTTPException(503, f"Audit pack storage failed ({code})") from error
    except Exception as error:
        raise HTTPException(503, "Audit export log is unavailable; pack was not released") from error

    return Response(
        pack,
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="audit-pack-{pack_id}.zip"',
            "X-Audit-Pack-Id": str(pack_id),
            "X-Audit-Pack-SHA256": pack_digest,
            "X-Audit-Manifest-SHA256": manifest["seal"]["manifest_sha256"],
            "X-Audit-Object": object_uri(request, bucket, object_key),
        },
    )
