import hashlib
import io
import json
import os
import sys
import unittest
import zipfile
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from coincurve import PrivateKey, PublicKeyXOnly
from starlette.requests import Request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "proxy"))

import audit_pack
from access_policy import Principal, current_principal


def signed_row(content="Audit decision", tags=None):
    key = PrivateKey()
    created_at = datetime.now(timezone.utc).replace(microsecond=0)
    tags = tags or []
    pubkey = key.public_key_xonly.format().hex()
    canonical = json.dumps([0, pubkey, int(created_at.timestamp()), 9, tags, content], separators=(",", ":")).encode()
    digest = hashlib.sha256(canonical).digest()
    return {
        "id": digest,
        "pubkey": bytes.fromhex(pubkey),
        "created_at": created_at,
        "kind": 9,
        "tags": tags,
        "content": content,
        "sig": key.sign_schnorr(digest),
        "received_at": created_at + timedelta(seconds=1),
        "deleted_at": None,
    }


class Body:
    def __init__(self, value):
        self.value = value

    def read(self, _limit):
        return self.value

    def close(self):
        pass


class FakeS3:
    def __init__(self, objects):
        self.objects = objects
        self.puts = []

    def get_object(self, Bucket, Key):
        return {"Body": Body(self.objects[(Bucket, Key)])}

    def put_object(self, **kwargs):
        self.puts.append(kwargs)


class FakePool:
    def __init__(self, events, records):
        self.events = events
        self.records = records
        self.executions = []

    async def fetch(self, query, *_args):
        return self.events if "FROM public.events" in query else self.records

    async def fetchrow(self, _query, *_args):
        return {"name": "Audit channel", "visibility": "private"}

    async def execute(self, query, *args):
        self.executions.append((query, args))


class AuditPackUnitTests(unittest.TestCase):
    def test_event_id_and_signature_are_verified(self):
        event = audit_pack.canonical_event(signed_row())
        self.assertEqual(event["verification"]["schnorr_signature"], "valid")

    def test_tampered_event_is_rejected(self):
        row = signed_row()
        row["content"] = "tampered"
        with self.assertRaisesRegex(ValueError, "identifier verification"):
            audit_pack.canonical_event(row)

    def test_asyncpg_json_text_tags_are_canonicalized(self):
        row = signed_row(tags=[["h", "channel"]])
        row["tags"] = json.dumps(row["tags"])
        self.assertEqual(audit_pack.canonical_event(row)["tags"], [["h", "channel"]])

    def test_attachment_reference_requires_relay_media_hash(self):
        digest = "b" * 64
        refs = audit_pack.attachment_refs([["imeta", f"url http://relay/media/{digest}.zip", f"x {digest}", "filename report.docx"]])
        self.assertEqual(refs[0]["object_key"], f"{digest}.zip")
        self.assertEqual(audit_pack.attachment_refs([["imeta", "url https://example.test/file"]]), [])

    def test_manifest_seal_is_publicly_verifiable(self):
        manifest = audit_pack.seal_manifest({"pack_id": "test"}, "1" * 64)
        seal = manifest["seal"]
        self.assertTrue(PublicKeyXOnly(bytes.fromhex(seal["public_key"])).verify(bytes.fromhex(seal["signature"]), bytes.fromhex(seal["manifest_sha256"])))

    def test_non_admin_identity_cannot_export(self):
        channel = str(uuid4())
        token = current_principal.set(Principal("user", channel, "private", role="member"))
        try:
            payload = audit_pack.AuditPackRequest(channel_id=channel, start_at=datetime.now(timezone.utc), end_at=datetime.now(timezone.utc) + timedelta(hours=1), purpose="Control test")
            with self.assertRaisesRegex(Exception, "owner or admin"):
                audit_pack._authorize(payload)
        finally:
            current_principal.reset(token)

    def test_admin_identity_does_not_require_stack_secret(self):
        channel = str(uuid4())
        token = current_principal.set(Principal("admin", channel, "private", role="admin"))
        try:
            with patch.dict(os.environ, {"STACK_API_SECRET": "secret"}, clear=False):
                audit_pack._stack_secret(None)
        finally:
            current_principal.reset(token)

    def test_export_workload_is_operation_and_channel_bound(self):
        channel=str(uuid4());other=str(uuid4())
        principal=Principal('audit-exporter',channel,'private',role='service',workload=True,operations=frozenset({'audit.export'}))
        token=current_principal.set(principal)
        try:
            payload=audit_pack.AuditPackRequest(channel_id=channel,start_at=datetime.now(timezone.utc),end_at=datetime.now(timezone.utc)+timedelta(hours=1),purpose='Approved export',approval_id='a'*64)
            self.assertEqual(audit_pack._authorize(payload),('audit-exporter','service'))
            with self.assertRaisesRegex(Exception,'outside approved scope'):
                audit_pack._authorize(payload.model_copy(update={'channel_id':other}))
        finally: current_principal.reset(token)


class AuditPackEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_pack_contains_verified_events_archive_and_attachment(self):
        channel = str(uuid4())
        attachment = b"docx bytes"
        attachment_digest = hashlib.sha256(attachment).hexdigest()
        tags = [["imeta", f"url http://relay/media/{attachment_digest}.zip", f"x {attachment_digest}", "filename evidence.docx"]]
        event_row = signed_row(tags=tags)
        event_id = event_row["id"].hex()
        original = b"Audit decision"
        markdown = b"# Audit decision\n"
        record_id = uuid4()
        record_manifest = json.dumps({"objects": {"markdown_sha256": hashlib.sha256(markdown).hexdigest()}}).encode()
        record = {
            "id": record_id,
            "document_id": uuid4(),
            "content_sha256": hashlib.sha256(original).hexdigest(),
            "bucket": "channel-bucket",
            "original_key": "bundle/original",
            "markdown_key": "bundle/content",
            "record_key": "bundle/record",
            "event_id": event_id,
            "status": "indexed",
            "metadata": {},
            "created_at": event_row["created_at"],
        }
        s3 = FakeS3({
            ("channel-bucket", "bundle/original"): original,
            ("channel-bucket", "bundle/content"): markdown,
            ("channel-bucket", "bundle/record"): record_manifest,
            ("buzz-media", f"{attachment_digest}.zip"): attachment,
        })
        pool = FakePool([event_row], [record])
        app = SimpleNamespace(state=SimpleNamespace(pool=pool, s3=s3))
        request = Request({"type": "http", "method": "POST", "path": "/api/audit-packs", "headers": [], "app": app})
        payload = audit_pack.AuditPackRequest(
            channel_id=channel,
            start_at=event_row["created_at"] - timedelta(minutes=1),
            end_at=event_row["created_at"] + timedelta(minutes=1),
            purpose="Quarterly control test",
        )
        with patch.dict(os.environ, {"STACK_API_SECRET": "secret", "AUDIT_PACK_SIGNING_KEY": "2" * 64}, clear=False):
            response = await audit_pack.create_audit_pack(payload, request, "secret")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(s3.puts), 1)
        self.assertEqual(len(pool.executions), 1)
        with zipfile.ZipFile(io.BytesIO(response.body)) as archive:
            names = set(archive.namelist())
            self.assertIn(f"events/{event_id}.json", names)
            self.assertIn(f"archive/{record_id}/record.json", names)
            self.assertIn(f"attachments/{event_id}/01-evidence.docx", names)
            manifest = json.loads(archive.read("manifest.json"))
        self.assertEqual(manifest["counts"], {"events": 1, "archive_records": 1, "attachments": 1})
        self.assertEqual(manifest["scope"]["channel_name"], "Audit channel")
        self.assertEqual(manifest["verification"]["event_signatures"], "verified")

    async def test_archive_hash_mismatch_fails_closed(self):
        channel = str(uuid4())
        event_row = signed_row()
        record = {
            "id": uuid4(), "document_id": uuid4(), "content_sha256": "0" * 64,
            "bucket": "bucket", "original_key": "original", "markdown_key": "markdown",
            "record_key": "record", "event_id": event_row["id"].hex(), "status": "indexed",
            "metadata": {}, "created_at": event_row["created_at"],
        }
        s3 = FakeS3({("bucket", "original"): b"changed", ("bucket", "markdown"): b"text", ("bucket", "record"): b"{}"})
        pool = FakePool([event_row], [record])
        request = Request({"type": "http", "method": "POST", "path": "/api/audit-packs", "headers": [], "app": SimpleNamespace(state=SimpleNamespace(pool=pool, s3=s3))})
        payload = audit_pack.AuditPackRequest(channel_id=channel, start_at=event_row["created_at"] - timedelta(minutes=1), end_at=event_row["created_at"] + timedelta(minutes=1), purpose="Integrity test")
        with patch.dict(os.environ, {"STACK_API_SECRET": "secret", "AUDIT_PACK_SIGNING_KEY": "3" * 64}, clear=False):
            with self.assertRaisesRegex(Exception, "failed hash verification"):
                await audit_pack.create_audit_pack(payload, request, "secret")


class PostgresModeAttachmentTest(unittest.IsolatedAsyncioTestCase):
    """With GCOR_OBJECT_STORE=postgres, attachments come from the relay's /media endpoint."""

    def request(self, mode):
        return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(object_store=mode, s3=None)))

    async def test_rejects_keys_that_are_not_content_addressed(self):
        for key in ("../secret", "a" * 63, "A" * 64, "a" * 64 + "/x"):
            with self.subTest(key=key):
                with self.assertRaises(audit_pack.HTTPException) as error:
                    await audit_pack.relay_media_bytes(key)
                self.assertEqual(409, error.exception.status_code)

    async def test_postgres_mode_reads_relay_media_and_labels_the_source(self):
        key = "b" * 64 + ".pdf"
        with patch.object(audit_pack, "relay_media_bytes", return_value=b"bytes") as fetch:
            data, source = await audit_pack.attachment_bytes(self.request("postgres"), key)
        fetch.assert_awaited_once_with(key)
        self.assertEqual(b"bytes", data)
        self.assertEqual(f"{audit_pack.RELAY_MEDIA_URL}/media/{key}", source)

    async def test_s3_mode_keeps_reading_the_media_bucket(self):
        with patch.object(audit_pack, "s3_bytes", return_value=b"bytes") as fetch:
            _, source = await audit_pack.attachment_bytes(self.request("s3"), "c" * 64)
        fetch.assert_awaited_once()
        self.assertEqual(f"s3://{audit_pack.MEDIA_BUCKET}/{'c' * 64}", source)

    def test_object_uri_names_the_backend(self):
        self.assertEqual("gcor-postgres://b/k", audit_pack.object_uri(self.request("postgres"), "b", "k"))
        self.assertEqual("s3://b/k", audit_pack.object_uri(self.request("s3"), "b", "k"))


if __name__ == "__main__":
    unittest.main()
