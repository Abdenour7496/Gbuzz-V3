"""S3 -> PostgreSQL object copy: history, digests, idempotency and dry run.

Needs the integration S3 stand-in (MINIO_*), the migrated database and schema-owner credentials
(POSTGRES_ADMIN_*). Run from the proxy directory: python /app/tests/integration/object_store_migration.py
"""
import hashlib
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
TOOL = next(path for path in (HERE.parents[2] / "proxy" / "migrate_objects.py", Path("/app/migrate_objects.py")) if path.exists())
sys.path.insert(0, str(TOOL.parent))

from object_store import PostgresObjectStore  # noqa: E402

bucket = f"migration-{uuid.uuid4().hex[:10]}"
s3 = boto3.client("s3", endpoint_url=os.environ["MINIO_ENDPOINT"], aws_access_key_id=os.environ["MINIO_ROOT_USER"],
                  aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"], region_name="us-east-1")
s3.create_bucket(Bucket=bucket)
s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
s3.put_object(Bucket=bucket, Key="bundles/doc/original.bin", Body=b"first draft", ContentType="application/pdf")
s3.put_object(Bucket=bucket, Key="bundles/doc/original.bin", Body=b"approved text", ContentType="application/pdf",
              Metadata={"event_id": "e-1"})
s3.put_object(Bucket=bucket, Key="bundles/doc/record.json", Body=json.dumps({"ok": True}).encode(), ContentType="application/json")
s3.put_object(Bucket=bucket, Key="governance/doc/events/1.json", Body=b'{"action":"approved"}')


def run(*extra):
    result = subprocess.run([sys.executable, str(TOOL), "--bucket", bucket, *extra],
                            capture_output=True, text=True, cwd=str(TOOL.parent), timeout=300)
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


dry = run("--dry-run")
assert dry["buckets"][bucket] == {"versions": 4, "copied": 0, "skipped": 0, "bytes": 0}, dry

first = run()
assert first["buckets"][bucket]["copied"] == 4, first
assert first["references"]["differing_count"] == 0 and first["references"]["missing_count"] == 0, first["references"]

store = PostgresObjectStore(host=os.environ["POSTGRES_HOST"], port=int(os.getenv("POSTGRES_PORT", "5432")),
                            user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"],
                            database=os.environ["POSTGRES_DB"])
try:
    for key in ("bundles/doc/original.bin", "bundles/doc/record.json", "governance/doc/events/1.json"):
        expected = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        stored = store.get_object(Bucket=bucket, Key=key)
        assert stored["Body"].read() == expected, key
        assert stored["ETag"] == f'"{hashlib.sha256(expected).hexdigest()}"', key
    original = store.get_object(Bucket=bucket, Key="bundles/doc/original.bin")
    source = s3.get_object(Bucket=bucket, Key="bundles/doc/original.bin")
    assert original["ContentType"] == "application/pdf", original
    # Metadata is copied exactly as S3 reports it (some S3 servers drop user metadata on versions).
    copied = {k: v for k, v in original["Metadata"].items() if k != "migrated_from_version"}
    assert copied == {k.lower(): v for k, v in (source.get("Metadata") or {}).items()}, (copied, source.get("Metadata"))
    assert original["Metadata"]["migrated_from_version"], original
    history = store.list_object_versions(Bucket=bucket, Prefix="bundles/doc/original.bin")["Versions"]
    assert [item["IsLatest"] for item in history] == [True, False], history
    older = store.get_object(Bucket=bucket, Key="bundles/doc/original.bin", VersionId=history[1]["VersionId"])
    assert older["Body"].read() == b"first draft"

    again = run()
    assert again["buckets"][bucket] == {"versions": 4, "copied": 0, "skipped": 4, "bytes": 0}, again

    s3.put_object(Bucket=bucket, Key="bundles/doc/record.json", Body=b'{"ok": "revised"}')
    delta = run()
    assert delta["buckets"][bucket]["copied"] == 1 and delta["buckets"][bucket]["skipped"] == 4, delta
    assert store.get_object(Bucket=bucket, Key="bundles/doc/record.json")["Body"].read() == b'{"ok": "revised"}'
finally:
    store.close()

print("PASS: S3 versions copied oldest first with digests verified, current version and history preserved, "
      "dry run writes nothing, re-runs skip copied versions and pick up later changes")
