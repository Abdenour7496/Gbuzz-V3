"""PostgreSQL object store (GCOR_OBJECT_STORE=postgres).

The pure tests always run. The database tests need a migrated database (0019 applied) and run when
GCOR_TEST_PG_DSN points at it as the restricted runtime role, e.g.
postgresql://gcor_runtime:runtime-secret@127.0.0.1:5499/integration
"""
import asyncio
import concurrent.futures
import hashlib
import os
import unittest
from uuid import uuid4

from botocore.exceptions import ClientError

import object_store
from object_store import PostgresObjectStore, body_bytes, decode_token, encode_token

DSN = os.getenv("GCOR_TEST_PG_DSN")


class PureHelpersTest(unittest.TestCase):
    def test_body_conversions(self):
        self.assertEqual(b"abc", body_bytes(b"abc"))
        self.assertEqual("é".encode(), body_bytes("é"))
        self.assertEqual(b"", body_bytes(None))

        class Reader:
            def read(self):
                return "streamed"
        self.assertEqual(b"streamed", body_bytes(Reader()))
        with self.assertRaises(TypeError):
            body_bytes(42)

    def test_continuation_token_round_trip_and_rejects_garbage(self):
        key = "bundles/é/record.json"
        self.assertEqual(key, decode_token(encode_token(key)))
        with self.assertRaises(ClientError) as error:
            decode_token("%%%")
        self.assertEqual("InvalidArgument", error.exception.response["Error"]["Code"])

    def test_errors_use_s3_codes(self):
        error = object_store.client_error("NoSuchKey", "GetObject", 404)
        self.assertEqual("NoSuchKey", error.response["Error"]["Code"])
        self.assertEqual(404, error.response["ResponseMetadata"]["HTTPStatusCode"])


@unittest.skipUnless(DSN, "set GCOR_TEST_PG_DSN to run against PostgreSQL")
class PostgresObjectStoreTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = PostgresObjectStore(dsn=DSN, timeout=10)

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def setUp(self):
        self.bucket = f"test-{uuid4().hex[:12]}"

    def test_put_get_head_round_trip(self):
        data = b"original bytes"
        put = self.store.put_object(Bucket=self.bucket, Key="docs/a.pdf", Body=data,
                                    ContentType="application/pdf", Metadata={"Event_ID": "e1"})
        self.assertEqual(f'"{hashlib.sha256(data).hexdigest()}"', put["ETag"])
        got = self.store.get_object(Bucket=self.bucket, Key="docs/a.pdf")
        self.assertEqual(data, got["Body"].read())
        got["Body"].close()
        self.assertEqual("application/pdf", got["ContentType"])
        self.assertEqual({"event_id": "e1"}, got["Metadata"])
        head = self.store.head_object(Bucket=self.bucket, Key="docs/a.pdf")
        self.assertEqual(len(data), head["ContentLength"])
        self.assertEqual(put["VersionId"], head["VersionId"])

    def test_missing_objects_raise_s3_errors(self):
        with self.assertRaises(ClientError) as error:
            self.store.get_object(Bucket=self.bucket, Key="missing")
        self.assertEqual("NoSuchKey", error.exception.response["Error"]["Code"])
        with self.assertRaises(ClientError) as error:
            self.store.head_object(Bucket=self.bucket, Key="missing")
        self.assertEqual("404", error.exception.response["Error"]["Code"])
        with self.assertRaises(ClientError):
            self.store.get_object(Bucket=self.bucket, Key="missing", VersionId="not-a-uuid")

    def test_overwrite_keeps_history_and_one_current_version(self):
        first = self.store.put_object(Bucket=self.bucket, Key="k", Body=b"v1")
        second = self.store.put_object(Bucket=self.bucket, Key="k", Body=b"v2")
        self.assertEqual(b"v2", self.store.get_object(Bucket=self.bucket, Key="k")["Body"].read())
        old = self.store.get_object(Bucket=self.bucket, Key="k", VersionId=first["VersionId"])
        self.assertEqual(b"v1", old["Body"].read())
        versions = self.store.list_object_versions(Bucket=self.bucket, Prefix="k")["Versions"]
        self.assertEqual([second["VersionId"], first["VersionId"]], [item["VersionId"] for item in versions])
        self.assertEqual([True, False], [item["IsLatest"] for item in versions])

    def test_conditional_write_refuses_existing_key(self):
        self.store.put_object(Bucket=self.bucket, Key="governance/e.json", Body=b"{}", IfNoneMatch="*")
        with self.assertRaises(ClientError) as error:
            self.store.put_object(Bucket=self.bucket, Key="governance/e.json", Body=b"{}", IfNoneMatch="*")
        self.assertEqual("PreconditionFailed", error.exception.response["Error"]["Code"])
        self.assertEqual(1, len(self.store.list_object_versions(Bucket=self.bucket, Prefix="governance/")["Versions"]))

    def test_listing_filters_prefix_and_pages_in_byte_order(self):
        keys = ["bundles/a/record.json", "bundles/B/record.json", "bundles/b/record.json", "other/x"]
        for key in keys:
            self.store.put_object(Bucket=self.bucket, Key=key, Body=key)
        seen, token = [], None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": "bundles/", "MaxKeys": 2}
            if token:
                kwargs["ContinuationToken"] = token
            page = self.store.list_objects_v2(**kwargs)
            seen.extend(item["Key"] for item in page["Contents"])
            if not page["IsTruncated"]:
                break
            token = page["NextContinuationToken"]
        self.assertEqual(sorted(keys[:3]), seen)  # Python sorts str by code point, as COLLATE "C" does.
        self.assertIn(self.bucket, [item["Name"] for item in self.store.list_buckets()["Buckets"]])

    def test_identical_content_is_stored_once(self):
        self.store.put_object(Bucket=self.bucket, Key="one", Body=b"same bytes")
        self.store.put_object(Bucket=self.bucket, Key="two", Body=b"same bytes")

        async def count():
            import asyncpg
            connection = await asyncpg.connect(DSN)
            try:
                return await connection.fetchval(
                    "SELECT count(*) FROM gcor.blobs WHERE sha256=$1", hashlib.sha256(b"same bytes").hexdigest())
            finally:
                await connection.close()
        self.assertEqual(1, asyncio.run(count()))

    def test_concurrent_writers_leave_exactly_one_current_version(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda n: self.store.put_object(Bucket=self.bucket, Key="race", Body=f"v{n}"), range(16)))
        versions = self.store.list_object_versions(Bucket=self.bucket, Prefix="race")["Versions"]
        self.assertEqual(16, len(versions))
        self.assertEqual(1, sum(item["IsLatest"] for item in versions))

    def test_runtime_role_cannot_rewrite_or_delete_objects(self):
        import asyncpg
        self.store.put_object(Bucket=self.bucket, Key="locked", Body=b"draft")
        self.store.put_object(Bucket=self.bucket, Key="locked", Body=b"evidence")

        async def attempt(statement):
            connection = await asyncpg.connect(DSN)
            try:
                await connection.execute(statement, self.bucket)
            finally:
                await connection.close()
        for statement in ("DELETE FROM gcor.objects WHERE bucket=$1",
                          "UPDATE gcor.objects SET sha256=sha256 WHERE bucket=$1",
                          "UPDATE gcor.objects SET is_latest=true WHERE bucket=$1 AND NOT is_latest"):
            with self.subTest(statement=statement):
                with self.assertRaises(asyncpg.PostgresError):
                    asyncio.run(attempt(statement))
        self.assertEqual(b"evidence", self.store.get_object(Bucket=self.bucket, Key="locked")["Body"].read())

    def test_bucket_housekeeping_is_a_no_op(self):
        self.assertEqual({}, self.store.create_bucket(Bucket=self.bucket))
        self.assertEqual({}, self.store.put_bucket_versioning(Bucket=self.bucket, VersioningConfiguration={"Status": "Enabled"}))
        self.assertEqual({}, self.store.head_bucket(Bucket=self.bucket))


if __name__ == "__main__":
    unittest.main()
