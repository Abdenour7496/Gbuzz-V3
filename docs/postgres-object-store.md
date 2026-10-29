# GCOR object storage in PostgreSQL

`GCOR_OBJECT_STORE=postgres` keeps every GCOR object in the database instead of MinIO: original
files, Markdown renditions, `record.json` recovery bundles, session bundles, published governance
evidence and audit packs. GCOR then needs no S3 credentials and backup of all knowledge is one
`pg_dump`. The Buzz relay is unaffected: its media still lives in S3, because the relay's media
client is S3-only.

`s3` stays the code and Compose default so an existing installation never switches by accident.
New installations get `postgres` from `.env.example` and `scripts/new-local-env.ps1`.

## How it works

Migration `0019_postgres_object_store.sql` adds two tables:

| Table | Holds | Guarantees |
| --- | --- | --- |
| `gcor.blobs` | Bytes, one row per SHA-256 | A check constraint recomputes the digest on insert; rows can never be updated or deleted |
| `gcor.objects` | Versions addressed by `(bucket, object_key)` | Append-only; one current version per key; the only allowed change is retiring the previous version |

`proxy/object_store.py` implements the small part of the S3 API that GCOR uses (put, get, head,
list, list versions, conditional `IfNoneMatch="*"` writes) on these tables and raises the same
error codes. Every existing code path — ingestion, recovery backfill and rebuild, the governance
outbox, audit packs — runs unchanged on either backend. Buckets become plain namespaces.

The runtime role (`gcor_app`) may read and insert objects and retire a current version. It cannot
change or delete any version or blob; that is stricter than the S3 credential it replaces, which
could overwrite objects. Audit-pack attachments, which belong to the relay, are read through the
relay's `/media/<sha256>` endpoint and verified by hash.

## New installation

1. Keep `GCOR_OBJECT_STORE=postgres` in `.env` (the template default).
2. Start the stack. `gcor-migrate` applies 0019; the proxy refuses to start in `postgres` mode if
   the table is missing.
3. `/health/ready` reports `object_storage: ok` from the database.

## Switching an existing installation

The copy never changes the bucket, so rollback is setting `GCOR_OBJECT_STORE=s3` again.

1. Take and verify a normal backup (`scripts/backup-stack.ps1`).
2. Deploy this release with `GCOR_OBJECT_STORE` unset (still `s3`). Migration 0019 is additive.
3. Dry run, then copy. Use the same Compose file set as the running stack:

   ```powershell
   docker compose <files> run --rm --no-deps gcor-proxy python migrate_objects.py --dry-run
   docker compose <files> run --rm --no-deps gcor-proxy python migrate_objects.py
   ```

   It copies every object version oldest first, verifies each SHA-256, skips versions already
   copied, and ends by checking that every object referenced by `ingestion_records`, published
   `governance_outbox` rows and `audit_pack_exports` exists in PostgreSQL with the digest S3 holds.
   It exits non-zero if anything is missing or differs, and prints a JSON report.
4. Stop ingestion briefly (or accept a second pass), run the copy once more to pick up objects
   written since step 3, then set `GCOR_OBJECT_STORE=postgres` in `.env` and recreate the GCOR
   services (`gcor-proxy`, `gcor-enterprise`, `buzz-knowledge`).
5. Check `/health/ready`, run an authenticated ingest and ask, and open one recovery record.

The scoped MinIO user, channel-bucket prefix and GCOR bucket versioning are then unused. Remove
them only after a backup cycle has completed on the new mode.

## Backups and restore drills

`pg_dump` now contains GCOR's objects. The backup's referential gate skips references that resolve
in `gcor.objects` and still checks the rest against the MinIO export, so a reference missing from
both stores fails the backup. The restore drill does the same. Relay media continues to be exported
from MinIO.

Database size grows with stored files (25 MB per file cap). Watch it past a few tens of GB of
files, and keep the off-host backup copy current: after the switch it is the only second copy of
GCOR's files.

## Verification

- `tests/test_object_store.py`: pure helpers always; with `GCOR_TEST_PG_DSN` set, round trips,
  versioning, conditional writes, byte-order paging, deduplication, 16 concurrent writers on one
  key, and the runtime role being unable to rewrite or delete.
- `tests/integration/object_store_migration.py`: dry run, versioned copy with history, digests,
  idempotent re-run, and pick-up of later changes.
- CI runs the whole integration lifecycle with `postgres`, then the real MinIO outage check with
  `s3`.
