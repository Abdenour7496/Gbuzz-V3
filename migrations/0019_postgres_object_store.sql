-- GCOR object storage inside PostgreSQL (GCOR_OBJECT_STORE=postgres).
--
-- GCOR keeps original files, Markdown renditions, record.json recovery bundles, governance
-- evidence and audit packs as objects addressed by (bucket, object_key). With this migration they
-- can live here instead of an S3 service: bytes are stored once per SHA-256 in gcor.blobs and each
-- object version points at a blob. The Buzz relay's own media is unaffected and stays in S3.
--
-- Integrity and history are enforced by the database, not by the service:
--   * a blob's digest is checked on insert, so stored bytes always match their key;
--   * object versions are append-only: the only permitted change is clearing is_latest when a
--     newer version is written, and nothing can delete a row (erasure will be a separate,
--     audited definer function);
--   * one current version per (bucket, object_key), the S3 versioning model.
--
-- Additive only. The tables stay empty while GCOR_OBJECT_STORE=s3.

CREATE TABLE IF NOT EXISTS gcor.blobs (
    sha256 TEXT PRIMARY KEY CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    size BIGINT NOT NULL CHECK (size >= 0),
    content BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT blobs_size_matches CHECK (octet_length(content) = size),
    CONSTRAINT blobs_digest_matches CHECK (encode(sha256(content), 'hex') = sha256)
);
-- Large files are already compressed formats or are compressed by TOAST; keep them out of line.
ALTER TABLE gcor.blobs ALTER COLUMN content SET STORAGE EXTERNAL;

CREATE TABLE IF NOT EXISTS gcor.objects (
    bucket TEXT NOT NULL CHECK (char_length(bucket) BETWEEN 1 AND 255),
    object_key TEXT NOT NULL CHECK (char_length(object_key) BETWEEN 1 AND 1024),
    version_id UUID NOT NULL DEFAULT gen_random_uuid(),
    sha256 TEXT NOT NULL REFERENCES gcor.blobs(sha256),
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    is_latest BOOLEAN NOT NULL DEFAULT true,
    PRIMARY KEY (bucket, object_key, version_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS objects_one_latest ON gcor.objects (bucket, object_key) WHERE is_latest;
CREATE INDEX IF NOT EXISTS objects_latest_listing ON gcor.objects (bucket, object_key text_pattern_ops) WHERE is_latest;
CREATE INDEX IF NOT EXISTS objects_sha256 ON gcor.objects (sha256);

CREATE OR REPLACE FUNCTION gcor.objects_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'gcor.objects is append-only' USING ERRCODE = 'insufficient_privilege';
    END IF;
    -- A new version may retire the previous one; nothing else about a version can change.
    IF OLD.is_latest AND NOT NEW.is_latest
       AND (NEW.bucket, NEW.object_key, NEW.version_id, NEW.sha256, NEW.content_type, NEW.metadata, NEW.created_at)
           IS NOT DISTINCT FROM
           (OLD.bucket, OLD.object_key, OLD.version_id, OLD.sha256, OLD.content_type, OLD.metadata, OLD.created_at) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'gcor.objects versions are immutable' USING ERRCODE = 'insufficient_privilege';
END
$$;
DROP TRIGGER IF EXISTS objects_append_only ON gcor.objects;
CREATE TRIGGER objects_append_only BEFORE UPDATE OR DELETE ON gcor.objects
FOR EACH ROW EXECUTE FUNCTION gcor.objects_append_only();

CREATE OR REPLACE FUNCTION gcor.blobs_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'gcor.blobs is immutable' USING ERRCODE = 'insufficient_privilege';
END
$$;
DROP TRIGGER IF EXISTS blobs_immutable ON gcor.blobs;
CREATE TRIGGER blobs_immutable BEFORE UPDATE OR DELETE ON gcor.blobs
FOR EACH ROW EXECUTE FUNCTION gcor.blobs_immutable();

-- Runtime services read and add objects; they can retire a current version but never rewrite or
-- delete one. Default privileges from 0011 granted more, so narrow them explicitly.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gcor_app') THEN
        REVOKE ALL ON gcor.blobs, gcor.objects FROM gcor_app;
        GRANT SELECT, INSERT ON gcor.blobs TO gcor_app;
        GRANT SELECT, INSERT ON gcor.objects TO gcor_app;
        GRANT UPDATE (is_latest) ON gcor.objects TO gcor_app;
    END IF;
END
$$;

COMMENT ON TABLE gcor.blobs IS 'Content-addressed GCOR object bytes (GCOR_OBJECT_STORE=postgres). Digest-checked and immutable.';
COMMENT ON TABLE gcor.objects IS 'Append-only object versions addressed by (bucket, object_key); the S3 versioning model inside PostgreSQL.';
