-- Bi-temporal facts: knowledge that tracks time, in Postgres only (no Graphiti/FalkorDB).
--
-- A fact is a (subject, predicate, object) assertion extracted from ONE approved source revision,
-- with two timelines:
--   * valid time   [valid_at, invalid_at)     when it was true in the world
--   * record time  [recorded_at, expired_at)  when the knowledge base held this version
-- Rows are versions and are never rewritten: a change closes the current version (expired_at)
-- and inserts the next one. "What is true now", "what was true on date W" and "what did we
-- believe on date K" are all plain queries over this table.
--
-- Additive only: no existing table, constraint, policy or query changes. Facts are produced by the
-- optional relationship projector; the runtime role can read them (scoped through the source
-- document's row-level security) but can never write them.
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Controlled predicate vocabulary. cardinality 'one' means a subject has a single value at a time
-- (a newer fact closes the older one); 'many' facts coexist. Operators may add predicates.
CREATE TABLE IF NOT EXISTS gcor.fact_predicates (
    predicate TEXT PRIMARY KEY CHECK (predicate ~ '^[A-Z][A-Z0-9_]{1,47}$'),
    cardinality TEXT NOT NULL CHECK (cardinality IN ('one', 'many')),
    description TEXT NOT NULL CHECK (char_length(description) BETWEEN 1 AND 300)
);
INSERT INTO gcor.fact_predicates (predicate, cardinality, description) VALUES
    ('OWNED_BY',    'one',  'Person or team accountable for the subject'),
    ('ASSIGNED_TO', 'one',  'Person or team the subject is currently assigned to'),
    ('STATUS',      'one',  'Current state of the subject (e.g. planned, active, retired)'),
    ('VERSION',     'one',  'Version or release of the subject in use'),
    ('LOCATED_AT',  'one',  'Where the subject is hosted, stored or located'),
    ('DEADLINE',    'one',  'Due date of the subject'),
    ('DECISION',    'one',  'The decision currently in force about the subject'),
    ('CONFIGURED_AS','one', 'The configured value or setting of the subject'),
    ('REPORTS_TO',  'one',  'Who the subject (a person or team) reports to'),
    ('HAS_ROLE',    'one',  'Role or title held by the subject (a person)'),
    ('DEPENDS_ON',  'many', 'Something the subject depends on'),
    ('USES',        'many', 'A tool, system or component the subject uses'),
    ('MEMBER_OF',   'many', 'A group the subject belongs to')
ON CONFLICT (predicate) DO NOTHING;

CREATE TABLE IF NOT EXISTS gcor.facts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),            -- this version
    fact_id UUID NOT NULL,                                    -- the logical assertion (stable across versions)
    channel_id TEXT NOT NULL,
    access_level TEXT NOT NULL,
    agent_id TEXT,
    subject TEXT NOT NULL CHECK (char_length(subject) BETWEEN 1 AND 160),
    subject_key TEXT NOT NULL CHECK (char_length(subject_key) BETWEEN 1 AND 160),
    predicate TEXT NOT NULL REFERENCES gcor.fact_predicates(predicate),
    cardinality TEXT NOT NULL CHECK (cardinality IN ('one', 'many')),   -- snapshot of the vocabulary at assertion time
    object TEXT NOT NULL CHECK (char_length(object) BETWEEN 1 AND 400),
    object_key TEXT NOT NULL CHECK (char_length(object_key) BETWEEN 1 AND 400),
    fact TEXT NOT NULL CHECK (char_length(fact) BETWEEN 1 AND 1000),
    -- What the source said. NULL = not stated.
    stated_valid_at TIMESTAMPTZ,
    stated_invalid_at TIMESTAMPTZ,
    -- Effective valid time after reconciliation. valid_at NULL = since an unknown start.
    valid_at TIMESTAMPTZ,
    valid_at_source TEXT NOT NULL CHECK (valid_at_source IN ('stated', 'document', 'unknown')),
    invalid_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('active', 'superseded', 'contradicted', 'corroborating')),
    invalidated_by UUID,                                      -- fact_id that superseded/contradicted this one
    corroborates UUID,                                        -- fact_id this one restates
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expired_at TIMESTAMPTZ,
    expiry_reason TEXT CHECK (expiry_reason IN ('reconciled', 'source_retired')),
    source_document_id UUID NOT NULL REFERENCES gcor.documents(id) ON DELETE CASCADE,
    source_revision CHAR(64) NOT NULL,
    source_time TIMESTAMPTZ NOT NULL,                         -- when the source became knowledge (approval time)
    evidence_node_ids UUID[] NOT NULL CHECK (cardinality(evidence_node_ids) BETWEEN 1 AND 50),
    confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    projector TEXT NOT NULL,
    model TEXT,
    run_id UUID,
    search_vector tsvector GENERATED ALWAYS AS (
        to_tsvector('english', subject || ' ' || replace(lower(predicate), '_', ' ') || ' ' || object || ' ' || fact)
    ) STORED,
    CHECK (stated_invalid_at IS NULL OR stated_valid_at IS NULL OR stated_invalid_at > stated_valid_at),
    CHECK (invalid_at IS NULL OR valid_at IS NULL OR invalid_at > valid_at),
    CHECK ((expired_at IS NULL) = (expiry_reason IS NULL)),
    CHECK (expired_at IS NULL OR expired_at >= recorded_at),
    CHECK ((status IN ('superseded', 'contradicted')) = (invalidated_by IS NOT NULL)),
    CHECK ((status = 'corroborating') = (corroborates IS NOT NULL)),
    CHECK (status <> 'superseded' OR invalid_at IS NOT NULL)
);

-- One current version per logical fact.
CREATE UNIQUE INDEX IF NOT EXISTS facts_one_current_version_idx
    ON gcor.facts (fact_id) WHERE expired_at IS NULL;
CREATE INDEX IF NOT EXISTS facts_group_current_idx
    ON gcor.facts (channel_id, access_level, subject_key, predicate) WHERE expired_at IS NULL;
CREATE INDEX IF NOT EXISTS facts_source_current_idx
    ON gcor.facts (source_document_id, source_revision) WHERE expired_at IS NULL;
CREATE INDEX IF NOT EXISTS facts_fact_id_idx ON gcor.facts (fact_id, recorded_at);
CREATE INDEX IF NOT EXISTS facts_search_vector_idx ON gcor.facts USING gin (search_vector);

-- A single-valued subject can never hold two values over overlapping valid time in the current
-- belief. Deferred so a reconciliation can close and open versions inside one transaction.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'facts_single_value_no_overlap') THEN
        ALTER TABLE gcor.facts ADD CONSTRAINT facts_single_value_no_overlap EXCLUDE USING gist (
            channel_id WITH =, access_level WITH =, (COALESCE(agent_id, '')) WITH =,
            subject_key WITH =, predicate WITH =,
            tstzrange(valid_at, invalid_at) WITH &&
        ) WHERE (expired_at IS NULL AND cardinality = 'one' AND status IN ('active', 'superseded'))
        DEFERRABLE INITIALLY DEFERRED;
    END IF;
END
$$;

-- History is immutable: the only permitted change to a version is closing it, once.
CREATE OR REPLACE FUNCTION gcor.facts_versions_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.expired_at IS NOT NULL THEN
        RAISE EXCEPTION 'fact version % is closed and immutable', OLD.id USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.expired_at IS NULL
       OR (to_jsonb(NEW) - ARRAY['expired_at', 'expiry_reason', 'search_vector'])
          IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['expired_at', 'expiry_reason', 'search_vector']) THEN
        RAISE EXCEPTION 'fact versions can only be closed; insert a new version instead' USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END
$$;
DROP TRIGGER IF EXISTS facts_versions_immutable ON gcor.facts;
CREATE TRIGGER facts_versions_immutable BEFORE UPDATE ON gcor.facts
    FOR EACH ROW EXECUTE FUNCTION gcor.facts_versions_immutable();

-- Runtime services read facts only through the source document's existing row-level security
-- (channel, access level and workload scope); they can never write them.
ALTER TABLE gcor.facts ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS facts_read_through_source ON gcor.facts;
CREATE POLICY facts_read_through_source ON gcor.facts FOR SELECT TO gcor_app
USING (EXISTS (
    SELECT 1 FROM gcor.documents d
    WHERE d.id = facts.source_document_id
      AND d.channel_id IS NOT DISTINCT FROM facts.channel_id
      AND d.access_level = facts.access_level
));
-- 0011 grants gcor_app full DML on new gcor tables by default; facts are projector-owned.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON gcor.facts FROM gcor_app;
GRANT SELECT ON gcor.facts TO gcor_app;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON gcor.fact_predicates FROM gcor_app;
GRANT SELECT ON gcor.fact_predicates TO gcor_app;

COMMENT ON TABLE gcor.facts IS
    'Bi-temporal fact versions derived from approved knowledge. Current belief: expired_at IS NULL. '
    'True at time W: current belief AND status <> ''contradicted'' AND tstzrange(valid_at, invalid_at) @> W. '
    'Believed at time K: recorded_at <= K AND (expired_at IS NULL OR expired_at > K).';
