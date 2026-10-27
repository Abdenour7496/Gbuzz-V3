-- Relationship-projector privileges for bi-temporal facts (table: migrations/0018_temporal_facts.sql).
-- Idempotent; runs after 0015_postgres_relationship_projection.sql. The projector may only insert
-- facts for an approved, evidence-current source revision in that source's own channel, citing that
-- source's own chunks, and may only close versions (enforced by gcor.facts_versions_immutable).
CREATE OR REPLACE FUNCTION gcor.relationship_fact_source_valid(
    candidate_document UUID,
    candidate_revision TEXT,
    candidate_channel TEXT,
    candidate_access TEXT,
    candidate_agent TEXT,
    candidate_evidence UUID[]
) RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, gcor
AS $$
    SELECT EXISTS (
        SELECT 1 FROM gcor.documents d
        WHERE d.id = candidate_document
          AND d.content_sha256 = candidate_revision
          AND d.metadata->>'knowledge_state' = 'approved'
          AND d.metadata->>'channel_id' = candidate_channel
          AND d.access_level = candidate_access
          AND d.agent_id IS NOT DISTINCT FROM candidate_agent
          AND gcor.knowledge_evidence_current(d.id)
    ) AND cardinality(candidate_evidence) > 0 AND NOT EXISTS (
        SELECT 1 FROM unnest(candidate_evidence) AS evidence(node_id)
        WHERE NOT EXISTS (
            SELECT 1 FROM gcor.chunks c
            WHERE c.node_id = evidence.node_id AND c.document_id = candidate_document
        )
    )
$$;
REVOKE ALL ON FUNCTION gcor.relationship_fact_source_valid(UUID, TEXT, TEXT, TEXT, TEXT, UUID[]) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION gcor.relationship_fact_source_valid(UUID, TEXT, TEXT, TEXT, TEXT, UUID[]) TO gcor_relationship_projector;

DROP POLICY IF EXISTS relationship_facts_select ON gcor.facts;
DROP POLICY IF EXISTS relationship_facts_insert ON gcor.facts;
DROP POLICY IF EXISTS relationship_facts_update ON gcor.facts;
CREATE POLICY relationship_facts_select ON gcor.facts FOR SELECT TO gcor_relationship_projector
    USING (gcor.scope_workload() = 'relationship-projector' AND projector = 'postgres_relationship_v1');
CREATE POLICY relationship_facts_insert ON gcor.facts FOR INSERT TO gcor_relationship_projector
    WITH CHECK (gcor.scope_workload() = 'relationship-projector' AND projector = 'postgres_relationship_v1'
                AND expired_at IS NULL
                AND gcor.relationship_fact_source_valid(
                    source_document_id, source_revision, channel_id, access_level, agent_id, evidence_node_ids));
CREATE POLICY relationship_facts_update ON gcor.facts FOR UPDATE TO gcor_relationship_projector
    USING (gcor.scope_workload() = 'relationship-projector' AND projector = 'postgres_relationship_v1')
    WITH CHECK (gcor.scope_workload() = 'relationship-projector' AND projector = 'postgres_relationship_v1');

REVOKE DELETE, TRUNCATE ON gcor.facts FROM gcor_relationship_projector;
GRANT SELECT, INSERT, UPDATE ON gcor.facts TO gcor_relationship_projector;
GRANT SELECT ON gcor.fact_predicates TO gcor_relationship_projector;
