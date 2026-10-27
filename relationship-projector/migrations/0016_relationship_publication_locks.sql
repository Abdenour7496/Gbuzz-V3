-- Fix: the relationship projector locks its source rows while claiming and publishing
-- (SELECT ... FOR UPDATE on gcor.documents, FOR SHARE on gcor.chunks) so a source cannot change
-- or lose approval mid-publication. PostgreSQL requires UPDATE privilege and a passing UPDATE
-- policy to take those row locks, which 0015 never granted, so claims and publication failed with
-- "permission denied for table documents". Grant one column each and add UPDATE policies whose
-- WITH CHECK is false: the projector can lock approved sources but can never modify them.
-- Idempotent; runs after 0015_postgres_relationship_projection.sql.
GRANT UPDATE (updated_at) ON gcor.documents TO gcor_relationship_projector;
GRANT UPDATE (ordinal) ON gcor.chunks TO gcor_relationship_projector;
DROP POLICY IF EXISTS relationship_documents_lock ON gcor.documents;
CREATE POLICY relationship_documents_lock ON gcor.documents FOR UPDATE TO gcor_relationship_projector
    USING (gcor.scope_workload() = 'relationship-projector' AND metadata->>'knowledge_state' = 'approved')
    WITH CHECK (false);
DROP POLICY IF EXISTS relationship_chunks_lock ON gcor.chunks;
CREATE POLICY relationship_chunks_lock ON gcor.chunks FOR UPDATE TO gcor_relationship_projector
    USING (gcor.scope_workload() = 'relationship-projector' AND EXISTS (SELECT 1 FROM gcor.documents d WHERE d.id = chunks.document_id AND d.metadata->>'knowledge_state' = 'approved'))
    WITH CHECK (false);
