-- Fix: gcor.knowledge_evidence_current() runs with the caller's privileges and, for chat-derived
-- knowledge, reads the Buzz relay's public.events. The relationship projector role must not read
-- raw relay events (every channel's messages), so its claim, stale and publish checks failed with
-- "permission denied for table events" as soon as approved knowledge cited Buzz messages.
-- This definer wrapper answers only "is this approved document's evidence still current" and
-- exposes nothing else. Idempotent; runs after 0016_relationship_publication_locks.sql.
CREATE OR REPLACE FUNCTION gcor.relationship_evidence_current(doc_id UUID) RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, gcor
AS $$
    SELECT EXISTS (
        SELECT 1 FROM gcor.documents d
        WHERE d.id = doc_id AND d.metadata->>'knowledge_state' = 'approved'
    ) AND gcor.knowledge_evidence_current(doc_id)
$$;
REVOKE ALL ON FUNCTION gcor.relationship_evidence_current(UUID) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION gcor.relationship_evidence_current(UUID) TO gcor_relationship_projector;
