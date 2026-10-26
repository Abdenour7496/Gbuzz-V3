-- Unanswered-question queue: questions that retrieval could not answer from authorised evidence
-- become visible work for the channel's people and agents instead of being discarded.
-- Additive: one table, scoped to the asking channel and access level by row-level security.
CREATE TABLE IF NOT EXISTS gcor.unanswered_questions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_id TEXT NOT NULL,
    access_level TEXT NOT NULL,
    question TEXT NOT NULL CHECK (char_length(question) BETWEEN 1 AND 2000),
    question_key TEXT NOT NULL,
    asked_count INTEGER NOT NULL DEFAULT 1 CHECK (asked_count > 0),
    askers TEXT[] NOT NULL DEFAULT '{}',
    first_asked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_asked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'answered', 'dismissed')),
    resolved_by TEXT,
    resolved_at TIMESTAMPTZ,
    resolution_note TEXT CHECK (resolution_note IS NULL OR char_length(resolution_note) <= 4000),
    answer_document_id UUID REFERENCES gcor.documents(id) ON DELETE SET NULL,
    UNIQUE (channel_id, access_level, question_key)
);
CREATE INDEX IF NOT EXISTS unanswered_questions_queue_idx
    ON gcor.unanswered_questions (channel_id, access_level, status, last_asked_at DESC);

ALTER TABLE gcor.unanswered_questions ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS unanswered_questions_scope ON gcor.unanswered_questions;
CREATE POLICY unanswered_questions_scope ON gcor.unanswered_questions FOR ALL TO gcor_app
USING (gcor.document_in_scope(channel_id, access_level) OR gcor.scope_workload() = 'ingestion-worker')
WITH CHECK (gcor.document_in_scope(channel_id, access_level) OR gcor.scope_workload() = 'ingestion-worker');

GRANT SELECT, INSERT, UPDATE, DELETE ON gcor.unanswered_questions TO gcor_app;
