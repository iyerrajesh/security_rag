"""Idempotent schema bootstrap. Run on app startup. Enables pgvector and
creates the security_events table + indexes. Requires pgvector >= 0.8 for
reliable filtered (WHERE + vector) iterative scans."""
from db import get_pool

DDL = """
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS security_events (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID NOT NULL DEFAULT gen_random_uuid(),
    detection_type  TEXT NOT NULL,
    detected_at     TIMESTAMPTZ NOT NULL,
    camera_id       TEXT NOT NULL,
    zone            TEXT,
    confidence      REAL,
    frame_paths     TEXT[],
    narration       TEXT,
    metadata        JSONB,
    embedding       VECTOR(1024),
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_embedding ON security_events
    USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);
CREATE INDEX IF NOT EXISTS idx_events_type   ON security_events (detection_type);
CREATE INDEX IF NOT EXISTS idx_events_time   ON security_events (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_camera ON security_events (camera_id);
CREATE INDEX IF NOT EXISTS idx_events_meta   ON security_events USING gin (metadata);
"""


def run_migrations():
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()
