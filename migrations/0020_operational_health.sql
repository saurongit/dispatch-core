CREATE TABLE IF NOT EXISTS worker_heartbeats (
    organization_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    consumer_key text NOT NULL DEFAULT '',
    instance_id text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    heartbeat_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, consumer_key, instance_id)
);

CREATE INDEX IF NOT EXISTS ix_worker_heartbeats_stale
    ON worker_heartbeats (organization_id, heartbeat_at);

-- Some pre-repository deployments created this redundant non-unique index.
-- The partial unique index uq_actors_active_bind_code is authoritative.
DROP INDEX IF EXISTS idx_actors_bind_code;
