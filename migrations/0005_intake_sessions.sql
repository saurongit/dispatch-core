CREATE TABLE IF NOT EXISTS intake_sessions (
    organization_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    actor_id text NOT NULL,
    provider text NOT NULL CHECK (provider IN ('telegram', 'max')),
    state jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, actor_id),
    FOREIGN KEY (organization_id, actor_id)
        REFERENCES actors(organization_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_intake_sessions_updated
    ON intake_sessions (organization_id, updated_at DESC);
