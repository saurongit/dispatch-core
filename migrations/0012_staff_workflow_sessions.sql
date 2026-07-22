-- Operator/master messenger state is role- and provider-scoped. One nano-owner
-- can therefore configure the service as admin and add a master as operator
-- without the two finite-state flows overwriting each other.

CREATE TABLE IF NOT EXISTS staff_workflow_sessions (
    organization_id text NOT NULL
        REFERENCES organizations(id) ON DELETE CASCADE,
    actor_id text NOT NULL,
    role text NOT NULL CHECK (role IN ('operator', 'master')),
    provider text NOT NULL CHECK (provider IN ('telegram', 'max')),
    state jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, actor_id, role, provider),
    FOREIGN KEY (organization_id, actor_id)
        REFERENCES actors(organization_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_staff_workflow_sessions_updated
    ON staff_workflow_sessions (organization_id, updated_at DESC);
