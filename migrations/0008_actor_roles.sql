-- A person is one actor. Roles are optional memberships, so a nano-service
-- owner can explicitly be admin, operator and master without duplicate actors.

CREATE TABLE IF NOT EXISTS actor_roles (
    organization_id text NOT NULL,
    actor_id text NOT NULL,
    role text NOT NULL CHECK (role IN ('admin', 'operator', 'master', 'client')),
    active boolean NOT NULL DEFAULT true,
    granted_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz,
    PRIMARY KEY (organization_id, actor_id, role),
    FOREIGN KEY (organization_id, actor_id)
        REFERENCES actors(organization_id, id) ON DELETE CASCADE,
    CHECK (
        (active AND revoked_at IS NULL)
        OR (NOT active AND revoked_at IS NOT NULL)
    )
);

INSERT INTO actor_roles (organization_id, actor_id, role)
SELECT organization_id, id, role
FROM actors
ON CONFLICT (organization_id, actor_id, role) DO UPDATE SET
    active = true,
    revoked_at = NULL;

CREATE INDEX IF NOT EXISTS ix_actor_roles_active_role
    ON actor_roles (organization_id, role, actor_id)
    WHERE active;

COMMENT ON COLUMN actors.role IS
    'Primary/default role for compatibility; authorization uses actor_roles';
