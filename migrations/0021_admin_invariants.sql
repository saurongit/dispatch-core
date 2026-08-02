CREATE UNIQUE INDEX IF NOT EXISTS uq_actor_roles_one_active_admin
    ON actor_roles (organization_id)
    WHERE role = 'admin' AND active;

ALTER TABLE config_sessions
    DROP CONSTRAINT config_sessions_pkey;

ALTER TABLE config_sessions
    ADD PRIMARY KEY (organization_id, actor_id, provider);
