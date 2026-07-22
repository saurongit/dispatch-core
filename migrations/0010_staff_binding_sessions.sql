-- A staff account is not an actor until a short-lived, rate-limited binding
-- ceremony has successfully consumed an administrator-issued code.

CREATE TABLE IF NOT EXISTS staff_binding_sessions (
    organization_id text NOT NULL
        REFERENCES organizations(id) ON DELETE CASCADE,
    provider text NOT NULL CHECK (provider IN ('telegram', 'max')),
    external_user_id text NOT NULL,
    consumer_key text NOT NULL CHECK (consumer_key IN ('operator', 'master')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 5),
    expires_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (
        organization_id, provider, external_user_id, consumer_key
    )
);

CREATE INDEX IF NOT EXISTS ix_staff_binding_sessions_expiry
    ON staff_binding_sessions (expires_at);
