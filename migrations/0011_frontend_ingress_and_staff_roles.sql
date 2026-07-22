-- Multiple role bots of one provider must never consume each other's updates.
-- MAX uses one shared staff frontend with an explicit, durable role selection.

-- Early development builds scoped external identities by consumer_key. The
-- current model keeps one person/actor and grants several roles through
-- actor_roles. Collapse those legacy rows before adding foreign keys that rely
-- on the canonical three-column identity key.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'external_identities'
          AND column_name = 'consumer_key'
    ) THEN
        CREATE TEMP TABLE legacy_external_identity_winners ON COMMIT DROP AS
        SELECT DISTINCT ON (
            identity.organization_id,
            identity.provider,
            identity.external_user_id
        )
            identity.organization_id,
            identity.provider,
            identity.external_user_id,
            identity.actor_id,
            identity.created_at
        FROM external_identities AS identity
        ORDER BY
            identity.organization_id,
            identity.provider,
            identity.external_user_id,
            CASE identity.consumer_key
                WHEN 'admin' THEN 0
                WHEN 'operator' THEN 1
                WHEN 'master' THEN 2
                WHEN 'client' THEN 3
                ELSE 4
            END,
            identity.created_at,
            identity.actor_id;

        INSERT INTO actor_roles (
            organization_id, actor_id, role, active, revoked_at
        )
        SELECT DISTINCT
            winner.organization_id,
            winner.actor_id,
            membership.role,
            true,
            NULL::timestamptz
        FROM legacy_external_identity_winners AS winner
        JOIN external_identities AS identity
          ON identity.organization_id = winner.organization_id
         AND identity.provider = winner.provider
         AND identity.external_user_id = winner.external_user_id
        JOIN actor_roles AS membership
          ON membership.organization_id = identity.organization_id
         AND membership.actor_id = identity.actor_id
         AND membership.active
        ON CONFLICT (organization_id, actor_id, role) DO UPDATE SET
            active = true,
            revoked_at = NULL;

        DELETE FROM external_identities;
        INSERT INTO external_identities (
            organization_id, provider, external_user_id, actor_id, created_at
        )
        SELECT organization_id, provider, external_user_id, actor_id, created_at
        FROM legacy_external_identity_winners;

        ALTER TABLE external_identities
            DROP CONSTRAINT IF EXISTS external_identities_pkey;
        ALTER TABLE external_identities DROP COLUMN consumer_key;
        ALTER TABLE external_identities ADD PRIMARY KEY (
            organization_id, provider, external_user_id
        );
    END IF;
END
$$;

ALTER TABLE inbox_events
    ADD COLUMN IF NOT EXISTS consumer_key text NOT NULL DEFAULT '';

ALTER TABLE inbox_events DROP CONSTRAINT IF EXISTS inbox_events_pkey;
ALTER TABLE inbox_events ADD CONSTRAINT inbox_events_pkey PRIMARY KEY (
    organization_id, provider, consumer_key, external_event_id
);

DROP INDEX IF EXISTS ix_inbox_events_claim;
CREATE INDEX ix_inbox_events_claim
    ON inbox_events (
        organization_id, consumer_key, status, next_attempt_at, received_at
    )
    WHERE status IN ('pending', 'processing');

ALTER TABLE staff_binding_sessions
    DROP CONSTRAINT IF EXISTS staff_binding_sessions_consumer_key_check;
ALTER TABLE staff_binding_sessions
    ADD CONSTRAINT staff_binding_sessions_consumer_key_check
    CHECK (consumer_key IN ('operator', 'master', 'staff'));

CREATE TABLE IF NOT EXISTS staff_role_selections (
    organization_id text NOT NULL,
    provider text NOT NULL CHECK (provider = 'max'),
    external_user_id text NOT NULL,
    role text NOT NULL CHECK (role IN ('admin', 'operator', 'master')),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, provider, external_user_id),
    FOREIGN KEY (organization_id, provider, external_user_id)
        REFERENCES external_identities (
            organization_id, provider, external_user_id
        ) ON DELETE CASCADE
);
