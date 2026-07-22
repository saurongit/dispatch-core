-- Multiple role bots of one provider must never consume each other's updates.
-- MAX uses one shared staff frontend with an explicit, durable role selection.

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
