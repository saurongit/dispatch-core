-- Independent bearer capability used by a master's browser to submit GPS.
-- It is deliberately distinct from the read-only client tracking token.

ALTER TABLE tracking_sessions
    ADD COLUMN IF NOT EXISTS location_token text;

CREATE UNIQUE INDEX IF NOT EXISTS uq_tracking_location_token
    ON tracking_sessions (location_token)
    WHERE location_token IS NOT NULL;

ALTER TABLE tracking_sessions
    DROP CONSTRAINT IF EXISTS tracking_sessions_location_token_strength;
ALTER TABLE tracking_sessions
    ADD CONSTRAINT tracking_sessions_location_token_strength
    CHECK (location_token IS NULL OR length(location_token) >= 43);

ALTER TABLE tracking_sessions
    DROP CONSTRAINT IF EXISTS tracking_sessions_capabilities_distinct;
ALTER TABLE tracking_sessions
    ADD CONSTRAINT tracking_sessions_capabilities_distinct
    CHECK (
        public_token IS NULL
        OR location_token IS NULL
        OR public_token <> location_token
    );
