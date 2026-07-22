-- Opaque bearer capability for the client-facing tracking view.
-- Existing active sessions remain private; every new trip receives a fresh token.

ALTER TABLE tracking_sessions
    ADD COLUMN IF NOT EXISTS public_token text;

CREATE UNIQUE INDEX IF NOT EXISTS uq_tracking_public_token
    ON tracking_sessions (public_token)
    WHERE public_token IS NOT NULL;

ALTER TABLE tracking_sessions
    DROP CONSTRAINT IF EXISTS tracking_sessions_public_token_strength;
ALTER TABLE tracking_sessions
    ADD CONSTRAINT tracking_sessions_public_token_strength
    CHECK (public_token IS NULL OR length(public_token) >= 43);
