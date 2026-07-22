-- One active browser-map capability per intake session. The opaque token lives
-- inside the JSON FSM state so it is revoked atomically when the address step
-- advances, is cancelled or expires with the session.

CREATE UNIQUE INDEX IF NOT EXISTS uq_intake_address_token
    ON intake_sessions ((state ->> 'address_token'))
    WHERE state ? 'address_token';
