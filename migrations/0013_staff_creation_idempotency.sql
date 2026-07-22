-- A retried inbound message must not create a second staff actor after the
-- first transaction committed but before its reply was durably queued.

ALTER TABLE actors
    ADD COLUMN IF NOT EXISTS staff_creation_key text;

CREATE UNIQUE INDEX IF NOT EXISTS uq_actors_staff_creation_key
    ON actors (organization_id, staff_creation_key)
    WHERE staff_creation_key IS NOT NULL;
