-- Align legacy role names with the messenger product language and add
-- controlled staff binding codes.

ALTER TABLE actors ADD COLUMN IF NOT EXISTS bind_code text;
ALTER TABLE actors ADD COLUMN IF NOT EXISTS bind_code_expires_at timestamptz;
ALTER TABLE actors ADD COLUMN IF NOT EXISTS phone text;

ALTER TABLE actors DROP CONSTRAINT IF EXISTS actors_role_check;

UPDATE actors SET role = 'operator' WHERE role = 'coordinator';
UPDATE actors SET role = 'master' WHERE role = 'executor';
UPDATE actors SET role = 'client' WHERE role = 'requester';

ALTER TABLE actors ADD CONSTRAINT actors_role_check
    CHECK (role IN ('admin', 'operator', 'master', 'client'));

ALTER TABLE callback_actions
    DROP CONSTRAINT IF EXISTS callback_actions_allowed_role_check;
ALTER TABLE callback_actions ADD CONSTRAINT callback_actions_allowed_role_check
    CHECK (
        allowed_role IS NULL
        OR allowed_role IN ('admin', 'operator', 'master', 'client')
    );

CREATE UNIQUE INDEX IF NOT EXISTS uq_actors_active_bind_code
    ON actors (organization_id, bind_code)
    WHERE bind_code IS NOT NULL;
