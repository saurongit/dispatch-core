-- A callback remains single-owner, while a retried inbox event may resume it.

ALTER TABLE callback_actions
    ADD COLUMN IF NOT EXISTS claim_key text;

ALTER TABLE callback_actions
    ADD COLUMN IF NOT EXISTS completed_at timestamptz;
