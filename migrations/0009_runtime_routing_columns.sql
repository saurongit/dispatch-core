-- Complete columns already required by the durable runtime implementation.

ALTER TABLE tracking_sessions
    ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE tracking_sessions
    ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE outbound_messages
    ADD COLUMN IF NOT EXISTS consumer_key text NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS ix_outbound_messages_consumer_claim
    ON outbound_messages (
        provider, consumer_key, status, next_attempt_at, created_at
    )
    WHERE status IN ('pending', 'processing');
