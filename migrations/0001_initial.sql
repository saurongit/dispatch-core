CREATE TABLE IF NOT EXISTS schema_migrations (
    version text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS organizations (
    id text PRIMARY KEY,
    name text NOT NULL,
    default_pool_mode text NOT NULL DEFAULT 'curated'
        CHECK (default_pool_mode IN ('curated', 'first_claim')),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS actors (
    organization_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    id text NOT NULL,
    role text NOT NULL CHECK (role IN ('admin', 'coordinator', 'executor', 'requester')),
    display_name text NOT NULL,
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, id)
);

CREATE TABLE IF NOT EXISTS external_identities (
    organization_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    provider text NOT NULL CHECK (provider IN ('telegram', 'max', 'web', 'api')),
    external_user_id text NOT NULL,
    actor_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, provider, external_user_id),
    FOREIGN KEY (organization_id, actor_id)
        REFERENCES actors(organization_id, id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS work_orders (
    organization_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    id text NOT NULL,
    work_type text NOT NULL,
    source text NOT NULL,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    requester_id text,
    coordinator_id text,
    evidence_requirements jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NOT NULL CHECK (
        status IN (
            'submitted', 'pool_open', 'assigned', 'accepted',
            'en_route', 'in_progress', 'completed', 'cancelled'
        )
    ),
    pool_mode text CHECK (pool_mode IN ('curated', 'first_claim')),
    assignee_id text,
    report jsonb,
    version integer NOT NULL CHECK (version >= 1),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (organization_id, id),
    CHECK ((status <> 'pool_open') OR pool_mode IS NOT NULL),
    CHECK (
        (status NOT IN ('assigned', 'accepted', 'en_route', 'in_progress', 'completed'))
        OR assignee_id IS NOT NULL
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_executor_one_active_order
    ON work_orders (organization_id, assignee_id)
    WHERE assignee_id IS NOT NULL
      AND status IN ('assigned', 'accepted', 'en_route', 'in_progress');

CREATE INDEX IF NOT EXISTS ix_work_orders_board
    ON work_orders (organization_id, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS pool_responses (
    organization_id text NOT NULL,
    work_order_id text NOT NULL,
    executor_id text NOT NULL,
    status text NOT NULL CHECK (
        status IN ('interested', 'selected', 'rejected', 'withdrawn')
    ),
    responded_at timestamptz NOT NULL,
    PRIMARY KEY (organization_id, work_order_id, executor_id),
    FOREIGN KEY (organization_id, work_order_id)
        REFERENCES work_orders(organization_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_pool_responses_interested
    ON pool_responses (organization_id, work_order_id, responded_at)
    WHERE status = 'interested';

CREATE TABLE IF NOT EXISTS tracking_sessions (
    organization_id text NOT NULL,
    id text NOT NULL,
    work_order_id text NOT NULL,
    executor_id text NOT NULL,
    status text NOT NULL CHECK (status IN ('active', 'completed', 'cancelled')),
    version integer NOT NULL CHECK (version >= 1),
    PRIMARY KEY (organization_id, id),
    FOREIGN KEY (organization_id, work_order_id)
        REFERENCES work_orders(organization_id, id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_tracking_one_active_per_order
    ON tracking_sessions (organization_id, work_order_id)
    WHERE status = 'active';

CREATE TABLE IF NOT EXISTS tracking_points (
    organization_id text NOT NULL,
    session_id text NOT NULL,
    sequence_no bigint NOT NULL,
    latitude double precision NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude double precision NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    captured_at timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL,
    source text NOT NULL CHECK (source IN ('telegram', 'max', 'web', 'mobile', 'import')),
    accuracy_m double precision CHECK (accuracy_m >= 0),
    PRIMARY KEY (organization_id, session_id, sequence_no),
    FOREIGN KEY (organization_id, session_id)
        REFERENCES tracking_sessions(organization_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_tracking_points_latest
    ON tracking_points (organization_id, session_id, sequence_no DESC);

CREATE TABLE IF NOT EXISTS inbox_events (
    provider text NOT NULL,
    external_event_id text NOT NULL,
    organization_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'processed', 'dead')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    claimed_at timestamptz,
    processed_at timestamptz,
    last_error text,
    received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (provider, external_event_id)
);

CREATE INDEX IF NOT EXISTS ix_inbox_events_claim
    ON inbox_events (status, next_attempt_at, received_at)
    WHERE status IN ('pending', 'processing');

CREATE TABLE IF NOT EXISTS inbox_cursors (
    provider text NOT NULL,
    consumer_key text NOT NULL,
    cursor_value text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (provider, consumer_key)
);

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id text PRIMARY KEY,
    organization_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    aggregate_type text NOT NULL,
    aggregate_id text NOT NULL,
    aggregate_version integer NOT NULL,
    event_name text NOT NULL,
    payload jsonb NOT NULL,
    occurred_at timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'delivered', 'dead')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    claimed_at timestamptz,
    delivered_at timestamptz,
    last_error text,
    external_message_id text,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (organization_id, aggregate_type, aggregate_id, aggregate_version)
);

CREATE INDEX IF NOT EXISTS ix_outbox_events_claim
    ON outbox_events (status, next_attempt_at, occurred_at)
    WHERE status IN ('pending', 'processing');

CREATE TABLE IF NOT EXISTS idempotency_keys (
    organization_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    scope text NOT NULL,
    idempotency_key text NOT NULL,
    request_hash text NOT NULL,
    response_status integer,
    response_body jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (organization_id, scope, idempotency_key)
);

CREATE INDEX IF NOT EXISTS ix_idempotency_expiry ON idempotency_keys (expires_at);

CREATE TABLE IF NOT EXISTS callback_actions (
    token text PRIMARY KEY,
    organization_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    action text NOT NULL,
    payload jsonb NOT NULL,
    allowed_role text CHECK (
        allowed_role IS NULL
        OR allowed_role IN ('admin', 'coordinator', 'executor', 'requester')
    ),
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_callback_actions_expiry
    ON callback_actions (expires_at)
    WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS outbound_messages (
    id bigserial PRIMARY KEY,
    deduplication_key text NOT NULL UNIQUE,
    organization_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    provider text NOT NULL CHECK (provider IN ('telegram', 'max')),
    recipient_id text NOT NULL,
    text_body text NOT NULL,
    buttons jsonb NOT NULL DEFAULT '[]'::jsonb,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'delivered', 'dead')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    claimed_at timestamptz,
    delivered_at timestamptz,
    last_error text,
    external_message_id text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_outbound_messages_claim
    ON outbound_messages (provider, status, next_attempt_at, created_at)
    WHERE status IN ('pending', 'processing');
