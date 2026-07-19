CREATE TABLE IF NOT EXISTS report_drafts (
    organization_id text NOT NULL,
    work_order_id text NOT NULL,
    executor_id text NOT NULL,
    photo_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    comment text,
    signature_ref text,
    customer_code text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, work_order_id, executor_id),
    FOREIGN KEY (organization_id, work_order_id)
        REFERENCES work_orders(organization_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_report_drafts_executor
    ON report_drafts (organization_id, executor_id, updated_at DESC);
