ALTER TABLE tracking_points
    ADD COLUMN IF NOT EXISTS source_event_id text;

CREATE UNIQUE INDEX IF NOT EXISTS uq_tracking_point_source_event
    ON tracking_points (organization_id, source, source_event_id)
    WHERE source_event_id IS NOT NULL;
