ALTER TABLE tracking_sessions
    ADD COLUMN IF NOT EXISTS point_count bigint NOT NULL DEFAULT 0
    CHECK (point_count >= 0);

UPDATE tracking_sessions AS session
SET point_count = counts.value
FROM (
    SELECT organization_id, session_id, count(*)::bigint AS value
    FROM tracking_points
    GROUP BY organization_id, session_id
) AS counts
WHERE session.organization_id = counts.organization_id
  AND session.id = counts.session_id
  AND session.point_count <> counts.value;
