CREATE TABLE IF NOT EXISTS organization_order_counters (
    organization_id text PRIMARY KEY
        REFERENCES organizations(id) ON DELETE CASCADE,
    last_value bigint NOT NULL DEFAULT 0 CHECK (last_value >= 0)
);

ALTER TABLE work_orders
    ADD COLUMN IF NOT EXISTS public_number text;

CREATE OR REPLACE FUNCTION pg_temp.dispatch_core_base36(value bigint)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
    alphabet constant text := '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';
    current_value bigint := value;
    encoded text := '';
BEGIN
    WHILE current_value > 0 LOOP
        encoded := substr(
            alphabet,
            ((current_value % 36) + 1)::integer,
            1
        ) || encoded;
        current_value := current_value / 36;
    END LOOP;
    RETURN encoded;
END;
$$;

WITH numbered AS (
    SELECT
        organization_id,
        id,
        row_number() OVER (
            PARTITION BY organization_id
            ORDER BY created_at, id
        ) AS sequence
    FROM work_orders
    WHERE public_number IS NULL
)
UPDATE work_orders AS orders
SET public_number = CASE
    WHEN numbered.sequence <= 26000 THEN
        chr(65 + ((numbered.sequence - 1) / 1000)::integer)
        || lpad(((numbered.sequence - 1) % 1000)::text, 3, '0')
    ELSE
        'X' || lpad(
            pg_temp.dispatch_core_base36(numbered.sequence),
            6,
            '0'
        )
    END
FROM numbered
WHERE orders.organization_id = numbered.organization_id
  AND orders.id = numbered.id;

INSERT INTO organization_order_counters (organization_id, last_value)
SELECT organization_id, count(*)::bigint
FROM work_orders
GROUP BY organization_id
ON CONFLICT (organization_id) DO UPDATE
SET last_value = GREATEST(
    organization_order_counters.last_value,
    EXCLUDED.last_value
);

ALTER TABLE work_orders
    ALTER COLUMN public_number SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_work_orders_public_number
    ON work_orders (organization_id, public_number);

DROP FUNCTION pg_temp.dispatch_core_base36(bigint);
