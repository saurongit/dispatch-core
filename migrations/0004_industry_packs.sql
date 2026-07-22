CREATE TABLE IF NOT EXISTS org_packs (
    organization_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    version integer NOT NULL CHECK (version >= 1),
    state text NOT NULL CHECK (state IN ('draft', 'active', 'archived')),
    definition jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    activated_at timestamptz,
    PRIMARY KEY (organization_id, version)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_org_packs_one_active
    ON org_packs (organization_id)
    WHERE state = 'active';

CREATE UNIQUE INDEX IF NOT EXISTS uq_org_packs_one_draft
    ON org_packs (organization_id)
    WHERE state = 'draft';
