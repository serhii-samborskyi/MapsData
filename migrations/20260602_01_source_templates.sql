BEGIN;

CREATE TABLE IF NOT EXISTS source_templates (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT '',
    source_type TEXT NOT NULL DEFAULT 'generic',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    config TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE search_campaigns
    ADD COLUMN IF NOT EXISTS source_template_id INTEGER;

ALTER TABLE contacts
    ADD COLUMN IF NOT EXISTS source_data JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_source_templates_enabled
    ON source_templates(enabled, name);

CREATE INDEX IF NOT EXISTS idx_contacts_source_data_gin
    ON contacts USING GIN (source_data);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'search_campaigns_source_template_id_fkey'
    ) THEN
        ALTER TABLE search_campaigns
        ADD CONSTRAINT search_campaigns_source_template_id_fkey
        FOREIGN KEY (source_template_id) REFERENCES source_templates(id) ON DELETE SET NULL;
    END IF;
END
$$;

COMMIT;
