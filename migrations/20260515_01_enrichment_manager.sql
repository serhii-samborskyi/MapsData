BEGIN;

CREATE TABLE IF NOT EXISTS enrichment_templates (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    service TEXT NOT NULL DEFAULT 'http_enrichment',
    api_config TEXT NOT NULL,
    input_mapping TEXT NOT NULL,
    output_mapping TEXT NOT NULL,
    schema_cache TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS enrichment_runs (
    id BIGSERIAL PRIMARY KEY,
    campaign_id INTEGER NOT NULL REFERENCES search_campaigns(id) ON DELETE CASCADE,
    template_id INTEGER NOT NULL REFERENCES enrichment_templates(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'paused', 'completed', 'failed', 'cancelled')),
    api_url TEXT NOT NULL,
    api_key TEXT,
    concurrency INTEGER NOT NULL DEFAULT 1,
    max_retries INTEGER NOT NULL DEFAULT 1,
    overwrite_existing BOOLEAN NOT NULL DEFAULT FALSE,
    skip_missing_input BOOLEAN NOT NULL DEFAULT TRUE,
    timeout_seconds INTEGER NOT NULL DEFAULT 120,
    input_mapping TEXT NOT NULL DEFAULT '{}',
    output_mapping TEXT NOT NULL DEFAULT '{}',
    required_inputs TEXT NOT NULL DEFAULT '[]',
    total_contacts INTEGER NOT NULL DEFAULT 0,
    processed_contacts INTEGER NOT NULL DEFAULT 0,
    enriched_contacts INTEGER NOT NULL DEFAULT 0,
    failed_contacts INTEGER NOT NULL DEFAULT 0,
    skipped_contacts INTEGER NOT NULL DEFAULT 0,
    current_contact_id INTEGER,
    current_contact_name TEXT,
    pause_requested BOOLEAN NOT NULL DEFAULT FALSE,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    latest_error TEXT,
    created_by TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE enrichment_runs ADD COLUMN IF NOT EXISTS input_mapping TEXT DEFAULT '{}';
ALTER TABLE enrichment_runs ADD COLUMN IF NOT EXISTS output_mapping TEXT DEFAULT '{}';
ALTER TABLE enrichment_runs ADD COLUMN IF NOT EXISTS required_inputs TEXT DEFAULT '[]';
ALTER TABLE enrichment_runs ADD COLUMN IF NOT EXISTS timeout_seconds INTEGER DEFAULT 120;
UPDATE enrichment_runs SET timeout_seconds = 120 WHERE timeout_seconds IS NULL OR timeout_seconds < 1;
ALTER TABLE enrichment_runs ALTER COLUMN timeout_seconds SET DEFAULT 120;
ALTER TABLE enrichment_runs ALTER COLUMN timeout_seconds SET NOT NULL;

CREATE TABLE IF NOT EXISTS enrichment_run_contacts (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES enrichment_runs(id) ON DELETE CASCADE,
    campaign_id INTEGER NOT NULL REFERENCES search_campaigns(id) ON DELETE CASCADE,
    contact_id INTEGER NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'enriched', 'failed', 'skipped')),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    response_payload JSONB,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, contact_id)
);

CREATE TABLE IF NOT EXISTS enrichment_run_logs (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES enrichment_runs(id) ON DELETE CASCADE,
    campaign_id INTEGER NOT NULL REFERENCES search_campaigns(id) ON DELETE CASCADE,
    contact_id INTEGER,
    level TEXT NOT NULL DEFAULT 'info',
    message TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_enrichment_runs_campaign_status
    ON enrichment_runs(campaign_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_enrichment_runs_template_id
    ON enrichment_runs(template_id);

CREATE INDEX IF NOT EXISTS idx_enrichment_run_contacts_run_status
    ON enrichment_run_contacts(run_id, status);

CREATE INDEX IF NOT EXISTS idx_enrichment_run_contacts_campaign_status
    ON enrichment_run_contacts(campaign_id, status);

CREATE INDEX IF NOT EXISTS idx_enrichment_run_logs_run_created
    ON enrichment_run_logs(run_id, created_at DESC);

COMMIT;
