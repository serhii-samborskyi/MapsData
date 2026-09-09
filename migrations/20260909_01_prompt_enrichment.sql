ALTER TABLE enrichment_runs ADD COLUMN IF NOT EXISTS service TEXT NOT NULL DEFAULT 'http_enrichment';
ALTER TABLE enrichment_runs ADD COLUMN IF NOT EXISTS prompt_config TEXT NOT NULL DEFAULT '{}';
CREATE TABLE IF NOT EXISTS enrichment_api_rate_limits (
    endpoint_key TEXT PRIMARY KEY,
    next_request_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
