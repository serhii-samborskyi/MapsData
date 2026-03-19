BEGIN;

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id BIGSERIAL PRIMARY KEY,
    campaign_id INTEGER NOT NULL REFERENCES search_campaigns(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed', 'canceled')),
    current_stage TEXT NOT NULL CHECK (current_stage IN ('maps_scrape', 'cleanup_contacts', 'email_fast', 'email_fallback', 'finalize')),
    retries INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    actor TEXT,
    worker_id TEXT,
    worker_metadata JSONB,
    lease_expires_at TIMESTAMP,
    last_heartbeat_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    failed_at TIMESTAMP,
    canceled_at TIMESTAMP,
    latest_error TEXT,
    error_payload JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_run_stages (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    campaign_id INTEGER NOT NULL REFERENCES search_campaigns(id) ON DELETE CASCADE,
    stage TEXT NOT NULL CHECK (stage IN ('maps_scrape', 'cleanup_contacts', 'email_fast', 'email_fallback', 'finalize')),
    stage_order INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed', 'canceled')),
    retries INTEGER NOT NULL DEFAULT 0,
    actor TEXT,
    worker_id TEXT,
    worker_metadata JSONB,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    failed_at TIMESTAMP,
    canceled_at TIMESTAMP,
    last_heartbeat_at TIMESTAMP,
    error_message TEXT,
    error_payload JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, stage),
    UNIQUE(run_id, stage_order)
);

CREATE TABLE IF NOT EXISTS pipeline_run_locks (
    run_id BIGINT PRIMARY KEY REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    campaign_id INTEGER NOT NULL REFERENCES search_campaigns(id) ON DELETE CASCADE,
    worker_id TEXT NOT NULL,
    lock_token TEXT NOT NULL,
    metadata JSONB,
    lease_expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_campaign_status ON pipeline_runs(campaign_id, status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_campaign_updated_at ON pipeline_runs(campaign_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_stages_run_order ON pipeline_run_stages(run_id, stage_order);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_stages_campaign_status ON pipeline_run_stages(campaign_id, status);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_locks_campaign_lease ON pipeline_run_locks(campaign_id, lease_expires_at);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_active_run_per_campaign
ON pipeline_runs(campaign_id)
WHERE status IN ('pending', 'running');

COMMIT;
