BEGIN;

ALTER TABLE search_campaigns
    ADD COLUMN IF NOT EXISTS maps_scrape_mode TEXT;

UPDATE search_campaigns
SET maps_scrape_mode = 'slow'
WHERE maps_scrape_mode IS NULL
   OR maps_scrape_mode NOT IN ('fast', 'slow');

ALTER TABLE search_campaigns
    ALTER COLUMN maps_scrape_mode SET DEFAULT 'slow';

ALTER TABLE search_campaigns
    ALTER COLUMN maps_scrape_mode SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'search_campaigns_maps_scrape_mode_check'
    ) THEN
        ALTER TABLE search_campaigns
            ADD CONSTRAINT search_campaigns_maps_scrape_mode_check
            CHECK (maps_scrape_mode IN ('fast', 'slow'));
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_pipeline_run_locks_worker_lease
    ON pipeline_run_locks(worker_id, lease_expires_at);

COMMIT;
