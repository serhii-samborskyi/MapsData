-- Enrichment run option: process only leads with valid email status
ALTER TABLE enrichment_runs
    ADD COLUMN IF NOT EXISTS valid_emails_only BOOLEAN;

UPDATE enrichment_runs
SET valid_emails_only = FALSE
WHERE valid_emails_only IS NULL;

ALTER TABLE enrichment_runs
    ALTER COLUMN valid_emails_only SET DEFAULT FALSE;

ALTER TABLE enrichment_runs
    ALTER COLUMN valid_emails_only SET NOT NULL;
