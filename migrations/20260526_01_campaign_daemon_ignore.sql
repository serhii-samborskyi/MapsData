-- Campaign-level daemon ignore flag
ALTER TABLE search_campaigns
    ADD COLUMN IF NOT EXISTS daemon_ignore BOOLEAN;

UPDATE search_campaigns
SET daemon_ignore = FALSE
WHERE daemon_ignore IS NULL;

ALTER TABLE search_campaigns
    ALTER COLUMN daemon_ignore SET DEFAULT FALSE;

ALTER TABLE search_campaigns
    ALTER COLUMN daemon_ignore SET NOT NULL;
