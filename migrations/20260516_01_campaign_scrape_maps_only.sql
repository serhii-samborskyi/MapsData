-- Add campaign-level flag to skip email stages in pipeline
ALTER TABLE search_campaigns
    ADD COLUMN IF NOT EXISTS scrape_maps_only BOOLEAN;

UPDATE search_campaigns
SET scrape_maps_only = FALSE
WHERE scrape_maps_only IS NULL;

ALTER TABLE search_campaigns
    ALTER COLUMN scrape_maps_only SET DEFAULT FALSE;

ALTER TABLE search_campaigns
    ALTER COLUMN scrape_maps_only SET NOT NULL;
