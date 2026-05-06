-- Speed up dashboard campaign list aggregates on requests/contacts
CREATE INDEX IF NOT EXISTS idx_requests_campaign_id ON requests(campaign_id);
CREATE INDEX IF NOT EXISTS idx_requests_campaign_status ON requests(campaign_id, status);
CREATE INDEX IF NOT EXISTS idx_contacts_campaign_id ON contacts(campaign_id);
