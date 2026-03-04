-- Migration: Ensure leads table exists for TbT error logs export
-- Description: Table to store error logs from inspection as leads in JSONB format
-- Schema: tbt (or your configured DB_SCHEMA)

-- Note: This script assumes the table might already exist
-- If it doesn't exist, it will be created. If it does, it will be left unchanged.

-- Create leads table if it doesn't exist
CREATE TABLE IF NOT EXISTS tbt.leads (
    id SERIAL PRIMARY KEY,
    pipeline_id UUID NOT NULL,
    source VARCHAR(50) NOT NULL,
    lead_data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create index on pipeline_id for faster queries
CREATE INDEX IF NOT EXISTS idx_leads_pipeline_id
ON tbt.leads(pipeline_id);

-- Create index on source for filtering by source
CREATE INDEX IF NOT EXISTS idx_leads_source
ON tbt.leads(source);

-- Create index on created_at for time-based queries
CREATE INDEX IF NOT EXISTS idx_leads_created_at
ON tbt.leads(created_at DESC);

-- Create GIN index on lead_data for JSONB queries
CREATE INDEX IF NOT EXISTS idx_leads_lead_data
ON tbt.leads USING GIN (lead_data);

-- Add comments
COMMENT ON TABLE tbt.leads IS 'Stores error logs from inspections as leads in JSONB format';
COMMENT ON COLUMN tbt.leads.id IS 'Auto-incrementing primary key';
COMMENT ON COLUMN tbt.leads.pipeline_id IS 'UUID identifying the pipeline run (equals sample_id)';
COMMENT ON COLUMN tbt.leads.source IS 'Source of the lead (e.g., tbt, hdr, etc.)';
COMMENT ON COLUMN tbt.leads.lead_data IS 'JSONB containing error log data (run_id, case_id, route_id, etc.)';
COMMENT ON COLUMN tbt.leads.created_at IS 'Timestamp when record was created';

-- Example lead_data structure:
/*
{
    "run_id": "some-run-id",
    "case_id": "some-case-id",
    "route_id": "route-123",
    "stretch": "some-stretch-data",
    "provider_route": "encoded-route-data",
    "competitor_route": "encoded-route-data",
    "country": "POL",
    "provider": "Orbis",
    "competitor": "Genesis",
    "product": "latest"
}
*/

-- Query examples:
/*
-- Get all leads for a pipeline_id
SELECT * FROM tbt.leads
WHERE pipeline_id = 'b294bb07-b9d6-4e6f-8100-b909fe6227df';

-- Get leads by source
SELECT * FROM tbt.leads
WHERE source = 'tbt';

-- Get leads for a specific country (JSONB query)
SELECT * FROM tbt.leads
WHERE lead_data->>'country' = 'POL';

-- Get leads with specific provider
SELECT pipeline_id, lead_data->>'provider' as provider, lead_data
FROM tbt.leads
WHERE lead_data->>'provider' = 'Orbis';

-- Count leads per pipeline
SELECT pipeline_id, COUNT(*) as lead_count
FROM tbt.leads
GROUP BY pipeline_id;

-- Count leads per source
SELECT source, COUNT(*) as lead_count
FROM tbt.leads
GROUP BY source;
*/

