-- Migration: Create pipeline_routes table for TbT inspection
-- Description: Table to store route data for inspection pipelines
-- Schema: tbt

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS tbt;

-- Create pipeline_routes table
CREATE TABLE IF NOT EXISTS tbt.pipeline_routes (
    id SERIAL PRIMARY KEY,
    pipeline_id UUID NOT NULL,
    route_data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create index on pipeline_id for faster queries
CREATE INDEX IF NOT EXISTS idx_pipeline_routes_pipeline_id
ON tbt.pipeline_routes(pipeline_id);

-- Create index on created_at for time-based queries
CREATE INDEX IF NOT EXISTS idx_pipeline_routes_created_at
ON tbt.pipeline_routes(created_at DESC);

-- Add GIN index on route_data for JSONB queries
CREATE INDEX IF NOT EXISTS idx_pipeline_routes_route_data
ON tbt.pipeline_routes USING GIN (route_data);

-- Create updated_at trigger function
CREATE OR REPLACE FUNCTION tbt.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to auto-update updated_at
DROP TRIGGER IF EXISTS update_pipeline_routes_updated_at ON tbt.pipeline_routes;
CREATE TRIGGER update_pipeline_routes_updated_at
    BEFORE UPDATE ON tbt.pipeline_routes
    FOR EACH ROW
    EXECUTE FUNCTION tbt.update_updated_at_column();

-- Add comments
COMMENT ON TABLE tbt.pipeline_routes IS 'Stores route data for TbT inspection pipelines';
COMMENT ON COLUMN tbt.pipeline_routes.id IS 'Auto-incrementing primary key';
COMMENT ON COLUMN tbt.pipeline_routes.pipeline_id IS 'UUID identifying a pipeline run';
COMMENT ON COLUMN tbt.pipeline_routes.route_data IS 'JSONB containing route information (origin, destination, metadata)';
COMMENT ON COLUMN tbt.pipeline_routes.created_at IS 'Timestamp when record was created';
COMMENT ON COLUMN tbt.pipeline_routes.updated_at IS 'Timestamp when record was last updated';

-- Example insert (for testing):
/*
INSERT INTO tbt.pipeline_routes (pipeline_id, route_data) VALUES
(
    'b294bb07-b9d6-4e6f-8100-b909fe6227df',
    '{
        "org": "genesis",
        "name": "Test Route",
        "origin": "POINT(22.50216 52.91886)",
        "country": "POL",
        "quality": "Q2",
        "tile_id": "13_4608_2671",
        "route_id": "9f81db8f-b0aa-4b74-a6c0-99ab5174339f",
        "to_coord": "52.91667, 22.51747",
        "sample_id": "0273e3cc-a095-4e4a-aa33-b760433ed8fe",
        "date_generated": "2026-03-04",
        "from_coord": "52.91886, 22.50216",
        "length_orbis": "24684.0",
        "length_genesis": "23894.0"
    }'::jsonb
);
*/

-- Query examples:
/*
-- Get all routes for a pipeline_id
SELECT * FROM tbt.pipeline_routes
WHERE pipeline_id = 'b294bb07-b9d6-4e6f-8100-b909fe6227df';

-- Get route data for a specific route
SELECT route_data FROM tbt.pipeline_routes
WHERE pipeline_id = 'b294bb07-b9d6-4e6f-8100-b909fe6227df'
  AND route_data->>'route_id' = '9f81db8f-b0aa-4b74-a6c0-99ab5174339f';

-- Count routes per pipeline
SELECT pipeline_id, COUNT(*) as route_count
FROM tbt.pipeline_routes
GROUP BY pipeline_id;

-- Get routes by country
SELECT pipeline_id, route_data
FROM tbt.pipeline_routes
WHERE route_data->>'country' = 'POL';
*/

