-- Database setup script for TbT Inspection Pipeline
-- This script creates the database and schema for storing inspection results
-- Run this script as PostgreSQL superuser or database owner

-- ============================================================================
-- DATABASE CREATION (Optional - run only if database doesn't exist)
-- ============================================================================
-- CREATE DATABASE mapex_tbt
--     WITH
--     OWNER = your_username
--     ENCODING = 'UTF8'
--     LC_COLLATE = 'en_US.UTF-8'
--     LC_CTYPE = 'en_US.UTF-8'
--     TABLESPACE = pg_default
--     CONNECTION LIMIT = -1;

-- Connect to the database
\c mapex_tbt

-- ============================================================================
-- SCHEMA CREATION (Optional - using 'public' schema by default)
-- ============================================================================
-- CREATE SCHEMA IF NOT EXISTS tbt_inspection;
-- GRANT ALL ON SCHEMA tbt_inspection TO your_username;

-- ============================================================================
-- TABLE CREATION (Optional - tables are auto-created by pandas.to_sql)
-- ============================================================================
-- Note: These tables will be automatically created by the export_to_database
-- function. This script is provided for reference and manual table creation
-- if needed.

-- Table: inspection_routes
-- Stores provider and competitor route comparisons with RAC state
CREATE TABLE IF NOT EXISTS public.inspection_routes (
    route_id VARCHAR(255),
    country VARCHAR(50),
    sample_id VARCHAR(255),
    competitor VARCHAR(100),
    provider_route TEXT,
    provider_route_time FLOAT,
    provider_route_length FLOAT,
    origin VARCHAR(255),
    destination VARCHAR(255),
    provider VARCHAR(100),
    competitor_route TEXT,
    competitor_route_length FLOAT,
    competitor_route_time FLOAT,
    rac_state VARCHAR(50),
    run_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: inspection_critical_sections
-- Stores critical sections with FCD state and metrics
CREATE TABLE IF NOT EXISTS public.inspection_critical_sections (
    route_id VARCHAR(255),
    case_id VARCHAR(255),
    stretch TEXT,
    stretch_length FLOAT,
    fcd_state VARCHAR(50),
    pra FLOAT,
    prb FLOAT,
    prab FLOAT,
    lift FLOAT,
    tot INTEGER,
    reference_case_id VARCHAR(255),
    run_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: critical_sections_with_mcp_feedback
-- Stores MCP feedback and error classification
CREATE TABLE IF NOT EXISTS public.critical_sections_with_mcp_feedback (
    run_id VARCHAR(255),
    route_id VARCHAR(255),
    case_id VARCHAR(255),
    mcp_state VARCHAR(50),
    reference_case_id VARCHAR(255),
    error_subtype VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: error_logs
-- Stores per-route error details
CREATE TABLE IF NOT EXISTS public.error_logs (
    run_id VARCHAR(255),
    route_id VARCHAR(255),
    case_id VARCHAR(255),
    stretch TEXT,
    provider_route TEXT,
    competitor_route TEXT,
    country VARCHAR(50),
    provider VARCHAR(100),
    competitor VARCHAR(100),
    product VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: inspection_metadata
-- Stores run metadata, timing, and sanity check results
CREATE TABLE IF NOT EXISTS public.inspection_metadata (
    run_id VARCHAR(255) PRIMARY KEY,
    sample_id VARCHAR(255),
    provider VARCHAR(100),
    endpoint VARCHAR(255),
    mapdate DATE,
    product VARCHAR(100),
    country VARCHAR(50),
    mode VARCHAR(50),
    competitor VARCHAR(100),
    mcp_tasks TEXT,
    completed VARCHAR(50),
    inspection_date DATE,
    comment TEXT,
    rac_elapsed_time FLOAT,
    fcd_elapsed_time FLOAT,
    total_elapsed_time FLOAT,
    api_calls TEXT,
    sanity_fail BOOLEAN,
    sanity_msg TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INDEXES (Optional - for better query performance)
-- ============================================================================

-- Indexes for inspection_routes
CREATE INDEX IF NOT EXISTS idx_routes_run_id ON public.inspection_routes(run_id);
CREATE INDEX IF NOT EXISTS idx_routes_sample_id ON public.inspection_routes(sample_id);
CREATE INDEX IF NOT EXISTS idx_routes_country ON public.inspection_routes(country);
CREATE INDEX IF NOT EXISTS idx_routes_provider ON public.inspection_routes(provider);

-- Indexes for inspection_critical_sections
CREATE INDEX IF NOT EXISTS idx_critical_run_id ON public.inspection_critical_sections(run_id);
CREATE INDEX IF NOT EXISTS idx_critical_route_id ON public.inspection_critical_sections(route_id);
CREATE INDEX IF NOT EXISTS idx_critical_fcd_state ON public.inspection_critical_sections(fcd_state);

-- Indexes for critical_sections_with_mcp_feedback
CREATE INDEX IF NOT EXISTS idx_mcp_run_id ON public.critical_sections_with_mcp_feedback(run_id);
CREATE INDEX IF NOT EXISTS idx_mcp_case_id ON public.critical_sections_with_mcp_feedback(case_id);
CREATE INDEX IF NOT EXISTS idx_mcp_state ON public.critical_sections_with_mcp_feedback(mcp_state);

-- Indexes for error_logs
CREATE INDEX IF NOT EXISTS idx_errors_run_id ON public.error_logs(run_id);
CREATE INDEX IF NOT EXISTS idx_errors_route_id ON public.error_logs(route_id);
CREATE INDEX IF NOT EXISTS idx_errors_country ON public.error_logs(country);

-- Indexes for inspection_metadata
CREATE INDEX IF NOT EXISTS idx_metadata_sample_id ON public.inspection_metadata(sample_id);
CREATE INDEX IF NOT EXISTS idx_metadata_inspection_date ON public.inspection_metadata(inspection_date);
CREATE INDEX IF NOT EXISTS idx_metadata_country ON public.inspection_metadata(country);

-- ============================================================================
-- PERMISSIONS (Optional - grant access to application user)
-- ============================================================================

-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO your_username;
-- GRANT USAGE ON SCHEMA public TO your_username;

-- ============================================================================
-- VIEWS (Optional - useful query views)
-- ============================================================================

-- View: Latest inspection results per sample
CREATE OR REPLACE VIEW public.v_latest_inspection_per_sample AS
SELECT DISTINCT ON (sample_id)
    run_id,
    sample_id,
    provider,
    competitor,
    country,
    inspection_date,
    total_elapsed_time,
    sanity_fail,
    created_at
FROM public.inspection_metadata
ORDER BY sample_id, inspection_date DESC, created_at DESC;

-- View: Summary of routes per run
CREATE OR REPLACE VIEW public.v_routes_summary_per_run AS
SELECT
    run_id,
    COUNT(*) as total_routes,
    COUNT(CASE WHEN rac_state = 'OK' THEN 1 END) as rac_ok,
    COUNT(CASE WHEN rac_state = 'ERROR' THEN 1 END) as rac_error,
    AVG(provider_route_length) as avg_provider_length,
    AVG(competitor_route_length) as avg_competitor_length
FROM public.inspection_routes
GROUP BY run_id;

-- View: Critical sections summary per run
CREATE OR REPLACE VIEW public.v_critical_sections_summary AS
SELECT
    run_id,
    COUNT(*) as total_critical_sections,
    COUNT(CASE WHEN fcd_state = 'FCD' THEN 1 END) as fcd_count,
    COUNT(CASE WHEN fcd_state = 'NO_FCD' THEN 1 END) as no_fcd_count,
    AVG(prab) as avg_prab,
    AVG(lift) as avg_lift
FROM public.inspection_critical_sections
GROUP BY run_id;

-- ============================================================================
-- MAINTENANCE
-- ============================================================================

-- Vacuum and analyze tables for optimal performance
-- Run periodically or after large data imports
-- VACUUM ANALYZE public.inspection_routes;
-- VACUUM ANALYZE public.inspection_critical_sections;
-- VACUUM ANALYZE public.critical_sections_with_mcp_feedback;
-- VACUUM ANALYZE public.error_logs;
-- VACUUM ANALYZE public.inspection_metadata;

-- ============================================================================
-- CLEANUP (Optional - use with caution)
-- ============================================================================

-- Delete old inspection data (older than 90 days)
-- DELETE FROM public.inspection_routes WHERE created_at < NOW() - INTERVAL '90 days';
-- DELETE FROM public.inspection_critical_sections WHERE created_at < NOW() - INTERVAL '90 days';
-- DELETE FROM public.critical_sections_with_mcp_feedback WHERE created_at < NOW() - INTERVAL '90 days';
-- DELETE FROM public.error_logs WHERE created_at < NOW() - INTERVAL '90 days';
-- DELETE FROM public.inspection_metadata WHERE created_at < NOW() - INTERVAL '90 days';

-- ============================================================================
-- BACKUP
-- ============================================================================

-- Create backup of database
-- pg_dump -U your_username -d mapex_tbt -F c -f mapex_tbt_backup.dump

-- Restore from backup
-- pg_restore -U your_username -d mapex_tbt -c mapex_tbt_backup.dump

