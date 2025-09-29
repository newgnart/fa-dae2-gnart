-- Simple PostgreSQL initialization for Week 02 Lab
-- This creates a basic table for landing data

-- Create  schema
CREATE SCHEMA IF NOT EXISTS fa02_staging;

-- Create simple data landing table
CREATE TABLE IF NOT EXISTS fa02_staging.raw_logs (
    id SERIAL PRIMARY KEY,
    data_content JSONB,
    file_name VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant permissions (user already exists from Docker environment)
GRANT ALL PRIVILEGES ON SCHEMA fa02_staging TO postgres;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA fa02_staging TO postgres;

GRANT USAGE,
SELECT ON ALL SEQUENCES IN SCHEMA fa02_staging TO postgres;