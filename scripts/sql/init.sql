-- Simple PostgreSQL initialization for Week 02 Lab
-- This creates a basic staging table for landing data

-- Create staging schema
CREATE SCHEMA IF NOT EXISTS staging;

-- Create simple data landing table
CREATE TABLE IF NOT EXISTS staging.raw_data (
    id SERIAL PRIMARY KEY,
    data_content TEXT,
    file_name VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant permissions (user already exists from Docker environment)
GRANT ALL PRIVILEGES ON SCHEMA staging TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO postgres;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA staging TO postgres;
