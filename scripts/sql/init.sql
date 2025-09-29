-- Simple PostgreSQL initialization for Week 02 Lab
-- This creates a basic table for landing data

-- Create  schema
CREATE SCHEMA IF NOT EXISTS fa02_staging;

-- Create logs table with proper schema to prevent DLT auto-creation
CREATE TABLE IF NOT EXISTS fa02_staging.logs (
    address VARCHAR(42),
    topics JSONB,
    data TEXT,
    block_number BIGINT,
    block_hash VARCHAR(66),
    time_stamp BIGINT,
    gas_price BIGINT,
    gas_used BIGINT,
    log_index INTEGER,
    transaction_hash VARCHAR(66),
    transaction_index INTEGER,
    chainid INTEGER,
    chain TEXT,
    contract_address VARCHAR(42)
);

-- Grant permissions (user already exists from Docker environment)
GRANT ALL PRIVILEGES ON SCHEMA fa02_staging TO postgres;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA fa02_staging TO postgres;

GRANT USAGE,
SELECT ON ALL SEQUENCES IN SCHEMA fa02_staging TO postgres;