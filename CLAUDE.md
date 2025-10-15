# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a capstone project for FA DAE2 focused on Ethereum blockchain data extraction and transformation. The project extracts smart contract logs and transactions from Etherscan API, loads them into PostgreSQL, and transforms them using dbt.

## Architecture

The project follows an ELT (Extract, Load, Transform) pipeline:

### 1. Extract Layer (`scripts/extract/`)
- **Primary script**: `runner.py` - Extracts logs and transactions from Etherscan API
- Uses the `onchaindata.data_extraction.etherscan` module
- Supports multiple blockchain networks via `EtherscanClient`
- Features automatic retry logic for failed block ranges with exponential backoff
- Data stored as Parquet files in `.data/raw/` directory
- Error tracking in `logging/extract_error/` with automatic retry mechanism

### 2. Load Layer (`scripts/load/`)
- **postgres_load.py**: Loads Parquet files into PostgreSQL `raw` schema
- **snowflake_load.py**: Optional Snowflake loading capabilities
- Uses `onchaindata.data_pipeline` module for loading operations
- Supports both dlt-based and direct psycopg-based loading

### 3. Transform Layer (dbt)
- **Location**: `dbt_project/`
- Standard dbt project structure with models organized by layer:
  - `models/staging/`: Raw data cleanup (e.g., `stg_logs_decoded`)
  - `models/intermediate/`: Business logic transformations
  - `models/marts/`: Final analytics tables
- Shared macros in `dbt_project/macros/` for Ethereum data type conversions:
  - `uint256_to_address`: Extracts Ethereum addresses from uint256 hex strings
  - `uint256_to_numeric`: Converts uint256 hex to numeric values
- Models reference source data from `raw` schema
- Configuration: [dbt_project.yml](dbt_project/dbt_project.yml), [profiles.yml](dbt_project/profiles.yml)

### 4. Package Structure (`src/onchaindata/`)
Reusable Python package with modules:
- `data_extraction/`: Etherscan API client with rate limiting
- `data_pipeline/`: Postgres and Snowflake loading utilities
- `utils/`: Database clients (PostgresClient, SnowflakeClient)
- `config/`: Configuration management

## Development Commands

### Environment Setup
```bash
# Create Docker network (first time only)
docker network create fa-dae2-capstone_kafka_network

# Start PostgreSQL container
docker-compose up -d

# Install dependencies using uv
uv sync

# Set up environment variables
cp .env.example .env
export $(cat .env | xargs)

# Initialize database schema
./scripts/sql/run_sql.sh ./scripts/sql/init.sql
```

### Data Extraction
```bash
# Extract logs and transactions for a specific contract address
# Supports K/M/B suffixes for block numbers (e.g., '18.5M')
uv run python scripts/extract/runner.py \
  -c ethereum \
  -a 0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72 \
  --logs --transactions \
  --from_block 18.5M --to_block 20M \
  -v  # verbose logging

# Extract data from last N days
uv run python scripts/extract/runner.py \
  -a 0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72 \
  --logs --transactions \
  --last_n_days 7

# Logging levels: no flag (WARNING), -v (INFO), -vv (DEBUG)
```

### Data Loading
```bash
# Load Parquet file to PostgreSQL
uv run python scripts/load/postgres_load.py \
  -f .data/raw/ethereum_0xaddress_logs_18500000_20000000.parquet \
  -t logs

# Load to Snowflake (requires SNOWFLAKE_* env vars)
uv run python scripts/load/snowflake_load.py
```

### dbt Operations
```bash
# Run dbt models
./scripts/dbt.sh run

# Run specific model
./scripts/dbt.sh run --select stg_logs_decoded

# Run tests
./scripts/dbt.sh test

# Other dbt commands
./scripts/dbt.sh compile                    # Compile models
./scripts/dbt.sh docs generate              # Generate documentation
./scripts/dbt.sh run --select staging.*     # Run all staging models
./scripts/dbt.sh deps                       # Install dbt packages

# Legacy script (still available for backward compatibility)
./scripts/run_dbt.sh staging run
```

### SQL Operations
```bash
# Run SQL scripts directly
./scripts/sql/run_sql.sh ./scripts/sql/init.sql

# Ad-hoc queries
./scripts/sql/run_sql.sh ./scripts/sql/ad_hoc.sql
```

## Environment Variables

Required variables (see `.env.example`):
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `DB_SCHEMA`: Default schema for operations (e.g., `fa02_staging`)
- `KAFKA_NETWORK_NAME`: Docker network name

Optional (for Snowflake):
- `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_PRIVATE_KEY_FILE_PATH`

## Key Data Flows

1. **Etherscan → Parquet**: `runner.py` extracts blockchain data to `.data/raw/*.parquet`
2. **Parquet → PostgreSQL**: `postgres_load.py` loads into `raw` schema tables
3. **PostgreSQL → dbt**: dbt models transform `raw.logs` → `staging.stg_logs_decoded`
4. Failed extractions are logged to `logging/extract_error/` and automatically retried with smaller chunk sizes

## dbt Project Structure

```
dbt_project/
├── dbt_project.yml          # Configuration
├── profiles.yml             # Database connections
├── models/
│   ├── staging/            # Raw data cleanup
│   │   ├── _staging__sources.yml
│   │   ├── _staging__models.yml
│   │   └── stg_logs_decoded.sql
│   ├── intermediate/       # Business logic transformations
│   └── marts/              # Final analytics tables
├── tests/                  # Data quality tests
│   ├── test_valid_address.sql
│   └── test_block_number_range.sql
├── macros/                 # Reusable SQL (ethereum_macros.sql)
└── packages.yml            # dbt dependencies (dbt_utils)
```

### Model Naming Conventions
- **Staging models**: `stg_<source>_<entity>.sql` (e.g., `stg_logs_decoded.sql`)
- **Intermediate models**: `int_<entity>_<verb>.sql` (e.g., `int_logs_filtered.sql`)
- **Fact tables**: `fct_<entity>.sql` (e.g., `fct_transfers.sql`)
- **Dimension tables**: `dim_<entity>.sql` (e.g., `dim_contracts.sql`)

## Database Schema

- **raw.logs**: Raw log data with columns: address, topics (JSONB), data, block_number, transaction_hash, etc.
- **raw.transactions**: Transaction data (structure similar to logs)
- **staging.stg_logs_decoded**: Decoded logs with parsed topics (topic0-topic3)
- dbt creates additional staging/intermediate/mart tables based on models in `dbt_project/models/`

## Project Structure Notes

- Runnable scripts are ONLY in `scripts/` directory
- Reusable code is packaged in `src/onchaindata/`
- dbt project located at `dbt_project/` with standard structure (staging → intermediate → marts)
- Data files: `.data/raw/` for extracted data, `sampledata/` for examples
- Always run Python scripts with `uv run python` (not direct python)
- Legacy `dbt_subprojects/` directory retained for reference (can be removed after migration)
