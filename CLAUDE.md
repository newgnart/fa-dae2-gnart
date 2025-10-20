# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a capstone project for FA DAE2 focused on Ethereum blockchain data extraction and transformation. The project extracts smart contract logs and transactions from Etherscan API, loads them into PostgreSQL, and transforms them using dbt.

## Architecture

The project follows an ELT (Extract, Load, Transform) pipeline:

### 1. Extract Layer (`scripts/el/`)
- **Primary script**: `extract_etherscan.py` - Extracts logs and transactions from Etherscan API
- Uses the `onchaindata.data_extraction.etherscan` module with `EtherscanClient`
- Supports multiple blockchain networks via chainid mapping in `src/onchaindata/config/chainid.json`
- Features automatic retry logic for failed block ranges with exponential backoff (reduces chunk size by 10x)
- Data stored as Parquet files in `.data/raw/` directory
- Error tracking in `logging/extract_error/` with automatic retry mechanism that logs failed ranges to CSV
- Supports K/M/B suffixes for block numbers (e.g., '18.5M' = 18,500,000)
- Additional extraction capabilities: `extract_graphql.py` for GraphQL-based extraction

### 2. Load Layer (`scripts/el/`)
- **load.py**: Unified loader script supporting both PostgreSQL and Snowflake
- Uses `onchaindata.data_pipeline.Loader` class with pluggable database clients
- Takes arguments: `-f` (file path), `-c` (client: postgres/snowflake), `-s` (schema), `-t` (table), `-w` (write disposition: append/replace/merge)
- Database clients in `src/onchaindata/utils/`: `PostgresClient`, `SnowflakeClient`

### 3. Transform Layer (dbt)
- **Location**: `dbt_project/`
- Standard dbt project structure with models organized by layer:
  - `models/01_staging/`: Raw data cleanup (e.g., `stg_logs_decoded.sql`)
  - `models/intermediate/`: Business logic transformations
  - `models/marts/`: Final analytics tables
- Materialization strategy:
  - staging: `view`
  - intermediate: `ephemeral`
  - marts: `table`
- Shared macros in `dbt_project/macros/ethereum_macros.sql`:
  - `uint256_to_address`: Extracts Ethereum addresses from uint256 hex strings
  - `uint256_to_numeric`: Converts uint256 hex to numeric values
- Sources defined in `models/01_staging/sources.yml` (references `raw` schema)
- Configuration: [dbt_project.yml](dbt_project/dbt_project.yml), [profiles.yml](dbt_project/profiles.yml)

### 4. Package Structure (`src/onchaindata/`)
Reusable Python package with modules:
- `data_extraction/`:
  - `etherscan.py`: EtherscanClient with rate limiting
  - `graphql.py`: GraphQL-based extraction
  - `rate_limiter.py`: Rate limiting utilities
  - `base.py`: Base classes for API clients
- `data_pipeline/`:
  - `loaders.py`: Loader class for database operations
- `utils/`:
  - `postgres_client.py`: PostgreSQL client with connection pooling
  - `snowflake_client.py`: Snowflake client
  - `chain.py`: Chain-related utilities
  - `base_client.py`: Base database client interface
- `config/`: Configuration files (chainid.json)

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

# Initialize database schema (if needed)
./scripts/sql_pg.sh ./scripts/sql/init.sql
```

### Data Extraction
```bash
# Extract logs and transactions for a specific contract address
# Supports K/M/B suffixes for block numbers (e.g., '18.5M')
uv run python scripts/el/extract_etherscan.py \
  -c ethereum \
  -a 0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72 \
  --logs --transactions \
  --from_block 18.5M --to_block 20M \
  -v  # verbose logging

# Extract data from last N days
uv run python scripts/el/extract_etherscan.py \
  -a 0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72 \
  --logs --transactions \
  --last_n_days 7

# Logging levels: no flag (WARNING), -v (INFO), -vv (DEBUG)
```

### Data Loading
```bash
# Load Parquet file to PostgreSQL
uv run python scripts/el/load.py \
  -f .data/raw/ethereum_0xaddress_logs_18500000_20000000.parquet \
  -c postgres \
  -s raw \
  -t logs \
  -w append

# Load to Snowflake (requires SNOWFLAKE_* env vars)
uv run python scripts/el/load.py \
  -f .data/raw/ethereum_0xaddress_logs_18500000_20000000.parquet \
  -c snowflake \
  -s raw \
  -t logs \
  -w append
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
```

### SQL Operations
```bash
# Run SQL scripts directly against PostgreSQL
./scripts/sql_pg.sh ./scripts/sql/init.sql

# Ad-hoc queries
./scripts/sql_pg.sh ./scripts/sql/ad_hoc.sql
```

## Environment Variables

Required variables (see `.env.example`):
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `DB_SCHEMA`: Default schema for operations (e.g., `fa02_staging`)
- `KAFKA_NETWORK_NAME`: Docker network name
- `ETHERSCAN_API_KEY`: For Etherscan API access

Optional (for Snowflake):
- `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_PRIVATE_KEY_FILE_PATH`

## Key Data Flows

1. **Etherscan → Parquet**: `extract_etherscan.py` extracts blockchain data to `.data/raw/*.parquet`
2. **Parquet → PostgreSQL/Snowflake**: `load.py` loads into `raw` schema tables
3. **PostgreSQL → dbt**: dbt models transform `raw.logs` → `staging.stg_logs_decoded`
4. Failed extractions are logged to `logging/extract_error/` and automatically retried with smaller chunk sizes (10x reduction)

## dbt Project Structure

```
dbt_project/
├── dbt_project.yml          # Configuration (project: stablecoins)
├── profiles.yml             # Database connections (dev=postgres, test/prod=snowflake)
├── models/
│   ├── 01_staging/         # Raw data cleanup (materialized as views)
│   │   ├── sources.yml     # Source definitions (raw schema)
│   │   ├── models.yml      # Model documentation
│   │   └── stg_logs_decoded.sql
│   ├── intermediate/       # Business logic (ephemeral)
│   └── marts/              # Final analytics (tables)
├── tests/                  # Data quality tests
├── macros/                 # ethereum_macros.sql (uint256_to_address, uint256_to_numeric)
└── packages.yml            # dbt dependencies
```

### Model Naming Conventions
- **Staging models**: `stg_<source>_<entity>.sql` (e.g., `stg_logs_decoded.sql`)
- **Intermediate models**: `int_<entity>_<verb>.sql` (e.g., `int_logs_filtered.sql`)
- **Fact tables**: `fct_<entity>.sql` (e.g., `fct_transfers.sql`)
- **Dimension tables**: `dim_<entity>.sql` (e.g., `dim_contracts.sql`)

## Database Schema

- **raw.logs**: Raw log data with columns: address, topics (JSONB), data, block_number, transaction_hash, time_stamp, etc.
- **raw.transactions**: Transaction data (structure similar to logs)
- **staging.stg_logs_decoded**: Decoded logs with parsed topics (topic0-topic3), indexed on (contract_address, transaction_hash, index)
- dbt creates additional staging/intermediate/mart tables based on models in `dbt_project/models/`

## Project Structure Notes

- Runnable scripts are ONLY in `scripts/` directory (organized as `scripts/el/` for extract/load)
- Reusable code is packaged in `src/onchaindata/` as an installable package
- dbt project located at `dbt_project/` with standard structure (staging → intermediate → marts)
- Data files: `.data/raw/` for extracted data, `sampledata/` for examples
- Always run Python scripts with `uv run python` (not direct python)
- Project uses `uv` for dependency management (see `pyproject.toml`)
