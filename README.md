# Ethereum Blockchain Data Analytics Platform

Capstone project for [Foundry AI Academy](https://www.foundry.academy/) Data & AI Engineering program. An ELT pipeline for extracting, loading, and transforming Ethereum blockchain data with focus on stablecoin analytics.

Inspired by [Visa on Chain Analytics](https://visaonchainanalytics.com/).

## Quick Start

### Prerequisites
```bash
# Create Docker network
docker network create fa-dae2-capstone_kafka_network

# Start PostgreSQL
docker-compose up -d

# Install dependencies
uv sync

# Setup environment
cp .env.example .env
export $(cat .env | xargs)
```

### Extract Data
```bash
# Extract logs and transactions from Etherscan
uv run python scripts/el/extract_etherscan.py \
  -c ethereum \
  -a 0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72 \
  --logs --transactions \
  --from_block 18.5M --to_block 20M \
  -v
```

### Load Data
```bash
# Load Parquet to PostgreSQL
uv run python scripts/el/load.py \
  -f .data/raw/ethereum_0xaddress_logs_18500000_20000000.parquet \
  -c postgres \
  -s raw \
  -t logs \
  -w append
```

### Transform Data
```bash
# Run dbt models
./scripts/dbt.sh run

# Run specific model
./scripts/dbt.sh run --select stg_logs_decoded
```

## Architecture

**Extract** → **Load** → **Transform**

1. **Extract** (`scripts/el/extract_etherscan.py`): Pulls blockchain data from Etherscan API to `.data/raw/*.parquet`
2. **Load** (`scripts/el/load.py`): Loads Parquet files into PostgreSQL/Snowflake `raw` schema
3. **Transform** (`dbt_project/`): dbt models transform raw data into analytics-ready tables

### Project Structure
```
├── scripts/el/              # Extract & Load scripts
├── src/onchaindata/         # Reusable Python package
│   ├── data_extraction/     # Etherscan/GraphQL clients
│   ├── data_pipeline/       # Loader classes
│   └── utils/              # Database clients
├── dbt_project/            # dbt transformation layer
│   ├── models/01_staging/  # Raw data cleanup (views)
│   ├── models/intermediate/# Business logic (ephemeral)
│   └── models/marts/       # Analytics tables (tables)
└── .data/raw/             # Extracted Parquet files
```

## Key Features

- **Multi-chain support**: Ethereum, Polygon, BSC via chainid mapping
- **Automatic retry**: Failed extractions retry with smaller chunks (10x reduction)
- **Flexible loading**: PostgreSQL and Snowflake support
- **Block number shortcuts**: Use `18.5M` instead of `18500000`
- **dbt transformations**: Staging → Intermediate → Marts layers

## Environment Variables

Required (see `.env.example`):
- `POSTGRES_*`: Database connection
- `ETHERSCAN_API_KEY`: API access
- `DB_SCHEMA`: Default schema

Optional (for Snowflake):
- `SNOWFLAKE_*`: Snowflake connection details

## Common Commands

```bash
# SQL operations
./scripts/sql_pg.sh ./scripts/sql/init.sql

# dbt operations
./scripts/dbt.sh test                    # Run tests
./scripts/dbt.sh docs generate           # Generate docs
./scripts/dbt.sh run --select staging.*  # Run staging models

# Extract with time range
uv run python scripts/el/extract_etherscan.py \
  -a 0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72 \
  --logs --transactions \
  --last_n_days 7
```

## Database Schema

- `raw.logs`: Raw event logs with JSONB topics
- `raw.transactions`: Transaction data
- `staging.stg_logs_decoded`: Decoded logs with parsed topics (topic0-topic3)
- Marts: Analytics tables created by dbt

## Documentation

For detailed documentation, see [CLAUDE.md](CLAUDE.md) or the [docs/](docs/) directory.
