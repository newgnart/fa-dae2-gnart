# Stablecoins dbt Project

This dbt project transforms raw Ethereum blockchain data extracted from Etherscan API into analytics-ready tables.

## Project Structure

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
├── macros/                 # Reusable SQL
│   └── ethereum_macros.sql
└── packages.yml            # dbt dependencies
```

## Quick Start

```bash
# Install dbt packages
./scripts/dbt.sh deps

# Test database connection
cd dbt_project && uv run dbt debug

# Compile models (without running)
./scripts/dbt.sh compile

# Run all models
./scripts/dbt.sh run

# Run specific model
./scripts/dbt.sh run --select stg_logs_decoded

# Run tests
./scripts/dbt.sh test

# Generate documentation
./scripts/dbt.sh docs generate
./scripts/dbt.sh docs serve
```

## Models

### Staging Layer (`models/staging/`)

**stg_logs_decoded**
- Decodes raw smart contract logs from `raw.logs`
- Parses JSONB topics into individual columns (topic0-topic3)
- Converts timestamps to proper datetime format
- Creates indexes for efficient querying

## Custom Macros

Located in `macros/ethereum_macros.sql`:

- **uint256_to_address**: Extracts Ethereum addresses from uint256 hex strings
- **uint256_to_numeric**: Converts uint256 hex to numeric values

## Data Quality Tests

Custom tests in `tests/`:
- **test_valid_address**: Validates Ethereum address format (42 chars, 0x prefix)
- **test_block_number_range**: Ensures block numbers are within reasonable bounds

Schema tests in `models/staging/_staging__models.yml`:
- NOT NULL checks on critical fields
- Unique combination test on (contract_address, transaction_hash, index)

## Database Schemas

- **raw**: Source data from Etherscan API (raw.logs, raw.transactions)
- **staging**: Cleaned and decoded data (staging.stg_logs_decoded)
- **intermediate**: Business logic transformations (future)
- **marts**: Final analytics tables (future)

## Configuration

The project uses environment variables for database connection (see `.env` file):
- POSTGRES_HOST
- POSTGRES_PORT
- POSTGRES_DB
- POSTGRES_USER
- POSTGRES_PASSWORD

These are referenced in `profiles.yml` using `{{ env_var('VAR_NAME') }}`.
