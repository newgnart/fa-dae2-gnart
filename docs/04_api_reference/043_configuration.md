# Configuration Guide

This guide covers all configuration options for the project.

## Environment Variables

The project uses environment variables for configuration. Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

### Required Variables

#### PostgreSQL Configuration
```bash
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
DB_SCHEMA=fa02_staging  # Default schema for operations
```

#### Docker Network
```bash
KAFKA_NETWORK_NAME=fa-dae2-capstone_kafka_network
```

### Optional Variables

#### Snowflake Configuration
For Snowflake data warehouse support:

```bash
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_PRIVATE_KEY_FILE_PATH=/path/to/private_key.p8
```

#### API Keys
For alternative data extraction methods:

```bash
ETHERSCAN_API_KEY=your_etherscan_api_key  # Optional, for Etherscan API extraction
```

---

## Database Setup

### PostgreSQL with Docker

1. Create Docker network (first time only):
```bash
docker network create fa-dae2-capstone_kafka_network
```

2. Start PostgreSQL container:
```bash
docker-compose up -d
```

3. Load environment variables:
```bash
export $(cat .env | xargs)
```

4. Initialize database schema (if needed):
```bash
./scripts/sql_pg.sh ./scripts/sql/init.sql
```

### Snowflake Setup

1. Generate private key for authentication:
```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_key.p8 -nocrypt
```

2. Add public key to Snowflake user:
```sql
ALTER USER your_user SET RSA_PUBLIC_KEY='<public_key>';
```

3. Set `SNOWFLAKE_PRIVATE_KEY_FILE_PATH` in `.env`

---

## Supported Chains

The project supports multiple EVM chains via chain ID mapping in `src/onchaindata/config/chainid.json`.

**Mainnets:**
- `ethereum` (1)
- `arbitrum_one` (42161)
- `base` (8453)
- `polygon` (137)
- `op` (10)
- `bnb_smart_chain` (56)
- `avalanche_c-_chain` (43114)
- And 50+ more chains...

**Testnets:**
- `sepolia_testnet` (11155111)
- `base_sepolia_testnet` (84532)
- `arbitrum_sepolia_testnet` (421614)
- And more...

**Usage:**
```python
from onchaindata.data_extraction.etherscan import EtherscanClient

# Use chain name
client = EtherscanClient(chain="ethereum")

# Or use chain ID directly
client = EtherscanClient(chainid=1)
```

---

## dbt Configuration

### profiles.yml

The dbt project uses `dbt_project/profiles.yml` for database connections:

**Development (PostgreSQL):**
```yaml
stablecoins:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('POSTGRES_HOST') }}"
      port: 5432
      user: "{{ env_var('POSTGRES_USER') }}"
      password: "{{ env_var('POSTGRES_PASSWORD') }}"
      dbname: "{{ env_var('POSTGRES_DB') }}"
      schema: staging
      threads: 4
```

**Production (Snowflake):**
```yaml
    prod:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      private_key_path: "{{ env_var('SNOWFLAKE_PRIVATE_KEY_FILE_PATH') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
      schema: "{{ env_var('SNOWFLAKE_SCHEMA') }}"
      threads: 8
```

### dbt_project.yml

Materialization strategy:
```yaml
models:
  stablecoins:
    staging:
      +materialized: view
    intermediate:
      +materialized: ephemeral
    marts:
      +materialized: table
```

---

## Rate Limiting

### GraphQL (HyperIndex)
No rate limiting required for local indexer.

### Etherscan API
Default rate limit: **5 requests/second**

Configure when creating client:
```python
client = EtherscanClient(
    chain="ethereum",
    api_key="your_api_key",
    calls_per_second=5.0  # Adjust based on your API tier
)
```

**API Tiers:**
- Free: 5 calls/second
- Standard: Higher limits (check Etherscan docs)

---

## Docker Compose Configuration

The `docker-compose.yml` defines the PostgreSQL service:

```yaml
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    ports:
      - "${POSTGRES_PORT}:5432"
    networks:
      - kafka_network
```

To modify PostgreSQL version or configuration, edit `docker-compose.yml`.

---

## Troubleshooting

### Database Connection Issues

**Error: Connection refused**
```bash
# Check if PostgreSQL is running
docker ps

# Restart PostgreSQL
docker-compose restart
```

**Error: Authentication failed**
```bash
# Verify environment variables are loaded
echo $POSTGRES_USER

# Reload environment
export $(cat .env | xargs)
```

### dbt Connection Issues

**Error: Could not connect to database**
```bash
# Test database connection
./scripts/sql_pg.sh -c "SELECT 1;"

# Verify dbt can connect
cd dbt_project && uv run dbt debug
```
