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
