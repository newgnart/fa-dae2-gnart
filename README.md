# 1. Set up the environment

## Postgres with Docker

```bash
docker network create fa-dae2-capstone_kafka_network
docker-compose up -d
```
## Python environment
The project structured as a package in `src/capstone_package` directory. Runnable scripts are in `scripts` directory only.

Install dependencies using uv:
```bash
uv sync
```

## Initialize the database

### Set environment variables

- Copy `.env.example` to `.env`
```bash
cp .env.example .env
```
- Set environment variables
```bash
export $(cat .env | xargs)
```

### The data
- Log and transaction data of a smart contract [0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72](https://etherscan.io/address/0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72) on Ethereum.
- Whole loading data in is parquet format
- Example in json format: 
  - [logs.json](data/ethereum_0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72/logs.json)
  - [transactions.json](data/ethereum_0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72/transactions.json)

### Load data to Postgres

There are two ways to load data to Postgres:

1. Using DLT
dlt will automatically create the table and load data to it.
```bash
python scripts/data_loading/postgres_loader.py
```
**Note**: for non-standard data types e.g. json, use [apply_hints](scripts/data_loading/postgres_loader.py#L28) to define the data type.

1. Without DLT, using psycopg to load data.

- Initialize the table manually
```bash
./scripts/sql/run_sql.sh ./scripts/sql/init.sql;
```

- Use `load_parquet_to_postgres_wo_dlt` function in [postgres_loader.py](scripts/data_loading/postgres_loader.py)

### Load data to Snowflake

1. raw data stored in `database/RAW_DATA.JSON_STAGE`
Use `upload_file_to_stage` function in [snowflake_loader.py](scripts/data_loading/snowflake_loader.py) to upload the data to Snowflake.
```bash
python scripts/data_loading/snowflake_loading.py
```