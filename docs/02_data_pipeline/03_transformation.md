# Modeling
To serve the analytics needs, data is modeled in a star schema.

## 3. Data Transformation

### 3.1 Raw data ingestion
You have few options when the indexer is running:

- Extract (save to parquet files) 
```bash
uv run scripts/el/extract_graphql.py 
```

- Load parquet files to database
```bash
uv run scripts/el/load.py \
  -f .data/raw/data.parquet_23588762_23617141.parquet \
  -c postgres \
  -s raw \
  -t stables_transfers
  -w append
```
- **Stream/load directly from the indexer to the database**
```bash
uv run scripts/el/stream_graphql.py \
  -e http://localhost:8080/v1/graphql \
  -q "query { stablesTransfers(first: 1000) { id, blockNumber, timestamp, contractAddress, from, to, value } }" \
  -c postgres \
  -s raw \
  -t stables_transfers
  -w append
```

### 3.2 Data Transformation with dbt

## Notes
- **Crypto data landscape**
- Database client
    - You can write your own database client base on [BaseDatabaseClient](https://github.com/newgnart/fa-dae2-stables-analytics/blob/main/src/onchaindata/utils/base_client.py). e.g. [PostgresClient](https://github.com/newgnart/fa-dae2-stables-analytics/blob/main/src/onchaindata/utils/postgres_client.py)
    - Currently supported databases:
        - PostgreSQL
        - Snowflake

