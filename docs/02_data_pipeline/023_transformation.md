## Raw data ingestion
- Load parquet files to database
```bash
uv run scripts/el/load.py \
-f .data/raw/demo/transfer_23652044_23652327.parquet \
-c postgres \
-s raw \
-t raw_transfers \
-w append
```

- **Stream/load directly from the indexer to the database**
```bash
uv run scripts/el/stream_graphql.py \
-e http://localhost:8080/v1/graphql \ 
--fields id,blockNumber,timestamp,contractAddress,from,to,value \ 
--graphql-table stablesTransfers \
-c postgres \ 
-s demo \ 
-t transfers \ 
```

## Data Transformation with dbt

## Notes
- **Crypto data landscape**
- Database client
    - You can write your own database client base on [BaseDatabaseClient](https://github.com/newgnart/fa-dae2-stables-analytics/blob/main/src/onchaindata/utils/base_client.py). e.g. [PostgresClient](https://github.com/newgnart/fa-dae2-stables-analytics/blob/main/src/onchaindata/utils/postgres_client.py)
    - Currently supported databases:
        - PostgreSQL
        - Snowflake
- Data Schema Reference

## dbt models


