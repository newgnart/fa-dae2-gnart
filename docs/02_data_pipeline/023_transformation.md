## Raw data ingestion
- Load parquet files to database
```bash
uv run scripts/el/load.py \
-f .data/raw/demo/transfer_23652044_23652327.parquet \ # Path to the Parquet file
-c snowflake \ # Database client, 'snowflake' or 'postgres'
-s demo \ # Schema name
-t transfers \ # Table name
-w append # Write disposition, 'append' or 'replace' or 'merge'
```

- **Stream/load directly from the indexer to the database**
```bash
uv run scripts/el/stream_graphql.py \
-e http://localhost:8080/v1/graphql \ # GraphQL endpoint URL
--fields id,blockNumber,timestamp,contractAddress,from,to,value \ # Fields to fetch
--graphql-table stablesTransfers \ # Name of the table of GraphQL Endpoint to query, e.g. 'stablesTransfers'
-c postgres \ # Database client, 'snowflake' or 'postgres'
-s demo \ # Schema name
-t transfers \ # Table name
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


