## Event data
Raw Events data is indexed with [HyperIndex](https://docs.envio.dev/docs/HyperIndex/overview), a blockchain indexing framework that transforms on-chain events into structured, queryable databases with GraphQL APIs.

More details: [Envio](https://docs.envio.dev/docs/HyperIndex/overview)

**To run the indexer:**
```bash
git clone https://github.com/newgnart/envio-stablecoins.git
pnpm dev
```

When the indexer is running, you have few options:

- **Extract/save to parquet files**, this will save the data to `.data/raw/transfer_{start_block}_{end_block}.parquet`

```bash
uv run scripts/el/extract_graphql.py \
--query-file scripts/el/stables_transfers.graphql \
-f transfer \
--from_block 23650000
--to_block 23660000
-v
```

- **Stream/load directly from the indexer to the postgres**
```bash
uv run scripts/el/stream_graphql.py \
-e http://localhost:8080/v1/graphql \
--fields id,blockNumber,timestamp,contractAddress,from,to,value \
--graphql-table stablesTransfers \
-c postgres \
-s raw \
-t raw_transfer
```

- **Move data from postgres to snowflake**
```bash
uv run python scripts/el/pg2sf_raw_transfer.py \
--from_block 23650000 \
--to_block 23660000 \
-v
```

