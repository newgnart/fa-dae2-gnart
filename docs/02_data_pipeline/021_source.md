## Event data
Raw Events data is indexed with [HyperIndex](https://docs.envio.dev/docs/HyperIndex/overview), a blockchain indexing framework that transforms on-chain events into structured, queryable databases with GraphQL APIs.

More details: [Envio](https://docs.envio.dev/docs/HyperIndex/overview)

**To run the indexer:**
```bash
git clone https://github.com/newgnart/envio-stablecoins.git
pnpm dev
```

When the indexer is running, you have few options:

- Extract/save to parquet files, this will save the data to `.data/raw/transfer_{start_block}_{end_block}.parquet`
```bash
uv run scripts/el/extract_graphql.py \
--query-file scripts/el/stables_transfers.graphql \
-f transfer \
-v
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




## Wallet labels data
- Get wallet addresses database, column `from` or `to`
```sql
SELECT DISTINCT address
FROM (
    SELECT "from" AS address
    FROM raw.transfers
    UNION ALL
    SELECT "to" AS address
    FROM raw.transfers
) combined
WHERE address IS NOT NULL
ORDER BY address;
```
Save those addresses to a file, e.g. `addresses.csv`

- Scrape wallet labels from Etherscan
```bash
uv run scripts/el/scrape_etherscan.py 
-i .data/addresses.csv \
-o .data/raw/labels.csv \
```

- Load wallet labels to database
```bash
uv run scripts/el/load.py \
-f .data/raw/wallet_labels.csv \
-c postgres \
-s demo \
-t wallet_labels \
-w append
```