*Workflow for local development.*

## Indexer
Clone the indexer repository and run

```bash
git clone https://github.com/newgnart/envio-stablecoins.git
# make changes
pnpm dev
```

## Clone this repository
```bash
git clone https://github.com/newgnart/stables-analytics.git
cd stables-analytics
uv sync # install dependencies
```

## Postgres with Docker
```bash
docker network create kafka_network
docker-compose up -d
```

## Streaming data from the indexer to the database

- Directly
```bash
uv run scripts/el/stream_graphql.py \
  -e http://localhost:8080/v1/graphql \
  -q "query { stablesTransfers { id, blockNumber, timestamp, contractAddress, from, to, value } }" \
  -c postgres \
  -s raw \
  -t stables_transfers
  -w append
```