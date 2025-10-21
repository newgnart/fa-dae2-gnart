*Workflow for local development.*

## Indexer
Clone the indexer repository and run

```bash
git clone https://github.com/newgnart/envio-stablecoins.git
pnpm dev
```

## Clone this repository
```bash
git clone https://github.com/newgnart/fa-dae2-stables-analytics.git
cd fa-dae2-stables-analytics
uv sync # install dependencies
```

## Postgres with Docker
```bash
docker network create stables_analytics_kafka_network
docker-compose up -d
```

## Streaming data from the indexer to the database
```bash
uv run scripts/el/stream_graphql.py \
  -e http://localhost:8080/v1/graphql \
  -q "query { stablesTransfers { id, blockNumber, timestamp, contractAddress, from, to, value } }" \
  -c postgres \
  -s raw \
  -t stables_transfers
  -w append
```

## Data transformation
```bash
uv run scripts/dbt.sh run
```

## Analytics
```bash
uv run streamlit/app.py
```