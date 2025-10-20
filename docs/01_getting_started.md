# Getting started

## Indexer
In a seperated terminal, clone the indexer repository and run

```bash
git clone https://github.com/newgnart/envio-stablecoins.git
pnpm dev
```

## Postgres with Docker
```bash
docker network create fa-dae2-capstone_kafka_network
docker-compose up -d
```

## Analytics Dashboard
```bash
uv run streamlit/app.py
```