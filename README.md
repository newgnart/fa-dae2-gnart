# 1. Set up the environment

## Postgres with Docker

```bash
docker network create fa-dae2-capstone_kafka_network
docker-compose up -d
```

## Initialize the database

### Set environment variables
```bash
export $(cat .env | xargs)
```

### Initialize the database

```bash
PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -f scripts/sql/init.sql
```