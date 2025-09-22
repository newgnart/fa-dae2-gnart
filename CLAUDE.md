# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a capstone project for FA DAE2 focused on GHRSST (Group for High Resolution Sea Surface Temperature) data retrieval and processing. The project uses NASA Harmony API to download satellite sea surface temperature data around Vietnam and stores it in a PostgreSQL database.

## Architecture

The project consists of several key components:

### GHRSST Data Retrieval (`scripts/ghrsst_downloader.py`)
- Downloads GHRSST data via NASA Harmony API using the harmony-py client
- Supports multiple processing levels: L2P (swath), L3S (daily gridded), L4 (gap-free daily)
- Default region: Vietnam (lat 8–22N, lon 102–112E)
- Requires EDL_TOKEN environment variable for NASA Earthdata authentication
- Downloads data to `./outputs/<job_id>/` directory

### Database Layer
- PostgreSQL database with staging schema
- Connection utilities in `scripts/database/`
- Uses psycopg for database connectivity
- Staging table: `staging.raw_data` with columns for data_content, file_name, loaded_at

### Infrastructure
- Docker Compose setup for PostgreSQL container
- Uses external Kafka network: `fa-dae2-capstone_kafka_network`
- Environment-based configuration via `.env` file

## Development Commands

### Environment Setup
```bash
# Install dependencies using uv
uv sync

# Start PostgreSQL container
docker-compose up -d

# Test database connection
python scripts/database/connection_test.py

# Run CRUD demo
python scripts/database/crud_demo.py
```

### GHRSST Data Operations
```bash
# Download L3S data (default)
EDL_TOKEN=your_token python scripts/ghrsst_downloader.py --level L3S

# Download L2P data with custom parameters
EDL_TOKEN=your_token python scripts/ghrsst_downloader.py --level L2P --bbox 7 23 100 114 --max-results 8

# Download L4 data for specific date
EDL_TOKEN=your_token python scripts/ghrsst_downloader.py --level L4 --date 2025-09-12
```

### Database Operations
```bash
# Test connection to staging database
python scripts/database/connection_test.py

# Demonstrate CRUD operations
python scripts/database/crud_demo.py
```

## Environment Variables

Required environment variables (see `.env.example`):
- `EDL_TOKEN`: NASA Earthdata Login token for API access
- `POSTGRES_HOST`: Database host (default: localhost)
- `POSTGRES_PORT`: Database port (default: 5433)
- `POSTGRES_DB`: Database name (default: postgres)
- `POSTGRES_USER`: Database user (default: postgres)
- `POSTGRES_PASSWORD`: Database password (default: postgres)

## Key Dependencies

- `harmony-py`: NASA Harmony API client for GHRSST data
- `psycopg`: PostgreSQL adapter for Python
- `python-dotenv`: Environment variable management
- `jupyter`: For notebook-based development

## Database Schema

The staging schema contains:
- `staging.raw_data`: Main table for storing downloaded GHRSST data files
  - `id`: Primary key
  - `data_content`: File content/metadata
  - `file_name`: Original filename
  - `loaded_at`: Timestamp of data insertion

## Known Data Collections

The project uses these NASA CMR concept IDs:
- L2P: `C2208421671-POCLOUD` (Himawari AHI GHRSST swath)
- L3S: `C2805339147-POCLOUD` (NOAA/STAR ACSPO L3S Daily 0.02°)
- L4: `C1996881146-POCLOUD` (JPL MUR L4 Global 0.01° Daily)