#!/bin/bash

# Script to run SQL files against PostgreSQL database
# Usage: ./scripts/sql_pg.sh <sql_file_path>
# Example: ./scripts/sql_pg.sh scripts/sql/init.sql

# Check if SQL file argument is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <sql_file_path>"
    echo "Example: $0 scripts/sql_pg.sh scripts/sql/init.sql"
    exit 1
fi

SQL_FILE="$1"

# Check if SQL file exists
if [ ! -f "$SQL_FILE" ]; then
    echo "Error: SQL file '$SQL_FILE' not found!"
    exit 1
fi

# Load environment variables from .env file if it exists
if [ -f .env ]; then
    echo "Loading environment variables from .env file..."
    source .env
fi

# Check if required environment variables are set
if [ -z "$POSTGRES_HOST" ] || [ -z "$POSTGRES_PORT" ] || [ -z "$POSTGRES_USER" ] || [ -z "$POSTGRES_PASSWORD" ] || [ -z "$POSTGRES_DB" ]; then
    echo "Error: Missing required environment variables!"
    echo "Please set: POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB"
    echo "You can create a .env file with these variables or export them in your shell."
    exit 1
fi

echo "Running SQL file: $SQL_FILE"
echo "Database: $POSTGRES_DB at $POSTGRES_HOST:$POSTGRES_PORT"

# Run the SQL file
PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -f "$SQL_FILE"

# Check if command was successful
if [ $? -eq 0 ]; then
    echo "✅ SQL file executed successfully!"
else
    echo "❌ Error executing SQL file!"
    exit 1
fi
