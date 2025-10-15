#!/bin/bash

# Usage: ./scripts/dbt.sh [dbt_commands...]
# Example: ./scripts/dbt.sh run
# Example: ./scripts/dbt.sh test
# Example: ./scripts/dbt.sh run --select stg_logs_decoded

# Load environment variables from .env file
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Check if any arguments provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 [dbt_commands...]"
    echo "Example: $0 run"
    echo "Example: $0 test"
    echo "Example: $0 run --select stg_logs_decoded"
    echo "Example: $0 docs generate"
    exit 1
fi

echo "Running dbt in dbt_project/ with args: $@"

# Change to dbt project directory and run dbt
cd dbt_project && uv run dbt "$@"
