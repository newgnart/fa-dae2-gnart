## GraphQL
Utilities for getting data from Envio GraphQL API endpoint 

### `GraphQLBatch`

For extracting data from a GraphQL endpoint and save to Parquet file.

**Parameters:**

- `endpoint` (str): GraphQL endpoint URL
- `query` (str): GraphQL query string

**Methods:**

- `extract()`: Execute GraphQL query and return results as dictionary
- `extract_to_dataframe()`: Execute GraphQL query and return results as Polars DataFrame


### `GraphQLStream`

For streaming data from a GraphQL endpoint and push to database directly.

**Parameters:**

- `endpoint` (str): GraphQL endpoint URL
- `table_name` (str): Name of the table (GraphQL table) to fetch
- `fields` (list): List of fields to fetch
- `poll_interval` (int): Polling interval in seconds

**Methods:**

- `stream()`: Stream data from GraphQL endpoint and push to database directly
  - Arguments:
    - `loader` (Loader): Loader instance for database operations
    - `schema` (str): Target schema name
    - `table_name` (str): Target table name
