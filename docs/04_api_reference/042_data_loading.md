## Database Clients

### `BaseDatabaseClient`

Abstract base class for database clients with common patterns.

**Location:** `onchaindata.utils.base_client`

**Methods:**
- `get_dlt_destination()`: Get DLT destination for this database
- `get_connection()`: Get a database connection


### `PostgresClient` and `SnowflakeClient`

PostgreSQL and Snowflake database clients with connection pooling.

**Location:** `onchaindata.utils.postgres_client` and `onchaindata.utils.snowflake_client`

**Constructor:**
Both can be constructed with classmethod `from_env` 

```python
client = PostgresClient.from_env()
```

**Methods:**






## Loader
*A wrapper class inheriting from Database Client and dlt.pipeline* 

**Location:** `onchaindata.data_pipeline.loaders`

**Constructor:**
```python
from onchaindata.data_pipeline import Loader
from onchaindata.utils import PostgresClient

client = PostgresClient.from_env()
loader = Loader(client=client)
```

**Parameters:**

- `client` (PostgresClient | SnowflakeClient): Database client instance

**Methods:**

#### `load_parquet()`
Load Parquet file to database using DLT.

```python
loader.load_parquet(
    file_path=".data/raw/data.parquet",
    schema="raw",
    table_name="stables_transfers",
    write_disposition="append"  # or "replace", "merge"
)
```

**Parameters:**

- `file_path` (str | Path): Path to the Parquet file
- `schema` (str): Target schema name
- `table_name` (str): Target table name
- `write_disposition` (str): How to handle existing data
  - `"append"`: Add new records (default)
  - `"replace"`: Drop and recreate table
  - `"merge"`: Update existing records

#### `load_dataframe()`
Load Polars DataFrame directly to database.

```python
import polars as pl

df = pl.read_parquet(".data/raw/data.parquet")
loader.load_dataframe(
    df=df,
    schema="raw",
    table_name="stables_transfers",
    write_disposition="append"
)
```

**Special Handling:**

- For `logs` table, automatically sets `topics` column as JSON type

---
