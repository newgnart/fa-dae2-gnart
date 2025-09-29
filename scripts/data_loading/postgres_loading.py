from dotenv import load_dotenv

load_dotenv()
from capstone_package.data_pipeline import (
    load_parquet_to_postgres,
)
from capstone_package.utils import PostgresClient


if __name__ == "__main__":
    load_parquet_to_postgres(
        file_path=".data/ethereum_0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72/logs.parquet",
        table_name="logs",
        postgres_client=PostgresClient.from_env(),
    )
#     load_parquet_to_postgres(
#         file_path=".data/ethereum_0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72/transactions.parquet",
#         table_name="transactions",
#         postgres_client=PostgresClient.from_env(),
#     )
