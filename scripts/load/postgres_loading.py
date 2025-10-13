from dotenv import load_dotenv

load_dotenv()
from onchaindata.data_pipeline import (
    load_parquet_to_postgres,
)
from onchaindata.utils import PostgresClient


if __name__ == "__main__":
    load_parquet_to_postgres(
        file_path=".data/etherscan_raw/ethereum_0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72_logs_18400000_18500000.parquet",
        table_name="logs",
        postgres_client=PostgresClient.from_env(),
    )
