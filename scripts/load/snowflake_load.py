from dotenv import load_dotenv

load_dotenv()
from capstone_package.data_pipeline.snowflake_loader import (
    upload_file_to_stage,
    load_parquet_to_snowflake,
)
from capstone_package.utils import SnowflakeClient


if __name__ == "__main__":
    # upload_file_to_stage(
    #     "data/ethereum_0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72/logs.json",
    #     "RAW_DATA.JSON_STAGE",
    # )
    load_parquet_to_snowflake(
        file_path=".data/ethereum_0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72/logs_sample.parquet",
        table_name="logs_sample",
        snowflake_client=SnowflakeClient(),
    )
