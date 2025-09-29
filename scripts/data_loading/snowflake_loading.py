from dotenv import load_dotenv

load_dotenv()
from capstone_package.data_pipeline.snowflake_loader import (
    upload_file_to_stage,
)


if __name__ == "__main__":
    upload_file_to_stage(
        "data/ethereum_0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72/logs.json",
        "RAW_DATA.JSON_STAGE",
    )
