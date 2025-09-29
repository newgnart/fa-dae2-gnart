from capstone_package.utils import SnowflakeClient


def upload_file_to_stage(file_path: str, stage_name: str) -> bool:
    """Upload file to Snowflake stage."""
    try:
        with SnowflakeClient().cursor() as cursor:
            put_command = f"PUT file://{file_path} @{stage_name}"
            cursor.execute(put_command)
            result = cursor.fetchone()
            print(f"✅ File uploaded: {result[0]} - {result[6]} status")
            return True
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False
