import airflow.hooks.S3_hook
from pathlib import Path


def upload_to_S3(from_path: Path):
    """Uploads a Eurostat CSV to s3"""
    bucket_name = "ebs-capstone-bucket"
    if not from_path.exists():
        raise Exception
    hook = airflow.hooks.S3_hook.S3Hook("s3_connection")
    hook.load_file(str(from_path), from_path.name, bucket_name, replace=True)
