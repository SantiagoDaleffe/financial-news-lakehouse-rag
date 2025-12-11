import os
import boto3
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_minio_client():
    """client conection to MinIO/s3 using env"""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

def save_to_bronze(data, filename, source_type, bucket="bronze"):
    """
    saves jsons to data lake (minio)
    folder structure: bronze/{source_type}/{YYYY-MM-DD}/{filename}
    """ 

    if not data:
        logging.warning(f"empty data for saving in {filename}")
        return

    s3 = get_minio_client()
    
    today = datetime.now().strftime("%Y-%m-%d")
    key = f"{source_type}/{today}/{filename}"
    
    try:
        json_buffer = json.dumps(data, ensure_ascii=False, default=str).encode('utf-8')
        
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json_buffer,
            ContentType="application/json"
        )
        logging.info(f"Saved on MinIO: s3://{bucket}/{key} ({len(data)} items)")
    except Exception as e:
        logging.error(f"error saving on MinIO: {e}")