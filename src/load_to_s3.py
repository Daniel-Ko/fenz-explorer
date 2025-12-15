import boto3
from loguru import logger

def configure_s3_client():
    try:
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        return s3
    except ClientError as e:
        logger.error(f"Failed to initialize Boto3 client: {e}")
        logger.error("Please ensure AWS environment variables are set.")
        return None

def upload(client, file, bucket, filename):
    try:
        with open(file, "rb") as dataf:
            client.upload_fileobj(dataf, bucket, filename)
    except Exception as e:
        logger.debug(e)
