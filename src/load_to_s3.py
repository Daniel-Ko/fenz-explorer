import boto3

def configure_s3_client():
    try:
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        return s3
    except ClientError as e:
        logger.error(f"Failed to initialize Boto3 client: {e}")
        logger.error("Please ensure AWS environment variables are set.")
        return None

def upload(file, bucket, filename):
    try:
        with open(file, "rb") as dataf:
            s3.upload_fileobj(dataf, "s3://fenz-datalake", filename)
    except ClientError as e:
        logger.debug(e)
