import boto3

try:
    s3 = boto3.client("s3", region_name="ap-southeast-2")
except ClientError as e:
    logger.error(f"Failed to initialize Boto3 client: {e}")
    logger.error("Please ensure AWS environment variables are set.")
    return None

def upload(file, filename, obj_name)
    try:
        with open(file, "rb") as dataf:
            s3.upload_fileobj(dataf, filename, obj_name)
    except ClientError as e:
        logger.debug(e)
