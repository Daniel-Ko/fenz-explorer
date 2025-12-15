import os
import io

from load_to_s3 import * 

from dotenv import load_dotenv


load_dotenv(override=True)

s3_bucket = os.getenv("S3_BUCKET")
s3_client = configure_s3_client()


buffer = io.BytesIO()
for fname in sorted(
    [
        fname
        for fname in os.listdir("../output/")
        if fname.startswith("initial")
    ],
    key=lambda x: int(x.split("_")[2])
):
    local_path = os.path.join("../output/", fname)
    upload(s3_client, local_path, s3_bucket, fname)

