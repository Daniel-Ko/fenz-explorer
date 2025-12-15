import os
import io

from load_to_s3 import upload

from dotenv import load_dotenv


load_dotenv()

s3_bucket = os.getenv("S3_BUCKET")

buffer = io.BytesIO()
for fname in sorted(
    [
        fname
        for fname in os.listdir("../output/")
        if fname.startswith("initial")
    ],
    key=lambda x: int(x.split("_")[2])
):
    print(fname)

#upload(f, s3_bucket, f)

