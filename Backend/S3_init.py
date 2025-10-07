import boto3
from config import S3_CONFIG


s3 = boto3.client(
    "s3",
    aws_access_key_id=S3_CONFIG["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=S3_CONFIG["AWS_SECRET_ACCESS_KEY"],
    region_name=S3_CONFIG["AWS_REGION"]
)

bucket_name = S3_CONFIG["S3_BUCKET_NAME"]

print("S3 client initialized.")
