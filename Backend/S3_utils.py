
import io
import json
import hashlib
import gzip
import base64
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from botocore.exceptions import ClientError
from S3_init import s3, bucket_name
from config import CLOUDFRONT_DOMAIN

def hash_email(email):
    return hashlib.sha256(email.encode('utf-8')).hexdigest()

def list_files(prefix: str = ""):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if "Contents" not in response:
        return []
    return [obj["Key"] for obj in response["Contents"]]

# def get_pdf_file(key: str):
#     s3_object = s3.get_object(Bucket=bucket_name, Key=key)
#     file_bytes = s3_object["Body"].read()
#     return StreamingResponse(io.BytesIO(file_bytes), media_type="application/pdf")

def get_json_file(email, endpoint):
    key = hash_email(email) + endpoint
    try:
        s3_object = s3.get_object(Bucket=bucket_name, Key=key)
        compressed_data = s3_object["Body"].read()

        decompressed_data = gzip.decompress(compressed_data)

        content = decompressed_data.decode("utf-8-sig")
        return json.loads(content)

    except s3.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail=f"File '{key}' not found in S3")
    except gzip.BadGzipFile:
        raise HTTPException(status_code=500, detail=f"File '{key}' is not valid gzip data")
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"File '{key}' is not valid JSON: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error reading '{key}': {e}")

def get_cloudfront_url(key: str) -> str:
    return f"https://{CLOUDFRONT_DOMAIN}/{key}"
    
def save_json_file(email, endpoint, data):
    key = hash_email(email) + endpoint
    try:
        json_str = json.dumps(data, ensure_ascii=False, indent=2)
        
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="wb") as gz_file:
            gz_file.write(json_str.encode("utf-8"))

        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/json",
            ContentEncoding="gzip",
        )
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Error saving '{key}' to S3: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error saving '{key}': {e}")