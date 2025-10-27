
import io
import json
import gzip
import base64
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from botocore.exceptions import ClientError
from S3_init import s3, bucket_name
from config import CLOUDFRONT_DOMAIN

def list_files(prefix: str = ""):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if "Contents" not in response:
        return []
    return [obj["Key"] for obj in response["Contents"]]

# def get_pdf_file(key: str):
#     s3_object = s3.get_object(Bucket=bucket_name, Key=key)
#     file_bytes = s3_object["Body"].read()
#     return StreamingResponse(io.BytesIO(file_bytes), media_type="application/pdf")

def get_json_file(hashed_email, endpoint):
    key = hashed_email + endpoint
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
    
def save_json_file(hashed_email, endpoint, data):
    key = hashed_email + endpoint
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
    
def ensure_json_file_exists(hashed_email: str, endpoint: str) -> None:
    """
    Ensures that the JSON file exists in S3. If it does not exist, creates it as an empty JSON array.
    """
    key = hashed_email + endpoint
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == "404":
            empty_data = json.dumps([]).encode("utf-8")
            compressed_data = gzip.compress(empty_data)
            s3.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=compressed_data,
                ContentType="application/json",
                ContentEncoding="gzip",
            )
        else:
            raise HTTPException(status_code=500, detail=f"Unexpected S3 error checking '{key}': {e}")