import os
import json
from typing import List, Dict, Any
import boto3
from botocore.config import Config as BotoConfig
from fastapi import HTTPException
from fastapi.responses import StreamingResponse

# ---- Lambda client config (env overridable) ----
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-2")
ZIP_LAMBDA_NAME = os.getenv("ZIP_LAMBDA_NAME", "download-into-zip")
LAMBDA_READ_TIMEOUT = int(os.getenv("LAMBDA_READ_TIMEOUT", "300"))  # seconds
s3 = boto3.client("s3", region_name=AWS_REGION)
ZIP_BUCKET = "vericarestorage"

_lambda_cfg = BotoConfig(
    region_name=AWS_REGION,
    read_timeout=LAMBDA_READ_TIMEOUT,
    connect_timeout=10,
    retries={"max_attempts": 1, "mode": "standard"},  # fail fast; your call
)
lambda_client = boto3.client("lambda", config=_lambda_cfg)

def _first_email(raw: List[Any]) -> str:
    if not raw:
        raise HTTPException(status_code=404, detail="No emails found for client")
    v = raw[0]
    if isinstance(v, dict) and v.get("email_address"):
        return v["email_address"].strip()
    if isinstance(v, str) and v.strip():
        return v.strip()
    raise HTTPException(status_code=400, detail="Invalid email format")

def _invoke_zip_lambda_for(emails: List[str]) -> dict:
    """
    Invoke Lambda to create ZIP of PDFs for given emails.

    emails (List[str]): List of email addresses to fetch PDFs for.

    Returns:
        dict: Lambda response body containing zip_key, presigned_url, etc.
    """
    # Prepare payload - Lambda expects {"emails": ["email1", "email2", ...]}
    payload_data = {"emails": emails}
    
    #invoke the lambda function
    resp = lambda_client.invoke(
        FunctionName=ZIP_LAMBDA_NAME,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload_data).encode("utf-8"),
    )
    payload = resp["Payload"].read().decode("utf-8")

    try:
        env = json.loads(payload) 
    except Exception:
        raise HTTPException(status_code=502, detail="Invalid response from ZIP Lambda")
    
    # Check Lambda response
    code = env.get("statusCode")
    body_raw = env.get("body")
    body = json.loads(body_raw) if isinstance(body_raw, str) else (body_raw or {})
    if code == 200 and "zip_key" in body:
        return body
    if code == 404:
        raise HTTPException(status_code=404, detail="No PDFs found for the provided emails")
    err = body.get("error") if isinstance(body, dict) else "ZIP Lambda error"
    raise HTTPException(status_code=502, detail=f"Lambda failed: {err}")

def _stream_s3_zip(key: str, download_name: str) -> StreamingResponse:
    '''
    Send a ZIP file from S3 as a streaming response

    key (str): The S3 object key for the ZIP file.
    download_name (str): The filename to suggest for download.

    Returns:
        StreamingResponse: The streaming response containing the ZIP file.
    '''
    try:
        obj = s3.get_object(Bucket=ZIP_BUCKET, Key=key)
    except s3.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail="ZIP not found in S3")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"S3 error: {e}")
    def it():
        for chunk in obj["Body"].iter_chunks(chunk_size=1024 * 1024):
            if chunk:
                yield chunk
    return StreamingResponse(
        it(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{download_name}"'},
    )