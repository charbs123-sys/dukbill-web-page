# email_collection.py
# New signature: main(user_token, user_email_hash, since_days=730, page_size=50)
# - Pulls Inbox messages since N days ago, only those with attachments
# - Groups by conversationId
# - Includes base64 strings for PDF attachments in "pdfencoded"
# - Returns: (threads_dict, threads_yet_to_process)  -- we return an empty list for the second item
#
# threads_dict = {
#   "<conversationId>": [
#       {
#         "body": <str>,
#         "date": <ISO8601 str>,
#         "from_": <str or None>,
#         "subject": <str or None>,
#         "pdfs": [<filename>, ...],
#         "pdfencoded": [<base64 string>, ...],
#         "processed": 0
#       }, ...
#   ],
#   ...
# }


from __future__ import annotations
import sys
import traceback
from typing import Dict, List, Tuple, Any
from datetime import datetime, timedelta, timezone

from graph import GraphClient

import os
import json
import gzip
import logging
from datetime import datetime

import botocore.session
import botocore.config
logger = logging.getLogger(__name__)
class Database:
    """
    Simple S3-backed batch saver. Saves new_emails into a gzipped JSON object.
    """

    def __init__(self, raw_emails: Dict, path: str):
        self.path = path
        self.new_emails = raw_emails or {}

        # Disable EC2 metadata lookups that can hang in some environments
        os.environ.setdefault('AWS_EC2_METADATA_DISABLED', 'true')

        self.bucket_name = os.getenv('AWS_S3_BUCKET_NAME')
        logger.info(f"[Database] bucket name: {self.bucket_name}")
        if not self.bucket_name:
            raise ValueError("AWS_S3_BUCKET_NAME environment variable not set")

        try:
            session = botocore.session.Session()
            config = botocore.config.Config(
                region_name='ap-southeast-2',
                signature_version='v4',
                retries={'max_attempts': 2, 'mode': 'standard'},
                connect_timeout=5,
                read_timeout=5
            )
            self.s3_client = session.create_client('s3', 'ap-southeast-2', config=config)
            logger.info("[Database] S3 client created")
        except Exception as e:
            logger.exception("Failed creating S3 client")
            raise

    def save_batch(self) -> bool:
        if not self.new_emails:
            logger.info("[Database] Nothing to save")
            return True

        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        batch_key = f"{os.path.dirname(self.path)}/batch_{timestamp}.json.gz"
        try:
            json_data = json.dumps(self.new_emails, ensure_ascii=False)
            compressed = gzip.compress(json_data.encode('utf-8'))
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=batch_key,
                Body=compressed,
                ContentType='application/json',
                ContentEncoding='gzip'
            )
            logger.info(f"[Database] Saved batch to s3://{self.bucket_name}/{batch_key}")
            return True
        except Exception:
            logger.exception("[Database] Failed to save batch to S3")
            raise



# Safety caps (tune if you need tighter bounds per invocation)
MAX_MESSAGES_SOFT_HINT = 10000  # soft guard if your Inbox is enormous

def _norm_email_from(msg: dict) -> str | None:
    try:
        frm = msg.get("from") or {}
        addr = (frm.get("emailAddress") or {})
        return addr.get("address") or addr.get("name")
    except Exception:
        return None

def _norm_body_content(msg: dict) -> str:
    # Prefer full body content if present; fall back to preview
    try:
        body = msg.get("body") or {}
        content = body.get("content")
        if isinstance(content, str) and content.strip():
            return content
    except Exception:
        pass
    return msg.get("bodyPreview") or ""

def _collect_pdfs_for_message(client: GraphClient, msg_id: str) -> tuple[list[str], list[str]]:
    pdf_names: List[str] = []
    pdf_b64s: List[str] = []

    atts = client.list_attachments_for_message(msg_id)
    for att in atts:
        name  = (att.get("name") or "")
        ctype = (att.get("contentType") or "")
        if not (name.lower().endswith(".pdf") or ctype.lower() == "application/pdf"):
            continue

        att_id = att.get("id")
        if not att_id:
            continue

        # Get base64 (contentBytes if present, else $value)
        try:
            b64 = client.get_attachment_content_b64(msg_id, att_id)
            if b64:
                pdf_names.append(name)
                pdf_b64s.append(b64)
        except Exception as e:
            print(f"Message {msg_id} attachment {att_id} fetch failed: {e}")

    return pdf_names, pdf_b64s


def main(
    user_token: str,
    user_email_hash: str,
    since_days: int = 730,
    page_size: int = 50
) -> Tuple[Dict[str, List[Dict[str, Any]]], List[str]]:
    """
    Pull all Inbox emails with attachments since `since_days` days ago,
    group by conversationId, include base64 for PDFs.

    Returns:
      threads_dict (dict), threads_yet_to_process (empty list for compatibility)
    """
    # Calculate lower bound (UTC ISO 8601)
    since_dt = datetime.now(timezone.utc) - timedelta(days=since_days)
    since_iso = since_dt.isoformat().replace("+00:00", "Z")

    client = GraphClient(user_token)

    # 1) List messages in Inbox since date (only those with attachments)
    try:
        messages = client.list_inbox_messages_since_with_attachments(since_iso, page_size=page_size)
    except Exception as e:
        print(f"Failed to list messages since {since_iso}: {e}")
        traceback.print_exc(file=sys.stdout)
        return {}, []

    if len(messages) > MAX_MESSAGES_SOFT_HINT:
        print(f"WARNING: Large result set: {len(messages)} messages. Consider narrowing since_days or lowering page_size.")

    # 2) Group by conversationId, collect PDFs
    threads: Dict[str, List[Dict[str, Any]]] = {}

    for idx, msg in enumerate(messages, 1):
        if not msg.get("hasAttachments", False):
            continue  # double check

        pdf_names, pdf_b64s = [], []
        try:
            pdf_names, pdf_b64s = _collect_pdfs_for_message(client, msg.get("id"))
        except Exception as e:
            print(f"Message {msg.get('id')} attachments fetch failed: {e}")
            continue

        if not pdf_names:
            continue  # skip messages with no actual PDFs

        entry = {
            "body": _norm_body_content(msg),
            "date": msg.get("receivedDateTime"),
            "from_": _norm_email_from(msg),
            "subject": msg.get("subject"),
            "pdfs": pdf_names,
            "pdfencoded": pdf_b64s,   # <-- base64 INCLUDED
            "processed": 0
        }

        conv_id = msg.get("conversationId") or msg.get("id")
        bucket = threads.setdefault(conv_id, [])
        bucket.append(entry)

        if idx % 100 == 0:
            print(f"Processed {idx} messages…")

    # 3) Sort each thread oldest -> newest for stable downstream behavior
    for conv_id, items in threads.items():
        items.sort(key=lambda x: x.get("date") or "")

    # Save batch to S3
    db = Database(threads, f"{user_email_hash}/raw_emails_history_broker/all_raw_emails.json")
    db.save_batch()

    print(f"Collected {sum(len(v) for v in threads.values())} message(s) with PDFs across {len(threads)} thread(s).")
    return threads, []  # keep second value for lambda compatibility (no requeueing here)
