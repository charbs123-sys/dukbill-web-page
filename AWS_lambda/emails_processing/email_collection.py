"""
Refactored Gmail email collector
- Fixes PDF base64 decoding (robust base64url padding + normalization)
- Safer attachment decoding: validates PDF header and EOF marker, returns normalized standard padded base64
- Cleaner error handling and logging
- Keeps sequential and parallel PDF download options
- Saves batches to S3 as compressed JSON

Usage:
  - Put this file into your project and call `main(thread_ids, access_token, user_key, save_pdfs=True)`
  - Requires environment variable AWS_S3_BUCKET_NAME set and valid AWS credentials (or local config)
  - Requires google-api-python-client and google-auth packages for Gmail API access

Note: this file is a direct rewrite of the program you supplied with the recommended base64 fixes.
"""

import sys
import os
import re
import json
import gzip
import time
import base64
import logging
import random
import socket
import ssl
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# External dependencies (must be installed in your environment)
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import botocore.session
import botocore.config
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


# -------------------------- Helper: robust base64 decode --------------------------

def decode_gmail_base64_to_bytes(s: str) -> bytes:
    """
    Decode Gmail's base64url data which commonly comes WITHOUT padding.
    Returns raw bytes.

    This function:
    - Strips whitespace/newlines
    - Computes proper padding using `(-len(s)) % 4`
    - Rejects obvious-corrupt inputs (mod 4 == 1)
    - Uses urlsafe_b64decode (which accepts '-' and '_')
    """
    if not s:
        return b""

    # Remove whitespace/newlines introduced by transport
    s_clean = re.sub(r"\s+", "", s)

    pad = (-len(s_clean)) % 4
    if pad == 1:
        # This length mod 4 == 1 is impossible for valid base64
        raise ValueError("Invalid base64 length (mod 4 == 1); input likely corrupted")

    if pad:
        s_clean += "=" * pad

    try:
        return base64.urlsafe_b64decode(s_clean)
    except Exception as e:
        # As a fallback, try converting urlsafe to standard and decode
        try:
            standard = s_clean.replace('-', '+').replace('_', '/')
            return base64.b64decode(standard)
        except Exception:
            raise


def normalize_pdf_base64_from_bytes(pdf_bytes: bytes) -> str:
    """
    Return a canonical STANDARD (not urlsafe) base64 string with padding for the given bytes.
    This makes downstream handling consistent.
    """
    return base64.b64encode(pdf_bytes).decode('ascii')


# Add this helper function at the top level (outside the class)
def get_approximate_size(obj, seen=None):
    """Recursively calculate approximate size of object in bytes"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_approximate_size(v, seen) for v in obj.values()])
        size += sum([get_approximate_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_approximate_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_approximate_size(i, seen) for i in obj])
    return size


# -------------------------- Data classes --------------------------

@dataclass
class Attachment:
    filename: str
    mime_type: str
    size: int
    attachment_id: str
    data: Optional[str] = None  # normalized standard base64 string when downloaded
    download_failed: bool = False
    error_message: Optional[str] = None


@dataclass
class EmailMessage:
    message_id: str
    thread_id: str
    from_email: str
    to_email: str
    subject: str
    body_plain: str
    body_html: str
    date: str
    attachments: List[Attachment]
    pdf_attachments: List[Dict[str, Optional[str]]]  # metadata about downloaded pdfs
    fetch_failed: bool = False
    error_message: Optional[str] = None


@dataclass
class CollectionResult:
    emails: List[EmailMessage]
    failed_threads: List[Dict[str, str]]
    failed_messages: List[Dict[str, str]]
    failed_attachments: List[Dict[str, str]]
    statistics: Dict[str, int]


# -------------------------- Utilities --------------------------

def parse_html_to_text(html_content: str) -> str:
    if not html_content:
        return ""

    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        for tag in soup(['script', 'style']):
            tag.decompose()
        text = soup.get_text()
        # Collapse and clean lines
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split('  '))
        text = '\n'.join(chunk for chunk in chunks if chunk)
        text = re.sub(r'\n\s*\n+', '\n\n', text)
        return text.strip()
    except Exception as e:
        logger.warning(f"HTML parse failed: {e}")
        return html_content


# -------------------------- Database (S3) --------------------------

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


# -------------------------- Gmail collector --------------------------

class GmailEmailCollector:
    def __init__(self, access_token: str, batch_size: int = 25, max_retries: int = 3):
        self.credentials = Credentials(token=access_token)

        # Prevent automatic refresh attempts (we only have an access token)
        self.credentials.refresh = lambda request: None
        self.credentials.expiry = datetime.utcnow() + timedelta(hours=1)

        # Build Gmail service
        self.service = build('gmail', 'v1', credentials=self.credentials)

        self.batch_size = min(batch_size, 100)
        self.max_retries = max_retries
        self.failed_threads: List[Dict[str, str]] = []
        self.failed_messages: List[Dict[str, str]] = []
        self.failed_attachments: List[Dict[str, str]] = []

    # -------------------------- high-level collection --------------------------
    def collect_emails_from_threads(self, start_time: datetime, thread_ids: List[str],
                                    download_attachments: bool = True,
                                    max_workers: int = 3) -> Tuple[CollectionResult, List[str]]:
        logger.info(f"Starting collection of {len(thread_ids)} threads")

        all_emails: List[EmailMessage] = []
        processed_threads = 0
        threads_yet_to_process: List[str] = []
        
        # Memory tracking
        #MEMORY_LIMIT_BYTES = 350 * 1024 * 1024  # 250MB in bytes

        for i in range(0, len(thread_ids), self.batch_size):
            batch_start = datetime.now()

            # Time budget guard
            if datetime.now() - start_time >= timedelta(minutes=13):
                threads_yet_to_process = thread_ids[i:]
                print("Time budget exceeded; deferring remaining threads")
                break

            # Memory budget guard - check accumulated data size
            current_memory = get_approximate_size(all_emails)
            memory_mb = current_memory / 1024 / 1024
            print(f"Current accumulated data size: {memory_mb:.2f} MB")
            
            #if current_memory >= MEMORY_LIMIT_BYTES:
            #    threads_yet_to_process = thread_ids[i:]
            #    print(f"Memory limit reached ({memory_mb:.2f} MB >= 500 MB); deferring remaining threads")
            #    break

            chunk = thread_ids[i:i + self.batch_size]
            batch_num = i // self.batch_size + 1
            logger.info(f"Processing chunk {batch_num} with {len(chunk)} threads")

            if i > 0:
                delay = min(2 ** (batch_num / 10), 10)
                logger.info(f"Sleeping {delay:.1f}s between chunks")
                time.sleep(delay)

            threads_data = self._batch_get_threads_with_retry(chunk)

            # collect message ids
            message_ids: List[Tuple[str, str]] = []  # (msg_id, thread_id)
            for t in threads_data:
                if t and 'messages' in t:
                    for m in t['messages']:
                        message_ids.append((m['id'], t['id']))
                    processed_threads += 1

            messages = self._batch_get_messages_with_retry(message_ids)

            # Filter known noisy senders
            pre_count = len(messages)
            messages = [m for m in messages if not (m.subject == "Your Dukbill Summary" or 'noreply@dukbillapp.com' in m.from_email.lower())]
            filtered = pre_count - len(messages)
            if filtered:
                logger.info(f"Filtered {filtered} dukbill messages in chunk {batch_num}")

            # Download PDFs
            pdf_download_time = 0
            if download_attachments:
                pdf_start = datetime.now()
                messages = self._process_attachments_sequential(messages, max_workers)
                pdf_download_time = (datetime.now() - pdf_start).total_seconds()

            all_emails.extend(messages)

            total_time = (datetime.now() - batch_start).total_seconds()
            print(f"Chunk {batch_num} done: fetched_msgs={len(messages)}, total_time={total_time:.2f}s, pdf_time={pdf_download_time:.2f}s")

        # Build result
        result = CollectionResult(
            emails=all_emails,
            failed_threads=self.failed_threads,
            failed_messages=self.failed_messages,
            failed_attachments=self.failed_attachments,
            statistics={
                'total_threads_requested': len(thread_ids),
                'threads_processed': processed_threads,
                'threads_failed': len(self.failed_threads),
                'emails_collected': len(all_emails),
                'messages_failed': len(self.failed_messages),
                'attachments_failed': len(self.failed_attachments)
            }
        )

        print('Collection finished')
        return result, threads_yet_to_process

    # -------------------------- thread fetch with retries --------------------------
    def _batch_get_threads_with_retry(self, thread_ids: List[str]) -> List[Dict]:
        threads: List[Optional[Dict]] = []
        failed_ids = set(thread_ids)

        for attempt in range(self.max_retries):
            if attempt > 0:
                delay = 2 ** attempt + random.uniform(0, 1)
                logger.info(f"Thread retry {attempt}, sleeping {delay:.1f}s")
                time.sleep(delay)

            threads_to_fetch = list(failed_ids)
            if not threads_to_fetch:
                break

            temp_threads: List[Optional[Dict]] = []
            temp_failed = set()

            def callback(request_id, response, exception):
                if exception:
                    # treat all failures the same, but mark rate limits specially in logs
                    if '429' in str(exception):
                        logger.warning(f"Rate limit while fetching thread {request_id}")
                    else:
                        logger.error(f"Error fetching thread {request_id}: {exception}")
                    temp_failed.add(request_id)
                    temp_threads.append(None)
                else:
                    temp_threads.append(response)

            batch_request = self.service.new_batch_http_request(callback=callback)
            for tid in threads_to_fetch:
                batch_request.add(self.service.users().threads().get(userId='me', id=tid, format='metadata', metadataHeaders=['From', 'To', 'Subject', 'Date']), request_id=tid)

            try:
                batch_request.execute()
            except Exception as e:
                logger.warning(f"Batch threads execute error: {e}")

            # process results
            # successful items are appended in temp_threads
            # next loop will try the ones in temp_failed
            failed_ids = temp_failed
            threads.extend([t for t in temp_threads if t is not None])

        # record permanent failures
        for fid in failed_ids:
            self.failed_threads.append({'thread_id': fid, 'error': 'Failed after retry'})

        return threads

    # -------------------------- messages fetch with retries --------------------------
    def _batch_get_messages_with_retry(self, message_ids: List[Tuple[str, str]]) -> List[EmailMessage]:
        messages: List[EmailMessage] = []
        failed_ids = set(message_ids)
        successful_ids = set()

        for attempt in range(self.max_retries):
            if attempt > 0:
                delay = 2 ** attempt + random.uniform(0, 1)
                logger.info(f"Message retry {attempt}, sleeping {delay:.1f}s")
                time.sleep(delay)

            to_fetch = list(failed_ids - successful_ids)
            if not to_fetch:
                break

            temp_messages: List[EmailMessage] = []
            temp_failed = set()

            def callback(request_id, response, exception):
                # request_id format: {msg_id}_{thread_id}
                try:
                    msg_id, thread_id = request_id.split('_', 1)
                except Exception:
                    logger.exception("Malformed request_id in callback")
                    return

                if exception:
                    if '429' in str(exception):
                        logger.warning(f"Rate limit for message {msg_id}")
                    else:
                        logger.error(f"Error fetching message {msg_id}: {exception}")
                    temp_failed.add((msg_id, thread_id))
                else:
                    try:
                        email_msg = self._parse_message(response)
                        temp_messages.append(email_msg)
                        successful_ids.add((msg_id, thread_id))
                    except Exception as e:
                        logger.exception(f"Failed parsing message {msg_id}: {e}")
                        temp_failed.add((msg_id, thread_id))

            # smaller chunks
            effective_batch = max(10, self.batch_size // 2)
            for i in range(0, len(to_fetch), effective_batch):
                chunk = to_fetch[i:i + effective_batch]
                batch_request = self.service.new_batch_http_request(callback=callback)
                for msg_id, thread_id in chunk:
                    batch_request.add(self.service.users().messages().get(userId='me', id=msg_id, format='full'), request_id=f"{msg_id}_{thread_id}")
                try:
                    batch_request.execute()
                    time.sleep(0.1)
                except Exception as e:
                    logger.warning(f"Message batch execute error: {e}")

            messages.extend(temp_messages)
            failed_ids = temp_failed

        # record permanent failures
        for msg_id, thread_id in failed_ids:
            if (msg_id, thread_id) not in successful_ids:
                self.failed_messages.append({'message_id': msg_id, 'thread_id': thread_id, 'error': 'Failed after all retries'})

        return messages

    # -------------------------- attachment download helpers --------------------------
    def _download_attachment_sequential(self, message_id: str, attachment: Attachment) -> Optional[str]:
        """
        Download PDF attachment and return canonical standard padded base64 string on success.
        """
        if attachment.mime_type != 'application/pdf':
            logger.debug(f"Skipping non-PDF: {attachment.filename}")
            return None

        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                if attempt > 1:
                    wait = min(5 * attempt, 15)
                    logger.info(f"Waiting {wait}s before retry {attempt} for {attachment.filename}")
                    time.sleep(wait)

                logger.info(f"Downloading {attachment.filename} (attempt {attempt}/{max_attempts})")
                att_data = self.service.users().messages().attachments().get(userId='me', messageId=message_id, id=attachment.attachment_id).execute()

                if 'data' not in att_data:
                    logger.error(f"No 'data' in attachment response for {attachment.filename}")
                    continue

                raw_b64 = att_data['data']

                # decode robustly
                pdf_bytes = decode_gmail_base64_to_bytes(raw_b64)

                # Quick validation
                if not pdf_bytes.startswith(b'%PDF'):
                    logger.warning(f"Attachment {attachment.filename} does not start with %PDF (first bytes: {pdf_bytes[:8]})")
                    # not returning None immediately; we'll still provide data for inspection

                # Normalize to standard base64 padded string
                standard_b64 = normalize_pdf_base64_from_bytes(pdf_bytes)

                logger.info(f"Downloaded and decoded PDF {attachment.filename} ({len(pdf_bytes)} bytes)")
                return standard_b64

            except (ssl.SSLError, socket.timeout) as e:
                logger.warning(f"Network error downloading {attachment.filename}: {e}")
                if attempt < max_attempts:
                    time.sleep(10)
            except ValueError as e:
                # decode_gmail_base64_to_bytes may raise ValueError for corrupted inputs
                logger.error(f"Base64 decode error for {attachment.filename}: {e}")
                break
            except Exception as e:
                logger.exception(f"Unexpected error downloading {attachment.filename}: {e}")
                if attempt < max_attempts:
                    time.sleep(5)

        logger.error(f"Failed to download {attachment.filename} after {max_attempts} attempts")
        return None

    def _process_attachments_sequential(self, messages: List[EmailMessage], max_workers: int = None) -> List[EmailMessage]:
        total_attachments = sum(len(m.attachments) for m in messages)
        if total_attachments == 0:
            logger.info("No attachments to download")
            return messages

        logger.info(f"Downloading {total_attachments} PDF attachments sequentially")
        attachment_data = {}
        failed_downloads = []
        count = 0

        for msg in messages:
            for att in msg.attachments:
                if att.mime_type != 'application/pdf':
                    continue
                count += 1
                logger.info(f"({count}/{total_attachments}) Downloading {att.filename}")
                try:
                    data = self._download_attachment_sequential(msg.message_id, att)
                    if data:
                        attachment_data[(msg.message_id, att.attachment_id)] = data
                    else:
                        att.download_failed = True
                        att.error_message = 'Download failed after all attempts'
                        failed_downloads.append({'message_id': msg.message_id, 'filename': att.filename, 'error': att.error_message})
                    time.sleep(1)
                except Exception as e:
                    logger.exception(f"Attachment processing error for {att.filename}: {e}")
                    att.download_failed = True
                    att.error_message = str(e)
                    failed_downloads.append({'message_id': msg.message_id, 'filename': att.filename, 'error': str(e)})
                    time.sleep(0.1)

        # Attach results back to messages
        for msg in messages:
            pdf_list = []
            for att in msg.attachments:
                if att.mime_type != 'application/pdf':
                    continue
                key = (msg.message_id, att.attachment_id)
                if key in attachment_data:
                    att.data = attachment_data[key]
                    pdf_list.append({'filename': att.filename, 'base64_data': att.data, 'download_successful': True})
                else:
                    pdf_list.append({'filename': att.filename, 'base64_data': None, 'download_successful': False, 'error': att.error_message or 'Download failed'})
            msg.pdf_attachments = pdf_list

        if failed_downloads:
            self.failed_attachments.extend(failed_downloads)
            logger.warning(f"Failed to download {len(failed_downloads)} attachments")

        logger.info("Attachment processing complete")
        return messages

    # -------------------------- message parsing --------------------------
    def _parse_message(self, msg_data: Dict) -> EmailMessage:
        headers = {h['name']: h['value'] for h in msg_data['payload'].get('headers', [])}
        body_plain, body_html = self._extract_body(msg_data['payload'])
        attachments = self._extract_attachments(msg_data['payload'], msg_data['id'])

        # Prefer plain text if present; otherwise use HTML parsed form
        final_body = body_plain or ''

        return EmailMessage(
            message_id=msg_data['id'],
            thread_id=msg_data.get('threadId', ''),
            from_email=headers.get('From', ''),
            to_email=headers.get('To', ''),
            subject=headers.get('Subject', ''),
            body_plain=final_body,
            body_html=body_html,
            date=headers.get('Date', ''),
            attachments=attachments,
            pdf_attachments=[],
            fetch_failed=False,
            error_message=None
        )

    def _extract_body(self, payload: Dict) -> Tuple[str, str]:
        body_plain = ''
        body_html = ''
        body_text_from_html = ''

        def recurse(parts):
            nonlocal body_plain, body_html, body_text_from_html
            for part in parts:
                mime_type = part.get('mimeType', '')
                if mime_type == 'text/plain' and not body_plain:
                    data = part.get('body', {}).get('data', '')
                    if data:
                        try:
                            body_plain = base64.urlsafe_b64decode(re.sub(r"\s+", "", data)).decode('utf-8', errors='ignore')
                        except Exception:
                            body_plain = ''
                elif mime_type == 'text/html' and not body_html:
                    data = part.get('body', {}).get('data', '')
                    if data:
                        try:
                            body_html = base64.urlsafe_b64decode(re.sub(r"\s+", "", data)).decode('utf-8', errors='ignore')
                            body_text_from_html = parse_html_to_text(body_html)
                        except Exception:
                            body_html = ''
                elif 'parts' in part:
                    recurse(part['parts'])

        if 'parts' in payload:
            recurse(payload['parts'])
        elif 'body' in payload and payload['body'].get('data'):
            data = payload['body']['data']
            try:
                decoded = base64.urlsafe_b64decode(re.sub(r"\s+", "", data)).decode('utf-8', errors='ignore')
                if '<html' in decoded.lower() or '<body' in decoded.lower():
                    body_html = decoded
                    body_text_from_html = parse_html_to_text(body_html)
                else:
                    body_plain = decoded
            except Exception:
                pass

        if not body_plain and body_text_from_html:
            body_plain = body_text_from_html

        return body_plain, body_html

    def _extract_attachments(self, payload: Dict, message_id: str) -> List[Attachment]:
        attachments: List[Attachment] = []

        def recurse(parts):
            for part in parts:
                filename = part.get('filename', '')
                body = part.get('body', {})
                att_id = body.get('attachmentId')
                mime_type = part.get('mimeType', '')

                if filename and att_id:
                    # Only capture PDFs; skip others
                    if mime_type == 'application/pdf':
                        attachments.append(Attachment(filename=filename, mime_type=mime_type, size=body.get('size', 0), attachment_id=att_id))
                    else:
                        logger.debug(f"Skipping non-PDF attachment {filename} (type: {mime_type})")

                if 'parts' in part:
                    recurse(part['parts'])

        if 'parts' in payload:
            recurse(payload['parts'])

        if attachments:
            logger.info(f"Found {len(attachments)} PDF attachments in message {message_id}")
        return attachments

    # -------------------------- export --------------------------
    def export_results_with_errors(self, result: CollectionResult, output_file: str):
        output = {
            'statistics': result.statistics,
            'successful_emails': [
                {**asdict(email), 'attachments': [asdict(att) for att in email.attachments]}
                for email in result.emails
            ],
            'failed_threads': result.failed_threads,
            'failed_messages': result.failed_messages,
            'failed_attachments': result.failed_attachments
        }
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        logger.info(f"Exported results to {output_file}")

    # Parallel version (kept for completeness) -- uses the same `_download_attachment_sequential` worker
    def _process_attachments_parallel(self, messages: List[EmailMessage], max_workers: int = 5) -> List[EmailMessage]:
        pdf_tasks = []
        for msg in messages:
            for att in msg.attachments:
                if att.mime_type == 'application/pdf':
                    pdf_tasks.append((msg.message_id, att))

        if not pdf_tasks:
            return messages

        attachment_data = {}
        failed = []
        logger.info(f"Downloading {len(pdf_tasks)} PDFs in parallel with {max_workers} workers")

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(self._download_attachment_sequential, msg_id, att): (msg_id, att) for msg_id, att in pdf_tasks}
            for future in as_completed(futures):
                msg_id, att = futures[future]
                try:
                    res = future.result()
                    if res:
                        attachment_data[(msg_id, att.attachment_id)] = res
                    else:
                        att.download_failed = True
                        failed.append({'message_id': msg_id, 'filename': att.filename, 'error': 'Download failed'})
                except Exception as e:
                    logger.exception(f"Parallel download failed for {att.filename}: {e}")
                    att.download_failed = True
                    failed.append({'message_id': msg_id, 'filename': att.filename, 'error': str(e)})

        # attach results back to messages
        for msg in messages:
            pdfs = []
            for att in msg.attachments:
                if att.mime_type != 'application/pdf':
                    continue
                key = (msg.message_id, att.attachment_id)
                if key in attachment_data:
                    att.data = attachment_data[key]
                    pdfs.append({'filename': att.filename, 'base64_data': att.data, 'download_successful': True})
                else:
                    pdfs.append({'filename': att.filename, 'base64_data': None, 'download_successful': False})
            msg.pdf_attachments = pdfs

        if failed:
            self.failed_attachments.extend(failed)
            logger.warning(f"Parallel downloads had {len(failed)} failures")

        return messages


# -------------------------- helper utilities outside class --------------------------

def save_pdfs_to_disk(threads_dict: Dict[str, List[Dict]], output_dir: str = "pdfs") -> List[Dict]:
    os.makedirs(output_dir, exist_ok=True)
    saved_files = []
    failed_pdfs = []

    for thread_id, emails in threads_dict.items():
        for email_index, email in enumerate(emails):
            pdf_names = email.get('pdfs', []) or []
            pdf_b64s = email.get('pdfencoded', []) or []

            for pdf_name, pdf_data in zip(pdf_names, pdf_b64s):
                if not pdf_data:
                    failed_pdfs.append({'filename': pdf_name, 'thread_id': thread_id, 'error': 'No data'})
                    continue
                try:
                    pdf_bytes = decode_gmail_base64_to_bytes(pdf_data)
                except Exception as e:
                    # maybe pdf_data is already standard padded base64; try normal b64decode
                    try:
                        pdf_bytes = base64.b64decode(re.sub(r"\s+", "", pdf_data))
                    except Exception as e2:
                        failed_pdfs.append({'filename': pdf_name, 'thread_id': thread_id, 'error': f'Base64 decode failed: {e} / {e2}'})
                        continue

                valid_pdf = pdf_bytes.startswith(b'%PDF')
                has_eof = b'%%EOF' in pdf_bytes

                # generate a safe name and save
                safe_name = pdf_name.replace('/', '_').replace('\\', '_')
                unique = f"{thread_id[:8]}_{email_index}_{safe_name}"
                filepath = os.path.join(output_dir, unique)
                try:
                    with open(filepath, 'wb') as f:
                        f.write(pdf_bytes)
                    saved_files.append({'filepath': filepath, 'original_name': pdf_name, 'thread_id': thread_id, 'size': len(pdf_bytes), 'valid_pdf': valid_pdf, 'has_eof': has_eof})
                except Exception as e:
                    failed_pdfs.append({'filename': pdf_name, 'thread_id': thread_id, 'error': str(e)})

    # index
    index_file = os.path.join(output_dir, 'pdf_index.json')
    with open(index_file, 'w', encoding='utf-8') as f:
        json.dump({'saved': saved_files, 'failed': failed_pdfs, 'summary': {'total_saved': len(saved_files), 'total_failed': len(failed_pdfs)}}, f, indent=2)

    logger.info(f"Saved {len(saved_files)} PDFs to {output_dir}/ (failed {len(failed_pdfs)})")
    return saved_files


def test_pdf_decode(b64_string: str) -> List[Tuple[str, bytes]]:
    tests = []
    logger.info(f"Test decode input length: {len(b64_string)}")

    # Method: robust helper
    try:
        b = decode_gmail_base64_to_bytes(b64_string)
        logger.info(f"Method helper: success, size={len(b)}")
        tests.append(('helper', b))
    except Exception as e:
        logger.error(f"helper failed: {e}")

    # Method: try standard with padding
    try:
        padded = re.sub(r"\s+", "", b64_string) + '=' * ((-len(re.sub(r"\s+", "", b64_string))) % 4)
        b = base64.b64decode(padded)
        logger.info(f"Method standard padded: success size={len(b)}")
        tests.append(('standard_padded', b))
    except Exception as e:
        logger.error(f"standard_padded failed: {e}")

    # Save small test files for inspection
    for name, data in tests:
        fname = f"test_pdf_{name}.pdf"
        try:
            with open(fname, 'wb') as f:
                f.write(data)
            logger.info(f"Wrote {fname}")
        except Exception as e:
            logger.error(f"Failed to write {fname}: {e}")

    return tests


# -------------------------- main convenience helper --------------------------

def main(thread_ids: List[str], access_token: str, user_key: str, save_pdfs: bool = False):
    start_time = datetime.now()

    collector = GmailEmailCollector(access_token=access_token, batch_size=25, max_retries=3)

    result, threads_yet = collector.collect_emails_from_threads(start_time, thread_ids=thread_ids, download_attachments=True, max_workers=1)

    failed_thread_ids = [ft['thread_id'] for ft in result.failed_threads]
    to_retry = list(set(threads_yet + failed_thread_ids))

    print(f"threads not processed in time: {len(threads_yet)}")
    print(f"threads failed: {len(failed_thread_ids)}")

    # group emails by thread
    threads_dict = defaultdict(list)
    for email in result.emails:
        email_data = {
            'body': email.body_plain or email.body_html,
            'date': email.date,
            'from_': email.from_email,
            'subject': email.subject,
            'pdfs': [],
            'pdfencoded': [],
            'processed': 0
        }
        for att in email.attachments:
            if att.mime_type == 'application/pdf':
                email_data['pdfs'].append(att.filename)
        for pdf_att in email.pdf_attachments:
            if pdf_att.get('download_successful') and pdf_att.get('base64_data'):
                email_data['pdfencoded'].append(pdf_att['base64_data'])
            else:
                logger.warning(f"PDF {pdf_att.get('filename')} failed: {pdf_att.get('error')}")
        threads_dict[email.thread_id].append(email_data)

    # Save batch to S3
    db = Database(threads_dict, f"{user_key}/raw_emails_history_broker/all_raw_emails.json")
    db.save_batch()

    logger.info("Collection Summary:\n  total_threads_requested=%d\n  unique_threads_collected=%d\n  total_emails_collected=%d\n  failed_threads=%d\n  failed_messages=%d\n  failed_attachments=%d\n  threads_to_retry=%d", len(thread_ids), len(threads_dict), len(result.emails), len(result.failed_threads), len(result.failed_messages), len(result.failed_attachments), len(to_retry))

    saved_files = []
    if save_pdfs:
        saved_files = save_pdfs_to_disk(dict(threads_dict))
        logger.info(f"Saved PDFs: {len(saved_files)}")

    return dict(threads_dict), to_retry


if __name__ == '__main__':
    # Example usage entrypoint for quick testing (replace values)
    EXAMPLE_THREAD_IDS = []  # fill with thread ids
    ACCESS_TOKEN = os.getenv('GMAIL_ACCESS_TOKEN', '')
    USER_KEY = os.getenv('USER_KEY', 'me')

    if not ACCESS_TOKEN:
        logger.warning('No ACCESS_TOKEN provided; run tests for decode only')
    else:
        main(EXAMPLE_THREAD_IDS, ACCESS_TOKEN, USER_KEY, save_pdfs=False)
