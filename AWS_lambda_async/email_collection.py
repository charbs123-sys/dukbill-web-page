"""
Async Gmail email collector with memory-based S3 batching using aiogoogle
- Asynchronous API calls for faster processing
- Automatically saves batches to S3 when memory threshold is reached
- Continues processing remaining threads after each batch save
- Each batch gets a unique timestamp-based name
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
import asyncio
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict

# External dependencies
from aiogoogle import Aiogoogle
from aiogoogle.auth.creds import UserCreds
import botocore.session
import botocore.config
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


# -------------------------- Helper: robust base64 decode --------------------------

def decode_gmail_base64_to_bytes(s: str) -> bytes:
    """Decode Gmail's base64url data which commonly comes WITHOUT padding."""
    if not s:
        return b""

    s_clean = re.sub(r"\s+", "", s)
    pad = (-len(s_clean)) % 4
    if pad == 1:
        raise ValueError("Invalid base64 length (mod 4 == 1); input likely corrupted")

    if pad:
        s_clean += "=" * pad

    try:
        return base64.urlsafe_b64decode(s_clean)
    except Exception as e:
        try:
            standard = s_clean.replace('-', '+').replace('_', '/')
            return base64.b64decode(standard)
        except Exception:
            raise


def normalize_pdf_base64_from_bytes(pdf_bytes: bytes) -> str:
    """Return a canonical STANDARD base64 string with padding."""
    return base64.b64encode(pdf_bytes).decode('ascii')


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
    data: Optional[str] = None
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
    pdf_attachments: List[Dict[str, Optional[str]]]
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
    """S3-backed batch saver with unique naming."""

    def __init__(self, user_key: str, base_path: str):
        self.user_key = user_key
        self.base_path = base_path
        self.batch_counter = 0

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

    def save_batch(self, threads_dict: Dict) -> bool:
        """Save a batch of threads to S3 with unique naming."""
        if not threads_dict:
            logger.info("[Database] Nothing to save")
            return True

        self.batch_counter += 1
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')
        batch_key = f"{self.base_path}/batch_{timestamp}_part{self.batch_counter}.json.gz"
        
        try:
            json_data = json.dumps(threads_dict, ensure_ascii=False)
            compressed = gzip.compress(json_data.encode('utf-8'))
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=batch_key,
                Body=compressed,
                ContentType='application/json',
                ContentEncoding='gzip'
            )
            num_threads = len(threads_dict)
            num_emails = sum(len(emails) for emails in threads_dict.values())
            logger.info(f"[Database] Saved batch #{self.batch_counter} to s3://{self.bucket_name}/{batch_key} ({num_threads} threads, {num_emails} emails, {len(compressed)} bytes compressed)")
            return True
        except Exception:
            logger.exception("[Database] Failed to save batch to S3")
            raise


# -------------------------- Async Gmail collector --------------------------

class GmailEmailCollector:
    def __init__(self, access_token: str, batch_size: int = 25, max_retries: int = 3):
        self.access_token = access_token
        self.batch_size = min(batch_size, 100)
        self.max_retries = max_retries
        self.failed_threads: List[Dict[str, str]] = []
        self.failed_messages: List[Dict[str, str]] = []
        self.failed_attachments: List[Dict[str, str]] = []
        
        # Create user credentials for aiogoogle
        self.user_creds = UserCreds(
            access_token=access_token,
            expires_at=(datetime.utcnow() + timedelta(hours=1)).isoformat()
        )

    async def collect_emails_from_threads(
        self, 
        start_time: datetime, 
        thread_ids: List[str],
        download_attachments: bool = True,
        max_workers: int = 5,
        memory_limit_mb: int = 50,
        db: Optional[Database] = None
    ) -> Tuple[CollectionResult, List[str]]:
        """
        Collect emails with automatic memory-based batching to S3.
        
        Args:
            start_time: When collection started (for time budget)
            thread_ids: List of Gmail thread IDs to process
            download_attachments: Whether to download PDF attachments
            max_workers: Number of parallel workers for downloads
            memory_limit_mb: Memory threshold in MB to trigger batch save
            db: Database instance for saving batches (required for batching)
        """
        logger.info(f"Starting async collection of {len(thread_ids)} threads with {memory_limit_mb}MB memory limit")

        all_emails: List[EmailMessage] = []
        threads_dict = defaultdict(list)
        processed_threads = 0
        threads_yet_to_process: List[str] = []
        memory_limit_bytes = memory_limit_mb * 1024 * 1024
        total_batches_saved = 0

        async with Aiogoogle(user_creds=self.user_creds) as aiogoogle:
            gmail = await aiogoogle.discover('gmail', 'v1')

            for i in range(0, len(thread_ids), self.batch_size):
                batch_start = datetime.now()

                # Time budget guard
                if datetime.now() - start_time >= timedelta(minutes=13):
                    threads_yet_to_process = thread_ids[i:]
                    logger.warning(f"Time budget exceeded; deferring {len(threads_yet_to_process)} remaining threads")
                    break

                chunk = thread_ids[i:i + self.batch_size]
                batch_num = i // self.batch_size + 1
                logger.info(f"Processing chunk {batch_num} with {len(chunk)} threads")

                if i > 0:
                    delay = min(2 ** (batch_num / 10), 10)
                    logger.info(f"Sleeping {delay:.1f}s between chunks")
                    await asyncio.sleep(delay)

                # Fetch threads asynchronously
                threads_data = await self._batch_get_threads_with_retry(aiogoogle, gmail, chunk)

                # Collect message ids
                message_ids: List[Tuple[str, str]] = []
                for t in threads_data:
                    if t and 'messages' in t:
                        for m in t['messages']:
                            message_ids.append((m['id'], t['id']))
                        processed_threads += 1

                # Fetch messages asynchronously
                messages = await self._batch_get_messages_with_retry(aiogoogle, gmail, message_ids)

                # Filter known noisy senders
                pre_count = len(messages)
                messages = [m for m in messages if not (m.subject == "Your Dukbill Summary" or 'noreply@dukbillapp.com' in m.from_email.lower())]
                filtered = pre_count - len(messages)
                if filtered:
                    logger.info(f"Filtered {filtered} dukbill messages in chunk {batch_num}")

                # Download PDFs asynchronously
                pdf_download_time = 0
                if download_attachments:
                    pdf_start = datetime.now()
                    messages = await self._process_attachments_async(aiogoogle, gmail, messages, max_workers)
                    pdf_download_time = (datetime.now() - pdf_start).total_seconds()

                all_emails.extend(messages)

                # Add emails to threads_dict
                for email in messages:
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

                # Memory budget check - save batch if threshold reached
                current_memory = get_approximate_size(threads_dict)
                memory_mb = current_memory / 1024 / 1024
                logger.info(f"Current accumulated data size: {memory_mb:.2f} MB")
                
                if current_memory >= memory_limit_bytes and db is not None:
                    logger.info(f"Memory limit reached ({memory_mb:.2f} MB >= {memory_limit_mb} MB); saving batch to S3")
                    db.save_batch(dict(threads_dict))
                    total_batches_saved += 1
                    
                    # Reset the payload to continue processing
                    threads_dict = defaultdict(list)
                    logger.info("Payload reset, continuing with remaining threads")

                total_time = (datetime.now() - batch_start).total_seconds()
                logger.info(f"Chunk {batch_num} done: fetched_msgs={len(messages)}, total_time={total_time:.2f}s, pdf_time={pdf_download_time:.2f}s")

        # Save any remaining data in threads_dict
        if threads_dict and db is not None:
            logger.info(f"Saving final batch with {len(threads_dict)} threads")
            db.save_batch(dict(threads_dict))
            total_batches_saved += 1

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
                'attachments_failed': len(self.failed_attachments),
                'batches_saved_to_s3': total_batches_saved
            }
        )

        logger.info('Collection finished')
        return result, threads_yet_to_process

    async def _batch_get_threads_with_retry(self, aiogoogle: Aiogoogle, gmail, thread_ids: List[str]) -> List[Dict]:
        """Async batch thread fetching with retry logic"""
        threads: List[Dict] = []
        failed_ids = set(thread_ids)

        for attempt in range(self.max_retries):
            if attempt > 0:
                delay = 2 ** attempt + random.uniform(0, 1)
                logger.info(f"Thread retry {attempt}, sleeping {delay:.1f}s")
                await asyncio.sleep(delay)

            threads_to_fetch = list(failed_ids)
            if not threads_to_fetch:
                break

            # Create tasks for all threads
            tasks = []
            for tid in threads_to_fetch:
                task = gmail.users.threads.get(
                    userId='me',
                    id=tid,
                    format='metadata',
                    metadataHeaders=['From', 'To', 'Subject', 'Date']
                )
                tasks.append((tid, aiogoogle.as_user(task)))

            # Execute all tasks concurrently
            results = await asyncio.gather(*[t[1] for t in tasks], return_exceptions=True)
            
            failed_ids = set()
            for (tid, _), result in zip(tasks, results):
                if isinstance(result, Exception):
                    if '429' in str(result):
                        logger.warning(f"Rate limit while fetching thread {tid}")
                    else:
                        logger.error(f"Error fetching thread {tid}: {result}")
                    failed_ids.add(tid)
                else:
                    threads.append(result)

        # Record permanently failed threads
        for fid in failed_ids:
            self.failed_threads.append({'thread_id': fid, 'error': 'Failed after retry'})

        return threads

    async def _batch_get_messages_with_retry(self, aiogoogle: Aiogoogle, gmail, message_ids: List[Tuple[str, str]]) -> List[EmailMessage]:
        """Async batch message fetching with retry logic"""
        messages: List[EmailMessage] = []
        failed_ids = set(message_ids)
        successful_ids = set()

        for attempt in range(self.max_retries):
            if attempt > 0:
                delay = 2 ** attempt + random.uniform(0, 1)
                logger.info(f"Message retry {attempt}, sleeping {delay:.1f}s")
                await asyncio.sleep(delay)

            to_fetch = list(failed_ids - successful_ids)
            if not to_fetch:
                break

            # Process in smaller batches to avoid overwhelming the API
            effective_batch = max(10, self.batch_size // 2)
            for i in range(0, len(to_fetch), effective_batch):
                chunk = to_fetch[i:i + effective_batch]
                
                # Create tasks for this chunk
                tasks = []
                for msg_id, thread_id in chunk:
                    task = gmail.users.messages.get(userId='me', id=msg_id, format='full')
                    tasks.append((msg_id, thread_id, aiogoogle.as_user(task)))

                # Execute concurrently
                results = await asyncio.gather(*[t[2] for t in tasks], return_exceptions=True)
                
                temp_failed = set()
                for (msg_id, thread_id, _), result in zip(tasks, results):
                    if isinstance(result, Exception):
                        if '429' in str(result):
                            logger.warning(f"Rate limit for message {msg_id}")
                        else:
                            logger.error(f"Error fetching message {msg_id}: {result}")
                        temp_failed.add((msg_id, thread_id))
                    else:
                        try:
                            email_msg = self._parse_message(result)
                            messages.append(email_msg)
                            successful_ids.add((msg_id, thread_id))
                        except Exception as e:
                            logger.exception(f"Failed parsing message {msg_id}: {e}")
                            temp_failed.add((msg_id, thread_id))

                failed_ids = temp_failed
                await asyncio.sleep(0.1)

        # Record permanently failed messages
        for msg_id, thread_id in failed_ids:
            if (msg_id, thread_id) not in successful_ids:
                self.failed_messages.append({
                    'message_id': msg_id,
                    'thread_id': thread_id,
                    'error': 'Failed after all retries'
                })

        return messages

    async def _download_attachment_async(self, aiogoogle: Aiogoogle, gmail, message_id: str, attachment: Attachment) -> Optional[str]:
        """Async PDF attachment download"""
        if attachment.mime_type != 'application/pdf':
            return None

        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                if attempt > 1:
                    wait = min(5 * attempt, 15)
                    logger.info(f"Waiting {wait}s before retry {attempt} for {attachment.filename}")
                    await asyncio.sleep(wait)

                logger.info(f"Downloading {attachment.filename} (attempt {attempt}/{max_attempts})")
                
                att_data = await aiogoogle.as_user(
                    gmail.users.messages.attachments.get(
                        userId='me',
                        messageId=message_id,
                        id=attachment.attachment_id
                    )
                )

                if 'data' not in att_data:
                    logger.error(f"No 'data' in attachment response for {attachment.filename}")
                    continue

                pdf_bytes = decode_gmail_base64_to_bytes(att_data['data'])
                
                if not pdf_bytes.startswith(b'%PDF'):
                    logger.warning(f"Attachment {attachment.filename} does not start with %PDF (first bytes: {pdf_bytes[:8]})")

                standard_b64 = normalize_pdf_base64_from_bytes(pdf_bytes)
                logger.info(f"Downloaded PDF {attachment.filename} ({len(pdf_bytes)} bytes)")
                return standard_b64

            except asyncio.TimeoutError:
                logger.warning(f"Timeout downloading {attachment.filename}")
                if attempt < max_attempts:
                    await asyncio.sleep(10)
            except ValueError as e:
                logger.error(f"Base64 decode error for {attachment.filename}: {e}")
                break
            except Exception as e:
                logger.exception(f"Error downloading {attachment.filename}: {e}")
                if attempt < max_attempts:
                    await asyncio.sleep(5)

        logger.error(f"Failed to download {attachment.filename} after {max_attempts} attempts")
        return None

    async def _process_attachments_async(self, aiogoogle: Aiogoogle, gmail, messages: List[EmailMessage], max_workers: int = 10) -> List[EmailMessage]:
        """Process all PDF attachments concurrently with semaphore for rate limiting"""
        total_attachments = sum(len(m.attachments) for m in messages)
        if total_attachments == 0:
            logger.info("No attachments to download")
            return messages

        logger.info(f"Downloading {total_attachments} PDF attachments concurrently (max {max_workers} parallel)")
        
        # Use semaphore to limit concurrent downloads
        semaphore = asyncio.Semaphore(max_workers)
        
        async def download_with_semaphore(msg, att):
            async with semaphore:
                try:
                    data = await self._download_attachment_async(aiogoogle, gmail, msg.message_id, att)
                    await asyncio.sleep(0.5)  # Rate limiting between downloads
                    return (msg.message_id, att.attachment_id, data, att.filename, None)
                except Exception as e:
                    logger.exception(f"Attachment error for {att.filename}: {e}")
                    return (msg.message_id, att.attachment_id, None, att.filename, str(e))

        # Create all download tasks
        tasks = []
        for msg in messages:
            for att in msg.attachments:
                if att.mime_type == 'application/pdf':
                    tasks.append(download_with_semaphore(msg, att))

        # Execute all downloads concurrently
        results = await asyncio.gather(*tasks)
        
        # Build attachment data dictionary
        attachment_data = {}
        failed_downloads = []
        
        for msg_id, att_id, data, filename, error in results:
            if data:
                attachment_data[(msg_id, att_id)] = data
            else:
                failed_downloads.append({
                    'message_id': msg_id,
                    'filename': filename,
                    'error': error or 'Download failed'
                })

        # Update messages with downloaded data
        for msg in messages:
            pdf_list = []
            for att in msg.attachments:
                if att.mime_type != 'application/pdf':
                    continue
                key = (msg.message_id, att.attachment_id)
                if key in attachment_data:
                    att.data = attachment_data[key]
                    pdf_list.append({
                        'filename': att.filename,
                        'base64_data': att.data,
                        'download_successful': True
                    })
                else:
                    att.download_failed = True
                    pdf_list.append({
                        'filename': att.filename,
                        'base64_data': None,
                        'download_successful': False,
                        'error': att.error_message or 'Download failed'
                    })
            msg.pdf_attachments = pdf_list

        if failed_downloads:
            self.failed_attachments.extend(failed_downloads)
            logger.warning(f"Failed to download {len(failed_downloads)} attachments")

        logger.info("Attachment processing complete")
        return messages

    def _parse_message(self, msg_data: Dict) -> EmailMessage:
        headers = {h['name']: h['value'] for h in msg_data['payload'].get('headers', [])}
        body_plain, body_html = self._extract_body(msg_data['payload'])
        attachments = self._extract_attachments(msg_data['payload'], msg_data['id'])

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
                    if mime_type == 'application/pdf':
                        attachments.append(Attachment(
                            filename=filename,
                            mime_type=mime_type,
                            size=body.get('size', 0),
                            attachment_id=att_id
                        ))
                    else:
                        logger.debug(f"Skipping non-PDF attachment {filename} (type: {mime_type})")

                if 'parts' in part:
                    recurse(part['parts'])

        if 'parts' in payload:
            recurse(payload['parts'])

        if attachments:
            logger.info(f"Found {len(attachments)} PDF attachments in message {message_id}")
        return attachments


# -------------------------- main convenience helper --------------------------

async def main_async(thread_ids: List[str], access_token: str, user_key: str, memory_limit_mb: int = 50):
    """
    Async main entry point with memory-based batching.
    
    Args:
        thread_ids: List of Gmail thread IDs to process
        access_token: Gmail API access token
        user_key: User identifier for S3 path
        memory_limit_mb: Memory threshold in MB (default 50)
    
    Returns:
        List of thread IDs that need to be retried
    """
    start_time = datetime.now()

    collector = GmailEmailCollector(access_token=access_token, batch_size=25, max_retries=3)
    
    # Initialize Database for S3 batching
    base_path = f"{user_key}/raw_emails_history_broker"
    db = Database(user_key=user_key, base_path=base_path)

    result, threads_yet = await collector.collect_emails_from_threads(
        start_time=start_time,
        thread_ids=thread_ids,
        download_attachments=True,
        max_workers=5,  # Can increase for more parallelism
        memory_limit_mb=memory_limit_mb,
        db=db
    )

    failed_thread_ids = [ft['thread_id'] for ft in result.failed_threads]
    to_retry = list(set(threads_yet + failed_thread_ids))

    logger.info(
        f"Collection Summary:\n"
        f"  total_threads_requested={len(thread_ids)}\n"
        f"  threads_processed={result.statistics['threads_processed']}\n"
        f"  total_emails_collected={len(result.emails)}\n"
        f"  failed_threads={len(result.failed_threads)}\n"
        f"  failed_messages={len(result.failed_messages)}\n"
        f"  failed_attachments={len(result.failed_attachments)}\n"
        f"  batches_saved_to_s3={result.statistics['batches_saved_to_s3']}\n"
        f"  threads_to_retry={len(to_retry)}"
    )

    return to_retry


if __name__ == '__main__':
    EXAMPLE_THREAD_IDS = []  # fill with thread ids
    ACCESS