import base64
from collections import defaultdict
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
import json
import logging
import time
import random
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import ssl
import socket
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import os
import botocore.session
import gzip

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, raw_emails, path, entry_id=None):
        self.path = path
        self.new_emails = raw_emails or {}
        # Disable EC2 metadata lookups that can hang
        os.environ['AWS_EC2_METADATA_DISABLED'] = 'true'

        self.bucket_name = os.getenv('AWS_S3_BUCKET_NAME')
        print(f"[Database Init] bucket name is: {self.bucket_name}")

        if not self.bucket_name:
            raise ValueError("AWS_S3_BUCKET_NAME environment variable not set")

        try:
            print("[Database Init] Creating S3 client...")
            # Use a more explicit configuration
            session = botocore.session.Session()

            # Set timeouts and retry policy
            config = botocore.config.Config(
                region_name='ap-southeast-2',
                signature_version='v4',
                retries={'max_attempts': 2, 'mode': 'standard'},
                connect_timeout=5,
                read_timeout=5
            )

            self.s3_client = session.create_client('s3', 'ap-southeast-2', config=config)
            print("[Database Init] S3 client created successfully")
            print("[Database Init] Skipping connection test in init")
        except Exception as e:
            print(f"[Database Init] ERROR with S3 client creation: {e}")
            raise

        print("[Database Init] Database initialization complete")

    def save_batch(self):
        """Just save new emails, don't merge in memory"""
        if not self.new_emails:
            return True
            
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        batch_key = f"{os.path.dirname(self.path)}/batch_{timestamp}.json.gz"
        
        # Compress and upload without loading old data
        json_data = json.dumps(self.new_emails, ensure_ascii=False)
        compressed = gzip.compress(json_data.encode('utf-8'))
        
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=batch_key,
            Body=compressed
        )
        return True

    def append_to_s3(self, max_retries=3):
        """Append new emails without loading old data"""
        if not self.old_emails:  # Nothing new to add
            return True
        
        # Generate a unique key for this batch using timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        batch_key = f"{self.path.rsplit('/', 1)[0]}/batch_{timestamp}.json.gz"
        
        try:
            json_string = json.dumps(self.old_emails, ensure_ascii=False)
            compressed_data = gzip.compress(json_string.encode('utf-8'))
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=batch_key,
                Body=compressed_data,
                ContentType='application/json',
                ContentEncoding='gzip'
            )
            print(f"[S3 Append] Saved batch to {batch_key}")
            return True
        except Exception as e:
            print(f"[S3 Append] Failed: {e}")
            raise

@dataclass
class Attachment:
    """Represents an email attachment"""
    filename: str
    mime_type: str
    size: int
    attachment_id: str
    data: Optional[str] = None
    download_failed: bool = False
    error_message: Optional[str] = None

@dataclass
class EmailMessage:
    """Represents a single email message"""
    message_id: str
    thread_id: str
    from_email: str
    to_email: str
    subject: str
    body_plain: str
    body_html: str
    date: str
    attachments: List[Attachment]
    pdf_attachments: List[Dict[str, str]]
    fetch_failed: bool = False
    error_message: Optional[str] = None

@dataclass
class CollectionResult:
    """Result of the collection process with statistics"""
    emails: List[EmailMessage]
    failed_threads: List[Dict[str, str]]
    failed_messages: List[Dict[str, str]]
    failed_attachments: List[Dict[str, str]]
    statistics: Dict[str, int]

def parse_html_to_text(html_content: str) -> str:
    """
    Parse HTML email content and extract clean text using BeautifulSoup
    
    Args:
        html_content: Raw HTML string from email
    
    Returns:
        Clean text content extracted from HTML
    """
    if not html_content:
        return ""
    
    try:
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Get text
        text = soup.get_text()
        
        # Clean up text
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = '\n'.join(chunk for chunk in chunks if chunk)
        
        # Remove excessive newlines
        text = re.sub(r'\n\s*\n+', '\n\n', text)
        
        return text.strip()
        
    except Exception as e:
        logger.error(f"Error parsing HTML: {e}")
        return html_content  # Return original if parsing fails



class GmailEmailCollector:
    """Efficient Gmail email collector with retry logic and better error handling"""
    
    def __init__(self, access_token: str, batch_size: int = 25, max_retries: int = 3):
        """
        Initialize the collector with an access token
        
        Args:
            access_token: OAuth2 access token for Gmail API
            batch_size: Number of requests to batch together (reduced from 50 to avoid rate limits)
            max_retries: Maximum number of retry attempts for failed requests
        """
        
        from datetime import datetime, timedelta
        
        # Create credentials with just the access token
        self.credentials = Credentials(token=access_token)
        
        # IMPORTANT: Disable refresh attempts to prevent the error
        # Override the refresh method to do nothing
        self.credentials.refresh = lambda request: None
        
        # Set expiry to a future time so it's never considered expired
        # This is settable unlike 'expired' which is read-only
        self.credentials.expiry = datetime.utcnow() + timedelta(hours=1)
        
        # Build the service
        self.service = build('gmail', 'v1', credentials=self.credentials)
        
        # Rest of initialization remains the same
        self.batch_size = min(batch_size, 100)
        self.max_retries = max_retries
        self.failed_threads = []
        self.failed_messages = []
        self.failed_attachments = []
        
    def collect_emails_from_threads(self, start_time, thread_ids: List[str], 
                                   download_attachments: bool = True,
                                   max_workers: int = 3) -> CollectionResult:
        """
        Collect all emails from the given thread IDs with better error handling
        """
        logger.info(f"Starting collection of {len(thread_ids)} threads")
        
        all_emails = []
        processed_threads = 0
        
        # Process threads in smaller chunks to avoid rate limits
        threads_yet_to_process = []
        for i in range(0, len(thread_ids), self.batch_size):
            if datetime.now() - start_time >= timedelta(minutes=13):
                threads_yet_to_process = thread_ids[i:]
                print("entered early exit")
                break
            chunk = thread_ids[i:i + self.batch_size]
            chunk_num = i // self.batch_size + 1
            logger.info(f"Processing chunk {chunk_num} ({len(chunk)} threads)")
            
            # Add exponential backoff between chunks
            if i > 0:
                delay = min(2 ** (chunk_num / 10), 10)  # Cap at 10 seconds
                logger.info(f"Waiting {delay:.1f}s to avoid rate limits...")
                time.sleep(delay)
            
            # Fetch threads with retry logic
            threads_data = self._batch_get_threads_with_retry(chunk)
            #print(f"Fetched {len([t for t in threads_data if t])} non-null threads")

            # Extract message IDs from successful threads
            message_ids = []
            for thread_data in threads_data:
                if thread_data and 'messages' in thread_data:
                    for msg in thread_data['messages']:
                        message_ids.append((msg['id'], thread_data['id']))
                    processed_threads += 1
            #print(f"Threads with messages: {len(threads_with_messages)}")
            #print(f"Total messages to fetch: {len(message_ids)}")
            
            # Fetch messages with retry logic
            messages = self._batch_get_messages_with_retry(message_ids)

            threads_in_messages = set(msg.thread_id for msg in messages)
            #print(f"Threads represented in fetched messages: {len(threads_in_messages)}")
            # FILTER MESSAGES BEFORE PROCESSING ATTACHMENTS
            original_count = len(messages)
            #print(f"Total messages before filtering: {original_count}")
            messages = [
                msg for msg in messages 
                if not (
                    msg.subject == "Your Dukbill Summary" or 
                    "noreply@dukbillapp.com" in msg.from_email.lower()
                )
            ]
            #print(f"total messages after filtering {len(messages)}")
            filtered_threads = set(msg.thread_id for msg in messages)
            #print(f"Threads after filtering: {len(filtered_threads)}")
            #print(f"Filtered out {original_count - len(messages)} messages")


            filtered_in_chunk = original_count - len(messages)
            if filtered_in_chunk > 0:
                logger.info(f"Filtered {filtered_in_chunk} Dukbill emails in chunk {chunk_num}")

            # Process attachments if needed
            if download_attachments:
                messages = self._process_attachments_sequential(messages, max_workers)
            
            all_emails.extend(messages)
            
            # Log progress
            logger.info(f"Chunk {chunk_num} complete: {len(messages)} emails collected")

        # Create result summary
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
        
        # Log final summary
        logger.info("=" * 60)
        logger.info("COLLECTION SUMMARY:")
        logger.info(f"  Threads: {processed_threads}/{len(thread_ids)} successful")
        logger.info(f"  Emails collected: {len(all_emails)}")
        logger.info(f"  Failed threads: {len(self.failed_threads)}")
        logger.info(f"  Failed messages: {len(self.failed_messages)}")
        logger.info(f"  Failed attachments: {len(self.failed_attachments)}")
        logger.info("=" * 60)
        
        return result, threads_yet_to_process
    
    def _batch_get_threads_with_retry(self, thread_ids: List[str]) -> List[Dict]:
        """Fetch threads with retry logic for failed requests"""
        threads = []
        failed_ids = set()
        
        for attempt in range(self.max_retries):
            if attempt > 0:
                # Exponential backoff
                delay = 2 ** attempt + random.uniform(0, 1)
                logger.info(f"Retry attempt {attempt} after {delay:.1f}s delay...")
                time.sleep(delay)
            
            threads_to_fetch = thread_ids if attempt == 0 else list(failed_ids)
            if not threads_to_fetch:
                break
            
            temp_threads = []
            temp_failed = set()
            
            def callback(request_id, response, exception):
                if exception is not None:
                    if '429' in str(exception):  # Rate limit error
                        logger.warning(f"Rate limit hit for thread {request_id}")
                        temp_failed.add(request_id)
                    else:
                        logger.error(f"Error fetching thread {request_id}: {exception}")
                        temp_failed.add(request_id)
                    temp_threads.append(None)
                else:
                    temp_threads.append(response)
            
            # Create batch request with smaller size
            batch_request = self.service.new_batch_http_request(callback=callback)
            
            for thread_id in threads_to_fetch:
                batch_request.add(
                    self.service.users().threads().get(
                        userId='me',
                        id=thread_id,
                        format='metadata',
                        metadataHeaders=['From', 'To', 'Subject', 'Date']
                    ),
                    request_id=thread_id
                )
            
            try:
                batch_request.execute()
            except Exception as e:
                logger.error(f"Batch request failed: {e}")
            
            # Process results
            if attempt == 0:
                threads = temp_threads
                failed_ids = temp_failed
            else:
                # Update with retry results
                success_count = sum(1 for t in temp_threads if t is not None)
                logger.info(f"Retry recovered {success_count} threads")
                threads.extend([t for t in temp_threads if t is not None])
                failed_ids = temp_failed
        
        # Record permanently failed threads
        for thread_id in failed_ids:
            self.failed_threads.append({
                'thread_id': thread_id,
                'error': 'Failed after all retry attempts'
            })
        
        return [t for t in threads if t is not None]
    
    def _batch_get_messages_with_retry(self, message_ids: List[tuple]) -> List[EmailMessage]:
        """Fetch messages with retry logic"""
        messages = []
        failed_ids = set(message_ids)
        successful_ids = set()
        
        for attempt in range(self.max_retries):
            if attempt > 0:
                delay = 2 ** attempt + random.uniform(0, 1)
                logger.info(f"Message retry attempt {attempt} after {delay:.1f}s...")
                time.sleep(delay)
            
            messages_to_fetch = list(failed_ids - successful_ids)
            if not messages_to_fetch:
                break
            
            temp_messages = []
            temp_failed = set()
            
            def callback(request_id, response, exception):
                msg_id, thread_id = request_id.split('_')
                if exception is not None:
                    if '429' in str(exception):
                        logger.warning(f"Rate limit hit for message {msg_id}")
                        temp_failed.add((msg_id, thread_id))
                    else:
                        logger.error(f"Error fetching message {msg_id}: {exception}")
                        temp_failed.add((msg_id, thread_id))
                else:
                    try:
                        email_msg = self._parse_message(response)
                        temp_messages.append(email_msg)
                        successful_ids.add((msg_id, thread_id))
                    except Exception as e:
                        print(f"Error parsing message {msg_id}: {e}")
                        temp_failed.add((msg_id, thread_id))
            
            # Process in smaller batches
            batch_size = max(10, self.batch_size // 2)  # Even smaller batches for messages
            for i in range(0, len(messages_to_fetch), batch_size):
                chunk = messages_to_fetch[i:i + batch_size]
                batch_request = self.service.new_batch_http_request(callback=callback)
                
                for msg_id, thread_id in chunk:
                    batch_request.add(
                        self.service.users().messages().get(
                            userId='me',
                            id=msg_id,
                            format='full'
                        ),
                        request_id=f"{msg_id}_{thread_id}"
                    )
                
                try:
                    batch_request.execute()
                    time.sleep(0.5)  # Small delay between message batches
                except Exception as e:
                    logger.error(f"Message batch request failed: {e}")
            
            messages.extend(temp_messages)
            failed_ids = temp_failed
        
        # Record permanently failed messages
        for msg_id, thread_id in failed_ids:
            if (msg_id, thread_id) not in successful_ids:
                self.failed_messages.append({
                    'message_id': msg_id,
                    'thread_id': thread_id,
                    'error': 'Failed after all retry attempts'
                })
        
        return messages
    
    def _download_attachment_sequential(self, message_id: str, attachment: Attachment) -> Optional[str]:
        """
        Sequential download with SSL fixes - PDF files only
        """
        # Early exit if not PDF
        if attachment.mime_type != 'application/pdf':
            logger.info(f"Skipping non-PDF attachment: {attachment.filename} ({attachment.mime_type})")
            return None
        
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                # Add delay between attempts
                if attempt > 0:
                    wait_time = min(5 * attempt, 15)
                    logger.info(f"Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                
                logger.info(f"Downloading PDF: {attachment.filename} (attempt {attempt + 1}/{max_attempts})")
                
                # Make the API call
                att_data = self.service.users().messages().attachments().get(
                    userId='me',
                    messageId=message_id,
                    id=attachment.attachment_id
                ).execute()
                
                if 'data' in att_data:
                    data = att_data['data']
                    
                    # Log the raw data length for debugging
                    logger.info(f"Raw base64 data length: {len(data)} chars")
                    
                    # Gmail returns URL-safe base64 WITHOUT padding
                    # We need to decode it properly
                    try:
                        # Direct decode using urlsafe_b64decode which handles padding automatically
                        pdf_bytes = base64.urlsafe_b64decode(data)
                        
                        # Validate it's a real PDF
                        if pdf_bytes[:4] != b'%PDF':
                            logger.error(f"Not a valid PDF! First bytes: {pdf_bytes[:20]}")
                            return None
                        
                        # Check for EOF marker
                        if b'%%EOF' not in pdf_bytes[-100:]:
                            logger.warning("PDF may be truncated - no %%EOF found in last 100 bytes")
                        
                        logger.info(f"✓ Successfully downloaded PDF: {attachment.filename} ({len(pdf_bytes)} bytes)")
                        
                        # Return the ORIGINAL data from Gmail, not re-encoded
                        return data
                        
                    except Exception as e:
                        logger.error(f"Failed to decode PDF attachment data: {e}")
                        
                        # Try with explicit padding
                        try:
                            padded_data = data + '=' * (4 - len(data) % 4)
                            pdf_bytes = base64.urlsafe_b64decode(padded_data)
                            
                            if pdf_bytes[:4] == b'%PDF':
                                logger.info(f"✓ Decoded PDF with padding: {len(pdf_bytes)} bytes")
                                return padded_data
                        except:
                            pass
                        
                        logger.error(f"All decode attempts failed for PDF: {attachment.filename}")
                        return None
                else:
                    logger.error("No 'data' field in attachment response")
                    return None
                        
            except (ssl.SSLError, socket.timeout) as e:
                logger.warning(f"Network error on attempt {attempt + 1} for {attachment.filename}: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(10)
                    
            except Exception as e:
                logger.error(f"Download attempt {attempt + 1} failed: {str(e)[:200]}")
                if "SSL" in str(e) or "decryption" in str(e).lower():
                    time.sleep(10)
        
        logger.error(f"Failed to download PDF {attachment.filename} after {max_attempts} attempts")
        return None

    def _process_attachments_sequential(self, messages: List[EmailMessage], max_workers: int = None) -> List[EmailMessage]:
        """
        Process attachments sequentially to avoid SSL/memory issues - PDF only
        """
        # Count total PDF attachments
        total_attachments = sum(len(msg.attachments) for msg in messages)
        
        if total_attachments == 0:
            logger.info("No PDF attachments to download")
            return messages
        
        logger.info(f"Downloading {total_attachments} PDF attachments sequentially...")
        
        attachment_data = {}
        failed_downloads = []
        downloaded_count = 0
        
        # Process each message's attachments sequentially
        for msg in messages:
            for att in msg.attachments:
                # Double-check it's a PDF (should already be filtered, but being safe)
                if att.mime_type != 'application/pdf':
                    logger.info(f"Skipping non-PDF file: {att.filename} ({att.mime_type})")
                    continue
                
                downloaded_count += 1
                logger.info(f"Processing PDF {downloaded_count}/{total_attachments}: {att.filename}")
                
                try:
                    # Download with retry logic
                    data = self._download_attachment_sequential(
                        message_id=msg.message_id,
                        attachment=att
                    )
                    
                    if data:
                        attachment_data[(msg.message_id, att.attachment_id)] = data
                    else:
                        failed_downloads.append({
                            'message_id': msg.message_id,
                            'filename': att.filename,
                            'error': 'Download failed after all attempts'
                        })
                        att.download_failed = True
                        att.error_message = 'Download failed'
                    
                    # CRITICAL: Add delay between downloads to prevent SSL issues
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error processing {att.filename}: {e}")
                    failed_downloads.append({
                        'message_id': msg.message_id,
                        'filename': att.filename,
                        'error': str(e)
                    })
                    att.download_failed = True
                    att.error_message = str(e)
                    
                    # Wait before next attempt
                    time.sleep(2)
        
        if failed_downloads:
            self.failed_attachments.extend(failed_downloads)
            logger.warning(f"Failed to download {len(failed_downloads)} PDF attachments")
        
        # Update messages with downloaded attachment data - PDF only
        for msg in messages:
            pdf_attachments = []
            for att in msg.attachments:
                # Only process PDFs
                if att.mime_type != 'application/pdf':
                    continue
                    
                key = (msg.message_id, att.attachment_id)
                if key in attachment_data:
                    att.data = attachment_data[key]
                    pdf_attachments.append({
                        'filename': att.filename,
                        'base64_data': att.data,
                        'download_successful': True
                    })
                else:
                    pdf_attachments.append({
                        'filename': att.filename,
                        'base64_data': None,
                        'download_successful': False,
                        'error': att.error_message or 'Download failed'
                    })
            
            msg.pdf_attachments = pdf_attachments
        
        logger.info(f"PDF attachment processing complete. Success: {len(attachment_data)}, Failed: {len(failed_downloads)}")
        return messages

    
    def _parse_message(self, msg_data: Dict) -> EmailMessage:
        """
        Parse a Gmail message into an EmailMessage object with HTML parsing
        """
        headers = {h['name']: h['value'] for h in msg_data['payload'].get('headers', [])}
        
        body_plain, body_html = self._extract_body(msg_data['payload'])
        attachments = self._extract_attachments(msg_data['payload'], msg_data['id'])
        
        # Use plain text if available, otherwise use HTML (which was converted to text)
        final_body = body_plain if body_plain else ""
        
        return EmailMessage(
            message_id=msg_data['id'],
            thread_id=msg_data['threadId'],
            from_email=headers.get('From', ''),
            to_email=headers.get('To', ''),
            subject=headers.get('Subject', ''),
            body_plain=final_body,  # This will now contain text extracted from HTML if needed
            body_html=body_html,     # Keep original HTML for reference
            date=headers.get('Date', ''),
            attachments=attachments,
            pdf_attachments=[],
            fetch_failed=False,
            error_message=None
        )
    
    def _extract_body(self, payload: Dict) -> tuple:
        """
        Extract plain and HTML body from message payload, with HTML parsing
        
        Returns:
            tuple: (body_plain, body_html, body_text_from_html)
        """
        body_plain = ''
        body_html = ''
        body_text_from_html = ''
        
        def recurse_parts(parts):
            nonlocal body_plain, body_html, body_text_from_html
            for part in parts:
                mime_type = part.get('mimeType', '')
                
                if mime_type == 'text/plain' and not body_plain:
                    data = part['body'].get('data', '')
                    if data:
                        body_plain = base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
                elif mime_type == 'text/html' and not body_html:
                    data = part['body'].get('data', '')
                    if data:
                        body_html = base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
                        # Parse HTML to extract text
                        body_text_from_html = parse_html_to_text(body_html)
                elif 'parts' in part:
                    recurse_parts(part['parts'])
        
        if 'parts' in payload:
            recurse_parts(payload['parts'])
        elif 'body' in payload and 'data' in payload['body']:
            data = payload['body']['data']
            decoded_content = base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
            
            # Try to determine if it's HTML or plain text
            if '<html' in decoded_content.lower() or '<body' in decoded_content.lower():
                body_html = decoded_content
                body_text_from_html = parse_html_to_text(body_html)
            else:
                body_plain = decoded_content
        
        # If we have HTML but no plain text, use the parsed HTML text
        if not body_plain and body_text_from_html:
            body_plain = body_text_from_html
        
        return body_plain, body_html
    
    def _extract_attachments(self, payload: Dict, message_id: str) -> List[Attachment]:
        """Extract attachment information from message payload - PDF files only"""
        attachments = []
        
        def recurse_parts(parts):
            for part in parts:
                filename = part.get('filename', '')
                
                # Only process if there's a filename and attachment ID
                if filename and part.get('body', {}).get('attachmentId'):
                    mime_type = part.get('mimeType', '')
                    
                    # Only process PDF attachments
                    if mime_type == 'application/pdf':
                        attachments.append(Attachment(
                            filename=filename,
                            mime_type=mime_type,
                            size=part.get('body', {}).get('size', 0),
                            attachment_id=part['body']['attachmentId'],
                            data=None,
                            download_failed=False,
                            error_message=None
                        ))
                    else:
                        logger.debug(f"Skipping non-PDF attachment: {filename} (type: {mime_type})")
                
                # Recurse into nested parts
                if 'parts' in part:
                    recurse_parts(part['parts'])
        
        if 'parts' in payload:
            recurse_parts(payload['parts'])
        
        # Log if non-PDF attachments were skipped
        if attachments:
            logger.info(f"Found {len(attachments)} PDF attachment(s) in message {message_id}")
        
        return attachments
    
    def export_results_with_errors(self, result: CollectionResult, output_file: str):
        """Export results including error information"""
        output = {
            'statistics': result.statistics,
            'successful_emails': [
                {
                    **asdict(email),
                    'attachments': [asdict(att) for att in email.attachments]
                }
                for email in result.emails
            ],
            'failed_threads': result.failed_threads,
            'failed_messages': result.failed_messages,
            'failed_attachments': result.failed_attachments
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Exported results to {output_file}")


def save_pdfs_to_disk(threads_dict, output_dir="pdfs"):
    """
    Save all PDFs from the threads to disk with proper decoding
    
    Args:
        threads_dict: Dictionary of threads with emails and PDFs
        output_dir: Directory to save PDFs (default: "pdfs")
    
    Returns:
        List of saved file paths
    """
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    saved_files = []
    total_pdfs = 0
    failed_pdfs = []
    
    print(f"\n📁 Saving PDFs to directory: {output_dir}/")
    
    for thread_id, emails in threads_dict.items():
        for email_index, email in enumerate(emails):
            # Skip if no PDFs
            if not email.get('pdfencoded') or not email.get('pdfs'):
                continue
            
            # Process each PDF
            for pdf_index, (pdf_name, pdf_data) in enumerate(zip(email['pdfs'], email['pdfencoded'])):
                
                if not pdf_data:
                    print(f"  ⚠️ Skipping {pdf_name} - no data")
                    continue
                
                try:
                    # Debug: Check the base64 data
                    print(f"\n  Processing {pdf_name}:")
                    print(f"    Base64 length: {len(pdf_data)} chars")
                    
                    # Gmail returns URL-safe base64 without padding
                    # Try to decode it properly
                    pdf_bytes = None
                    decode_method = None
                    
                    # Method 1: Direct urlsafe decode (handles padding automatically)
                    try:
                        pdf_bytes = base64.urlsafe_b64decode(pdf_data)
                        decode_method = "urlsafe_b64decode"
                    except Exception as e1:
                        print(f"    urlsafe_b64decode failed: {e1}")
                        
                        # Method 2: Add padding then decode
                        try:
                            padded = pdf_data + '=' * (4 - len(pdf_data) % 4)
                            pdf_bytes = base64.urlsafe_b64decode(padded)
                            decode_method = "urlsafe_b64decode with padding"
                        except Exception as e2:
                            print(f"    urlsafe with padding failed: {e2}")
                            
                            # Method 3: Convert to standard base64
                            try:
                                standard = pdf_data.replace('-', '+').replace('_', '/')
                                padded_standard = standard + '=' * (4 - len(standard) % 4)
                                pdf_bytes = base64.b64decode(padded_standard)
                                decode_method = "standard b64decode"
                            except Exception as e3:
                                print(f"    standard b64decode failed: {e3}")
                                
                                # Method 4: Try without any modifications
                                try:
                                    pdf_bytes = base64.b64decode(pdf_data)
                                    decode_method = "direct b64decode"
                                except Exception:
                                    print("    All decode methods failed!")
                                    failed_pdfs.append({
                                        'filename': pdf_name,
                                        'thread_id': thread_id,
                                        'error': 'Failed to decode base64'
                                    })
                                    continue
                    
                    # Validate PDF
                    if not pdf_bytes:
                        print("    ERROR: No bytes decoded")
                        continue
                        
                    print(f"    Decoded using: {decode_method}")
                    print(f"    Decoded size: {len(pdf_bytes)} bytes")
                    print(f"    First 8 bytes: {pdf_bytes[:8] if len(pdf_bytes) >= 8 else pdf_bytes}")

                    print("checking if the following is valid")
                    print(pdf_name)
                    # Check for valid PDF
                    if not pdf_bytes.startswith(b'%PDF'):
                        print("checking if the following is valid")
                        print(pdf_name)
                        print(f"    WARNING: Not a valid PDF! Starts with: {pdf_bytes[:20]}")
                        failed_pdfs.append({
                            'filename': pdf_name,
                            'thread_id': thread_id,
                            'error': f'Invalid PDF header: {pdf_bytes[:20]}'
                        })

                        # Still save it for inspection
                    
                    # Check for EOF
                    if b'%%EOF' not in pdf_bytes:
                        print("    WARNING: PDF may be truncated (no %%EOF marker)")
                    else:
                        eof_pos = pdf_bytes.rfind(b'%%EOF')
                        print(f"    %%EOF found at position {eof_pos}/{len(pdf_bytes)}")
                    
                    # Create unique filename
                    safe_name = pdf_name.replace('/', '_').replace('\\', '_')
                    unique_filename = f"{thread_id[:8]}_{email_index}_{safe_name}"
                    filepath = os.path.join(output_dir, unique_filename)
                    
                    # Save to file
                    with open(filepath, 'wb') as f:
                        f.write(pdf_bytes)
                    
                    saved_files.append({
                        'filepath': filepath,
                        'original_name': pdf_name,
                        'thread_id': thread_id,
                        'email_index': email_index,
                        'size': len(pdf_bytes),
                        'decode_method': decode_method,
                        'valid_pdf': pdf_bytes.startswith(b'%PDF'),
                        'has_eof': b'%%EOF' in pdf_bytes,
                        'from': email.get('from_', 'Unknown'),
                        'date': email.get('date', 'Unknown'),
                        'subject': email.get('subject', 'No subject')
                    })
                    
                    total_pdfs += 1
                    print(f"  ✓ Saved: {unique_filename}")
                    
                except Exception as e:
                    print(f"  ✗ Error saving {pdf_name}: {e}")
                    failed_pdfs.append({
                        'filename': pdf_name,
                        'thread_id': thread_id,
                        'error': str(e)
                    })
    
    # Create an index file with metadata
    if saved_files or failed_pdfs:
        index_file = os.path.join(output_dir, 'pdf_index.json')
        with open(index_file, 'w') as f:
            json.dump({
                'saved': saved_files,
                'failed': failed_pdfs,
                'summary': {
                    'total_saved': len(saved_files),
                    'total_failed': len(failed_pdfs),
                    'valid_pdfs': sum(1 for f in saved_files if f['valid_pdf']),
                    'truncated_pdfs': sum(1 for f in saved_files if not f['has_eof'])
                }
            }, f, indent=2)
        print(f"\n📋 Created index file: {index_file}")
    
    print(f"\n✅ Saved {total_pdfs} PDFs to {output_dir}/")
    if failed_pdfs:
        print(f"⚠️  Failed to save {len(failed_pdfs)} PDFs")
    print(f"   Total size: {sum(f['size'] for f in saved_files):,} bytes")
    
    return saved_files


def test_pdf_decode(b64_string):
    """
    Test function to debug PDF decoding issues
    
    Usage:
        from email_collection import test_pdf_decode
        test_pdf_decode(your_base64_string)
    """
    import base64
    
    print("Testing PDF decode...")
    print(f"Input length: {len(b64_string)} chars")
    print(f"First 50 chars: {b64_string[:50]}")
    print(f"Last 50 chars: {b64_string[-50:]}")
    
    results = []
    
    # Test 1: URL-safe decode (Gmail's format)
    try:
        pdf_bytes = base64.urlsafe_b64decode(b64_string)
        print("\n✓ Method 1 (urlsafe_b64decode): Success")
        print(f"  Size: {len(pdf_bytes)} bytes")
        print(f"  Header: {pdf_bytes[:8]}")
        print(f"  Footer: {pdf_bytes[-20:]}")
        print(f"  Valid PDF: {pdf_bytes.startswith(b'%PDF')}")
        print(f"  Has EOF: {b'%%EOF' in pdf_bytes}")
        results.append(('urlsafe', pdf_bytes))
    except Exception as e:
        print(f"\n✗ Method 1 failed: {e}")
    
    # Test 2: Add padding
    try:
        padded = b64_string + '=' * (4 - len(b64_string) % 4)
        pdf_bytes = base64.urlsafe_b64decode(padded)
        print("\n✓ Method 2 (urlsafe with padding): Success")
        print(f"  Size: {len(pdf_bytes)} bytes")
        print(f"  Header: {pdf_bytes[:8]}")
        print(f"  Valid PDF: {pdf_bytes.startswith(b'%PDF')}")
        results.append(('urlsafe_padded', pdf_bytes))
    except Exception as e:
        print(f"\n✗ Method 2 failed: {e}")
    
    # Test 3: Standard base64
    try:
        standard = b64_string.replace('-', '+').replace('_', '/')
        padded = standard + '=' * (4 - len(standard) % 4)
        pdf_bytes = base64.b64decode(padded)
        print("\n✓ Method 3 (standard base64): Success")
        print(f"  Size: {len(pdf_bytes)} bytes")
        print(f"  Header: {pdf_bytes[:8]}")
        print(f"  Valid PDF: {pdf_bytes.startswith(b'%PDF')}")
        results.append(('standard', pdf_bytes))
    except Exception as e:
        print(f"\n✗ Method 3 failed: {e}")
    
    # Save test files
    for method, data in results:
        filename = f"test_pdf_{method}.pdf"
        with open(filename, 'wb') as f:
            f.write(data)
        print(f"\nSaved test file: {filename}")
    
    return results


def main(thread_ids, access_token, user_key, save_pdfs=False):
    start_time = datetime.now()
    
    collector = GmailEmailCollector(
        access_token=access_token,
        batch_size=25,
        max_retries=3
    )
    
    # Collect emails with attachments
    result, threads_yet_to_process = collector.collect_emails_from_threads(
        start_time,
        thread_ids=thread_ids,
        download_attachments=True,
        max_workers=1
    )
    
    # Extract failed thread IDs from the failed_threads list
    failed_thread_ids = [failed['thread_id'] for failed in result.failed_threads]
    
    # Combine unprocessed threads with failed threads
    # Use a set to avoid duplicates, then convert back to list
    all_threads_to_retry = list(set(threads_yet_to_process + failed_thread_ids))
    
    print(f"Threads not processed in time: {len(threads_yet_to_process)}")
    print(f"Threads that failed: {len(failed_thread_ids)}")
    print(f"Total threads to retry: {len(all_threads_to_retry)}")
    
    # Log any failures for debugging
    if result.failed_threads:
        print(f"Warning: {len(result.failed_threads)} threads failed to fetch")
        for failed in result.failed_threads:
            print(f"  - Thread {failed['thread_id']}: {failed['error']}")
    
    if result.failed_attachments:
        print(f"Warning: {len(result.failed_attachments)} attachments failed to download")
    
    # Group emails by thread_id first
    threads_dict = defaultdict(list)
    
    # Process successful emails from result.emails
    for email in result.emails:
        email_data = {
            "body": email.body_plain if email.body_plain else email.body_html,
            "date": email.date,
            "from_": email.from_email,
            "subject": email.subject,
            "pdfs": [],
            "pdfencoded": [],
            "processed": 0
        }
        
        # Extract PDF filenames from all attachments
        for attachment in email.attachments:
            if attachment.mime_type == 'application/pdf':
                email_data["pdfs"].append(attachment.filename)
        
        # Extract encoded PDF data
        for pdf_attachment in email.pdf_attachments:
            if pdf_attachment.get('download_successful', False) and pdf_attachment.get('base64_data'):
                email_data["pdfencoded"].append(pdf_attachment['base64_data'])
            elif not pdf_attachment.get('download_successful', True):
                print(f"  Warning: PDF '{pdf_attachment['filename']}' failed to download: {pdf_attachment.get('error', 'Unknown error')}")
        
        # Add to the thread group
        threads_dict[email.thread_id].append(email_data)
    
    threads_list = threads_dict.copy()
    
    # Initialize database
    db_emails = Database(threads_list, f"{user_key}/raw_emails_history_broker/all_raw_emails.json")
    db_emails.save_batch()
    
    # Print summary statistics
    print("\nCollection Summary:")
    print(f"  Total threads requested: {len(thread_ids)}")
    print(f"  Unique threads collected: {len(threads_dict)}")
    print(f"  Total emails collected: {len(result.emails)}")
    print(f"  Failed threads: {len(result.failed_threads)}")
    print(f"  Failed messages: {len(result.failed_messages)}")
    print(f"  Failed attachments: {len(result.failed_attachments)}")
    print(f"  Threads to retry: {len(all_threads_to_retry)}")
    
    # Save PDFs if requested
    if save_pdfs:
        saved_files = save_pdfs_to_disk(threads_list)
        print(f"  Saved PDFs: {len(saved_files)}")
    
    # Return the processed threads and ALL threads that need to be retried
    return threads_list, all_threads_to_retry