import sys
import json
import gzip
import asyncio
import concurrent.futures
from datetime import datetime
import hashlib
import os
from dotenv import load_dotenv
import re
from threading import Semaphore
import base64
import tiktoken
import botocore.session
import time
load_dotenv()
from broker_langchain import chunked_emails_true_batch_async
from classify_subject import chunked_subject_batch_async
# Your existing imports
from broker_logic import *
from person_attributes import *
from database_interaction import *
from send_email import *
from broker_langchain import *
from send_email_broker import *
from classify_subject import *
from langchain_openai import ChatOpenAI

start = time.time()
encoding = tiktoken.encoding_for_model("gpt-4o")  # or "gpt-3.5-turbo"
end = time.time()

USER_KEY_QUEUE_URL = os.environ.get('USER_KEY_QUEUE_URL')

class Database_Retrieve:
    def __init__(self, user_key):
        self.user_key = user_key
        # Update paths for new structure
        self.raw_emails_prefix = f"{user_key}/raw_emails_history_broker/"
        self.batch_path = f"{user_key}/broker_batches/batches.json"
        self.processed_tracker_path = f"{user_key}/raw_emails_history_broker/processed_batches.json"
        
        # Disable EC2 metadata lookups that can hang
        os.environ['AWS_EC2_METADATA_DISABLED'] = 'true'

        self.bucket_name = os.getenv('AWS_S3_BUCKET_NAME')
        print(f"[Database Init] bucket name is: {self.bucket_name}")

        if not self.bucket_name:
            raise ValueError("AWS_S3_BUCKET_NAME environment variable not set")

        try:
            print("[Database Init] Creating S3 client...")
            session = botocore.session.Session()

            config = botocore.config.Config(
                region_name='ap-southeast-2',
                signature_version='v4',
                retries={'max_attempts': 2, 'mode': 'standard'},
                connect_timeout=5,
                read_timeout=5
            )

            self.s3_client = session.create_client('s3', 'ap-southeast-2', config=config)
            print("[Database Init] S3 client created successfully")
        except Exception as e:
            print(f"[Database Init] ERROR with S3 client creation: {e}")
            raise

        print("[Database Init] Database initialization complete")

    def get_processed_batches(self):
        """
        Retrieve the list of already processed batch files.
        Returns a set of batch filenames that have been processed.
        """
        try:
            print(f"[BATCH TRACKER] Reading processed batches from {self.processed_tracker_path}")
            s3_object = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.processed_tracker_path)
            body = s3_object['Body'].read()
            
            # Try to decompress if gzipped
            try:
                decompressed_body = gzip.decompress(body)
                data = json.loads(decompressed_body.decode('utf-8'))
            except (OSError, gzip.BadGzipFile):
                # Not gzipped, try plain JSON
                data = json.loads(body.decode('utf-8'))
            
            processed_batches = set(data.get('processed_batches', []))
            print(f"[BATCH TRACKER] Found {len(processed_batches)} processed batches")
            return processed_batches
            
        except self.s3_client.exceptions.NoSuchKey:
            print("[BATCH TRACKER] No processed batches tracker found, starting fresh")
            return set()
        except Exception as e:
            print(f"[BATCH TRACKER] Error reading processed batches: {e}")
            return set()

    def update_processed_batches(self, batch_filename):
        """
        Add a batch filename to the processed batches tracker.
        """
        try:
            # Get existing processed batches
            processed_batches = self.get_processed_batches()
            processed_batches.add(batch_filename)
            
            # Convert to list for JSON serialization
            data = {'processed_batches': sorted(list(processed_batches))}
            
            # Save back to S3
            json_string = json.dumps(data, ensure_ascii=False)
            utf8_bytes = json_string.encode('utf-8')
            compressed_data = gzip.compress(utf8_bytes)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.processed_tracker_path,
                Body=compressed_data,
                ContentType='application/json',
                ContentEncoding='gzip',
                Metadata={
                    'timestamp': datetime.now().isoformat(),
                    'batch_count': str(len(processed_batches))
                }
            )
            
            print(f"[BATCH TRACKER] Successfully marked {batch_filename} as processed")
            return True
            
        except Exception as e:
            print(f"[BATCH TRACKER] Error updating processed batches: {e}")
            return False

    def list_batch_files(self):
        """
        List all batch files in the raw_emails_history_broker directory.
        Returns a sorted list of batch filenames (oldest first).
        """
        try:
            print(f"[S3 LIST] Listing batch files in {self.raw_emails_prefix}")
            
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=self.raw_emails_prefix
            )
            
            batch_files = []
            batch_pattern = re.compile(r'batch_\d{8}_\d{6}_\d+_part\d+\.json\.gz$')
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        # Extract just the filename from the full key
                        filename = os.path.basename(key)
                        if batch_pattern.match(filename):
                            batch_files.append(filename)
            
            # Sort by timestamp in filename (YYYYMMDD_HHMMSS format naturally sorts chronologically)
            batch_files.sort()
            
            print(f"[S3 LIST] Found {len(batch_files)} total batch files")
            return batch_files
            
        except Exception as e:
            print(f"[S3 LIST] Error listing batch files: {e}")
            return []

    def get_next_unprocessed_batch(self):
        """
        Get the next unprocessed batch file.
        Returns tuple: (batch_filename, batch_data) or (None, None) if no unprocessed batches.
        """
        try:
            # Get all batch files
            all_batches = self.list_batch_files()
            if not all_batches:
                print("[BATCH SELECT] No batch files found")
                return None, None
            
            # Get processed batches
            processed_batches = self.get_processed_batches()
            
            # Find unprocessed batches
            unprocessed_batches = [b for b in all_batches if b not in processed_batches]
            
            if not unprocessed_batches:
                print("[BATCH SELECT] All batches have been processed")
                return None, None
            
            # Select the oldest unprocessed batch
            next_batch = unprocessed_batches[0]
            print(f"[BATCH SELECT] Next unprocessed batch: {next_batch}")
            print(f"[BATCH SELECT] {len(unprocessed_batches)} unprocessed batches remaining")
            
            # Load the batch data
            batch_key = os.path.join(self.raw_emails_prefix, next_batch)
            batch_data = self.load_batch_file(batch_key)
            
            if batch_data:
                return next_batch, batch_data
            else:
                return None, None
                
        except Exception as e:
            print(f"[BATCH SELECT] Error getting next unprocessed batch: {e}")
            return None, None

    def load_batch_file(self, batch_key):
        """
        Load a specific batch file from S3.
        """
        try:
            print(f"[S3 GET] Loading batch file: {batch_key}")
            s3_object = self.s3_client.get_object(Bucket=self.bucket_name, Key=batch_key)
            compressed_body = s3_object['Body'].read()
            
            try:
                decompressed_body = gzip.decompress(compressed_body)
                data = json.loads(decompressed_body.decode('utf-8'))
                print(f"[S3 GET] Successfully loaded and decompressed {batch_key}")
            except (OSError, gzip.BadGzipFile) as gz_err:
                print(f"[S3 GET] gzip decompression failed ({gz_err}), trying plain JSON...")
                data = json.loads(compressed_body.decode('utf-8'))
                print(f"[S3 GET] Successfully parsed plain JSON for {batch_key}")
            
            return data
            
        except self.s3_client.exceptions.NoSuchKey:
            print(f"[S3 GET] Batch file not found: {batch_key}")
            return None
        except Exception as e:
            print(f"[S3 GET] Error loading batch file: {e}")
            return None

    def check_for_more_batches(self):
        """
        Check if there are more unprocessed batches remaining.
        """
        all_batches = self.list_batch_files()
        processed_batches = self.get_processed_batches()
        unprocessed_count = len([b for b in all_batches if b not in processed_batches])
        print(f"[BATCH CHECK] {unprocessed_count} unprocessed batches remaining")
        return unprocessed_count > 0


    def check_and_retrieve_batches(self):
        """
        Check if there are pending batches at user_key/broker_batches/
        Returns the pending_data dict with 'emails' and '_batch_metadata' if exists, None otherwise.
        Retrieves the first file found in the directory (expects only one file).
        """
        try:
            # Get the directory path (remove filename if present)
            batch_dir = os.path.dirname(self.batch_path) + '/'
            print(f"[S3 BATCH CHECK] Checking for pending batches in {batch_dir}")
            
            # List all files in the broker_batches directory
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=batch_dir
            )
            
            # Check if any files exist
            if 'Contents' not in response or len(response['Contents']) == 0:
                print(f"[S3 BATCH CHECK] No files found in {batch_dir}")
                return None
            
            # Get the first file (should be the only one)
            first_file = response['Contents'][0]
            file_key = first_file['Key']
            file_size = first_file['Size']
            
            print(f"[S3 BATCH CHECK] Found file: {file_key} (size: {file_size} bytes)")
            
            # If there are multiple files, warn but proceed
            if len(response['Contents']) > 1:
                print(f"[S3 BATCH CHECK] WARNING: Found {len(response['Contents'])} files, expected only 1")
                print(f"[S3 BATCH CHECK] Files found: {[obj['Key'] for obj in response['Contents']]}")
            
            # Retrieve the file
            s3_object = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
            body = s3_object['Body'].read()
            
            # Try to decompress if gzipped
            try:
                decompressed_body = gzip.decompress(body)
                data = json.loads(decompressed_body.decode('utf-8'))
                print(f"[S3 BATCH CHECK] Found compressed pending batches from {file_key}")
            except (OSError, gzip.BadGzipFile):
                # Not gzipped, try plain JSON
                data = json.loads(body.decode('utf-8'))
                print(f"[S3 BATCH CHECK] Found uncompressed pending batches from {file_key}")
            
            # Store the actual filename for later reference
            self.current_batch_file = file_key
            self.current_batch_filename = os.path.basename(file_key)
            
            # Validate structure: should have 'emails' and '_batch_metadata'
            if isinstance(data, dict) and 'emails' in data and '_batch_metadata' in data:
                email_count = len(data['emails']) if isinstance(data['emails'], (list, dict)) else 0
                retry_count = data['_batch_metadata'].get('retry_count', 0)
                original_batch = data['_batch_metadata'].get('original_batch_filename', 'unknown')
                
                print(f"[S3 BATCH CHECK] Retrieved pending batch with {email_count} emails")
                print(f"[S3 BATCH CHECK] Original batch: {original_batch}, Retry count: {retry_count}")
                return data
            else:
                print("[S3 BATCH CHECK] Invalid pending batch structure")
                print(f"[S3 BATCH CHECK] Expected dict with 'emails' and '_batch_metadata', got: {type(data)}")
                return None
                
        except Exception as e:
            print(f"[S3 BATCH CHECK] Error checking for batches: {e}")
            return None

    def clear_pending_batches(self):
        """
        Delete all files in the user_key/broker_batches/ directory
        Returns True if successful, False otherwise
        """
        try:
            # Get the directory path
            batch_dir = os.path.dirname(self.batch_path) + '/'
            print(f"[S3 BATCH CLEAR] Clearing all files in {batch_dir}")
            
            # List all files in the broker_batches directory
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=batch_dir
            )
            
            # Check if any files exist
            if 'Contents' not in response or len(response['Contents']) == 0:
                print(f"[S3 BATCH CLEAR] No files to delete in {batch_dir}")
                return True
            
            # Collect all file keys to delete
            files_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            file_count = len(files_to_delete)
            
            print(f"[S3 BATCH CLEAR] Found {file_count} file(s) to delete")
            print(f"[S3 BATCH CLEAR] Files: {[obj['Key'] for obj in files_to_delete]}")
            
            # Delete all files
            delete_response = self.s3_client.delete_objects(
                Bucket=self.bucket_name,
                Delete={
                    'Objects': files_to_delete,
                    'Quiet': False
                }
            )
            
            # Check for errors
            if 'Errors' in delete_response and len(delete_response['Errors']) > 0:
                print("[S3 BATCH CLEAR] Errors occurred while deleting:")
                for error in delete_response['Errors']:
                    print(f"[S3 BATCH CLEAR] - {error['Key']}: {error['Message']}")
                return False
            
            deleted_count = len(delete_response.get('Deleted', []))
            print(f"[S3 BATCH CLEAR] Successfully deleted {deleted_count} file(s)")
            return True
            
        except Exception as e:
            print(f"[S3 BATCH CLEAR] Error clearing pending batches: {e}")
            return False

    def save_pending_batches(self, emails_to_process, current_batch_file_name):
        """
        Save emails that couldn't be processed to user_key/broker_batches/batches.json
        Overwrites existing data if present
        """
        if not emails_to_process:
            print("[S3 BATCH SAVE] No emails to save as pending batches")
            return False
        
        try:
            # Prepare the data
            json_string = json.dumps(emails_to_process, ensure_ascii=False)
            utf8_bytes = json_string.encode('utf-8')
            compressed_data = gzip.compress(utf8_bytes)
            
            s3_key = f"{self.user_key}/broker_batches/{current_batch_file_name}"
            print(f"[S3 BATCH SAVE] Saving {len(emails_to_process)} items to {self.batch_path}")
            print(f"[S3 BATCH SAVE] Original size: {len(utf8_bytes)} bytes, Compressed: {len(compressed_data)} bytes")
            
            # Upload to S3 (overwrites if exists)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=compressed_data,
                ContentType='application/json',
                ContentEncoding='gzip',
                Metadata={
                    'timestamp': datetime.now().isoformat(),
                    'item_count': str(len(emails_to_process))
                }
            )
            
            print(f"[S3 BATCH SAVE] Successfully saved pending batches to {self.batch_path}")
            return True
            
        except Exception as e:
            print(f"[S3 BATCH SAVE] Error saving pending batches: {e}")
            raise

    '''
    def clear_pending_batches(self):
        """
        Delete the pending batches file after successful processing
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=self.batch_path)
            print(f"[S3 BATCH CLEAR] Cleared pending batches at {self.batch_path}")
            return True
        except Exception as e:
            print(f"[S3 BATCH CLEAR] Error clearing pending batches: {e}")
            return False
    '''

    def retrieving_raw_emails_from_s3(self):
        """
        Legacy method name kept for compatibility.
        Now retrieves the next unprocessed batch file.
        """
        #batch_data -> has all of the data from a singular batch inside
        batch_filename, batch_data = self.get_next_unprocessed_batch()
        self.current_batch_filename = batch_filename
        return batch_data
        '''
        if batch_data:
            # Store the current batch filename for later marking as processed
            self.current_batch_filename = batch_filename
            self.old_data = batch_data or {}
            return self.old_data
        else:
            print("[S3 GET] No unprocessed batches available")
            self.current_batch_filename = None
            self.old_data = {}
            return self.old_data
        '''
    def save_to_s3(self, data, path):
        try:
            # Prepare the data
            json_string = json.dumps(data, ensure_ascii=False)
            utf8_bytes = json_string.encode('utf-8')
            compressed_data = gzip.compress(utf8_bytes)

            
            # Upload to S3 (overwrites if exists)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=path,
                Body=compressed_data,
                ContentType='application/json',
                ContentEncoding='gzip',
                Metadata={
                    'timestamp': datetime.now().isoformat(),
                    'item_count': str(len(data))
                }
            )
            
            print(f"[S3 DATA SAVE] Successfully saved pending batches to {self.batch_path}")
            return True
            
        except Exception as e:
            print(f"[S3 DATA SAVE] Error saving pending batches: {e}")
            raise



def pdf_filtering(emails):
    filtered_emails = {}
    pdf_count = 0
    
    for thread_id, messages in emails.items():
        thread_has_pdf = False
        
        for message in messages:
            pdfencoded = message.get('pdfencoded', [])
            if pdfencoded and len(pdfencoded) > 0:
                thread_has_pdf = True
                pdf_count += len(pdfencoded)
                break
        
        if thread_has_pdf:
            filtered_emails[thread_id] = messages
    
    print(f"Found {len(filtered_emails)} threads with PDFs")
    print(f"Total PDFs found: {pdf_count}")
    return filtered_emails


def retrieve_anonymized_threadids(anonymized_emails):
    """
    Extract relevant threadids from anonymized emails
    """
    threadids = set()
    relevant_bdc = {}
    for classification in anonymized_emails:
        if (classification.get("broker_document_category") != "NA"): #and 
            #classification.get("broker_document_category") != "Miscellaneous or Unclassified"):
            
            threadids.add(classification["threadid"])

            broker_doc_cat = classification["broker_document_category"]
            if broker_doc_cat not in relevant_bdc:
                relevant_bdc[broker_doc_cat] = []
            relevant_bdc[broker_doc_cat].append(classification["threadid"])

    return threadids, relevant_bdc


def retrieve_threadids_from_anonymized_emails(anonymized_emails):
    """
    Extract relevant threadids from anonymized emails
    """
    threadids = set()
    for classification in anonymized_emails:
        threadids.add(classification["threadid"])
    
    print(f"[THREADIDS] Found {len(threadids)} relevant thread IDs")
    return threadids


def collect_emails_from_all_batches(db_function, threadids, save_path=None):
    """
    Search through all batch files and collect emails matching the given threadids.
    """
    filtered_emails = {}
    
    if not threadids:
        print("[COLLECT] No threadids provided, returning empty dict")
        return filtered_emails
    
    # Get all batch files
    all_batches = db_function.list_batch_files()
    
    if not all_batches:
        print("[COLLECT] No batch files found")
        return filtered_emails
    
    print(f"[COLLECT] Searching {len(all_batches)} batch files for {len(threadids)} thread IDs")
    
    # Track progress
    batches_searched = 0
    threads_found = set()
    
    # Search through each batch file
    for batch_filename in all_batches:
        batch_key = os.path.join(db_function.raw_emails_prefix, batch_filename)
        
        try:
            # Load the batch file
            batch_data = db_function.load_batch_file(batch_key)
            
            if batch_data:
                batches_searched += 1
                
                # Search for matching threadids
                for threadid in threadids:
                    if threadid in batch_data and threadid not in filtered_emails:
                        filtered_emails[threadid] = batch_data[threadid]
                        threads_found.add(threadid)
                
                print(f"[COLLECT] Batch {batch_filename}: Found {len(threads_found)}/{len(threadids)} threads so far")
                
                # Early exit if all threads found
                if len(threads_found) == len(threadids):
                    print(f"[COLLECT] All threads found after searching {batches_searched} batches")
                    break
                    
        except Exception as e:
            print(f"[COLLECT] Error processing batch {batch_filename}: {e}")
            continue
    
    print(f"[COLLECT] Search complete. Found {len(filtered_emails)} out of {len(threadids)} threads")
    
    # Report missing threads if any
    missing_threads = threadids - threads_found
    if missing_threads:
        print(f"[COLLECT] Missing threads: {missing_threads}")
    
    # Optionally save to S3
    if save_path and filtered_emails:
        try:
            # Save filtered emails with compression
            json_string = json.dumps(filtered_emails, ensure_ascii=False)
            utf8_bytes = json_string.encode('utf-8')
            compressed_data = gzip.compress(utf8_bytes)
            
            db_function.s3_client.put_object(
                Bucket=db_function.bucket_name,
                Key=save_path,
                Body=compressed_data,
                ContentType='application/json',
                ContentEncoding='gzip',
                Metadata={
                    'timestamp': datetime.now().isoformat(),
                    'thread_count': str(len(filtered_emails))
                }
            )
            print(f"[COLLECT] Saved filtered emails to {save_path}")
        except Exception as e:
            print(f"[COLLECT] Error saving filtered emails: {e}")
    
    return filtered_emails, threads_found


# Replace the synchronous handle_new_entry_broker with async version
async def handle_new_entry_broker_async(user_email):
    """
    ASYNC VERSION: Main entry point for broker email processing with async operations.
    
    This version uses:
    - Async Tesseract OCR for PDF processing
    - Async OpenAI API calls for email classification
    - Concurrent processing for improved performance
    """
    
    start_time = datetime.now()
    user_key = hashlib.sha256(user_email.encode('utf-8')).hexdigest()

    # Initialize database handler
    db_function = Database_Retrieve(user_key)
    all_anonymized_emails = []
    all_processed_emails = {}
    
    entered = 0
    API_KEY_OPENAI = os.getenv("API_KEY_OPENAI")
    llm = ChatOpenAI(temperature=0, model_name="gpt-4o", api_key=API_KEY_OPENAI, max_tokens=10000, request_timeout=60)

    structured_llm = llm.with_structured_output(schema=BrokerData)
    structured_llm_2 = llm.with_structured_output(schema=RelevantList)

    # Main processing loop
    while True:
        processing_new_batch = False
        current_batch_filename = None
        original_emails_dict = {}
        
        pending_batches = db_function.check_and_retrieve_batches()
        emails = []
        
        if pending_batches:
            response = pending_batches["emails"]
            print("[BATCHES TO PROCESS] - follow from last invocation")
            current_batch_filename = db_function.current_batch_filename
            processing_new_batch = False
        else:
            emails = db_function.retrieving_raw_emails_from_s3()
            current_batch_filename = db_function.current_batch_filename
            processing_new_batch = True
            
            emails = pdf_filtering(emails)
            if not emails:
                print("[BATCH PROCESSING] No PDFs found in batch, marking as processed")
                if current_batch_filename:
                    db_function.update_processed_batches(current_batch_filename)
                continue

        threads_json = emails
        
        # Process threads with ASYNC OCR
        if threads_json:
            print("Processing threads_json with ASYNC OCR")
            # Initialize Person with Tesseract enabled
            threads_container = Person(
                threads_json,
                use_tesseract=False,              # Enable Tesseract OCR
                max_tesseract_workers=4,      # Auto-detect based on Lambda memory
                use_parallel_textract=True,       # Keep async Textract for fallback
                max_textract_workers=10,
                smart_textract=True,
                textract_only_if_empty=False
            )
            
            threads_container.store_unique_pdf()
            
            # ASYNC PDF to text conversion
            print("[ASYNC] Starting async PDF to text conversion...")
            await threads_container.pdf_to_text_async(
                max_pages=1,
                use_textract_fallback=True,
                min_chars_threshold=50
            )
            print("[ASYNC] PDF to text conversion complete")
            
            threads_container.combine_text()
            threads_container.combining_pdf_text()

            response_subject = combine_subject_response_for_async(threads_container)
            response = combine_response_for_async(threads_container)
            
            # ASYNC subject classification
            print(f"[ASYNC] Processing {len(response_subject)} subjects with async API calls")
            results_subject, subjects_to_process = await chunked_subject_batch_async(
                response_subject, 
                structured_llm_2, 
                start_time, 
                encoding
            )
            print("[ASYNC] Subject classification complete")
            
            response = filter_response_on_subject_output(results_subject, response)
            print(f"Filtered to {len(response)} relevant emails")
        else:
            final_json = [{
                "amount": "NA",
                "company": "NA",
                "date": "NA",
                "threadid": "NA",
                "broker_document_category": "NA",
                "email_summary": "NA",
                "subject": "NA"
            }]
        
        # ASYNC email classification
        print(f"[ASYNC] Processing {len(response)} emails with async API calls")
        if pending_batches:
            results, emails_to_process = await chunked_emails_true_batch_async(
                response, 
                structured_llm, 
                start_time, 
                encoding,
                already_batched=False
            )
        else:
            results, emails_to_process = await chunked_emails_true_batch_async(
                response, 
                structured_llm, 
                start_time, 
                encoding,
                already_batched=False
            )
        print("[ASYNC] Email classification complete")
        
        # Process the results from this batch
        final_json = combine_chatgpt_responses_broker(results)
        print(f"Generated {len(final_json)} classifications")

        if final_json is not None:
            cleaned_final_json = final_json
            print("Cleaned final JSON:", cleaned_final_json)

            # Initialize Database for processed emails
            data_base = Database(cleaned_final_json, user_key, emails)
            
            # Save processed threads
            anonymized_emails = data_base.add_anonymized_threads(
                cleaned_final_json, 
                f"{user_key}/broker_anonymized/emails_anonymized.json"
            )
            print("Database work completed for current batch")
        else:
            html_content = generate_no_findings_html_broker()
            send_email(
                to_email=user_email,
                subject="Your Dukbill Summary",
                html_content=html_content,
                old=False,
                pdf_path=False,
                zip_path=False
            )

        # Check for unprocessed emails
        if emails_to_process:
            print(f"[BATCH SAVE] {len(emails_to_process)} emails couldn't be processed in time")
            
            current_retry_count = 0
            if isinstance(pending_batches, dict) and '_batch_metadata' in pending_batches:
                current_retry_count = pending_batches['_batch_metadata'].get('retry_count', 0)
            
            pending_data = {
                'emails': emails_to_process,
                '_batch_metadata': {
                    'original_batch_filename': current_batch_filename,
                    'timestamp': datetime.now().isoformat(),
                    'retry_count': current_retry_count + 1,
                    'original_count': len(emails) if not isinstance(pending_batches, dict) else pending_batches.get('_batch_metadata', {}).get('original_count', len(emails)),
                    'remaining_count': len(emails_to_process)
                }
            }
            
            retry_count = pending_data['_batch_metadata']['retry_count']
            
            # Check for excessive retries
            MAX_RETRIES = 5
            if retry_count >= MAX_RETRIES:
                print(f"[SAFETY] Max retries ({MAX_RETRIES}) reached for batch {current_batch_filename}")
                failed_path = f"{user_key}/broker_failed/failed_{current_batch_filename or 'unknown'}"
                
                try:
                    json_string = json.dumps(emails_to_process, ensure_ascii=False)
                    utf8_bytes = json_string.encode('utf-8')
                    compressed_data = gzip.compress(utf8_bytes)
                    
                    db_function.s3_client.put_object(
                        Bucket=db_function.bucket_name,
                        Key=failed_path,
                        Body=compressed_data,
                        ContentType='application/json',
                        ContentEncoding='gzip',
                        Metadata={
                            'timestamp': datetime.now().isoformat(),
                            'item_count': str(len(emails_to_process)),
                            'status': 'failed_max_retries',
                            'retry_count': str(retry_count)
                        }
                    )
                    print(f"[SAFETY] Saved failed emails to {failed_path}")
                except Exception as e:
                    print(f"[SAFETY] Error saving failed batch: {e}")
                
                if current_batch_filename:
                    db_function.update_processed_batches(current_batch_filename)
                
                db_function.clear_pending_batches()
                continue
            
            # Save unprocessed emails
            db_function.save_pending_batches(pending_data, current_batch_filename)
            print(f"[BATCH SAVE] Unprocessed emails saved (retry {retry_count}/{MAX_RETRIES})")
            
            # Re-queue for next invocation
            print("Time limit approaching, re-queuing for next invocation")
            send_user_key_to_queue(user_email)
            return True
        else:
            print("[BATCH PROCESSING] Current batch processed successfully")
            
            db_function.update_processed_batches(current_batch_filename)
            print(f"[BATCH PROCESSING] Marked {current_batch_filename} as processed")
            
            if not processing_new_batch:
                db_function.clear_pending_batches()
            
            if not db_function.check_for_more_batches():
                print("No more batches to process")
                break
            
            print("Moving to next batch in same invocation...")
            continue

    # Retrieve existing anonymized emails
    if entered == 0:
        try:
            anonymized_path = f"{user_key}/broker_anonymized/emails_anonymized.json"
            print(f"[RETRIEVE] Attempting to retrieve existing anonymized emails from {anonymized_path}")
            
            s3_object = db_function.s3_client.get_object(Bucket=db_function.bucket_name, Key=anonymized_path)
            body = s3_object['Body'].read()
            
            try:
                decompressed_body = gzip.decompress(body)
                existing_data = json.loads(decompressed_body.decode('utf-8'))
                print("[RETRIEVE] Successfully retrieved compressed anonymized emails")
            except (OSError, gzip.BadGzipFile):
                existing_data = json.loads(body.decode('utf-8'))
                print("[RETRIEVE] Successfully retrieved uncompressed anonymized emails")
            
            if existing_data:
                all_anonymized_emails = existing_data if isinstance(existing_data, list) else [existing_data]
                print(f"[RETRIEVE] Loaded {len(all_anonymized_emails)} existing anonymized email records")
            
        except db_function.s3_client.exceptions.NoSuchKey:
            print("[RETRIEVE] No existing anonymized emails found, starting fresh")
            all_anonymized_emails = []
        except Exception as e:
            print(f"[RETRIEVE] Error retrieving anonymized emails: {e}")
            all_anonymized_emails = []

    # Final summary preparation
    print("All processing complete, preparing summary email")
    
    threadids, relevant_bdc = retrieve_anonymized_threadids(all_anonymized_emails)
    
    if threadids:
        filtered_cache_path = f"{user_key}/broker_filtered/filtered_emails.json"
        
        all_processed_emails, threadids = collect_emails_from_all_batches(
            db_function,
            threadids,
            save_path=filtered_cache_path
        )
        print(f"[SUMMARY] Collected {len(all_processed_emails)} relevant emails for zip file")

        # Download and save PDFs
        downloading_pdfs_to_path(relevant_bdc, all_processed_emails, user_key, db_function, all_anonymized_emails )
    else:
        print("[SUMMARY] No relevant threadids found, using empty dict for zip")
        all_processed_emails = {}
    
    # Find unused categories
    unused = find_unused_broker_doc_categories(all_anonymized_emails)
    db_function.save_to_s3(list(unused), f"{user_key}/pending_categories.json")
    
    print("[COMPLETE] handle_new_entry_broker_async finished successfully")
    return True


# Synchronous wrapper for Lambda handler
def handle_new_entry_broker(user_email):
    """
    Synchronous wrapper that runs the async version.
    Uses asyncio.run() which properly handles event loop lifecycle.
    
    This is the cleanest approach for Lambda and handles:
    - Event loop creation
    - Event loop cleanup
    - Lambda container reuse
    """
    return asyncio.run(handle_new_entry_broker_async(user_email))

def send_user_key_to_queue(user_key):
    """
    Send a single user_key to a dedicated SQS queue.
    """
    import boto3
    
    if not USER_KEY_QUEUE_URL:
        raise RuntimeError("USER_KEY_QUEUE_URL environment variable is not set")
    
    sqs = boto3.client('sqs', region_name='ap-southeast-2')
    
    entry = {
        "Id": "0",
        "MessageBody": json.dumps({"user_key": user_key})
    }
    
    resp = sqs.send_message_batch(QueueUrl=USER_KEY_QUEUE_URL, Entries=[entry])
    
    failed = resp.get('Failed', [])
    if failed:
        print(f"SQS send_user_key batch failures: {failed}")
        raise RuntimeError(f"SQS send_user_key batch failed: {failed}")
    
    print(f"Successfully sent user_key '{user_key}' to {USER_KEY_QUEUE_URL}")

def save_anonymized_emails_to_path(relevant_bdc, raw_emails_relevant, user_key, db_function):
    """
    Search through all batch files and collect emails matching the given threadids.
    """
    #relevant_bdc -> {broker_document_category: threadid}

    #collate all anonymized_emails of a certain bdc
    print("starting save anonymized emails")
    for broker_doc_category, threadids in relevant_bdc.items():
        print(f"starting save anonymized emails - {broker_doc_category}")
        path = f"{user_key}/categorised/{broker_doc_category}/relevant_emails/emails.json"
        temp_broker_docs = {thread: raw_emails_relevant[thread] for thread in threadids if raw_emails_relevant.get(thread, None)}

        json_string = json.dumps(temp_broker_docs, ensure_ascii=False)
        utf8_bytes = json_string.encode('utf-8')
        compressed_data = gzip.compress(utf8_bytes)

        db_function.s3_client.put_object(
            Bucket=db_function.bucket_name,
            Key=path,
            Body=compressed_data,
            ContentType='application/json',
            ContentEncoding='gzip',
            Metadata={
                'timestamp': datetime.now().isoformat(),
                'thread_count': str(len(threadids))
            }
        )

def downloading_pdfs_to_path(relevant_bdc, raw_emails_relevant, user_key, db_function, all_anonymized_emails):
    """
    Upload PDFs in parallel using threading to reduce total time.
    Includes metadata from anonymized emails for each PDF.
    """
    # Create lookup dictionary for quick metadata access
    anonymized_lookup = {item['threadid']: item for item in all_anonymized_emails}
    
    upload_tasks = []
    
    for broker_doc_category, threadids in relevant_bdc.items():
        path = f"{user_key}/categorised/{broker_doc_category}/pdfs/"
        print(f"[PDF UPLOAD] Processing category: {broker_doc_category}")
        
        for thread in threadids:
            email_single = raw_emails_relevant.get(thread, None)
            anonymized_data = anonymized_lookup.get(thread, {})
            
            message = []
            if email_single and isinstance(email_single, list):
                message = email_single[0]
            
            if 'pdfencoded' in message and isinstance(message["pdfencoded"], list):
                pdf_names = message.get('pdfs', [])
                
                for pdf_idx, encoded_pdf in enumerate(message['pdfencoded']):
                    if pdf_idx < len(pdf_names):
                        pdf_name = pdf_names[pdf_idx]
                        # Remove .pdf extension if it already exists
                        if pdf_name.lower().endswith('.pdf'):
                            pdf_name = pdf_name[:-4]
                        filename = f"{thread}_{pdf_idx}_{pdf_name}.pdf"
                    else:
                        filename = f"{thread}_{pdf_idx}.pdf"

                    s3_key = f"{path}{filename}"
                    
                    # Prepare metadata from anonymized data
                    # S3 metadata values must be strings and have size limits (2KB per key)
                    metadata = {
                        'threadid': str(thread),
                        'broker_category': broker_doc_category,
                        'company': str(anonymized_data.get('company', 'Unknown'))[:1024],
                        'amount': str(anonymized_data.get('amount', 'N/A'))[:256],
                        'date': str(anonymized_data.get('date', 'N/A'))[:64],
                        'subject': str(anonymized_data.get('subject', 'N/A'))[:1024],
                        'email_summary': str(anonymized_data.get('email_summary', 'N/A'))[:1024],
                        'timestamp': datetime.now().isoformat(),
                        'pdf_index': str(pdf_idx),
                        'pdf_name': pdf_name if pdf_idx < len(pdf_names) else f'document_{pdf_idx}'
                    }
                    
                    upload_tasks.append({
                        'key': s3_key,
                        'data': encoded_pdf,
                        'thread': thread,
                        'metadata': metadata
                    })
    
    print(f"[PDF UPLOAD] Preparing to upload {len(upload_tasks)} PDFs in parallel")
    
    max_workers = 10
    semaphore = Semaphore(max_workers)
    
    def upload_single_pdf(task):
        with semaphore:
            try:
                # CRITICAL FIX: Handle the list structure
                encoded_data = task['data']
                
                # If it's a list, get the first element (the base64 string)
                if isinstance(encoded_data, list):
                    if len(encoded_data) > 0:
                        encoded_data = encoded_data[0]
                    else:
                        print(f"[PDF UPLOAD] Empty list for {task['key']}")
                        return False
                
                # Now decode the base64 string
                if isinstance(encoded_data, str):
                    # Clean whitespace and fix padding
                    encoded_data = encoded_data.strip().replace('\n', '').replace('\r', '').replace(' ', '')
                    
                    # Fix padding
                    missing_padding = len(encoded_data) % 4
                    if missing_padding:
                        encoded_data += '=' * (4 - missing_padding)
                    
                    pdf_data = base64.b64decode(encoded_data)
                elif isinstance(encoded_data, bytes):
                    pdf_data = encoded_data
                else:
                    print(f"[PDF UPLOAD] Unexpected type for {task['key']}: {type(encoded_data)}")
                    return False
                
                # Verify PDF header
                if not pdf_data.startswith(b'%PDF'):
                    print(f"[PDF UPLOAD] Warning: {task['key']} doesn't start with PDF header")
                    print(f"[PDF UPLOAD] First 20 bytes: {pdf_data[:20]}")
                
                # Upload with enriched metadata
                db_function.s3_client.put_object(
                    Bucket=db_function.bucket_name,
                    Key=task['key'],
                    Body=pdf_data,
                    ContentType='application/pdf',
                    Metadata=task['metadata']  # Include all metadata from task
                )
                
                print(f"[PDF UPLOAD] Uploaded: {task['key']} ({len(pdf_data)} bytes) "
                      f"[Company: {task['metadata']['company'][:30]}..., Category: {task['metadata']['broker_category']}]")
                return True
                
            except Exception as e:
                print(f"[PDF UPLOAD] Error uploading {task['key']}: {str(e)}")
                import traceback
                traceback.print_exc()
                return False
    
    # Upload PDFs in parallel
    successful_uploads = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(upload_single_pdf, task) for task in upload_tasks]
        
        for future in concurrent.futures.as_completed(futures):
            if future.result():
                successful_uploads += 1
    
    print(f"[PDF UPLOAD] Successfully uploaded {successful_uploads}/{len(upload_tasks)} PDFs with metadata")

#results_subject -> [classification: , threadid: ]
#response -> [{"threadid": ,"from_": , "subject": ,"pdf_contents": ..., "email_text": ...}]
def filter_response_on_subject_output(results_subject, response):
    relevant_list = []
    #subject -> {classification, threadid}
    for subject in results_subject:
        for subject_ind in subject.subject_individual:
            if subject_ind.is_relevant:
                for result in response:
                    if subject_ind.threadid == result["threadid"]:
                        relevant_list.append(result)    
    return relevant_list

def get_size(obj, seen=None):
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size