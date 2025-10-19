import json
import os
import email_collection
import boto3
import hashlib
from datetime import datetime

sqs = boto3.client('sqs')
s3 = boto3.client('s3')

BUCKET_NAME = os.environ.get('AWS_S3_BUCKET_NAME')
NEXT_QUEUE_URL = os.environ.get('NEXT_QUEUE_URL')
NEXT_QUEUE_IS_FIFO = False
USER_KEY_QUEUE_URL = os.environ.get('USER_KEY_QUEUE_URL')

def lambda_handler(event, context):
    #have to implement check for gmail access token expiration
    #see how to add to emails endpoint without having to retrieve the emails first

    for message in event['Records']:
        # Parse the message body
        body = json.loads(message['body'])
        
        # Extract basic fields
        thread_ids = body['thread_ids']
        user_token = body['user_token']
        user_email = body['user_email']
        
        # Extract metadata if it exists
        job_metadata = body.get('job_metadata', None)
        
        # Get retry count for requeue purposes
        original_retry = int(body.get('retry_count', 0))
        
        print(f"Processing - thread_ids: {len(thread_ids)}, user_email: {user_email}")
        if job_metadata:
            print(f"Job metadata - job_id: {job_metadata['job_id']}, batch: {job_metadata['batch_number']}/{job_metadata['total_batches']}")
        
        # Hash the email for privacy
        user_email_hash = hashlib.sha256(user_email.encode('utf-8')).hexdigest()

        
        # Process the threads
        threads_list, threads_yet_to_process = email_collection.main(thread_ids, user_token, user_email_hash)
        print(threads_yet_to_process)
        if threads_yet_to_process:
            # There are still threads to process - requeue them with metadata
            requeue_threads(
                threads=threads_yet_to_process,
                user_token=user_token,
                user_email=user_email,
                original_retry=original_retry,
                job_metadata=job_metadata  # Pass metadata along
            )
            print(f"Requeued {len(threads_yet_to_process)} threads for retry")
            
        else:
            # This batch is complete - track completion if we have metadata
            if job_metadata:
                print(f"Batch {job_metadata['batch_number']} complete for job {job_metadata['job_id']}")
                is_complete = track_batch_completion_s3(job_metadata, user_email)
                
                if is_complete:
                    # All batches are complete - send to final processing
                    send_user_key_to_queue(user_email)
                    print(f"All batches complete for job {job_metadata['job_id']} - sent user_key to next SQS for processing")
                else:
                    print(f"Batch {job_metadata['batch_number']} complete but waiting for other batches")
            else:
                # No metadata means this is a single batch job - send directly to final processing
                send_user_key_to_queue(user_email)
                print("Single batch complete - sent user_key to next SQS for processing")
        
    return {"status": "ok"}

def requeue_threads(threads, user_token, user_email, original_retry=0, job_metadata=None):
    """
    threads: list of thread ids to requeue (all threads in a single message)
    user_token/user_email: context to include
    original_retry: integer from original message (if any)
    job_metadata: metadata about the job/batch to preserve
    """
    if not NEXT_QUEUE_URL:
        raise RuntimeError("NEXT_QUEUE_URL environment variable is not set")
    
    # Create message body with all threads together
    message_body = {
        "thread_ids": threads,  # Send ALL threads in one message
        "user_token": user_token,
        "user_email": user_email,
        "retry_count": original_retry + 1
    }
    
    # Include job metadata if it exists
    if job_metadata:
        message_body["job_metadata"] = job_metadata
    
    # Create a single entry with all threads
    entry = {
        "Id": "0",  # Single entry ID
        "MessageBody": json.dumps(message_body)
    }
    
    # If the target queue is FIFO, provide MessageGroupId and DeduplicationId
    if NEXT_QUEUE_IS_FIFO:
        entry["MessageGroupId"] = user_email or "default-group"
        # Use a combination of retry count and thread count for deduplication
        entry["MessageDeduplicationId"] = f"{user_email}-retry{original_retry + 1}-count{len(threads)}"
    
    # Send the single message with all threads
    try:
        resp = sqs.send_message_batch(QueueUrl=NEXT_QUEUE_URL, Entries=[entry])
    except Exception as exc:
        print(f"Failed to send_message_batch to {NEXT_QUEUE_URL}: {exc}")
        raise
    
    # Handle per-entry failures returned by SQS
    failed = resp.get('Failed', [])
    if failed:
        print(f"SQS send_message_batch had failures: {failed}")
        raise RuntimeError(f"SQS batch send had failures: {failed}")
    
    # Log success
    successful = resp.get('Successful', [])
    if successful:
        print(f"Requeued {len(threads)} threads in a single message to {NEXT_QUEUE_URL}")

def send_user_key_to_queue(user_key):
    """
    Send a single user_key to a dedicated SQS queue.
    """
    if not USER_KEY_QUEUE_URL:
        raise RuntimeError("USER_KEY_QUEUE_URL environment variable is not set")
    
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

def track_batch_completion_s3(metadata, user_email):
    """
    Track completion using S3 objects.
    Returns True if all batches are complete, False otherwise.
    """
    if not BUCKET_NAME:
        raise RuntimeError("AWS_S3_BUCKET_NAME environment variable is not set")
    
    job_id = metadata['job_id']
    batch_number = metadata['batch_number']
    total_batches = metadata['total_batches']
    
    # Write a marker file for this batch
    key = f"jobs/{job_id}/batch_{batch_number}.json"
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps({
                'batch_number': batch_number,
                'completed_at': datetime.now().isoformat(),
                'user_email': user_email,
                'total_threads': metadata.get('total_threads', 'unknown')
            })
        )
        print(f"Written completion marker for batch {batch_number} to S3: {key}")
    except Exception as e:
        print(f"Failed to write batch marker to S3: {e}")
        raise
    
    # Check if all batches are complete
    prefix = f"jobs/{job_id}/batch_"
    try:
        response = s3.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=prefix
        )
        
        # Count completed batch markers (excluding the _completed.json file)
        completed_count = 0
        if 'Contents' in response:
            for obj in response['Contents']:
                if not obj['Key'].endswith('_completed.json'):
                    completed_count += 1
        
        print(f"Job {job_id}: {completed_count}/{total_batches} batches completed")
        
        if completed_count >= total_batches:
            # All batches complete - write completion marker
            completion_key = f"jobs/{job_id}/_completed.json"
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=completion_key,
                Body=json.dumps({
                    'job_id': job_id,
                    'total_batches': total_batches,
                    'completed_at': datetime.now().isoformat(),
                    'user_email': user_email
                })
            )
            print(f"Job {job_id} fully complete - written completion marker to {completion_key}")
            return True
            
    except Exception as e:
        print(f"Failed to check batch completion status: {e}")
        raise
    
    return False