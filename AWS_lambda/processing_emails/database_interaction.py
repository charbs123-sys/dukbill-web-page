import gzip
import os
import requests
from dateutil.relativedelta import relativedelta
from datetime import datetime
import json

        
os.environ['AWS_EC2_METADATA_DISABLED'] = 'true'

class Database:
    def __init__(self, final_json, user_key, raw_emails, entry_id=None):
        print(f"[Database Init] Starting initialization for user: {user_key}")
        
        # Disable EC2 metadata lookups that can hang
        os.environ['AWS_EC2_METADATA_DISABLED'] = 'true'
        
        self.final_json = final_json #anonymized json
        self.user_key = user_key #user email
        self.raw_emails = raw_emails #raw emails
        self.existing_data = None
        
        self.bucket_name = os.getenv('AWS_S3_BUCKET_NAME')
        print(f"bucket name is: {self.bucket_name}")
        
        if not self.bucket_name:
            raise ValueError("AWS_S3_BUCKET_NAME environment variable not set")
        
        try:
            print("[Database Init] Creating S3 client...")
            
            # Use a more explicit configuration
            import botocore.session
            session = botocore.session.Session()
            
            # Set timeouts
            config = botocore.config.Config(
                region_name='ap-southeast-2',
                signature_version='v4',
                retries={
                    'max_attempts': 1,
                    'mode': 'standard'
                },
                connect_timeout=5,
                read_timeout=5
            )
            
            self.s3_client = session.create_client('s3', 'ap-southeast-2', config=config)
            print("[Database Init] S3 client created successfully")
            
            # Don't test connection in __init__ - do it lazily
            print("[Database Init] Skipping connection test in init")
            
        except Exception as e:
            print(f"[Database Init] ERROR with S3: {e}")
            raise
        
        print("[Database Init] Database initialization complete")


    def update_endpoint(self, items, path):
        self.upload_compressed_emails_to_s3(items, self.bucket_name, path, self.s3_client)

    def retrieve_data(self, path):
        return self.retrieving_raw_emails_from_s3(path, self.bucket_name, self.s3_client)

    def add_anonymized_threads(self, anonymized_emails, path):
        # Retrieve existing data from S3
        existing_threads = self.retrieve_data(path)
        
        # Convert to list if it's a dict
        if isinstance(existing_threads, dict):
            existing_threads = list(existing_threads.values())
        elif not isinstance(existing_threads, list):
            existing_threads = []
        
        # Append each new email to the list
        if isinstance(anonymized_emails, list):
            for email in anonymized_emails:
                existing_threads.append(email)
        
        # Upload the updated list back to S3
        self.update_endpoint(existing_threads, path)
        return existing_threads

    
    def retrieving_raw_emails_from_s3(self, endpoint, bucket_name, s3_client):
        try:
            s3_object = s3_client.get_object(Bucket=bucket_name, Key=endpoint)
            compressed_body = s3_object['Body'].read()
            
            # Return empty dict/list if file is empty
            if not compressed_body:
                return {}
            
            decompressed_body = gzip.decompress(compressed_body)
            data = json.loads(decompressed_body.decode('utf-8'))
            return data
            
        except (s3_client.exceptions.NoSuchKey, 
                gzip.BadGzipFile, 
                json.JSONDecodeError,
                EOFError) as e:
            print(f"Warning: Could not retrieve/parse data from {endpoint}: {e}")
            return {}  # or raise exception if you want calling code to handle it

    
    def upload_compressed_emails_to_s3(self, email_data, bucket_name, s3_key, s3_client):
        """
        Compress email data as gzipped JSON and upload to S3
        """
        try:
            # Convert to JSON string
            json_string = json.dumps(email_data, ensure_ascii=False)
            
            # Encode as UTF-8
            utf8_bytes = json_string.encode('utf-8')
            
            # Compress with gzip
            compressed_data = gzip.compress(utf8_bytes)
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=compressed_data,
                ContentType='application/json',
                ContentEncoding='gzip'
            )
            
            print(f"[S3 Upload] Successfully uploaded compressed emails to {s3_key}")
            print(f"[S3 Upload] Original size: {len(utf8_bytes)} bytes, Compressed size: {len(compressed_data)} bytes")
            
        except Exception as e:
            print(f"[S3 Upload Error] Failed to upload compressed emails: {e}")
            raise

    def add_one_month(self, ts):
        try:
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            dt_plus_one_month = dt + relativedelta(months=1)
            return dt_plus_one_month.isoformat().replace('+00:00', 'Z')
        except Exception as e:
            print(f"[Date Error] Failed to add one month to {ts}: {e}")
            return ts  # Return original as fallback

    def save_next_trigger(self, earliest_entry):

        try:
            next_trigger = self.add_one_month(earliest_entry["timestamp"])
            dict_trigger = {"timestamp": next_trigger}

            self.next_trigger_reference.set(dict_trigger)
        except requests.RequestException as e:
            print(f"[HTTP Error - PUT] Failed to save next trigger: {e}")
        except Exception as e:
            print(f"[Processing Error] Could not save next trigger: {e}")

    def delete_trigger_raw_anonymized(self, earliest_key):

        try:
            self.broker_scan.delete()
            self.broker_trigger_reference.delete()
            self.current_trigger_reference.delete()
            self.raw_email_reference.delete()
            #self.anonymized_email_reference.delete()
        except requests.RequestException as e:
            print(f"[HTTP Error - DELETE] Failed to delete trigger for {earliest_key}: {e}")