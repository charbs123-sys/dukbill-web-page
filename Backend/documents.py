from db_utils import *
from S3_utils import *
from helper import *
from config import DOCUMENT_CATEGORIES
from fastapi import UploadFile
import uuid
from redis_utils import (
    get_or_load_emails_json, 
    save_emails_json_to_cache,  # NEW - write-back
    force_sync_to_s3  # NEW - for critical operations
)

def get_client_dashboard(client_id: str, emails: list) -> list:
    """
    Returns a structured dashboard for a client based on all emails.
    """
    if not verify_client_by_id(client_id):
        raise HTTPException(status_code=403, detail="Invalid client")

    all_documents = []

    for email_entry in emails:
        email = email_entry["email_address"] if isinstance(email_entry, dict) else email_entry
        hashed_email = hash_email(email)

        try:
            documents = get_or_load_emails_json(hashed_email, "/broker_anonymized/emails_anonymized.json")
            for doc in documents:
                doc["hashed_email"] = hashed_email
            all_documents.extend(documents)
        except HTTPException:
            continue

    categories_map = {}
    for doc in all_documents:
        category = doc.get("broker_document_category", "Uncategorized")
        for heading, cat_list in DOCUMENT_CATEGORIES.items():
            if category in cat_list:
                categories_map.setdefault(category, []).append({
                    "id": doc.get("threadid"),
                    "company_name": doc.get("company", "Unknown"),
                    "payment_amount": parse_amount(doc.get("amount")),
                    "due_date": normalize_date(doc.get("date")),
                    "hashed_email": doc.get("hashed_email"),
                })
                break

    categories_present = set(doc.get("broker_document_category", "Uncategorized") for doc in all_documents)

    headings = []
    for heading, cat_list in DOCUMENT_CATEGORIES.items():
        categories = [
            {"category_name": cat, "cards": categories_map.get(cat, [])}
            for cat in cat_list
            if cat in categories_map
        ]
        missing = [cat for cat in cat_list if cat not in categories_present]
        headings.append({
            "heading": heading,
            "categories": categories,
            "missing_categories": missing
        })
    return headings

def get_client_verified_ids_dashboard(client_id: str, emails: list) -> list:
    """
    Returns a structured dashboard for a client based on verified IDs.
    """
    if not verify_client_by_id(client_id):
        raise HTTPException(status_code=403, detail="Invalid client")

    all_documents = []

    for email_entry in emails:
        email = email_entry["email_address"] if isinstance(email_entry, dict) else email_entry
        hashed_email = hash_email(email)

        try:
            from S3_utils import list_s3_files
            verified_ids_path = "/verified_ids"
            files = list_s3_files(hashed_email, verified_ids_path)
            
            pdf_files = [f for f in files if f.endswith('.pdf')]
            
            # Group files by document type
            doc_type_map = {}
            for filename in pdf_files:
                # Extract document type (everything before _front or _back)
                if "_front.pdf" in filename:
                    doc_type = filename.replace("_front.pdf", "")
                elif "_back.pdf" in filename:
                    doc_type = filename.replace("_back.pdf", "")
                else:
                    # For passport or other single-sided docs
                    doc_type = filename.replace(".pdf", "")
                
                if doc_type not in doc_type_map:
                    doc_type_map[doc_type] = []
                doc_type_map[doc_type].append(filename)
            
            # Create an entry for each document type
            for doc_type, file_list in doc_type_map.items():
                all_documents.append({
                    "hashed_email": hashed_email,
                    "doc_type": doc_type,
                    "files": file_list
                })
        except Exception:
            continue

    # Group by document type for categories
    categories_map = {}
    for doc in all_documents:
        doc_type = doc.get("doc_type")
        formatted_type = doc_type.replace("_", " ").title()  # e.g., "Driving License"
        
        if formatted_type not in categories_map:
            categories_map[formatted_type] = []
        
        categories_map[formatted_type].append({
            "id": doc_type,  # e.g., "driving_license"
            "company_name": formatted_type,  # e.g., "Driving License"
            "payment_amount": 0,
            "due_date": None,
            "hashed_email": doc.get("hashed_email"),
            "files": doc.get("files", []),
            "file_count": len(doc.get("files", []))
        })

    headings = []
    if categories_map:
        categories = [
            {
                "category_name": category_name,
                "cards": cards
            }
            for category_name, cards in categories_map.items()
        ]
        
        headings.append({
            "heading": "Identity Verification",
            "categories": categories,
            "missing_categories": []
        })
    
    return headings


def get_client_category_documents(client_id: str, emails: list, category: str) -> list:
    """
    Returns all documents for a client filtered by category across multiple emails.
    """
    if not verify_client_by_id(client_id):
        raise HTTPException(status_code=403, detail="Invalid client")

    all_filtered_docs = []

    for email_entry in emails:
        email = email_entry["email_address"] if isinstance(email_entry, dict) else email_entry
        hashed_email = hash_email(email)

        try:
            documents = get_or_load_emails_json(hashed_email, "/broker_anonymized/emails_anonymized.json")
        except HTTPException:
            continue

        prefix = f"{hashed_email}/categorised/{category}/truncated/"

        s3_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        files = s3_objects.get("Contents", [])

        threadid_to_keys = {}
        for obj in files:
            key = obj["Key"]
            filename = key.split("/")[-1]
            for doc in documents:
                threadid = doc.get("threadid")
                if not threadid:
                    continue
                if filename.startswith(threadid + "_") or filename.startswith(threadid):
                    threadid_to_keys.setdefault(threadid, []).append(key)

        for doc in documents:
            if doc.get("broker_document_category", "Uncategorized") != category:
                continue
            threadid = doc.get("threadid")
            pdf_keys = threadid_to_keys.get(threadid, [])
            if not pdf_keys:
                continue

            urls = [get_cloudfront_url(k) for k in pdf_keys]

            all_filtered_docs.append({
                "id": threadid,
                "category": category,
                "company": doc.get("company", "Unknown"),
                "amount": parse_amount(doc.get("amount")),
                "due_date": normalize_date(doc.get("date")),
                "url": urls,
                "hashed_email": hashed_email,
            })
    return all_filtered_docs

def get_client_verified_ids_documents(client_id: str, emails: list, category: str) -> list:
    """
    Returns all verified ID documents for a client across multiple emails filtered by category.
    
    Args:
        client_id: The client identifier
        emails: List of email addresses
        category: The document category to filter (e.g., "Driving License", "Id Card", "Passport")
    """
    if not verify_client_by_id(client_id):
        raise HTTPException(status_code=403, detail="Invalid client")

    all_verified_docs = []
    
    # Convert category to the file prefix format (e.g., "Driving License" -> "driving_license")
    category_to_prefix = {
        "Driving License": "driving_license",
        "Id Card": "id_card",
        "Passport": "passport"
    }
    
    # Get the prefix for the requested category
    doc_type_prefix = category_to_prefix.get(category)
    
    # If category is not a verified ID type, return empty list
    if not doc_type_prefix:
        return []

    for email_entry in emails:
        email = email_entry["email_address"] if isinstance(email_entry, dict) else email_entry
        hashed_email = hash_email(email)

        try:
            prefix = f"{hashed_email}/verified_ids/"
            s3_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            files = s3_objects.get("Contents", [])

            pdf_keys = [obj["Key"] for obj in files if obj["Key"].endswith('.pdf')]
            
            # Filter PDFs by the requested document type
            doc_type_keys = []
            for key in pdf_keys:
                filename = key.split("/")[-1]
                # Check if filename starts with the requested doc_type_prefix
                if filename.startswith(doc_type_prefix):
                    doc_type_keys.append(key)
            
            # Only create entry if matching files found
            if doc_type_keys:
                urls = [get_cloudfront_url(k) for k in doc_type_keys]
                
                all_verified_docs.append({
                    "id": f"{hashed_email}_{doc_type_prefix}",  # Unique ID per doc type
                    "category": "Verified IDs",
                    "company": category,  # Use the formatted category name
                    "amount": 0,
                    "due_date": None,
                    "url": urls,
                    "hashed_email": hashed_email,
                })
        except Exception as e:
            logging.error(f"Error fetching verified IDs for {hashed_email}: {e}")
            continue

    return all_verified_docs


def move_pdfs_to_new_category(hashed_email: str, threadid: str, old_category: str, new_category: str) -> None:
    """
    Moves PDFs from an old category folder to a new category folder in S3.
    """
    folders = ["pdfs", "truncated"]

    for folder in folders:
        old_prefix = f"{hashed_email}/categorised/{old_category}/{folder}/"
        new_prefix = f"{hashed_email}/categorised/{new_category}/{folder}/"

        s3_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=old_prefix)
        files = s3_objects.get("Contents", [])

        for obj in files:
            key = obj["Key"]
            filename = key.split("/")[-1]
            if filename.startswith(threadid + "_") or filename.startswith(threadid):
                new_key = new_prefix + filename
                s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': key}, Key=new_key)
                s3.delete_object(Bucket=bucket_name, Key=key)


def edit_client_document(hashed_email: str, update_data: dict) -> dict:
    """
    Edits a client document metadata and moves PDFs if category changes.
    """
    card_id = update_data.get("id")
    if not card_id:
        raise HTTPException(status_code=400, detail="Missing document id")

    documents = get_or_load_emails_json(hashed_email, "/broker_anonymized/emails_anonymized.json")

    doc_index = next((i for i, d in enumerate(documents) if d.get("threadid") == card_id), None)
    if doc_index is None:
        raise HTTPException(status_code=404, detail=f"Document with id '{card_id}' not found")

    old_category = documents[doc_index].get("broker_document_category")

    field_mapping = {
        "id": "threadid",
        "category": "broker_document_category",
        "company": "company",
        "amount": "amount",
        "date": "date"
    }

    for frontend_field, json_field in field_mapping.items():
        if frontend_field in update_data:
            value = update_data[frontend_field]
            if frontend_field == "date" and value:
                try:
                    documents[doc_index][json_field] = datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")
                except ValueError:
                    raise HTTPException(status_code=400, detail="Invalid date format, must be YYYY-MM-DD")
            elif frontend_field == "amount" and value is not None:
                documents[doc_index][json_field] = float(value)
            else:
                documents[doc_index][json_field] = value

    save_emails_json_to_cache(hashed_email, documents)

    new_category = documents[doc_index].get("broker_document_category")
    if old_category != new_category:
        move_pdfs_to_new_category(hashed_email, card_id, old_category, new_category)

    return documents[doc_index]


def delete_client_document(hashed_email: str, threadid: str) -> None:
    """
    Deletes a client document metadata and associated PDFs in S3.
    """
    if not threadid:
        raise HTTPException(status_code=400, detail="Missing threadid")

    documents = get_or_load_emails_json(hashed_email, "/broker_anonymized/emails_anonymized.json")
    doc_index = next((i for i, d in enumerate(documents) if d.get("threadid") == threadid), None)
    if doc_index is None:
        raise HTTPException(status_code=404, detail=f"Document with threadid '{threadid}' not found")

    doc_to_delete = documents.pop(doc_index)
    save_emails_json_to_cache(hashed_email, documents)

    category = doc_to_delete.get("broker_document_category", "Uncategorized")
    hashed_email = hash_email(hashed_email)

    prefixes = [
        f"{hashed_email}/categorised/{category}/pdfs/",
        f"{hashed_email}/categorised/{category}/truncated/",
    ]

    for prefix in prefixes:
        s3_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        files = s3_objects.get("Contents", [])

        for obj in files:
            key = obj["Key"]
            filename = key.split("/")[-1]
            if filename.startswith(threadid + "_") or filename.startswith(threadid):
                s3.delete_object(Bucket=bucket_name, Key=key)


def delete_client_document_identity(doc_name: str, hashed_email: str):
    """
    Delete verified identity documents (both front and back) from S3.
    
    Args:
        doc_name: The base document name (e.g., "driving_license", "id_card")
        hashed_email: The hashed email identifier
    """
    try:
        # Construct the S3 keys for front and back
        front_key = f"{hashed_email}/verified_ids/{doc_name}_front.pdf"
        back_key = f"{hashed_email}/verified_ids/{doc_name}_back.pdf"
        
        # List of keys to delete
        keys_to_delete = [front_key, back_key]
        
        # Delete each file
        deleted_count = 0
        for key in keys_to_delete:
            try:
                s3.delete_object(Bucket=bucket_name, Key=key)
                logging.info(f"✓ Deleted {key}")
                deleted_count += 1
            except Exception as e:
                # File might not exist (e.g., passport has no back)
                logging.warning(f"Could not delete {key}: {e}")
        
        if deleted_count > 0:
            logging.info(f"✓ Deleted {deleted_count} file(s) for {doc_name}")
            return True
        else:
            logging.warning(f"No files deleted for {doc_name}")
            return False
            
    except Exception as e:
        logging.error(f"Error deleting identity documents: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete identity documents")


async def upload_client_document(client_email: str, category: str, company: str, amount: float, date: str, file: UploadFile) -> dict:
    """
    Uploads a new client document to S3 (full PDF and truncated first page)
    and updates the JSON metadata file. Ensures metadata file exists.
    """
    hashed_email = hash_email(client_email)

    ensure_json_file_exists(hashed_email, "/broker_anonymized/emails_anonymized.json")
    documents = get_or_load_emails_json(hashed_email, "/broker_anonymized/emails_anonymized.json")

    threadid = str(uuid.uuid4())
    filename = f"{threadid}_1_{file.filename}"
    pdf_key = f"{hashed_email}/categorised/{category}/pdfs/{filename}"
    truncated_key = f"{hashed_email}/categorised/{category}/truncated/{filename}"

    file_bytes = await file.read()

    s3.upload_fileobj(io.BytesIO(file_bytes), bucket_name, pdf_key, ExtraArgs={"ContentType": "application/pdf"})

    truncated_bytes = truncate_pdf(file_bytes)
    if truncated_bytes:
        s3.upload_fileobj(io.BytesIO(truncated_bytes), bucket_name, truncated_key, ExtraArgs={"ContentType": "application/pdf"})

    new_doc = {
        "threadid": threadid,
        "broker_document_category": category,
        "company": company,
        "amount": float(amount),
        "date": date,
        "uploaded_at": datetime.utcnow().isoformat()
    }
    documents.append(new_doc)
    
    save_emails_json_to_cache(hashed_email, documents)

    return new_doc

async def upload_bytes_to_s3(file_bytes: bytes, s3_key: str, bucket_name: str = None):
    """
    Upload bytes directly to S3 without saving to disk first
    
    Args:
        file_bytes: File content as bytes
        s3_key: S3 key/path (e.g., "user_123/ref-123_front.jpg")
        bucket_name: S3 bucket name (optional, reads from env if not provided)
    
    Returns:
        str: S3 URL of uploaded file, or None if failed
    """
    if bucket_name is None:
        bucket_name = os.environ.get("S3_BUCKET_NAME")
    
    if not bucket_name:
        print("❌ S3_BUCKET_NAME not configured")
        return None
    
    try:
        # Upload bytes directly
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=file_bytes,
            ContentType='application/pdf',
            ACL='private'
        )
        
        s3_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"
        print(f"✅ Uploaded to S3: {s3_url}")
        
        return s3_url
        
    except ClientError as e:
        print(f"❌ Failed to upload to S3: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error uploading to S3: {e}")
        return None
    
