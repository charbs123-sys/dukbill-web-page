from Database.db_utils import *
from Database.S3_utils import *
from helpers.helper import *
from config import DOCUMENT_CATEGORIES
from fastapi import UploadFile
import uuid
import os

# ------------------------
# Scanned Email Documents
# ------------------------
def get_client_dashboard(client_id: str, emails: list) -> list:
    """
    Returns a structured dashboard for a client based on all emails.
    
    headings of the form

    [
        {
            heading: "Income & Employment Documents",
            categories: 
            [
                {
                    "category_name": category_name,
                    "cards": 
                    [
                        {
                            "id": id
                            "category_data": 
                            {
                                data1, data2, data3
                            },
                            "hashed_email": doc.get("hashed_email"),
                            "broker_comment" : broker_comment,
                        }
                    ]
                }
            ]
        }
    ]

    """
    if not verify_client_by_id(client_id):
        raise HTTPException(status_code=403, detail="Invalid client")

    all_documents = []

    for email_entry in emails:
        email = email_entry["email_address"] if isinstance(email_entry, dict) else email_entry
        hashed_email = hash_email(email)

        try:
            documents = get_json_file(hashed_email, "/broker_anonymized/emails_anonymized.json")
            for doc in documents:
                doc["hashed_email"] = hashed_email
            all_documents.extend(documents)
        except HTTPException:
            continue
        
    categories_map = {}
    xero_map = {}
    myob_map = {}
    for doc in all_documents:
        category = doc.get("broker_document_category", "Uncategorized")
        for heading, cat_list in DOCUMENT_CATEGORIES.items():
            if category in cat_list:
                categories_map.setdefault(category, []).append({
                    "id": doc.get("threadid"),
                    "category_data": doc.get("category_data"),
                    "hashed_email": doc.get("hashed_email"),
                    "broker_comment": doc.get("broker_comment", "")
                })
                break

        for xero_report in doc.get("xero_reports", []):
            xero_map.setdefault(xero_report.get("filename", ""), []).append({
                "id": xero_report.get("filename", ""),
                "category_data": [],
                "hashed_email": doc.get("hashed_email"),
                "broker_comment": xero_report.get("broker_comment", ""),
            })   

        for myob_report in doc.get("myob_reports", []):
            myob_map.setdefault(myob_report.get("filename", ""), []).append({
                "id": myob_report.get("filename", ""),
                "category_data": [],
                "hashed_email": doc.get("hashed_email"),
                "broker_comment": myob_report.get("broker_comment", "")
            })


    categories_present = set(doc.get("broker_document_category", "Uncategorized") for doc in all_documents)
    
    headings = []
    
    #standard headings
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

    #xero headings
    if xero_map:
        xero_categories = [
            {
                "category_name": category_name,
                "cards": cards
            }
            for category_name, cards in xero_map.items()
        ]

        headings.append({
            "heading": "Xero Reports",
            "categories": xero_categories,
            "missing_categories": []
        })

    #myob headings
    if myob_map:
        myob_categories = [
            {
                "category_name": category_name,
                "cards": cards
            }
            for category_name, cards in myob_map.items()
        ]

        headings.append({
            "heading": "MYOB Reports",
            "categories": myob_categories,
            "missing_categories": []
        })

    return headings

###provide better comments for this
def get_client_category_documents(client_id: str, emails: list, category: str) -> list:
    """
    Returns all documents for a client filtered by category across multiple emails.

    client_id (str): the client id
    emails (list): List of email addresses
    category (str): The document category to filter (e.g., "Income & Employment Documents")

    Returns:
        list: List of documents in the specified category
    """
    if not verify_client_by_id(client_id):
        raise HTTPException(status_code=403, detail="Invalid client")

    all_filtered_docs = []

    for email_entry in emails:
        email = email_entry["email_address"] if isinstance(email_entry, dict) else email_entry
        hashed_email = hash_email(email)

        try:
            documents = get_json_file(hashed_email, "/broker_anonymized/emails_anonymized.json")
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
                "category_data": doc.get("category_data"),
                "url": urls,
                "hashed_email": hashed_email,
            })

    return all_filtered_docs

# ------------------------
# Verified Id Documents
# ------------------------
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
            from Database.S3_utils import list_s3_files
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
            "category_data": {
                "Identity Verification": formatted_type  # preserve the previous company_name
            },
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

            # Filter PDFs by the requested document type
            doc_type_keys = []
            for obj in files:
                key = obj["Key"]
                if not key.endswith('.pdf'):
                    continue
                filename = key.split("/")[-1]
                # Check if filename starts with the requested doc_type_prefix
                if filename.startswith(doc_type_prefix):
                    doc_type_keys.append(key)
            
            # Only create entry if matching files found
            if doc_type_keys:
                urls = [get_cloudfront_url(k) for k in doc_type_keys]
                
                all_verified_docs.append({
                    "id": f"{hashed_email}_{doc_type_prefix}",  # Unique ID per doc type
                    "category": category,  # Use the actual category parameter
                    "category_data": {
                        "Identity Verification": category
                    },
                    "url": urls,
                    "hashed_email": hashed_email,
                })

        except Exception as e:
            logging.error(f"Error fetching verified IDs for {hashed_email}: {e}")
            continue

    return all_verified_docs

# ------------------------
# Xero Documents
# ------------------------
def update_anonymized_json_general(hashed_email: str, parent_header: str, sibling_header: list[str]) -> None:

    # Implementation for updating anonymized JSON
    ensure_json_file_exists(hashed_email, "/broker_anonymized/emails_anonymized.json")
    documents = get_json_file(hashed_email, "/broker_anonymized/emails_anonymized.json")
    documents.append({parent_header: [{"filename": sibling} for sibling in sibling_header]})
    save_json_file(hashed_email, "/broker_anonymized/emails_anonymized.json", documents)
    return True

# ------------------------
# MYOB Documents
# ------------------------
#provide better comments
def get_docs_general(client_id: str, emails: list, category: str) -> list:
    """
    General dispatcher to get documents based on category.
    """
    if not verify_client_by_id(client_id):
        raise HTTPException(status_code=403, detail="Invalid client")

    docs = []
    
    #determine type of document to fetch
    if category.startswith("Broker_"):
        prefix = "myob_reports"
        category_label = "MYOB Reports"
        category_to_filename = {
            "Broker_Payroll_Summary.pdf": "Payroll Summary",
            "Broker_Sales_Summary.pdf": "Sales Summary",
            "Broker_Banking_Summary.pdf": "Banking Summary",
            "Broker_Purchases_Summary.pdf": "Purchases Summary",
        }
    elif category.startswith("xero_"):
        prefix = "xero_reports"
        category_label = "Xero Reports"
        category_to_filename = {
            "xero_accounts_report.pdf": "Accounts Report",
            "xero_bank_transfers_report.pdf": "Bank Transfers Report",
            "xero_credit_notes_report.pdf": "Credit Notes Report",
            "xero_financial_reports.pdf": "Financial Reports",
            "xero_invoices_report.pdf": "Invoices Report",
            "xero_payments_report.pdf": "Payments Report",
            "xero_payroll_report.pdf": "Payroll Report",
            "xero_transactions_report.pdf": "Transactions Report",
        }
    else:
        return []
    # Get the filename for the requested category
    doc_category_name = category_to_filename.get(category)
    # If category is not any of the above report types, return empty list
    if not doc_category_name:
        return []

    for email_entry in emails:
        email = email_entry["email_address"] if isinstance(email_entry, dict) else email_entry
        hashed_email = hash_email(email)

        try:
            s3_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=hashed_email + '/' + prefix)
            files = s3_objects.get("Contents", [])

            pdf_keys = [obj["Key"] for obj in files if obj["Key"].endswith('.pdf')]
            # Filter PDFs by the requested document type
            doc_type_keys = []
            for key in pdf_keys:
                filename = key.split("/")[-1]
                # Check if filename matches the requested doc_filename
                if filename == f"{category}":
                    doc_type_keys.append(key)
            
            # Only create entry if matching files found
            if doc_type_keys:
                urls = [get_cloudfront_url(k) for k in doc_type_keys]
                            
            docs.append({
                "id": f"{hashed_email}_{category}",  # Unique ID per doc type
                "category": category_label,
                "category_data": {
                    category_label: doc_category_name  # preserve the previous company/category value
                },
                "url": urls,
                "hashed_email": hashed_email,
            })
        
        except Exception as e:
            logging.error(f"Error fetching MYOB reports for {hashed_email}: {e}")
            continue

    return docs

# ------------------------
# Upload Documents
# ------------------------
async def upload_client_document(client_email: str, category: str, category_data: dict, file: UploadFile) -> dict:
    """
    Uploads a new client document to S3 and updates the JSON metadata file.
    Expects category_data as a dictionary.
    """
    hashed_email = hash_email(client_email)

    ensure_json_file_exists(hashed_email, "/broker_anonymized/emails_anonymized.json")
    documents = get_json_file(hashed_email, "/broker_anonymized/emails_anonymized.json")

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
        "category_data": category_data,
        "uploaded_at": datetime.utcnow().isoformat()
    }
    
    documents.append(new_doc)
    save_json_file(hashed_email, "/broker_anonymized/emails_anonymized.json", documents)
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
        
        return s3_url
        
    except ClientError as e:
        return None
    except Exception as e:
        return None

# Adding Comments

def add_comment_docs_general(client_id: str, hashed_email: str, category: str, comment: str, parent_header: str) -> None:
    """
    Adds a broker comment to a general document in the anonymized JSON.
    """
    if not verify_client_by_id(client_id):
        raise HTTPException(status_code=403, detail="Invalid client")

    documents = get_json_file(hashed_email, "/broker_anonymized/emails_anonymized.json")
    updated = False

    for doc in documents:
        for report in doc.get(parent_header, []):
            if report.get("filename") == category:
                existing_comments = report.get("broker_comment", "")
                report["broker_comment"] = existing_comments + "\n" + comment
                updated = True
                break
        if updated:
            break

    if updated:
        save_json_file(hashed_email, "/broker_anonymized/emails_anonymized.json", documents)
    else:
        raise HTTPException(status_code=404, detail=f"Document with category '{category}' not found")

def add_comment_client_document(client_id: str, hashed_email: str, category: str, comment: str) -> None:
    """
    Adds a broker comment to a client document in the anonymized JSON.
    """
    if not verify_client_by_id(client_id):
        raise HTTPException(status_code=403, detail="Invalid client")

    documents = get_json_file(hashed_email, "/broker_anonymized/emails_anonymized.json")
    updated = False

    for doc in documents:
        if doc.get("broker_document_category") == category:
            doc["broker_comment"] = comment
            updated = True
            break

    if updated:
        save_json_file(hashed_email, "/broker_anonymized/emails_anonymized.json", documents)
    else:
        raise HTTPException(status_code=404, detail=f"Document with category '{category}' not found")

#deleting comments

def remove_comment_docs_general(client_id: str, hashed_email: str, category: str, parent_header: str) -> None:
    """
    Removes a broker comment from a general document in the anonymized JSON.

    client_id (str): The client identifier
    hashed_email (str): The hashed email identifier
    category (str): The document category to filter (e.g., "Income & Employment Documents")
    parent_header (str): The parent header in the JSON structure (e.g., "xero_reports", "myob_reports")

    Returns:
        None
    """
    if not verify_client_by_id(client_id):
        raise HTTPException(status_code=403, detail="Invalid client")

    documents = get_json_file(hashed_email, "/broker_anonymized/emails_anonymized.json")
    updated = False

    for doc in documents:
        for report in doc.get(parent_header, []):
            if report.get("filename") == category:
                if "broker_comment" in report:
                    del report["broker_comment"]
                    updated = True
                break
        if updated:
            break

    if updated:
        save_json_file(hashed_email, "/broker_anonymized/emails_anonymized.json", documents)
    else:
        raise HTTPException(status_code=404, detail=f"Document with category '{category}' not found")

def remove_comment_client_document(client_id: str, hashed_email: str, category: str) -> None:
    """
    Removes a broker comment from a client document in the anonymized JSON.
    """
    if not verify_client_by_id(client_id):
        raise HTTPException(status_code=403, detail="Invalid client")

    documents = get_json_file(hashed_email, "/broker_anonymized/emails_anonymized.json")
    updated = False

    for doc in documents:
        if doc.get("broker_document_category") == category:
            if "broker_comment" in doc:
                del doc["broker_comment"]
                updated = True
            break

    if updated:
        save_json_file(hashed_email, "/broker_anonymized/emails_anonymized.json", documents)
    else:
        raise HTTPException(status_code=404, detail=f"Document with category '{category}' not found")

# ------------------------
# Delete Documents
# ------------------------
def delete_client_document(hashed_email: str, threadid: str) -> None:
    """
    Deletes a client document metadata and associated PDFs in S3.
    """
    if not threadid:
        raise HTTPException(status_code=400, detail="Missing threadid")

    documents = get_json_file(hashed_email, "/broker_anonymized/emails_anonymized.json")
    doc_index = next((i for i, d in enumerate(documents) if d.get("threadid") == threadid), None)
    if doc_index is None:
        raise HTTPException(status_code=404, detail=f"Document with threadid '{threadid}' not found")

    doc_to_delete = documents.pop(doc_index)
    save_json_file(hashed_email, "/broker_anonymized/emails_anonymized.json", documents)

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

def delete_email_documents(hashed_email: str) -> None:
    """
    Deletes all associated email documents and folders for a given hashed_email in S3.
    """
    if not hashed_email:
        raise HTTPException(status_code=400, detail="Missing hashed_email")

    # List of files and folders to delete
    targets = [
        f"{hashed_email}/batch_tracker.json",
        f"{hashed_email}/pending_categories.json",
        f"{hashed_email}/broker_anonymized/",
        f"{hashed_email}/broker_filtered/",
        f"{hashed_email}/categorised/",
        f"{hashed_email}/raw_emails_history_broker/"
    ]

    for target in targets:
        if target.endswith('/'):
            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket_name, Prefix=target):
                for obj in page.get("Contents", []):
                    s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
        else:
            try:
                s3.delete_object(Bucket=bucket_name, Key=target)
            except s3.exceptions.NoSuchKey:
                pass

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

#deleting documents for myob and xero, eventually any document type
def delete_docs_general(report_name: str, hashed_email: str, report_type: str):
    """
    Delete a general document from S3.
    
    Args:
        report_name: The name of the document to delete
        hashed_email: The hashed email identifier
    """
    try:
        # Construct the S3 key
        anonymized_key = f"/broker_anonymized/emails_anonymized.json"
        report_key = f"{hashed_email}/{report_type}/{report_name}"
        
        # Delete the file
        try:
            s3.delete_object(Bucket=bucket_name, Key=report_key)
            logging.info(f"✓ Deleted {report_key}")
        except Exception as e:
            logging.error(f"Could not delete {report_key}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to delete document: {e}")
    
        #delete from anonymized json
        documents = get_json_file(hashed_email, anonymized_key)
        for doc in documents:
            reports = doc.get(report_type, [])
            for report_item in reports:
                if report_item.get("filename", "None") == report_name:
                    reports.remove(report_item)
            doc[report_type] = reports
        save_json_file(hashed_email, anonymized_key, documents)

    except Exception as e:
        logging.error(f"Error deleting document: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete document")

# ------------------------
# Move Documents
# ------------------------
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

# ------------------------
# Edit Documents
# ------------------------          
def edit_client_document(hashed_email: str, update_data: dict) -> dict:
    card_id = update_data.get("id")
    if not card_id:
        raise HTTPException(status_code=400, detail="Missing document id")

    documents = get_json_file(hashed_email, "/broker_anonymized/emails_anonymized.json")

    doc_index = next((i for i, d in enumerate(documents) if d.get("threadid") == card_id), None)
    if doc_index is None:
        raise HTTPException(status_code=404, detail=f"Document with id '{card_id}' not found")

    old_category = documents[doc_index].get("broker_document_category")

    if "category" in update_data:
        documents[doc_index]["broker_document_category"] = update_data["category"]

    if "category_data" in update_data:
        documents[doc_index]["category_data"] = dict(update_data["category_data"])
      
    save_json_file(hashed_email, "/broker_anonymized/emails_anonymized.json", documents)

    new_category = documents[doc_index].get("broker_document_category")
    if old_category != new_category:
        move_pdfs_to_new_category(hashed_email, card_id, old_category, new_category)
    
    return documents[doc_index]

# ------------------------
# Download Documents
# ------------------------
def get_download_urls(hashed_email: str, category: str, threadid: str) -> str:
    prefix = f"{hashed_email}/categorised/{category}/pdfs/"
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    files = response.get("Contents", [])
    matched_urls = []

    for obj in files:
        key = obj["Key"]
        filename = key.split("/")[-1]
        if filename.startswith(threadid):
            url = f"https://{CLOUDFRONT_DOMAIN}/{key}"
            matched_urls.append(url)

    if not matched_urls:
        raise FileNotFoundError(f"No PDFs found for threadid '{threadid}' in category '{category}'")

    return matched_urls
