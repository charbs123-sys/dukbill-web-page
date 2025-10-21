from db_utils import *
from S3_utils import *
from helper import *
from config import DOCUMENT_CATEGORIES
from fastapi import UploadFile
import uuid

def get_client_dashboard(client_id, email):
    if not verify_client_by_id(client_id):
        raise HTTPException(status_code=403, detail="Invalid client")

    documents = get_json_file(email, "/broker_anonymized/emails_anonymized.json")

    categories_map = {}
    for doc in documents:
        category = doc.get("broker_document_category", "Uncategorized")
        for heading, cat_list in DOCUMENT_CATEGORIES.items():
            if category in cat_list:
                categories_map.setdefault(category, []).append({
                    "id": doc.get("threadid"),
                    "company_name": doc.get("company", "Unknown"),
                    "payment_amount": parse_amount(doc.get("amount")),
                    "due_date": normalize_date(doc.get("date")),
                })
                break

    # Compute missing categories
    categories_present = set(doc.get("broker_document_category", "Uncategorized") for doc in documents)
    all_categories = {cat for cat_list in DOCUMENT_CATEGORIES.values() for cat in cat_list}

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


def get_client_category_documents(client_id, email, category):
    if not verify_client_by_id(client_id):
        raise HTTPException(status_code=403, detail="Invalid client")
    
    documents = get_json_file(email, "/broker_anonymized/emails_anonymized.json")
    filtered_docs = []

    hashed_email = hash_email(email)
    prefix = f"{hashed_email}/categorised/{category}/pdfs/"
    
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

        urls = [get_presigned_url(k) for k in pdf_keys]

        filtered_docs.append({
            "id": doc.get("threadid"),
            "category": category,
            "company": doc.get("company", "Unknown"),
            "amount": parse_amount(doc.get("amount")),
            "due_date": normalize_date(doc.get("date")),
            "url": urls,
        })
    return filtered_docs

def move_pdfs_to_new_category(email: str, threadid: str, old_category: str, new_category: str):
    hashed_email = hash_email(email)

    old_prefix = f"{hashed_email}/categorised/{old_category}/pdfs/"
    new_prefix = f"{hashed_email}/categorised/{new_category}/pdfs/"

    s3_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=old_prefix)
    files = s3_objects.get("Contents", [])

    for obj in files:
        key = obj["Key"]
        filename = key.split("/")[-1]

        if filename.startswith(threadid + "_") or filename.startswith(threadid):
            new_key = new_prefix + filename

            s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': key}, Key=new_key)
            s3.delete_object(Bucket=bucket_name, Key=key)


def edit_client_document(client_email, update_data):
    card_id = update_data.get("id")
    if not card_id:
        raise HTTPException(status_code=400, detail="Missing document id")

    documents = get_json_file(client_email, "/broker_anonymized/emails_anonymized.json")

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

    save_json_file(client_email, "/broker_anonymized/emails_anonymized.json", documents)

    new_category = documents[doc_index].get("broker_document_category")
    if old_category != new_category:
        move_pdfs_to_new_category(client_email, card_id, old_category, new_category)

    return documents[doc_index]

def delete_client_document(client_email: str, threadid: str):
    if not threadid:
        raise HTTPException(status_code=400, detail="Missing threadid")

    documents = get_json_file(client_email, "/broker_anonymized/emails_anonymized.json")

    doc_index = next((i for i, d in enumerate(documents) if d.get("threadid") == threadid), None)
    if doc_index is None:
        raise HTTPException(status_code=404, detail=f"Document with threadid '{threadid}' not found")

    doc_to_delete = documents.pop(doc_index)

    save_json_file(client_email, "/broker_anonymized/emails_anonymized.json", documents)

    category = doc_to_delete.get("broker_document_category", "Uncategorized")
    hashed_email = hash_email(client_email)
    prefix = f"{hashed_email}/categorised/{category}/pdfs/"

    s3_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = s3_objects.get("Contents", [])

    deleted_files = []

    for obj in files:
        key = obj["Key"]
        filename = key.split("/")[-1]
        if filename.startswith(threadid + "_") or filename.startswith(threadid):
            s3.delete_object(Bucket=bucket_name, Key=key)
            deleted_files.append(key)

    return 

async def upload_client_document(email, category, company, amount, date, file: UploadFile):
    documents = get_json_file(email, "/broker_anonymized/emails_anonymized.json")

    threadid = str(uuid.uuid4())

    hashed_email = hash_email(email)
    file_ext = file.filename.split(".")[-1]
    filename = f"{threadid}_1_{file.filename}"
    s3_key = f"{hashed_email}/categorised/{category}/pdfs/{filename}"

    file_bytes = await file.read()
    s3.upload_fileobj(
        io.BytesIO(file_bytes),
        bucket_name,
        s3_key,
        ExtraArgs={"ContentType": "application/pdf"}
    )

    new_doc = {
        "threadid": threadid,
        "broker_document_category": category,
        "company": company,
        "amount": float(amount),
        "date": date,
        "uploaded_at": datetime.utcnow().isoformat()
    }

    documents.append(new_doc)
    save_json_file(email, "/broker_anonymized/emails_anonymized.json", documents)

    return new_doc
