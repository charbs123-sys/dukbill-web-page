from db_utils import *
from S3_utils import *
from helper import *
from config import DOCUMENT_CATEGORIES
import uuid

def find_user(auth0_id):
    user = search_user_by_auth0(auth0_id)
    return user

def find_client(user_id):
    client = retrieve_client(user_id)
    return client

def find_broker(user_id):
    broker = retrieve_broker(user_id)
    return broker

def verify_user(user_id):
    return verify_user_by_id(user_id)

def verify_broker(broker_id):
    return verify_broker_by_id(broker_id)

def verify_client(client_id):
    return verify_client_by_id(client_id)

def get_user_from_client(client_id):
    return get_user_by_client_id(client_id)

def register_user(auth0_id, email, picture, profileComplete):
    user_id = add_user(auth0_id, email, picture, profileComplete)
    return user_id

def update_profile(auth0_id: str, profile_data: dict):
    user = search_user_by_auth0(auth0_id)
    if not user:
        raise ValueError(f"User with auth0_id {auth0_id} not found")

    update_user_profile(auth0_id, profile_data)
    updated_user = search_user_by_auth0(auth0_id)
    return updated_user

def register_client(user_id, broker_id):
    if verify_user(user_id) and verify_broker(broker_id):
        return add_client(user_id, broker_id)
    else:
        raise HTTPException(status_code=403, detail="Invalid Broker ID")

def register_broker(user_id):
    if verify_user(user_id):
        return add_broker(user_id)
    
def get_broker_clients(broker_id):
    if verify_broker(broker_id):
        return get_clients_for_broker(broker_id)

def toggle_broker_access(client_id):
    if not verify_client(client_id):
        raise HTTPException(status_code=403, detail="Invalid client")

    toggle_broker_access_db(client_id)


def get_client_dashboard(client_id, email):
    if not verify_client_by_id(client_id):
        raise HTTPException(status_code=403, detail="Invalid client")

    documents = get_json_file(email, "/broker_anonymized/emails_anonymized.json")
    missing_categories = set(get_json_file(email, "/pending_categories.json"))

    categories_map = {}
    for doc in documents:
        category = doc.get("broker_document_category", "Uncategorized")
        for heading, cat_list in DOCUMENT_CATEGORIES.items():
            if category in cat_list:
                categories_map.setdefault(category, []).append({
                    "id": str(uuid.uuid4()),
                    "company_name": doc.get("company", "Unknown"),
                    "payment_amount": parse_amount(doc.get("amount")),
                    "due_date": normalize_date(doc.get("date")),
                })
                break

    headings = []
    for heading, cat_list in DOCUMENT_CATEGORIES.items():
        categories = [
            {"category_name": cat, "cards": categories_map.get(cat, [])}
            for cat in cat_list
            if cat in categories_map
        ]
        missing = [cat for cat in cat_list if cat in missing_categories]
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

    threadid_to_key = {}
    for obj in files:
        key = obj["Key"]
        filename = key.split("/")[-1]
        for doc in documents:
            doc_threadid = doc.get("threadid")
            if doc_threadid and doc_threadid in filename:
                threadid_to_key[doc_threadid] = key

    for doc in documents:
        if doc.get("broker_document_category", "Uncategorized") != category:
            continue

        threadid = doc.get("threadid")
        pdf_key = threadid_to_key.get(threadid)
        if not pdf_key:
            continue 

        url = get_presigned_url(pdf_key)

        filtered_docs.append({
            "category": category,
            "company": doc.get("company", "Unknown"),
            "amount": parse_amount(doc.get("amount")),
            "due_date": normalize_date(doc.get("date")),
            "url": url
        })
    return filtered_docs

def add_basiq_id(user_id, basiq_id):
    add_basiq_id_db(user_id, basiq_id)