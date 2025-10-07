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
        
def register_broker(user_id):
    if verify_user(user_id):
        return add_broker(user_id)
    
def get_broker_clients(broker_id):
    if verify_broker(broker_id):
        return get_clients_for_broker(broker_id)

def get_client_dashboard(client_id, email):
    if not verify_client_by_id(client_id):
        raise HTTPException(status_code=403, detail="Invalid client")

    documents = get_json_file(email, "/broker_anonymized/emails_anonymized.json")
    missing_categories = list(get_json_file(email, "/pending_categories.json"))

    categories_map = {}
    for doc in documents:
        category = doc.get("broker_document_category", "Uncategorized")
        if category not in categories_map:
            categories_map[category] = []

        categories_map[category].append({
            "id": str(uuid.uuid4()),
            "company_name": doc.get("company", "Unknown"),
            "payment_amount": parse_amount(doc.get("amount")),
            "due_date": normalize_date(doc.get("date")),
        })

    categories = [
        {"category_name": cat, "cards": cards}
        for cat, cards in categories_map.items()
        if cat in DOCUMENT_CATEGORIES
    ]

    filtered_missing = [
        c for c in DOCUMENT_CATEGORIES if c in missing_categories
    ]

    return {
        "categories": categories,
        "missing_categories": filtered_missing,
    }
