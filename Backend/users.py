from db_utils import *
from S3_utils import *
import uuid
from datetime import datetime
from config import DOCUMENT_CATEGORIES

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

def normalize_date(date_str):
    """Convert dates to ISO format (YYYY-MM-DD)."""
    if not date_str:
        return None
    try:
        # Try to parse multiple common formats
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(date_str, fmt).date().isoformat()
            except ValueError:
                continue
        return None  # Unrecognized format
    except Exception:
        return None


def get_client_dashboard(client_id, email):
    if not verify_client_by_id(client_id):
        raise HTTPException(status_code=403, detail="Invalid client")

    documents = get_json_file(email, "/broker_anonymized/emails_anonymized.json")
    missing_categories = list(get_json_file(email, "/pending_categories.json"))

    # Helper: normalize date formats to ISO (YYYY-MM-DD)
    # Group documents by category
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

    # Build the structured response
    categories = [
        {"category_name": cat, "cards": cards}
        for cat, cards in categories_map.items()
        if cat in DOCUMENT_CATEGORIES
    ]

    # Filter missing categories so they match config
    filtered_missing = [
        c for c in DOCUMENT_CATEGORIES if c in missing_categories
    ]

    return {
        "categories": categories,
        "missing_categories": filtered_missing,
    }


def parse_amount(amount_str):
    """Convert '$1,234.56' → 1234.56 (float)."""
    if not amount_str:
        return 0.0
    try:
        cleaned = amount_str.replace("$", "").replace(",", "")
        return float(cleaned)
    except ValueError:
        return 0.0
