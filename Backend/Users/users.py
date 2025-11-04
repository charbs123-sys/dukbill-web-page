from Backend.Database.db_utils import *
from fastapi import HTTPException

# ------------------------
# Retrieval User/Client/Broker
# ------------------------
def find_user(auth0_id):
    user = search_user_by_auth0(auth0_id)
    return user

def find_client(user_id):
    client = retrieve_client(user_id)
    return client

def find_broker(user_id):
    broker = retrieve_broker(user_id)
    return broker

def get_user_from_client(client_id):
    return get_user_by_client_id(client_id)

# ------------------------
# Verify User/Client/Broker/Email
# ------------------------
def verify_user(user_id):
    return verify_user_by_id(user_id)

def verify_broker(broker_id):
    return verify_broker_by_id(broker_id)

def verify_client(client_id):
    return verify_client_by_id(client_id)

def verify_email(client_id, email):
    return verify_email_db(client_id, email)

# ------------------------
# Register User/Client/Broker
# ------------------------
def register_user(auth0_id, email, picture, profileComplete):
    user_id = add_user(auth0_id, email, picture, profileComplete)
    return user_id

def register_client(user_id, broker_id):
    if verify_user(user_id) and verify_broker(broker_id):
        return add_client(user_id, broker_id)
    else:
        raise HTTPException(status_code=403, detail="Invalid Broker ID")

def register_broker(user_id):
    if verify_user(user_id):
        return add_broker(user_id)

# ------------------------
#  User profile
# ------------------------
def update_profile(auth0_id: str, profile_data: dict):
    user = search_user_by_auth0(auth0_id)
    if not user:
        raise ValueError(f"User with auth0_id {auth0_id} not found")

    update_user_profile(auth0_id, profile_data)
    updated_user = search_user_by_auth0(auth0_id)
    return updated_user

# ------------------------
#  Client
# ------------------------
def toggle_broker_access(client_id):
    if not verify_client(client_id):
        raise HTTPException(status_code=403, detail="Invalid client")
    toggle_broker_access_db(client_id)

def get_client_emails(client_id):
    if verify_client(client_id):
        return get_client_emails_db(client_id)
# ------------------------
#  Broker
# ------------------------
def get_broker_clients(broker_id):
    if verify_broker(broker_id):
        return get_clients_for_broker(broker_id)

def toggle_client_verification(client_id):
    if not verify_client(client_id):
        raise HTTPException(status_code=403, detail="Invalid client")
    toggle_client_verify_db(client_id)

# ------------------------
#  Emails
# ------------------------
def client_add_email(client_id, domain, email):
    if not verify_client(client_id):
        return False

    try:
        add_email_db(client_id, domain, email)
        return True
    except mysql.connector.errors.IntegrityError:
        # Duplicate email for this client, ignore
        return False
    except Exception as e:
        print(f"Failed to add email {email}: {e}")
        return False
# ------------------------
#  Basiq
# ------------------------
def add_basiq_id(user_id, basiq_id):
    add_basiq_id_db(user_id, basiq_id)