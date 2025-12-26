from Database.db_utils import *
from fastapi import HTTPException

# ------------------------
# Retrieval User/Client/Broker
# ------------------------
def handle_registration(auth0_id: str, profile: dict) -> dict:
    '''
    Initial user onboarding process

    auth0_id (str): The Auth0 user ID.
    profile (dict): The user profile information from Auth0.
    
    Returns:
        dict: User information {user: user_id, isNewUser: bool, missingFields: missing entries, profileComplete: bool}
    '''
    user_obj = find_user(auth0_id)
    missing_fields = []

    #Adding new user
    if not user_obj:
        user_id = register_user(
            auth0_id,
            profile["email"],
            profile["picture"],
            profileComplete=False
        )
        #check if they still need to fill onboarding form
        missing_fields = ["name", "company", "phone"]
        return {
            "user": user_id,
            "isNewUser": True,
            "missingFields": missing_fields,
            "profileComplete": False,
        }

    # Existing user: check if they have filled onborading form
    if user_obj.get("profile_complete"):
        return {
            "user": user_obj["user_id"],
            "isNewUser": False,
            "missingFields": [],
            "profileComplete": True,
        }

    # Identify missing fields for incomplete profiles
    for field in ["name", "company", "phone"]:
        if not user_obj.get(field):
            missing_fields.append(field)

    return {
        "user": user_obj["user_id"],
        "isNewUser": False,
        "missingFields": missing_fields,
        "profileComplete": False,
    }
    
# ------------------------
# Retrieval User/Client/Broker
# ------------------------
def find_user(auth0_id: str) -> dict:
    '''
    Intermediate function handling user existence
    '''
    user = search_user_by_auth0(auth0_id)
    return user

def find_client(user_id):
    '''
    Intermediate function handling client existence
    '''
    client = retrieve_client(user_id)
    return client

def find_broker(user_id):
    '''
    Intermediate function handling broker existence
    '''
    broker = retrieve_broker(user_id)
    return broker

def get_user_from_client(client_id):
    return get_user_by_client_id(client_id)

# ------------------------
# Verify User/Client/Broker/Email
# ------------------------

#######Change these later, don't have to return so much information and can probably put into a single general function#########
def verify_user(user_id: str) -> dict:
    '''
    Intermediate function to check user existence
    '''
    return verify_user_by_id(user_id)

def verify_broker(broker_id):
    '''
    Intermediate function to check broker existence
    '''
    return verify_broker_by_id(broker_id)

def verify_client(client_id):
    '''
    Intermediate function to check client existence
    '''
    return verify_client_by_id(client_id)

def verify_email(client_id, email):
    '''
    Intermediate function to check email existence
    '''
    return verify_email_db(client_id, email)

# ------------------------
# Register User/Client/Broker
# ------------------------
def register_user(auth0_id, email, picture, profileComplete):
    '''
    Handle user registration process
    '''
    user_id = add_user(auth0_id, email, picture, profileComplete)
    return user_id

def register_client(user_id, broker_id):
    '''
    Adding client to database

    user_id (str): The user ID.
    broker_id (str): The broker ID.

    Returns:
        str: The newly created client ID.
    '''
    if verify_user(user_id) and verify_broker(broker_id):
        client_id = add_client(user_id)
        add_client_broker(client_id, broker_id)
        return client_id
    else:
        raise HTTPException(status_code=403, detail="Invalid Broker ID")

def register_broker(user_id: str) -> str:
    '''
    intermediate function to add broker
    '''
    if verify_user(user_id):
        return add_broker(user_id)

# ------------------------
#  User profile
# ------------------------
def update_profile(auth0_id: str, profile_data: dict):
    '''
    Intermediate function to update user entries from onboarding form
    '''
    user = search_user_by_auth0(auth0_id)
    if not user:
        raise ValueError(f"User with auth0_id {auth0_id} not found")

    update_user_profile(auth0_id, profile_data)
    updated_user = search_user_by_auth0(auth0_id)
    return updated_user

# ------------------------
#  Client
# ------------------------
def toggle_broker_access(client_id, broker_id):
    '''
    Intermediate function for toggling broker access to client documents
    '''
    if not verify_client(client_id) and not verify_broker(broker_id):
        raise HTTPException(status_code=403, detail="Invalid client")
    return toggle_broker_access_db(client_id, broker_id)

def get_client_emails(client_id):
    if verify_client(client_id):
        return get_client_emails_db(client_id)

def get_client_brokers(client_id):
    '''
    Intermediate function to get all brokers for a client
    '''
    if verify_client(client_id):
        return get_brokers_for_client(client_id)
    
def register_client_broker(client_id, broker_id):
    '''
    Intermediate function to add a new client-broker relationship
    '''
    if verify_client(client_id) and verify_broker(broker_id):
        return add_client_broker(client_id, broker_id)
    else:
        raise HTTPException(status_code=403, detail="Invalid client or broker")

def remove_client_broker(client_id, broker_id):
    '''
    Intermediate function to delete a client-broker relationship
    '''
    if not verify_client(client_id) or not verify_broker(broker_id):
        raise HTTPException(status_code=403, detail="Invalid client or broker")
    
    delete_client_broker_db(client_id, broker_id)

# ------------------------
#  Broker
# ------------------------
def get_broker_clients(broker_id):
    if verify_broker(broker_id):
        return get_clients_for_broker(broker_id)

def toggle_client_verification(client_id, broker_id):
    if not verify_client(client_id) and not verify_broker(broker_id):
        raise HTTPException(status_code=403, detail="Invalid client")
    return toggle_client_verify_db(client_id, broker_id)

#
# Client Broker
#

def get_client_broker_list(client_id):
    if not verify_client(client_id):
        raise HTTPException(status_code=403, detail="Invalid client or broker")
    
    return get_client_brokers_db(client_id)

# ------------------------
#  Emails
# ------------------------
def client_add_email(client_id: str, domain: str, email: str) -> bool:
    '''
    Add the client email to emails table
    '''
    if not verify_client(client_id) or verify_email(client_id, email):
        return True

    try:
        add_email_db(client_id, domain, email)
        return True
    except mysql.connector.errors.IntegrityError:
        return False
    except Exception as e:
        print(f"Failed to add email {email}: {e}")
        return False

def get_client_emails(client_id):
    '''
    Retrieving all emails associated with the client
    '''
    if not verify_client(client_id):
        return False
    
    return get_client_emails_db(client_id)

def get_client_emails_dashboard(client_id):
    if not verify_client(client_id):
        return False
    
    return get_client_emails_dashboard_db(client_id)

def delete_client_email(client_id: str, email: str) -> None:
    if not verify_client(client_id) or not verify_email(client_id, email):
        raise HTTPException(status_code=403, detail="Invalid client or email")
    
    return delete_email_db(client_id, email)
        
# ------------------------
#  Basiq
# ------------------------
def add_basiq_id(user_id, basiq_id):
    add_basiq_id_db(user_id, basiq_id)