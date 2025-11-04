import random
import mysql.connector
from Backend.helpers.helper import *
from Backend.Database.db_init import *
from sqlalchemy.orm import Session
from sqlalchemy import select

# ------------------------
# Generate Client/Broker ID
# ------------------------
def generate_id(length=6):
    return ''.join(random.choices("0123456789", k=length))

def get_unique_client_id():
    with Session(engine) as session:
        while True:
            candidate = generate_id()
            existing = session.get(Clients, candidate)
            if existing is None:
                return candidate

def get_unique_broker_id():
    with Session(engine) as session:
        while True:
            candidate = generate_id()
            existing = session.get(Brokers, candidate)
            if existing is None:
                return candidate

# ------------------------
# Retrieve User/Client/Broker
# ------------------------
def search_user_by_auth0(auth0_id):
    with Session(engine) as session:
        stmt = select(Users).where(Users.auth0_id == auth0_id)
        result = session.execute(stmt).scalar_one_or_none()
        if result:
            # Return as dictionary to match original behavior
            return {
                'user_id': result.user_id,
                'auth0_id': result.auth0_id,
                'basiq_id': result.basiq_id,
                'name': result.name,
                'email': result.email,
                'phone': result.phone,
                'company': result.company,
                'picture': result.picture,
                'isBroker': result.isBroker,
                'profile_complete': result.profile_complete
            }
        return None

def retrieve_broker(user_id):
    with Session(engine) as session:
        stmt = select(Brokers).where(Brokers.user_id == user_id)
        result = session.execute(stmt).scalar_one_or_none()
        if result:
            return {
                'broker_id': result.broker_id,
                'user_id': result.user_id
            }
        return None

def retrieve_client(user_id):
    with Session(engine) as session:
        stmt = select(Clients).where(Clients.user_id == user_id)
        result = session.execute(stmt).scalar_one_or_none()
        if result:
            return {
                'client_id': result.client_id,
                'user_id': result.user_id,
                'broker_id': result.broker_id,
                'broker_verify': result.broker_verify,
                'brokerAccess': result.brokerAccess
            }
        return None

def get_user_by_client_id(client_id):
    with Session(engine) as session:
        stmt = select(Users).join(Clients).where(Clients.client_id == client_id)
        result = session.execute(stmt).scalar_one_or_none()
        if result:
            return {
                'user_id': result.user_id,
                'auth0_id': result.auth0_id,
                'basiq_id': result.basiq_id,
                'name': result.name,
                'email': result.email,
                'phone': result.phone,
                'company': result.company,
                'picture': result.picture,
                'isBroker': result.isBroker,
                'profile_complete': result.profile_complete
            }
        return None

# ------------------------
# Verify User/Client/Broker
# ------------------------
def verify_user_by_id(user_id):
    with Session(engine) as session:
        result = session.get(Users, user_id)
        if result:
            return {
                'user_id': result.user_id,
                'auth0_id': result.auth0_id,
                'basiq_id': result.basiq_id,
                'name': result.name,
                'email': result.email,
                'phone': result.phone,
                'company': result.company,
                'picture': result.picture,
                'isBroker': result.isBroker,
                'profile_complete': result.profile_complete
            }
        return None

def verify_client_by_id(client_id):
    with Session(engine) as session:
        result = session.get(Clients, client_id)
        if result:
            return {
                'client_id': result.client_id,
                'user_id': result.user_id,
                'broker_id': result.broker_id,
                'broker_verify': result.broker_verify,
                'brokerAccess': result.brokerAccess
            }
        return None

def verify_broker_by_id(broker_id):
    with Session(engine) as session:
        result = session.get(Brokers, broker_id)
        if result:
            return {
                'broker_id': result.broker_id,
                'user_id': result.user_id
            }
        return None

def verify_email_db(client_id, email):
    with Session(engine) as session:
        stmt = select(Emails).where(
            Emails.client_id == client_id,
            Emails.email_address == email
        )
        result = session.execute(stmt).scalar_one_or_none()
        if result:
            return {
                'email_id': result.email_id,
                'client_id': result.client_id,
                'domain': result.domain,
                'email_address': result.email_address
            }
        return None

# ------------------------
# Add User/Client/Broker
# ------------------------
def add_user(auth0_id, email, picture, profileComplete=False):
    with Session(engine) as session:
        new_user = Users(
            auth0_id=auth0_id,
            email=email,
            picture=picture,
            profile_complete=profileComplete
        )
        session.add(new_user)
        session.commit()
        return new_user.user_id

def add_client(user_id, broker_id):
    client_id = get_unique_client_id()
    with Session(engine) as session:
        new_client = Clients(
            client_id=client_id,
            user_id=user_id,
            broker_id=broker_id
        )
        session.add(new_client)
        session.commit()
        return new_client.client_id

def add_broker(user_id):
    broker_id = get_unique_broker_id()
    with Session(engine) as session:
        new_broker = Brokers(
            broker_id=broker_id,
            user_id=user_id
        )
        session.add(new_broker)
        session.commit()
        return new_broker.broker_id

# ------------------------
# User profile
# ------------------------
def update_user_profile(auth0_id: str, profile_data: dict):
    with Session(engine) as session:
        # Get the user
        stmt = select(Users).where(Users.auth0_id == auth0_id)
        user = session.execute(stmt).scalar_one_or_none()
        
        if not user:
            return
        
        # Handle user_type
        if "user_type" in profile_data:
            user_type = profile_data.pop("user_type")
            if user_type == "client":
                user.isBroker = False
            elif user_type == "broker":
                user.isBroker = True
        
        # Handle phone formatting
        if "phone" in profile_data:
            phone = profile_data.pop("phone")
            if phone:
                formatted_phone = format_phonenumber(phone)
                user.phone = formatted_phone
        
        # Update other fields
        for key, value in profile_data.items():
            if value is not None and hasattr(user, key):
                setattr(user, key, value)
        
        # Set profile as complete
        user.profile_complete = True
        
        session.commit()

# ------------------------
# Client
# ------------------------
def toggle_broker_access_db(client_id):
    with Session(engine) as session:
        client = session.get(Clients, client_id)
        if client:
            client.brokerAccess = not client.brokerAccess
            session.commit()

# ------------------------
# Broker
# ------------------------
def get_clients_for_broker(broker_id):
    with Session(engine) as session:
        stmt = select(
            Clients.client_id,
            Users.name,
            Users.picture,
            Clients.broker_verify,
            Clients.brokerAccess
        ).join(Users).where(Clients.broker_id == broker_id)
        
        results = session.execute(stmt).all()
        
        clients = []
        for row in results:
            clients.append({
                'id': row.client_id,
                'name': row.name,
                'picture': row.picture,
                'broker_verify': row.broker_verify,
                'brokerAccess': row.brokerAccess
            })
        
        return clients

def toggle_client_verify_db(client_id):
    with Session(engine) as session:
        client = session.get(Clients, client_id)
        if client:
            client.broker_verify = not client.broker_verify
            session.commit()

# ------------------------
# Email
# ------------------------
def add_email_db(client_id, domain, email):
    with Session(engine) as session:
        new_email = Emails(
            client_id=client_id,
            domain=domain,
            email_address=email
        )
        session.add(new_email)
        session.commit()

def get_client_emails_db(client_id):
    with Session(engine) as session:
        stmt = select(Emails.email_address).where(Emails.client_id == client_id)
        results = session.execute(stmt).scalars().all()
        return [{'email_address': email} for email in results]

# ------------------------
# Basiq API
# ------------------------
def add_basiq_id_db(user_id, basiq_id):
    with Session(engine) as session:
        user = session.get(Users, user_id)
        if user:
            user.basiq_id = basiq_id
            session.commit()