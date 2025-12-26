import random
import mysql.connector
from helpers.helper import *
from Database.db_init import *
from sqlalchemy.orm import Session
from sqlalchemy import select, delete

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
def search_user_by_auth0(auth0_id: str) -> dict:
    '''
    Search for whether a user exists

    auth0_id (str): The Auth0 user ID.

    Returns:
        dict: User information
    '''
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

def retrieve_broker(user_id: str) -> dict:
    '''
    Search for whether a broker exists

    user_id (str): The user ID.

    Returns:
        dict: Broker information
    '''
    with Session(engine) as session:
        stmt = select(Brokers).where(Brokers.user_id == user_id)
        result = session.execute(stmt).scalar_one_or_none()
        if result:
            return {
                'broker_id': result.broker_id,
                'user_id': result.user_id
            }
        return None

def retrieve_client(user_id: str) -> dict:
    '''
    Search for whether a client exists

    user_id (str): The user ID.

    Returns:
        dict: Client information
    '''
    with Session(engine) as session:
        stmt = select(Clients).where(Clients.user_id == user_id)
        result = session.execute(stmt).scalar_one_or_none()
        if result:
            return {
                'client_id': result.client_id,
                'user_id': result.user_id,
            }
        return []

def get_user_by_client_id(client_id):
    '''
    From clientid extract user information
    '''
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

def get_client_broker_access_db(client_id, broker_id):
    with Session(engine) as session:
        stmt = select(ClientBroker).where(
            ClientBroker.client_id == client_id,
            ClientBroker.broker_id == broker_id
        )
        result = session.execute(stmt).scalar_one_or_none()
        if result:
            return result.brokerAccess
        return None


def get_client_brokers_db(client_id):
    with Session(engine) as session:
        stmt = select(ClientBroker).where(ClientBroker.client_id == client_id)
        results = session.execute(stmt).scalars().all()
        brokers = []
        for result in results:
            brokers.append({
                'broker_id': result.broker_id,
                'broker_verify': result.broker_verify,
                'brokerAccess': result.brokerAccess
            })
        return brokers

# ------------------------
# Verify User/Client/Broker
# ------------------------
def verify_user_by_id(user_id: str) -> dict:
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
def add_user(auth0_id: str, email: str, picture: str, profileComplete=False) -> int:
    '''
    Adding new user entry into database

    auth0_id (str): The Auth0 user ID.
    email (str): The user's email address.
    picture (str): The user's profile picture URL.
    profileComplete (bool): Whether they have finished onboarding process

    Returns:
        int: user ID
    '''
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

def add_client(user_id: str) -> str:
    '''
    Creating a new client entry in the database

    user_id (str): The user ID.

    Returns:
        str: The newly created client ID.
    '''
    client_id = get_unique_client_id()
    with Session(engine) as session:
        new_client = Clients(
            client_id=client_id,
            user_id=user_id,
        )
        session.add(new_client)
        session.commit()
        return new_client.client_id

def add_client_broker(client_id: str, broker_id: str) -> str:
    '''
    Add a new relationship between clients and brokers in the database

    client_id (str): The client ID.
    broker_id (str): The broker ID.

    Returns:
        str: The newly created client_broker ID.
    '''
    with Session(engine) as session:
        new_client_broker = ClientBroker(
            client_id=client_id,
            broker_id=broker_id,
            broker_verify=False,
            brokerAccess=False
        )
        session.add(new_client_broker)
        session.commit()
        return new_client_broker.broker_id

def add_broker(user_id: str) -> str:
    '''
    Adding a new broker entry in the database

    user_id (str): The user ID.

    Returns:
        str: The newly created broker ID.
    '''
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
def update_user_profile(auth0_id: str, profile_data: dict) -> None:
    '''
    Modifying user_type, phone number, name, company in the Users table
    
    auth0_id (str): The Auth0 user ID.
    profile_data (dict): The profile data to update.

    Returns:
        None
    '''
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
def toggle_broker_access_db(client_id: str, broker_id: str) -> bool | None:
    '''
    Changing the broker access status for a client-broker relationship

    client_id (str): The client ID.
    broker_id (str): The broker ID.

    Returns:
        bool | None: The updated broker access status or None if not found
    '''
    with Session(engine) as session:
        cb = session.execute(
            select(ClientBroker)
            .where(ClientBroker.client_id == client_id)
            .where(ClientBroker.broker_id == broker_id)
        ).scalar_one_or_none()

        if not cb:
            return None

        cb.brokerAccess = not cb.brokerAccess
        session.commit()
        session.refresh(cb)
        return cb.brokerAccess

def get_brokers_for_client(client_id: str) -> list:
    '''
    Fetching all the clients associated with a broker

    client_id (str): The client ID.

    Returns:
        list: List of brokers associated with the client
    '''
    with Session(engine) as session:
        stmt = (
            select(
                Brokers.broker_id,
                Users.name,
                Users.picture,
                ClientBroker.brokerAccess
            )
            .join(ClientBroker, Brokers.broker_id == ClientBroker.broker_id)
            .join(Users, Brokers.user_id == Users.user_id)
            .where(ClientBroker.client_id == client_id)
        )

        results = session.execute(stmt).all()

        brokers = []
        for row in results:
            brokers.append({
                'broker_id': row.broker_id,
                'name': row.name,
                'picture': row.picture,
                'brokerAccess': row.brokerAccess
            })

        return brokers

def delete_client_broker_db(client_id: str, broker_id: str) -> None:
    '''
    Delete relationship between client and broker
    '''
    with Session(engine) as session:
        stmt = delete(ClientBroker).where(
            ClientBroker.client_id == client_id,
            ClientBroker.broker_id == broker_id
        )
        session.execute(stmt)
        session.commit()
        return


# ------------------------
# Broker
# ------------------------
def get_clients_for_broker(broker_id: str) -> list:
    '''
    Fetch all clients related to a broker

    broker_id (str): The broker ID.

    Returns:
        list: List of clients associated with the broker
    '''
    with Session(engine) as session:
        stmt = (
            select(
                ClientBroker.client_id,
                Users.name,
                Users.picture,
                ClientBroker.broker_verify,
                ClientBroker.brokerAccess,
            )
            .select_from(ClientBroker)
            .join(Clients, Clients.client_id == ClientBroker.client_id)
            .join(Users, Users.user_id == Clients.user_id)
            .where(ClientBroker.broker_id == broker_id)
        )
        
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

def toggle_client_verify_db(client_id: str, broker_id: str) -> bool | None:
    '''
    Toggle the verification status of a client document for a specific broker.

    client_id (str): The client ID.
    broker_id (str): The broker ID.

    Returns:
        bool | None: The updated verification status or None if not found
    '''
    with Session(engine) as session:
        cb = session.execute(
            select(ClientBroker)
            .where(ClientBroker.client_id == client_id)
            .where(ClientBroker.broker_id == broker_id)
        ).scalar_one_or_none()

        if not cb:
            return None

        cb.broker_verify = not cb.broker_verify
        session.commit()
        session.refresh(cb)
        return cb.broker_verify

# ------------------------
# Email
# ------------------------
def add_email_db(client_id: str, domain: str, email: str) -> None:
    '''
    Add email to the emails table

    client_id (str): The client ID.
    domain (str): The email domain.
    email (str): The email address.

    Returns:
        None
    '''
    with Session(engine) as session:
        new_email = Emails(
            client_id=client_id,
            domain=domain,
            email_address=email
        )
        session.add(new_email)
        session.commit()

def get_client_emails_db(client_id: str) -> list:
    '''
    Fetch all emails associated with a client

    client_id (str): The client ID.

    Returns:
        list: List of email addresses associated with the client
    '''
    with Session(engine) as session:
        stmt = select(Emails.email_address).where(Emails.client_id == client_id)
        results = session.execute(stmt).scalars().all()
        return [{'email_address': email} for email in results]

def get_client_emails_dashboard_db(client_id: str) -> list:
    '''
    Collecting all emails associated with a client for dashboard display

    client_id (str): The client ID.

    Returns:
        list: List of email addresses and their domains associated with the client
    '''
    with Session(engine) as session:
        stmt = select(Emails.email_address, Emails.domain).where(Emails.client_id == client_id)
        results = session.execute(stmt).all()
        return [
            {
                'email_address': email_address,
                'domain': domain
            }
            for email_address, domain in results
        ]

def delete_email_db(client_id: str, email: str) -> None:
    '''
    Delete the email associated with the client

    client_id (str): The client ID.
    email (str): The email address to delete.

    Returns:
        None
    '''
    with Session(engine) as session:
        stmt = delete(Emails).where(
            Emails.client_id == client_id,
            Emails.email_address == email
        )
        session.execute(stmt)
        session.commit()
        
        return
# ------------------------
# Basiq API
# ------------------------
def add_basiq_id_db(user_id, basiq_id):
    with Session(engine) as session:
        user = session.get(Users, user_id)
        if user:
            user.basiq_id = basiq_id
            session.commit()