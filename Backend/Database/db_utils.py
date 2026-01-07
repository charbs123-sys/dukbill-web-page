import random

from Database.db_init import (
    Brokers,
    ClientBroker,
    Clients,
    Emails,
    IDMERITVerification,
    Users,
    engine,
    Accountants,
    ClientAccountant,
)
from helpers.helper import format_phonenumber
from sqlalchemy import delete, select
from sqlalchemy.orm import Session
from datetime import date, timedelta, datetime

# ------------------------
# Generate Client/Broker ID
# ------------------------
def generate_id(length=6):
    return "".join(random.choices("0123456789", k=length))


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

def get_unique_accountant_id():
    with Session(engine) as session:
        while True:
            candidate = generate_id()
            existing = session.get(Accountants, candidate)
            if existing is None:
                return candidate

# ------------------------
# Retrieve User/Client/Broker
# ------------------------
def search_user_by_auth0(auth0_id: str) -> dict:
    """
    Search for whether a user exists

    auth0_id (str): The Auth0 user ID.

    Returns:
        dict: User information
    """
    with Session(engine) as session:
        stmt = select(Users).where(Users.auth0_id == auth0_id)
        result = session.execute(stmt).scalar_one_or_none()
        if result:
            return {
                "user_id": result.user_id,
                "auth0_id": result.auth0_id,
                "name": result.name,
                "email": result.email,
                "phone": result.phone,
                "company": result.company,
                "picture": result.picture,
                "isBroker": result.isBroker,
                "isAccountant": result.isAccountant,
                "profile_complete": result.profile_complete,
            }
        return None


def retrieve_broker(user_id: str) -> dict:
    """
    Search for whether a broker exists

    user_id (str): The user ID.

    Returns:
        dict: Broker information
    """
    with Session(engine) as session:
        stmt = select(Brokers).where(Brokers.user_id == user_id)
        result = session.execute(stmt).scalar_one_or_none()
        if result:
            return {"broker_id": result.broker_id, "user_id": result.user_id}
        return None

def retrieve_accountant(user_id: str) -> dict:
    """
    Search for whether an accountant exists

    user_id (str): The user ID.

    Returns:
        dict: Accountant information
    """
    with Session(engine) as session:
        stmt = select(Accountants).where(Accountants.user_id == user_id)
        result = session.execute(stmt).scalar_one_or_none()
        if result:
            return {"accountant_id": result.accountant_id, "user_id": result.user_id}
        return None

def retrieve_client(user_id: str) -> dict:
    """
    Search for whether a client exists

    user_id (str): The user ID.

    Returns:
        dict: Client information
    """
    with Session(engine) as session:
        stmt = select(Clients).where(Clients.user_id == user_id)
        result = session.execute(stmt).scalar_one_or_none()
        if result:
            return {
                "client_id": result.client_id,
                "user_id": result.user_id,
            }
        return []


def get_user_by_client_id(client_id):
    """
    From clientid extract user information
    """
    with Session(engine) as session:
        stmt = select(Users).join(Clients).where(Clients.client_id == client_id)
        result = session.execute(stmt).scalar_one_or_none()
        if result:
            return {
                "user_id": result.user_id,
                "auth0_id": result.auth0_id,
                "name": result.name,
                "email": result.email,
                "phone": result.phone,
                "company": result.company,
                "picture": result.picture,
                "isBroker": result.isBroker,
                "profile_complete": result.profile_complete,
            }
        return None


"""
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
"""


def get_client_brokers_db(client_id: str) -> list:
    """
    Retrieving all the brokers associated with a client

    client_id (str): The client ID.

    Returns:
        list: List of brokers associated with the client
    """
    with Session(engine) as session:
        stmt = select(ClientBroker).where(ClientBroker.client_id == client_id)
        results = session.execute(stmt).scalars().all()
        brokers = []
        for result in results:
            brokers.append(
                {
                    "broker_id": result.broker_id,
                    "broker_verify": result.broker_verify,
                    "brokerAccess": result.brokerAccess,
                }
            )
        return brokers

def get_clients_accountant_db(client_id: str) -> list:
    """
    Retrieving all the accountants associated with a client

    client_id (str): The client ID.

    Returns:
        list: List of accountants associated with the client
    """
    with Session(engine) as session:
        stmt = select(ClientAccountant).where(ClientAccountant.client_id == client_id)
        results = session.execute(stmt).scalars().all()
        accountants = []
        for result in results:
            accountants.append(
                {
                    "accountant_id": result.accountant_id,
                    "accountant_verify": result.accountant_verify,
                    "accountantAccess": result.accountantAccess,
                }
            )
        return accountants

# ------------------------
# Verify User/Client/Broker
# ------------------------
def verify_user_by_id(user_id: str) -> dict | None:
    """
    verify the existence of a user by user_id

    user_id (str): The user ID.

    Returns:
        dict: User information
    """
    with Session(engine) as session:
        result = session.get(Users, user_id)
        if result:
            return {
                "user_id": result.user_id,
                "auth0_id": result.auth0_id,
                "name": result.name,
                "email": result.email,
                "phone": result.phone,
                "company": result.company,
                "picture": result.picture,
                "isBroker": result.isBroker,
                "profile_complete": result.profile_complete,
            }
        return None


def verify_client_by_id(client_id: str) -> dict | None:
    """
    verify the existence of a client by client_id

    client_id (str): The client ID.

    Returns:
        dict: Client information
    """
    with Session(engine) as session:
        result = session.get(Clients, client_id)
        if result:
            return {
                "client_id": result.client_id,
                "user_id": result.user_id,
            }
        return None


def verify_broker_by_id(broker_id: str) -> dict | None:
    """
    verify the existence of a broker by broker_id

    broker_id (str): The broker ID.

    Returns:
        dict: Broker information
    """
    with Session(engine) as session:
        result = session.get(Brokers, broker_id)
        if result:
            return {"broker_id": result.broker_id, "user_id": result.user_id}
        return None

def verify_accountant_by_id(accountant_id: str) -> dict | None:
    """
    verify the existence of an accountant by accountant_id

    accountant_id (str): The accountant ID.

    Returns:
        dict: Accountant information
    """
    with Session(engine) as session:
        result = session.get(Accountants, accountant_id)
        if result:
            return {"accountant_id": result.accountant_id, "user_id": result.user_id}
        return None

def verify_email_db(client_id: str, email: str) -> dict | None:
    """
    verify the existence of an email for a client

    client_id (str): The client ID.
    email (str): The email address.

    Returns:
        dict: Email information
    """
    with Session(engine) as session:
        stmt = select(Emails).where(
            Emails.client_id == client_id, Emails.email_address == email
        )
        result = session.execute(stmt).scalar_one_or_none()
        if result:
            return {
                "email_id": result.email_id,
                "client_id": result.client_id,
                "domain": result.domain,
                "email_address": result.email_address,
            }
        return None


# ------------------------
# Add User/Client/Broker
# ------------------------
def add_user(auth0_id: str, email: str, picture: str, profileComplete=False) -> int:
    """
    Adding new user entry into database

    auth0_id (str): The Auth0 user ID.
    email (str): The user's email address.
    picture (str): The user's profile picture URL.
    profileComplete (bool): Whether they have finished onboarding process

    Returns:
        int: user ID
    """
    with Session(engine) as session:
        new_user = Users(
            auth0_id=auth0_id,
            email=email,
            picture=picture,
            profile_complete=profileComplete,
        )
        session.add(new_user)
        session.commit()
        return new_user.user_id


def add_client(user_id: str) -> str:
    """
    Creating a new client entry in the database

    user_id (str): The user ID.

    Returns:
        str: The newly created client ID.
    """
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
    """
    Add a new relationship between clients and brokers in the database

    client_id (str): The client ID.
    broker_id (str): The broker ID.

    Returns:
        str: The newly created client_broker ID.
    """
    with Session(engine) as session:
        new_client_broker = ClientBroker(
            client_id=client_id,
            broker_id=broker_id,
            broker_verify=False,
            brokerAccess=False,
        )
        session.add(new_client_broker)
        session.commit()
        return new_client_broker.broker_id

def add_client_accountant(client_id: str, accountant_id: str) -> str:
    """
    Add a new relationship between clients and accountants in the database

    client_id (str): The client ID.
    accountant_id (str): The accountant ID.

    Returns:
        str: The newly created client_accountant ID.
    """
    with Session(engine) as session:
        new_client_accountant = ClientAccountant(
            client_id=client_id,
            accountant_id=accountant_id,
            accountant_verify=False,
            accountantAccess=False,
        )
        session.add(new_client_accountant)
        session.commit()
        return new_client_accountant.accountant_id

def add_broker(user_id: str) -> str:
    """
    Adding a new broker entry in the database

    user_id (str): The user ID.

    Returns:
        str: The newly created broker ID.
    """
    broker_id = get_unique_broker_id()
    with Session(engine) as session:
        new_broker = Brokers(broker_id=broker_id, user_id=user_id)
        session.add(new_broker)
        session.commit()
        return new_broker.broker_id

def add_accountant(user_id: str) -> str:
    """
    Adding a new accountant entry in the database

    user_id (str): The user ID.

    Returns:
        str: The newly created accountant ID.
    """
    accountant_id = get_unique_accountant_id()
    #email_send_date = date.today() + timedelta(weeks=1)
    #temporarily just make that day today
    email_send_date = date.today()
    with Session(engine) as session:
        new_accountant = Accountants(accountant_id=accountant_id, user_id=user_id, email_send_date=email_send_date, documents_collected=False, emails_sent=0, refuse_email_service=False)
        session.add(new_accountant)
        session.commit()
        return new_accountant.accountant_id

# ------------------------
# User profile
# ------------------------
def update_user_profile(auth0_id: str, profile_data: dict) -> None:
    """
    Modifying user_type, phone number, name, company in the Users table

    auth0_id (str): The Auth0 user ID.
    profile_data (dict): The profile data to update.

    Returns:
        None
    """
    with Session(engine) as session:
        # Get the user
        stmt = select(Users).where(Users.auth0_id == auth0_id)
        user = session.execute(stmt).scalar_one_or_none()

        if not user:
            return

        # Handle user_type
        if "user_type" in profile_data:
            user_type = profile_data.pop("user_type")
            if user_type == "accountant":
                user.isAccountant = True
                user.isBroker = False
            elif user_type == "client":
                user.isAccountant = False
                user.isBroker = False
            elif user_type == "broker":
                user.isAccountant = False
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
    """
    Changing the broker access status for a client-broker relationship

    client_id (str): The client ID.
    broker_id (str): The broker ID.

    Returns:
        bool | None: The updated broker access status or None if not found
    """
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

def toggle_accountant_access_db(client_id: str, accountant_id: str) -> bool | None:
    """
    Changing the accountant access status for a client-accountant relationship

    client_id (str): The client ID.
    accountant_id (str): The accountant ID.

    Returns:
        bool | None: The updated accountant access status or None if not found
    """
    with Session(engine) as session:
        ca = session.execute(
            select(ClientAccountant)
            .where(ClientAccountant.client_id == client_id)
            .where(ClientAccountant.accountant_id == accountant_id)
        ).scalar_one_or_none()

        if not ca:
            return None

        ca.accountantAccess = not ca.accountantAccess
        session.commit()
        session.refresh(ca)
        return ca.accountantAccess

def get_brokers_for_client(client_id: str) -> list:
    """
    Fetching all the clients associated with a broker

    client_id (str): The client ID.

    Returns:
        list: List of brokers associated with the client
    """
    with Session(engine) as session:
        stmt = (
            select(
                Brokers.broker_id,
                Users.name,
                Users.picture,
                ClientBroker.brokerAccess,
                ClientBroker.broker_verify,
            )
            .join(ClientBroker, Brokers.broker_id == ClientBroker.broker_id)
            .join(Users, Brokers.user_id == Users.user_id)
            .where(ClientBroker.client_id == client_id)
        )

        results = session.execute(stmt).all()

        brokers = []
        for row in results:
            brokers.append(
                {
                    "broker_id": row.broker_id,
                    "name": row.name,
                    "picture": row.picture,
                    "brokerAccess": row.brokerAccess,
                    "broker_verify": row.broker_verify,
                }
            )

        return brokers

def get_accountants_for_client(client_id: str) -> list:
    """
    Fetching all the accountants associated with a client

    client_id (str): The client ID.

    Returns:
        list: List of accountants associated with the client
    """
    with Session(engine) as session:
        stmt = (
            select(
                Accountants.accountant_id,
                Users.name,
                Users.picture,
                ClientAccountant.accountantAccess,
                ClientAccountant.accountant_verify,
            )
            .join(ClientAccountant, Accountants.accountant_id == ClientAccountant.accountant_id)
            .join(Users, Accountants.user_id == Users.user_id)
            .where(ClientAccountant.client_id == client_id)
        )

        results = session.execute(stmt).all()

        accountants = []
        for row in results:
            accountants.append(
                {
                    "accountant_id": row.accountant_id,
                    "name": row.name,
                    "picture": row.picture,
                    "accountantAccess": row.accountantAccess,
                    "accountant_verify": row.accountant_verify,
                }
            )

        return accountants

def delete_client_broker_db(client_id: str, broker_id: str) -> None:
    """
    Delete relationship between client and broker
    """
    with Session(engine) as session:
        stmt = delete(ClientBroker).where(
            ClientBroker.client_id == client_id, ClientBroker.broker_id == broker_id
        )
        session.execute(stmt)
        session.commit()
        return

def delete_client_accountant_db(client_id: str, accountant_id: str) -> None:
    """
    Delete relationship between client and accountant
    """
    with Session(engine) as session:
        stmt = delete(ClientAccountant).where(
            ClientAccountant.client_id == client_id, ClientAccountant.accountant_id == accountant_id
        )
        session.execute(stmt)
        session.commit()
        return

# ------------------------
# Broker
# ------------------------
def get_clients_for_broker(broker_id: str) -> list:
    """
    Fetch all clients related to a broker

    broker_id (str): The broker ID.

    Returns:
        list: List of clients associated with the broker
    """
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
            clients.append(
                {
                    "id": row.client_id,
                    "name": row.name,
                    "picture": row.picture,
                    "broker_verify": row.broker_verify,
                    "brokerAccess": row.brokerAccess,
                }
            )

        return clients

def get_clients_for_accountant(accountant_id: str) -> list:
    """
    Fetch all clients related to an accountant

    accountant_id (str): The accountant ID.

    Returns:
        list: List of clients associated with the accountant
    """
    with Session(engine) as session:
        stmt = (
            select(
                ClientAccountant.client_id,
                Users.name,
                Users.picture,
                ClientAccountant.accountant_verify,
                ClientAccountant.accountantAccess,
            )
            .select_from(ClientAccountant)
            .join(Clients, Clients.client_id == ClientAccountant.client_id)
            .join(Users, Users.user_id == Clients.user_id)
            .where(ClientAccountant.accountant_id == accountant_id)
        )

        results = session.execute(stmt).all()

        clients = []
        for row in results:
            clients.append(
                {
                    "id": row.client_id,
                    "name": row.name,
                    "picture": row.picture,
                    "accountant_verify": row.accountant_verify,
                    "accountantAccess": row.accountantAccess,
                }
            )

        return clients

def toggle_client_verify_db(client_id: str, broker_id: str) -> bool | None:
    """
    Toggle the verification status of a client document for a specific broker.

    client_id (str): The client ID.
    broker_id (str): The broker ID.

    Returns:
        bool | None: The updated verification status or None if not found
    """
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
# Accountant
# ------------------------

def find_accountant_emails(today: date) -> None:
    with Session(engine) as session:
        accountants = session.query(Accountants).filter(
            Accountants.email_send_date == today
        ).all()
        return accountants
    return None

def update_accountant_email_date(accountant_id: str, next_email_date: date) -> None:
    with Session(engine) as session:
        accountant = session.get(Accountants, accountant_id)
        if accountant:
            accountant.email_send_date = next_email_date
            session.commit()
    return None

def set_accountant_opt_out_db(accountant_id: str) -> None:
    with Session(engine) as session:
        accountant = session.get(Accountants, accountant_id)
        if accountant:
            accountant.refuse_email_service = True
            session.commit()
    return None

# ------------------------
# Email
# ------------------------
def add_email_db(client_id: str, domain: str, email: str) -> None:
    """
    Add email to the emails table

    client_id (str): The client ID.
    domain (str): The email domain.
    email (str): The email address.

    Returns:
        None
    """
    with Session(engine) as session:
        new_email = Emails(client_id=client_id, domain=domain, email_address=email)
        session.add(new_email)
        session.commit()


def get_client_emails_db(client_id: str) -> list:
    """
    Fetch all emails associated with a client

    client_id (str): The client ID.

    Returns:
        list: List of email addresses associated with the client
    """
    with Session(engine) as session:
        stmt = select(Emails.email_address).where(Emails.client_id == client_id)
        results = session.execute(stmt).scalars().all()
        return [{"email_address": email} for email in results]


def get_client_emails_dashboard_db(client_id: str) -> list:
    """
    Collecting all emails associated with a client for dashboard display

    client_id (str): The client ID.

    Returns:
        list: List of email addresses and their domains associated with the client
    """
    with Session(engine) as session:
        stmt = select(Emails.email_address, Emails.domain).where(
            Emails.client_id == client_id
        )
        results = session.execute(stmt).all()
        return [
            {"email_address": email_address, "domain": domain}
            for email_address, domain in results
        ]


def delete_email_db(client_id: str, email: str) -> None:
    """
    Delete the email associated with the client

    client_id (str): The client ID.
    email (str): The email address to delete.

    Returns:
        None
    """
    with Session(engine) as session:
        stmt = delete(Emails).where(
            Emails.client_id == client_id, Emails.email_address == email
        )
        session.execute(stmt)
        session.commit()

        return


# ------------------------
# IDMERIT API
# ------------------------


def save_verification_request_to_db(requestID: str, client_id: str) -> None:
    """Save the verification request to the database"""
    with Session(engine) as session:
        new_verification = IDMERITVerification(
            unique_uuid=requestID, client_id=client_id
        )
        session.add(new_verification)
        session.commit()


def fetch_clientid_from_requestid(requestID: str) -> dict | None:
    """Fetch the client ID associated with a given request ID"""
    with Session(engine) as session:
        stmt = select(IDMERITVerification).where(
            IDMERITVerification.unique_uuid == requestID
        )
        result = session.execute(stmt).scalar_one_or_none()
        if result:
            return {
                "idmerit_id": result.idmerit_id,
                "client_id": result.client_id,
                "unique_uuid": result.unique_uuid,
            }
        return None


def delete_row_from_requestid(requestID: str) -> None:
    """Delete the verification request from the database"""
    with Session(engine) as session:
        stmt = delete(IDMERITVerification).where(
            IDMERITVerification.unique_uuid == requestID
        )
        session.execute(stmt)
        session.commit()
        return


"""
# ------------------------
# Basiq API
# ------------------------
def add_basiq_id_db(user_id, basiq_id):
    with Session(engine) as session:
        user = session.get(Users, user_id)
        if user:
            user.basiq_id = basiq_id
            session.commit()
"""
