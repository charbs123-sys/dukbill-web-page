# ------------------------
# FastAPI App Imports
# ------------------------
# from EmailScanners.outlook_connect import get_outlook_auth_url, exchange_outlook_code_for_tokens, run_outlook_scan
# ------------------------
# Python Imports
# ------------------------
import json
import os
import threading
import time
import urllib
from urllib.parse import urlencode

import requests
from auth import verify_google_token, verify_token, verify_xero_auth

# ------------------------
# File Imports
# ------------------------
from config import (
    AUTH0_DOMAIN,
    EXPECTED_REPORTS_IDMERIT,
    EXPECTED_REPORTS_MYOB,
    EXPECTED_REPORTS_XERO,
    IDMERIT_CALLBACK_URL,
    XERO_SCOPES,
)
from cryptography.fernet import Fernet
from Database.db_init import initialize_database
from Database.S3_init import bucket_name
from Documents.documents import (
    add_comment_client_document,
    add_comment_docs_general,
    delete_client_document,
    delete_docs_general,
    delete_email_documents,
    edit_client_document,
    get_client_category_documents,
    get_client_dashboard,
    get_docs_general,
    get_download_urls,
    hash_email,
    remove_comment_client_document,
    remove_comment_docs_general,
    update_anonymized_json_general,
    upload_client_document,
)
from Database.db_utils import (
    search_user_by_auth0,
    verify_user_by_id,
)
from Documents.file_downloads import _invoke_zip_lambda_for, _stream_s3_zip
from EmailScanners.gmail_connect import (
    exchange_code_for_tokens,
    get_google_auth_url,
    run_gmail_scan,
)
from fastapi import (
    BackgroundTasks,
    Body,
    Depends,
    FastAPI,
    File,
    Form,
    HTTPException,
    Request,
    UploadFile,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from helpers.idmerit_helpers import (
    idmerit_fetch_clientid,
    send_idmerit_verification_message,
    upload_idmerit_user_image_s3,
)
from helpers.myob_helper import build_auth_url, process_myob_data
from helpers.sending_email import send_broker_to_client

# from shufti import shufti_url
# from helpers.id_helpers import
from helpers.xero_helpers import (
    AUTH_URL,
    TOKEN_URL,
    XERO_CLIENT_ID,
    XERO_REDIRECT_URI,
    fetch_all_data,
    generate_all_reports_xero,
    generate_xero_preview,
    get_basic_auth,
    tokens,
)

from helpers.helper import get_email_domain

# ------------------------
# Model Imports
# ------------------------
from pydantic import BaseModel
from users import (
    client_add_email,
    delete_client_email,
    find_broker,
    find_client,
    find_user,
    get_broker_clients,
    get_client_broker_list,
    get_client_brokers,
    get_client_emails,
    get_client_emails_dashboard,
    get_user_from_client,
    handle_registration,
    register_broker,
    register_client,
    register_client_broker,
    remove_client_broker,
    toggle_broker_access,
    toggle_client_verification,
    update_profile,
    verify_client,
)

# ------------------------
# FastAPI App Initialization
# ------------------------
app = FastAPI(title="Dukbill API", version="1.0.0")
REDIRECT_URL = os.environ.get(
    "REDIRECT_DUKBILL",
    "https://314dbc1f-20f1-4b30-921e-c30d6ad9036e-00-19bw6chuuv0n8.riker..dev/dashboard",
)
STATE_PARAMETER = os.environ.get("STATE_SECRET_KEY")
Encryption_function = Fernet(STATE_PARAMETER)
# ------------------------
# CORS Settings
# ------------------------
origins = [
    "https://314dbc1f-20f1-4b30-921e-c30d6ad9036e-00-19bw6chuuv0n8.riker.replit.dev",
    "https://api.vericare.com.au",
    "http://localhost:5000",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5000",
    "https://*.replit.dev",
    "https://dukbillapp.com",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize database
initialize_database()

# Security dependency
security = HTTPBearer()


# ------------------------
# Pydantic Models
# ------------------------
class GoogleTokenRequest(BaseModel):
    googleToken: str


class XeroAuthRequest(BaseModel):
    code: str


# Work on implementing organization based login later
# implement unit tests later
# implement rate limiting later ---------------- important especially for routes that send emails, or per use endpoints like idmerit (limit to a couple times a day)
# ------------------------
# Dependencies
# ------------------------
def get_user_info_from_auth0(access_token: str) -> dict:
    """
    Fetch user information from Auth0 using the provided access token.

    access_token (str): The Auth0 access token.

    Returns:
        dict: The user profile information. {"sub": auth0id, "given_name": ..., "nickname": ..., "name": ..., "picture": ..., "locale": ..., "updated_at": ..., "email": ..., "email_verified": ...}
    """
    userinfo_url = f"https://{AUTH0_DOMAIN}/userinfo"
    session = requests.Session()
    try:
        response = session.get(
            userinfo_url, headers={"Authorization": f"Bearer {access_token}"}, timeout=5
        )
        if response.status_code != 200:
            raise HTTPException(
                status_code=401, detail="Failed to fetch user profile from Auth0"
            )
        return response.json()
    except requests.RequestException as e:
        raise HTTPException(status_code=503, detail=f"Auth0 request failed: {str(e)}")


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    Retrieve and verify the current user based on the provided HTTP Bearer token.

    credentials (HTTPAuthorizationCredentials): The HTTP Bearer token credentials.

    Returns:
        claims (dict): The decoded JWT claims of the user. {iss: issuer, sub: login type, aud: audient, iat: issued at, exp: expiration, scope: scopes..., azp: authorized party}
        token (str): The original JWT token.
    """
    token = credentials.credentials
    claims = verify_token(token)
    if not claims:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return claims, token


# ------------------------
# Auth Routes
# ------------------------
@app.post("/api/google-signup")
async def google_signup(req: GoogleTokenRequest) -> dict:
    """
    Sign up user via google token

    req (GoogleTokenRequest): The request body containing the Google token.

    Returns:
        dict: Success message on succesful registration
    """
    payload = verify_google_token(req.googleToken)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid Google token")
    return {"success": "User registered successfully"}


@app.post("/auth/register")
async def register(user=Depends(get_current_user)):
    """
    Start user registration process

    user (tuple): The current user information from the dependency.

    Returns:
        dict: Core user information {user: user_id, isNewUser: bool, missingFields: missing entries, profileComplete: bool}
    """
    _, access_token = user
    profile = get_user_info_from_auth0(access_token)
    auth0_id = profile["sub"]

    # Call the extracted core logic function
    result = handle_registration(auth0_id, profile)
    return result


@app.post("/auth/check-verification")
async def user_email_authentication(user=Depends(get_current_user)):
    """
    Simple check on whether the users email has been verified
    """
    _, access_token = user
    profile = get_user_info_from_auth0(access_token)
    return {"email_verified": profile["email_verified"]}


# ------------------------
# User Profile
# ------------------------
@app.get("/user/profile")
async def fetch_user_profile(user=Depends(get_current_user)):
    """
    Collect user profile information

    user (tuple): The current user information from the dependency.

    Returns:
        dict: User profile information {name: ..., id: ..., picture: ..., user_type: "broker" or "client", email_verified: bool}
    """
    claims, access_token = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)

    # need to cache email_verified in local DB
    jwt_info = get_user_info_from_auth0(access_token)

    if user_obj["isBroker"]:
        profile = find_broker(user_obj["user_id"])
        profile_id = profile["broker_id"]
        user_type = "broker"
    else:
        profile = find_client(user_obj["user_id"])
        profile_id = profile["client_id"]
        user_type = "client"

    return {
        "name": user_obj["name"],
        "id": profile_id,
        "picture": user_obj["picture"],
        "user_type": user_type,
        "email_verified": jwt_info["email_verified"],
    }


@app.patch("/users/onboarding")
async def complete_profile(profile_data: dict, user=Depends(get_current_user)) -> dict:
    """
    Finalizing user onboarding process

    profile_data (dict): The profile data to update.
    user (tuple): The current user information from the dependency.

    Returns:
        dict: Updated user information {user: user_id, profileComplete: bool, missingFields: list of missing entries, validatedBroker: bool}
    """
    claims, access_token = user
    profile = get_user_info_from_auth0(access_token)
    auth0_id = profile["sub"]
    user_obj = search_user_by_auth0(auth0_id)
    user_type = profile_data["user_type"]
    broker_id = profile_data.pop("broker_id", None)
    validatedBroker = False

    if user_type == "client":
        # adding client to database
        client_id = register_client(user_obj["user_id"], broker_id)
        client_add_email(
            client_id, get_email_domain(user_obj["email"]), user_obj["email"]
        )
        validatedBroker = bool(client_id)

    elif user_type == "broker":
        # adding broker to database
        register_broker(user_obj["user_id"])
        validatedBroker = True

    user_obj = update_profile(auth0_id, profile_data)

    return {
        "user": user_obj["user_id"],
        "profileComplete": user_obj["profile_complete"],
        "missingFields": [
            f
            for f in ["full_name", "phone_number", "company_name"]
            if not user_obj.get(f)
        ],
        "validatedBroker": validatedBroker,
    }


# ------------------------
# Handling Emails
# ------------------------


@app.post("/add/email")
async def add_email(email: str, user=Depends(get_current_user)):
    """
    Adding new emails to database when scanning email

    email (str): The email address to add.
    user (tuple): The current user information from the dependency.

    Returns:
        dict: Success message on succesful addition
    """
    claims, access_token = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)

    client = find_client(user_obj["user_id"])
    try:
        domain = get_email_domain(email)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    client_add_email(client["client_id"], domain, email)
    return {"message": "Email added successfully"}


@app.get("/get/emails")
async def get_emails(user=Depends(get_current_user)):
    """
    Retrieving all emails associated with the client

    user (tuple): The current user information from the dependency.

    Returns:
        list: List of email addresses associated with the client
    """
    claims, access_token = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    client = find_client(user_obj["user_id"])
    return get_client_emails_dashboard(client["client_id"]) if client else []


@app.delete("/delete/email")
async def delete_email(request: Request, user=Depends(get_current_user)):
    """
    Deleting the email associated with the client

    request (Request): The request object containing the email to delete. {email: email_address}
    """
    claims, access_token = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)

    data = await request.json()
    email = data.get("email")

    client = find_client(user_obj["user_id"])
    delete_client_email(client["client_id"], email)

    delete_email_documents(hash_email(email))
    return {"message": "Email deleted successfully"}


# ------------------------
# Client Routes
# ------------------------
@app.get("/clients/dashboard")
async def get_client_documents(user=Depends(get_current_user)):
    """
    Generating the client dashboard view

    user (tuple): The current user information from the dependency.

    Returns:
        dict: Client dashboard information {headings: [...], BrokerAccess: [...], loginEmail: ...}
    """
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    if not user_obj or user_obj["isBroker"]:
        raise HTTPException(status_code=403, detail="Access denied")

    client = find_client(user_obj["user_id"])
    emails = get_client_emails(client["client_id"])
    # Extract email addresses for comparison
    email_addresses = [e["email_address"] for e in emails]

    # Add login email if not present
    if user_obj["email"] not in email_addresses:
        emails.append({"email_address": user_obj["email"]})

    # get emails, xero and myob docs
    headings = get_client_dashboard(client["client_id"], emails)

    return {
        "headings": headings,
        "BrokerAccess": get_client_broker_list(client["client_id"]),
        "loginEmail": user_obj["email"],
    }


@app.post("/clients/category/documents")
async def get_category_documents(request: dict, user=Depends(get_current_user)):
    """
    Fetching documents of an individual category

    request (dict) -> {category: category_name}
    user (tuple): The current user information from the dependency.

    Returns:
        list: List of documents in the specified category
    """
    claims, _ = user
    auth0_id = claims["sub"]
    category = request.get("category")
    user_obj = find_user(auth0_id)
    if not user_obj or user_obj["isBroker"]:
        raise HTTPException(status_code=403, detail="Access denied")

    client = find_client(user_obj["user_id"])
    emails = get_client_emails(client["client_id"])

    # Extract email addresses for comparison
    email_addresses = [e["email_address"] for e in emails]

    # Add login email if not present
    if user_obj["email"] not in email_addresses:
        emails.append({"email_address": user_obj["email"]})

    documents = get_client_category_documents(client["client_id"], emails, category)

    # retrieving xero/myob documents
    documents.extend(
        get_docs_general(client["client_id"], [user_obj["email"]], category)
    )

    return documents


@app.post("/clients/remove/comment")
async def remove_client_document_comment(request: dict, user=Depends(get_current_user)):
    """
    Give clients the ability to remove comments

    request (dict) -> {category: category_name, hashed_email: hashed_email}
    user (tuple): The current user information from the dependency.

    Returns:
        dict: Success message on succesful removal
    """
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    client = find_client(user_obj["user_id"])
    category = request.get("category")
    hashed_user_email = request.get("hashed_email")

    # determine which type of document to remove comment from
    if category.startswith("xero_"):
        remove_comment_docs_general(
            client["client_id"], hashed_user_email, category, "xero_reports"
        )
    elif category.startswith("Broker_"):
        remove_comment_docs_general(
            client["client_id"], hashed_user_email, category, "myob_reports"
        )
    elif category.startswith("idmerit_"):
        remove_comment_docs_general(
            client["client_id"], hashed_user_email, category, "idmerit_docs"
        )
    else:
        remove_comment_client_document(
            client["client_id"], hashed_user_email, request.get("threadid", None)
        )

    return {"message": "Comment removed successfully"}


@app.post("/add/broker")
async def add_broker(broker_id: str, user=Depends(get_current_user)):
    """
    Allow clients to add brokers to their account

    broker_id (str): The broker ID to add.
    user (tuple): The current user information from the dependency.

    Returns:
        dict: Registered broker information {broker_id: ...}
    """
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    client = find_client(user_obj["user_id"])
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")

    registered_broker_id = register_client_broker(client["client_id"], broker_id)
    if not registered_broker_id:
        raise HTTPException(status_code=400, detail="Invalid broker ID")

    return {"broker_id": registered_broker_id}


@app.get("/get/brokers")
async def get_brokers(user=Depends(get_current_user)) -> list:
    """
    Fetch all the brokers associated with the client

    user (tuple): The current user information from the dependency.

    Returns:
        list: List of brokers associated with the client
    """
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    client = find_client(user_obj["user_id"])
    get_broker_info = get_client_brokers(client["client_id"])
    return get_broker_info


@app.post("/broker/access")
async def toggle_broker_access_route(
    broker_id: str, user=Depends(get_current_user)
) -> dict:
    """
    Toggle whether broker has access to client documents

    broker_id (str): The broker ID to toggle access for.
    user (tuple): The current user information from the dependency.

    Returns:
        dict: Updated broker access status {BrokerAccess: bool}
    """
    claims, _ = user
    auth0_id = claims["sub"]
    user = find_user(auth0_id)
    client = find_client(user["user_id"])

    return {"BrokerAccess": toggle_broker_access(client["client_id"], broker_id)}


@app.post("/client/broker/delete")
async def delete_client_broker(broker_id: str, user=Depends(get_current_user)) -> dict:
    """
    Allowing client to remove a broker from their account

    broker_id (str): The broker ID to remove.
    user (tuple): The current user information from the dependency.

    Returns:
        dict: Success message on succesful removal
    """
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    client = find_client(user_obj["user_id"])
    remove_client_broker(client["client_id"], broker_id)
    return {"message": "Broker removed successfully"}


# ------------------------
# Broker Routes
# ------------------------
@app.get("/brokers/client/list")
async def get_client_list(user=Depends(get_current_user)) -> dict:
    """
    Fetch all clients related to a broker

    user (tuple): The current user information from the dependency.

    Returns:
        dict: List of clients associated with the broker {clients: [...]}
    """
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    broker = find_broker(user_obj["user_id"])
    clients = get_broker_clients(broker["broker_id"])
    return {"clients": clients}


@app.get("/brokers/client/{client_id}/dashboard")
async def get_client_dashboard_broker(
    client_id: int, user=Depends(get_current_user)
) -> dict:
    """
    Get the client dashboard view for brokers

    client_id (int): The client ID to fetch the dashboard for.
    user (tuple): The current user information from the dependency.

    Returns:
        dict: Client dashboard information {headings: [...], BrokerAccess: [...], loginEmail: ...}
    """
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    broker = find_broker(user_obj["user_id"])
    client = verify_client(client_id)
    if not client:
        return {"error": "Access denied"}
    client_user = get_user_from_client(client_id)
    is_broker_access = get_client_broker_list(client["client_id"])

    # find the broker and determine if they have access
    for client_brokers in is_broker_access:
        if client_brokers.get("broker_id") == broker[
            "broker_id"
        ] and not client_brokers.get("brokerAccess", False):
            return {"error": "Access denied"}

    emails = get_client_emails(client_id)

    # Extract email addresses for comparison
    email_addresses = [e["email_address"] for e in emails]

    # Add login email if not present
    if client_user["email"] not in email_addresses:
        emails.append({"email_address": client_user["email"]})

    headings = get_client_dashboard(client_id, emails)

    return {
        "headings": headings,
        "BrokerAccess": is_broker_access,
        "loginEmail": client_user["email"],
    }


@app.post("/brokers/client/{client_id}/category/documents")
async def get_category_documents_broker(
    client_id: int, request: dict, user=Depends(get_current_user)
):
    """
    Fetch the documents of an individual category on client_id for brokers to view

    client_id (int): The client ID to fetch documents for.
    request (dict) -> {category: category_name}
    user (tuple): The current user information from the dependency.

    Returns:
        list: List of documents in the specified category
    """
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    broker = find_broker(user_obj["user_id"])
    client = verify_client(client_id)
    is_broker_access = get_client_broker_list(client["client_id"])

    for client_brokers in is_broker_access:
        if client_brokers.get("broker_id") == broker[
            "broker_id"
        ] and not client_brokers.get("brokerAccess", False):
            return {"error": "Access denied"}

    category = request.get("category")
    client_user = get_user_from_client(client_id)
    emails = get_client_emails(client_id)

    # Extract email addresses for comparison
    email_addresses = [e["email_address"] for e in emails]

    # Add login email if not present
    if client_user["email"] not in email_addresses:
        emails.append({"email_address": client_user["email"]})

    documents = get_client_category_documents(client_id, emails, category)

    # retrieving xero/myob documents
    documents.extend(
        get_docs_general(client["client_id"], [client_user["email"]], category)
    )

    return documents


@app.get("/brokers/client/{client_id}/documents/download")
async def download_client_documents(
    client_id: int, user=Depends(get_current_user)
) -> StreamingResponse:
    """
    Allow broker to download all client documents as a zip file

    client_id (int): The client ID to download documents for.
    user (tuple): The current user information from the dependency.

    Returns:
        StreamingResponse: The streaming response containing the zip file.
    """
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    broker = find_broker(user_obj["user_id"])
    client = verify_client(client_id)
    is_broker_access = get_client_broker_list(client["client_id"])
    for client_brokers in is_broker_access:
        if client_brokers.get("broker_id") == broker[
            "broker_id"
        ] and not client_brokers.get("brokerAccess", False):
            return {"error": "Access denied"}

    client_user = get_user_from_client(client_id)
    emails = get_client_emails(client_id)

    # Extract email addresses for comparison
    email_addresses = [e["email_address"] for e in emails]

    # Add login email if not present
    if client_user["email"] not in email_addresses:
        email_addresses.append(client_user["email"])

    result = _invoke_zip_lambda_for(email_addresses)
    zip_key = result["zip_key"]
    filename = f"client_{client_id}_documents.zip"
    return _stream_s3_zip(zip_key, filename)


@app.post("/brokers/client/{client_id}/verify")
async def verify_client_documents(
    client_id: int, user=Depends(get_current_user)
) -> dict:
    """
    Allow brokers to toggle documents as verified or unverified
    """
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    broker = find_broker(user_obj["user_id"])

    broker_verify = toggle_client_verification(client_id, broker["broker_id"])
    if broker_verify:
        send_broker_to_client(
            broker_name=user_obj["name"],
            broker_email=user_obj["email"],
            client_first_name=get_user_from_client(client_id)["name"],
            client_email=get_user_from_client(client_id)["email"],
            msg_contents="Your documents have been successfully verified.",
            msg_type="verification_success",
            subject="Document Verification Success",
        )
    return {"broker_verify": broker_verify}


# see if we can get threadid as well if it exists on a file
@app.post("/brokers/add_comment")
async def add_document_comment(
    client_id: int, request: dict, user=Depends(get_current_user)
) -> dict:
    """
    Allow brokers to add comments to client documents

    client_id (int): The client ID to add comment for.
    request (dict) -> {category: category_name, comment: comment_text, hashed_email: hashed_email}
    user (tuple): The current user information from the dependency.

    Returns:
        dict: Success message on succesful addition
    """
    claims, _ = user
    client = verify_client(client_id)
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    broker = find_broker(user_obj["user_id"])
    is_broker_access = get_client_broker_list(client["client_id"])
    for client_brokers in is_broker_access:
        if client_brokers.get("broker_id") == broker[
            "broker_id"
        ] and not client_brokers.get("brokerAccess", False):
            return {"error": "Access denied"}

    category = request.get("category")
    hashed_user_email = request.get("hashed_email")

    if category.startswith("xero_"):
        add_comment_docs_general(
            client_id,
            hashed_user_email,
            category,
            request.get("comment", ""),
            "xero_reports",
        )
    elif category.startswith("Broker_"):
        add_comment_docs_general(
            client_id,
            hashed_user_email,
            category,
            request.get("comment", ""),
            "myob_reports",
        )
    elif category.startswith("idmerit_"):
        add_comment_docs_general(
            client_id,
            hashed_user_email,
            category,
            request.get("comment", ""),
            "idmerit_docs",
        )
    else:
        add_comment_client_document(
            client_id,
            hashed_user_email,
            category,
            request.get("comment", ""),
            request.get("threadid", None),
        )

    return {"message": "Comment added successfully"}


@app.post("/brokers/remove_comment")
async def remove_document_comment(
    client_id: int, request: dict, user=Depends(get_current_user)
) -> dict:
    """
    Allow brokers to remove comments from client documents

    client_id (int): The client ID to remove comment for.
    request (dict) -> {category: category_name, hashed_email: hashed_email}
    user (tuple): The current user information from the dependency.

    Returns:
        dict: Success message on succesful removal
    """
    claims, _ = user
    client = verify_client(client_id)
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    broker = find_broker(user_obj["user_id"])
    is_broker_access = get_client_broker_list(client["client_id"])
    for client_brokers in is_broker_access:
        if client_brokers.get("broker_id") == broker[
            "broker_id"
        ] and not client_brokers.get("brokerAccess", False):
            return {"error": "Access denied"}

    category = request.get("category")
    hashed_user_email = request.get("hashed_email")

    if category.startswith("xero_"):
        remove_comment_docs_general(
            client_id, hashed_user_email, category, "xero_reports"
        )
    elif category.startswith("Broker_"):
        remove_comment_docs_general(
            client_id, hashed_user_email, category, "myob_reports"
        )
    elif category.startswith("idmerit_"):
        remove_comment_docs_general(
            client_id, hashed_user_email, category, "idmerit_docs"
        )
    else:
        remove_comment_client_document(
            client_id, hashed_user_email, request.get("threadid", None)
        )

    return {"message": "Comment removed successfully"}


@app.post("/brokers/client/{client_id}/email/send")
def send_email_to_client(
    client_first_name: str,
    client_email: str,
    email_data: dict,
    user=Depends(get_current_user),
) -> dict:
    """
    Allow brokers to send emails to clients

    client_id (int): The client ID to send email to.
    email_data (dict): The email data containing subject and body.
    user (tuple): The current user information from the dependency.

    Returns:
        dict: Success message on succesful sending
    """
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    broker = find_broker(user_obj["user_id"])
    if not broker:
        raise HTTPException(status_code=404, detail="Broker not found")

    subject = f"Broker {user_obj['name']} invited you to sign up"
    body = email_data.get("body", "")
    send_broker_to_client(
        user_obj["name"],
        user_obj["email"],
        client_first_name,
        client_email,
        body,
        "onboarding",
        subject,
    )
    return {"message": "Email sent successfully"}


# ------------------------
# Document Routes
# ------------------------
@app.post("/edit/document/card")
async def edit_client_document_endpoint(
    updates: dict = Body(...), user=Depends(get_current_user)
) -> dict:
    """
    Allow clients to edit document metadata

    updates (dict): The updates to apply to the document.
    user (tuple): The current user information from the dependency.

    Returns:
        dict: Success message on succesful edit
    """
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found")

    if (
        updates["id"].startswith("xero_")
        or updates["id"].startswith("Broker_")
        or updates["id"].startswith("idmerit_")
    ):
        raise HTTPException(
            status_code=400,
            detail="Editing Xero, MYOB, or Identity Verification documents is not supported",
        )

    hashed_email = updates.pop("hashed_email", None)
    if not hashed_email:
        raise HTTPException(status_code=400, detail="Missing hashed_email")

    edit_client_document(hashed_email, updates)
    return {"status": "success"}


@app.delete("/delete/document/card")
async def delete_client_document_endpoint(
    request: Request, user=Depends(get_current_user)
) -> dict:
    """
    Allow clients to delete documents

    request (Request): The request object containing the document ID and hashed email. {id: document_id, hashed_email: hashed_email}
    user (tuple): The current user information from the dependency.

    Returns:
        dict: Success message on succesful deletion
    """
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found")
    data = await request.json()
    threadid = data.get("id")
    hashed_email = data.get("hashed_email")
    if not threadid or not hashed_email:
        raise HTTPException(status_code=400, detail="Missing id or hashed_email")

    # Check if it's a verified identity document by checking the id
    if threadid.startswith("idmerit_"):
        delete_docs_general(threadid, hashed_email, "idmerit_docs")
    # Check if it's a Xero report
    elif threadid.startswith("xero_"):
        delete_docs_general(threadid, hashed_email, "xero_reports")
    # Check if it's a MYOB report
    elif threadid.startswith("Broker_"):
        delete_docs_general(threadid, hashed_email, "myob_reports")
    else:
        delete_client_document(hashed_email, threadid)

    return {"status": "success"}


@app.post("/upload/document/card")
async def upload_document_card(
    category: str = Form(...),
    category_data: str = Form(...),
    file: UploadFile = File(...),
    user=Depends(get_current_user),
) -> dict:
    """
    Allow upload logic for client documents

    category (str): The category of the document.
    category_data (str): The JSON string containing additional category data.
    file (UploadFile): The file to upload.
    user (tuple): The current user information from the dependency.

    Returns:
        dict: Success message with uploaded document information {status: "success", uploaded_document: {...}}
    """
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found")

    email = user_obj["email"]

    try:
        category_data_dict = json.loads(category_data)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid category_data JSON")

    new_doc = await upload_client_document(email, category, category_data_dict, file)

    return {"status": "success", "uploaded_document": new_doc}


@app.get("/download/document")
async def download_document(
    id: str, category: str, hashed_email: str, user=Depends(get_current_user)
) -> dict:
    """
    Download any document by generating pre-signed URLs

    id (str): The document ID.
    category (str): The document category.
    hashed_email (str): The hashed email associated with the document.
    user (tuple): The current user information from the dependency.

    Returns:
        dict: Pre-signed URLs for downloading the document {urls: [...]}
    """
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found")

    try:
        urls = get_download_urls(hashed_email, category, id)
        return {"urls": urls}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ------------------------
# Gmail Integration
# ------------------------
@app.post("/gmail/scan")
async def gmail_scan(user=Depends(get_current_user)) -> dict:
    """
    Initiate Gmail scanning process

    user (tuple): The current user information from the dependency.

    Returns:
        dict: Consent URL for Gmail authorization {consent_url: ...}
    """
    claims, token = user

    encoded_token = token.encode()
    encrypted_message = Encryption_function.encrypt(encoded_token)

    consent_url = get_google_auth_url(encrypted_message)
    return {"consent_url": consent_url}


@app.get("/gmail/callback")
async def gmail_callback(code: str, state: str) -> RedirectResponse:
    """
    Parse the gmail callback and send to lambda function for processing

    code (str): The authorization code from Gmail.
    state (str): The encrypted state containing the user token.

    Returns:
        RedirectResponse: Redirects to the frontend with scan started indication.
    """
    decrypted_key = Encryption_function.decrypt(state)
    jwt_key = decrypted_key.decode()

    claims = verify_token(jwt_key)
    if not claims:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    auth0_id = claims["sub"]

    user_obj = find_user(auth0_id)
    client = find_client(user_obj["user_id"])
    tokens = exchange_code_for_tokens(code)
    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")

    threading.Thread(
        target=run_gmail_scan,
        args=(client["client_id"], user_obj["email"], access_token, refresh_token),
        daemon=True,
    ).start()

    return RedirectResponse(REDIRECT_URL + "?scan=started")


"""
#
# Outlook integration
#

@app.post("/outlook/scan")
async def outlook_scan(user=Depends(get_current_user)):
    claims, token = user

    # Encode user token into 'state' to persist session across redirect
    encoded_token = token.encode()
    encrypted_message = Encryption_function.encrypt(encoded_token)

    consent_url = get_outlook_auth_url(encrypted_message)
    return {"consent_url": consent_url}


@app.get("/outlook/callback")
async def outlook_callback(code: str, state: str):
    # 1. Decrypt state to get back the original user token
    try:
        decrypted_key = Encryption_function.decrypt(state)
        jwt_key = decrypted_key.decode()
        claims = verify_token(jwt_key) # Validate the user is still authenticated
        if not claims:
            raise Exception("Invalid token")
    except Exception:
         raise HTTPException(status_code=401, detail="Invalid or expired session state")

    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    client = find_client(user_obj["user_id"])

    # 2. Exchange Authorization Code for Microsoft Tokens
    try:
        tokens = exchange_outlook_code_for_tokens(code)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Token exchange failed: {str(e)}")

    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token") # Will be None if 'offline_access' scope is missing

    # 3. Start Background Scan
    threading.Thread(
        target=run_outlook_scan, # You need to define this function similar to run_gmail_scan
        args=(client["client_id"], user_obj["email"], access_token, refresh_token),
        daemon=True,
    ).start()
   
    # 4. Redirect user back to frontend
    return RedirectResponse(
        REDIRECT_URL + "?scan=started"
    )
"""

# ------------------------
# IDMERIT Routes
# ------------------------
# change it so that we redirect to the backend then to frontend
@app.post("/idmerit/user_text")
async def send_verification_text(request: Request, user=Depends(get_current_user)):
    """ """
    data = await request.json()
    claims, token = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found")
    client = find_client(user_obj["user_id"])

    # Create verification request
    response = send_idmerit_verification_message(
        client["client_id"],
        data.get("phone_number"),
        data.get("name"),
        data.get("country"),
        data.get("dob"),
        "https://dukbillapp.com/dashboard",
        IDMERIT_CALLBACK_URL,
    )

    if not response:
        raise HTTPException(status_code=500, detail="Failed to create verification")

    return {"message": "Verification text sent"}


@app.post("/idmerit/callback")
async def idmerit_callback(request: Request):
    response_data = await request.json()
    print(response_data)
    client_info = idmerit_fetch_clientid(response_data["requestId"])
    client_id = client_info.get("client_id")
    claims = verify_client(client_id)

    user_id = claims.get("user_id")
    user_obj = verify_user_by_id(user_id)
    hashed_email = hash_email(user_obj["email"])

    if response_data.get("status") == "verified":
        upload_idmerit_user_image_s3(
            response_data.get("scanImage"),
            response_data.get("scanImageBack"),
            hashed_email,
            response_data.get("documentType"),
        )

        update_anonymized_json_general(
            hashed_email, "idmerit_docs", EXPECTED_REPORTS_IDMERIT
        )

        return {"status": "User verified successfully"}

    return {"status": "User verification failed"}


# ------------------------
# Xero Routes
# ------------------------
@app.post("/api/xero-signup")
async def xero_signup(req: XeroAuthRequest) -> dict:
    """
    Xero signup workflow

    req (XeroAuthRequest): The request body containing the authorization code.

    Returns:
        dict: Success message on successful registration
    """
    user_data = await verify_xero_auth(req.code)
    if not user_data:
        raise HTTPException(status_code=401, detail="Invalid Xero authorization")

    return {"success": "User registered successfully"}


@app.post("/api/xero-signin")
async def xero_signin(req: XeroAuthRequest) -> dict:
    """
    Xero signin workflow

    req (XeroAuthRequest): The request body containing the authorization code.

    Returns:
        dict: Success message on successful sign-in
    """
    user_data = await verify_xero_auth(req.code)
    if not user_data:
        raise HTTPException(status_code=401, detail="Invalid Xero authorization")

    # Look up existing user in your database

    return {"success": "User signed in successfully"}


@app.get("/connect/xero")
async def connect_xero(user=Depends(get_current_user)) -> dict:
    # need to deny multiple connections for now
    """
    User connect to Xero app

    user (tuple): The current user information from the dependency.

    Returns:
        dict: Authorization URL for Xero OAuth2 {auth_url: ...}
    """
    claims, token = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found")

    encoded_token = token.encode()
    encrypted_message = Encryption_function.encrypt(encoded_token)
    params = {
        "response_type": "code",
        "client_id": XERO_CLIENT_ID,
        "redirect_uri": XERO_REDIRECT_URI,
        "scope": XERO_SCOPES,
        "state": encrypted_message,
    }

    auth_url = f"{AUTH_URL}?{urlencode(params)}"
    return {"auth_url": auth_url}


@app.get("/callback/xero")
async def callback_xero(code: str = "", state: str = "") -> RedirectResponse:
    """
    Handle OAuth callback from Xero

    code (str): The authorization code from Xero.
    state (str): The encrypted state containing the user token.

    Returns:
        RedirectResponse: Redirects to the frontend after processing
    """

    decrypted_key = Encryption_function.decrypt(state)
    jwt_key = decrypted_key.decode()

    claims = verify_token(jwt_key)
    if not claims:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found")
    hashed_email = hash_email(user_obj["email"])

    # Exchange code for tokens
    token_response = requests.post(
        TOKEN_URL,
        headers={
            "Authorization": f"Basic {get_basic_auth()}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": XERO_REDIRECT_URI,
        },
        timeout=10,
    )

    if token_response.status_code != 200:
        raise HTTPException(400, f"Token exchange failed: {token_response.text}")

    # Save tokens
    token_data = token_response.json()
    tokens["access_token"] = token_data["access_token"]
    tokens["refresh_token"] = token_data.get("refresh_token")
    tokens["expires_at"] = int(time.time()) + int(token_data.get("expires_in", 1800))
    tokens["scope"] = token_data.get("scope", "")

    # Get tenant/organization connections
    connections_response = requests.get(
        "https://api.xero.com/connections",
        headers={"Authorization": f"Bearer {tokens['access_token']}"},
        timeout=10,
    )

    if connections_response.status_code != 200:
        raise HTTPException(
            400, f"Failed to get connections: {connections_response.text}"
        )

    connections = connections_response.json()
    if not connections:
        return RedirectResponse(url=REDIRECT_URL, status_code=303)

    # Use first organization
    tenant_id = connections[0]["tenantId"]
    org_name = connections[0]["tenantName"]

    # Fetch all data
    all_data = fetch_all_data(tenant_id)
    preview = generate_xero_preview(all_data)

    result = {
        "status": "success",
        "organization": org_name,
        "tenant_id": tenant_id,
        "preview": preview,
    }

    if all_data.get("errors"):
        result["errors"] = all_data["errors"]

    # Generate all PDF reports
    try:
        s3_keys = generate_all_reports_xero(result, hashed_email)
        result["pdf_reports"] = s3_keys
        result["s3_bucket"] = bucket_name
    except Exception as e:
        result["pdf_error"] = str(e)

    update_anonymized_json_general(hashed_email, "xero_reports", EXPECTED_REPORTS_XERO)

    return RedirectResponse(url=REDIRECT_URL, status_code=303)


@app.get("/xero/connections")
async def get_xero_connections(user=Depends(get_current_user)) -> dict:
    """
    Fetch all Xero connections (organizations) for the logged-in user.

    user (tuple): The current user information from the dependency.

    Returns:
        dict: List of Xero connections {connections: [...]}
    """
    claims, _ = user
    auth0_id = claims["sub"]

    # Get user and client info
    user_obj = find_user(auth0_id)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found")

    client = find_client(user_obj["user_id"])
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")

    # Check if user has Xero tokens
    if not tokens.get("access_token"):
        raise HTTPException(
            status_code=400, detail="No Xero access token available. Connect first."
        )

    # Fetch connections from Xero
    response = requests.get(
        "https://api.xero.com/connections",
        headers={"Authorization": f"Bearer {tokens['access_token']}"},
        timeout=10,
    )

    if response.status_code != 200:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Failed to fetch Xero connections: {response.text}",
        )
    connections = response.json()
    return {"connections": connections}


@app.delete("/xero/connections/{connection_id}")
async def delete_xero_connection(
    connection_id: str, user=Depends(get_current_user)
) -> dict:
    """
    Delete a specific Xero connection (organization) for the logged-in user.

    connection_id (str): The ID of the Xero connection to delete.
    user (tuple): The current user information from the dependency.

    Returns:
        dict: Success message on successful deletion
    """
    claims, _ = user
    auth0_id = claims["sub"]

    # Get user and client info
    user_obj = find_user(auth0_id)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found")

    client = find_client(user_obj["user_id"])
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")

    # Check if user has Xero access token
    if not tokens.get("access_token"):
        raise HTTPException(
            status_code=400, detail="No Xero access token available. Connect first."
        )

    # Make request to Xero API to disconnect the connection
    response = requests.delete(
        f"https://api.xero.com/connections/{connection_id}",
        headers={"Authorization": f"Bearer {tokens['access_token']}"},
        timeout=10,
    )

    if response.status_code not in [200, 204]:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Failed to delete Xero connection: {response.text}",
        )

    return {"status": "success", "message": f"Xero connection {connection_id} deleted"}


# ------------------------
# MYOB
# ------------------------
@app.post("/myob/user_redirect")
async def myob_redirect(user=Depends(get_current_user)) -> dict:
    """
    Redirect the user to MYOB auth flow

    user (tuple): The current user information from the dependency.

    Returns:
        dict: Verification URL and state for MYOB authorization {verification_url: ..., state: ...}
    """
    claims, token = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found")

    encoded_token = token.encode()
    encrypted_message = Encryption_function.encrypt(encoded_token)

    url_to_redirect = build_auth_url(state=encrypted_message)

    return {"verification_url": url_to_redirect, "state": encrypted_message}


@app.get("/myob/callback")
async def myob_callback_compilation(
    request: Request, background_tasks: BackgroundTasks
) -> RedirectResponse:
    """
    Parse the MYOB callback and generate pdfs in the background

    request (Request): The request object containing query parameters.
    background_tasks (BackgroundTasks): The background tasks manager.

    Returns:
        RedirectResponse: Redirects to the frontend after processing
    """
    query_params = request.query_params
    code = query_params.get("code")
    code = urllib.parse.unquote(code)
    business_id = query_params.get("businessId")
    state = query_params.get("state")

    decrypted_key = Encryption_function.decrypt(state)
    jwt_key = decrypted_key.decode()

    claims = verify_token(jwt_key)
    if not claims:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found")

    hashed_user_email = hash_email(user_obj["email"])

    background_tasks.add_task(
        process_myob_data, code, business_id, state, hashed_user_email
    )

    update_anonymized_json_general(
        hashed_user_email, "myob_reports", EXPECTED_REPORTS_MYOB
    )

    return RedirectResponse(url=REDIRECT_URL)


# ------------------------
# Health Checks
# ------------------------
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "dukbill"}


# ------------------------
# Run App
# ------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
