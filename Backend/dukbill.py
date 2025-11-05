# ------------------------
# FastAPI App Imports
# ------------------------
from fastapi import FastAPI, HTTPException, Depends, Body, Request, File, Form, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import RedirectResponse
from fastapi import BackgroundTasks

# ------------------------
# Model Imports
# ------------------------
from pydantic import BaseModel
from External_APIs.basiq_api import BasiqAPI

# ------------------------
# File Imports
# ------------------------
from config import AUTH0_DOMAIN, XERO_SCOPES
from auth import *
from users import *
from Documents.documents import *
from Database.db_init import initialize_database
from Database.S3_utils import *
from EmailScanners.gmail_connect import get_google_auth_url, run_gmail_scan, exchange_code_for_tokens
from Documents.file_downloads import _first_email, _invoke_zip_lambda_for, _stream_s3_zip
from redis_utils import start_expiry_listener
from shufti import shufti_url
from helpers.id_helpers import *
from helpers.xero_helpers import *
from External_APIs.xero_pdf_generation import *
from helpers.myob_helper import build_auth_url, retrieve_endpoints_myob, get_access_token_myob
from External_APIs.myob_pdf_generation import generate_payroll_pdf, generate_sales_pdf, generate_banking_pdf, generate_purchases_pdf

# ------------------------
# Python Imports
# ------------------------
import requests
import threading
import os
import secrets
import time
import urllib
from urllib.parse import urlencode

# ------------------------
# FastAPI App Initialization
# ------------------------
app = FastAPI(title="Dukbill API", version="1.0.0")
basiq = BasiqAPI()
oauth_states = {}
verification_states_myob = {}
verification_states_shufti = {}
session_state = {}
REDIRECT_URL = os.environ.get("REDIRECT_DUKBILL", "https://314dbc1f-20f1-4b30-921e-c30d6ad9036e-00-19bw6chuuv0n8.riker.replit.dev/dashboard")

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
    "https://dukbillapp.com"
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

# ------------------------
# Dependencies
# ------------------------
def get_user_info_from_auth0(access_token: str):
    userinfo_url = f"https://{AUTH0_DOMAIN}/userinfo"
    session = requests.Session()
    try:
        response = session.get(
            userinfo_url,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=5
        )
        if response.status_code != 200:
            raise HTTPException(status_code=401, detail="Failed to fetch user profile from Auth0")
        return response.json()
    except requests.RequestException as e:
        raise HTTPException(status_code=503, detail=f"Auth0 request failed: {str(e)}")
    
def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    claims = verify_token(token)
    if not claims:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return claims, token

# ------------------------
# Redis Startup
# ------------------------
@app.on_event("startup")
async def startup_event():
    start_expiry_listener()

# ------------------------
# Auth Routes
# ------------------------
@app.post("/api/google-signup")
async def google_signup(req: GoogleTokenRequest):
    payload = verify_google_token(req.googleToken)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid Google token")
    return {"success": "User registered successfully"}

@app.post("/auth/register")
async def register(user=Depends(get_current_user)):
    claims, access_token = user
    profile = get_user_info_from_auth0(access_token)
    auth0_id = profile["sub"]

    # Call the extracted core logic function
    result = handle_registration(auth0_id, profile)
    return result

@app.post("/auth/check-verification")
async def user_email_authentication(user=Depends(get_current_user)):
    claims, access_token = user
    profile = get_user_info_from_auth0(access_token)
    return {"email_verified": profile["email_verified"]}

# ------------------------
# User Profile
# ------------------------
@app.get("/user/profile")
async def fetch_user_profile(user=Depends(get_current_user)):
    claims, access_token = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)

    jwt_info = get_user_info_from_auth0(access_token)
    
    if user_obj["isBroker"]:
        profile = find_broker(user_obj["user_id"])
        profile_id = profile["broker_id"]
        user_type = "broker"
    else:
        profile = find_client(user_obj["user_id"])
        profile_id = profile["client_id"]
        user_type = "client"

    return {"name": user_obj["name"], "id": profile_id, "picture": user_obj["picture"], "user_type": user_type, "email_verified": jwt_info["email_verified"]}

@app.patch("/users/onboarding")
async def complete_profile(profile_data: dict, user=Depends(get_current_user)):
    claims, access_token = user
    profile = get_user_info_from_auth0(access_token)
    auth0_id = profile["sub"]
    user_type = profile_data["user_type"]
    broker_id = profile_data.pop("broker_id", None)
    user_obj = update_profile(auth0_id, profile_data)
    validatedBroker = False

    if user_type == "client":
        client_id = register_client(user_obj["user_id"], broker_id)
        client_add_email(client_id, get_email_domain(user_obj["email"]), user_obj["email"])
        validatedBroker = bool(client_id)
        
    elif user_type == "broker":
        register_broker(user_obj["user_id"])
        validatedBroker = True

    return {
        "user": user_obj["user_id"],
        "profileComplete": user_obj["profile_complete"],
        "missingFields": [f for f in ["full_name", "phone_number", "company_name"] if not user_obj.get(f)],
        "validatedBroker": validatedBroker,
    }

@app.post("/add/email")
async def add_email(email: str, user=Depends(get_current_user)):
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

# ------------------------
# Client Routes
# ------------------------
@app.get("/clients/dashboard")
async def get_client_documents(user=Depends(get_current_user)):
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    client = find_client(user_obj["user_id"])
    emails = get_client_emails(client["client_id"])
    headings = get_client_dashboard(client["client_id"], emails)
    verified_headings = get_client_verified_ids_dashboard(client["client_id"], emails)
    xero_verified_documents = get_xero_verified_documents_dashboard(client["client_id"], emails)
    myob_verified_documents = get_myob_verified_documents_dashboard(client["client_id"], emails)

    if verified_headings:
        headings.extend(verified_headings)
    if xero_verified_documents:
        headings.extend(xero_verified_documents)
    if myob_verified_documents:
        headings.extend(myob_verified_documents)
    return {"headings": headings, "BrokerAccess": client["brokerAccess"]}

@app.post("/clients/category/documents")
async def get_category_documents(request: dict, user=Depends(get_current_user)):
    claims, _ = user
    auth0_id = claims["sub"]
    category = request.get("category")
    user_obj = find_user(auth0_id)
    client = find_client(user_obj["user_id"])
    emails = get_client_emails(client["client_id"])

    documents = get_client_category_documents(client["client_id"], emails, category)

    if category in ["Driving License", "Id Card", "Passport"]:
        verified_docs = get_client_verified_ids_documents(client["client_id"], emails, category)
        documents.extend(verified_docs)
    
    # Check if category is a Xero report type
    if category in ["Accounts Report", "Bank Transfers Report", "Credit Notes Report", 
                    "Financial Reports", "Invoices Report", "Payments Report", 
                    "Payroll Report", "Transactions Report"]:
        xero_docs = get_client_xero_documents(client["client_id"], emails, category)
        documents.extend(xero_docs)
    
    # Check if category is a MYOB report type
    if category in ["Payroll Summary", "Sales Summary", "Banking Summary", "Purchases Summary"]:
        myob_docs = get_client_myob_documents(client["client_id"], emails, category)
        documents.extend(myob_docs)

    return documents

@app.post("/broker/access")
async def toggle_broker_access_route(user=Depends(get_current_user)):
    claims, _ = user
    auth0_id = claims["sub"]
    user = find_user(auth0_id)
    client = find_client(user["user_id"])
    
    toggle_broker_access(client["client_id"])
    return {"BrokerAccess": not client["brokerAccess"]}

# ------------------------
# Broker Routes
# ------------------------
@app.get("/brokers/client/list")
async def get_client_list(user=Depends(get_current_user)):
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    broker = find_broker(user_obj["user_id"])
    clients = get_broker_clients(broker["broker_id"])
    return {"clients": clients}

@app.get("/brokers/client/{client_id}/dashboard")
async def get_client_dashboard_broker(client_id: int, user=Depends(get_current_user)):
    claims, _ = user
    client = verify_client(client_id)
    client_user = get_user_from_client(client_id)
    emails = get_client_emails(client_id)
    headings = get_client_dashboard(client_id, emails)
    return {"headings": headings, "BrokerAccess": client["brokerAccess"]}

@app.post("/brokers/client/{client_id}/category/documents")
async def get_category_documents_broker(client_id: int, request: dict, user=Depends(get_current_user)):
    claims, _ = user
    client = verify_client(client_id)
    if not client["brokerAccess"]:
        return {"error": "Access denied"}
    category = request.get("category")
    client_user = get_user_from_client(client_id)
    emails = get_client_emails(client_id)
    return get_client_category_documents(client_id, emails, category)

@app.get("/brokers/client/{client_id}/documents/download")
async def download_client_documents(client_id: int, user=Depends(get_current_user)):
    client = verify_client(client_id)
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    if not client.get("brokerAccess"):
        raise HTTPException(status_code=403, detail="Access denied")

    email = _first_email(get_client_emails(client_id))
    result = _invoke_zip_lambda_for(email)
    zip_key = result["zip_key"]
    filename = f"client_{client_id}_documents.zip"
    return _stream_s3_zip(zip_key, filename)

@app.post("/brokers/client/{client_id}/verify")
async def verify_client_documents(client_id: int, user=Depends(get_current_user)):
    toggle_client_verification(client_id)
    client = verify_client(client_id)
    return {"broker_verify": int(client["broker_verify"])}

# ------------------------
# Document Routes
# ------------------------
@app.post("/edit/document/card")
async def edit_client_document_endpoint(
    updates: dict = Body(...),
    user=Depends(get_current_user)
):
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found")

    hashed_email = updates.pop("hashed_email", None)
    if not hashed_email:
        raise HTTPException(status_code=400, detail="Missing hashed_email")

    edit_client_document(hashed_email, updates)
    return {"status": "success"}

@app.delete("/delete/document/card")
async def delete_client_document_endpoint(
    request: Request,
    user=Depends(get_current_user)
):
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
    
    # Known identity document types
    identity_doc_types = ["driving_license", "id_card", "passport"]
    
    # Known Xero report types
    xero_report_types = [
        "xero_accounts_report",
        "xero_bank_transfers_report",
        "xero_credit_notes_report",
        "xero_financial_reports",
        "xero_invoices_report",
        "xero_payments_report",
        "xero_payroll_report",
        "xero_transactions_report"
    ]
    
    # Known MYOB report types
    myob_report_types = [
        "Broker_Payroll_Summary",
        "Broker_Sales_Summary",
        "Broker_Banking_Summary",
        "Broker_Purchases_Summary"
    ]
    
    # Check if it's a verified identity document by checking the id
    if threadid in identity_doc_types:
        delete_client_document_identity(threadid, hashed_email)
    # Check if it's a Xero report
    elif threadid in xero_report_types:
        delete_client_xero_report(threadid, hashed_email)
    # Check if it's a MYOB report
    elif threadid in myob_report_types:
        delete_client_myob_report(threadid, hashed_email)
    else:
        delete_client_document(hashed_email, threadid)
    
    return {"status": "success"}

@app.post("/upload/document/card")
async def upload_document_card(
    category: str = Form(...),
    category_data: str = Form(...),
    file: UploadFile = File(...),
    user=Depends(get_current_user),
):
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
    id: str,
    category: str,
    hashed_email: str,
    user=Depends(get_current_user)
):
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
async def gmail_scan(user=Depends(get_current_user)):
    claims, _ = user
    auth0_id = claims["sub"]

    state = secrets.token_urlsafe(32)
    oauth_states[state] = {
        "auth0_id": auth0_id,
        "expires": time.time() + 600
    }
    
    consent_url = get_google_auth_url(state)
    return {"consent_url": consent_url}

@app.get("/gmail/callback")
async def gmail_callback(code: str, state: str):
    state_data = oauth_states.get(state)
    
    if not state_data:
        raise HTTPException(status_code=403, detail="Invalid or expired state token")
    
    if state_data["expires"] < time.time():
        del oauth_states[state]
        raise HTTPException(status_code=403, detail="State token expired")
    
    auth0_id = state_data["auth0_id"]
    del oauth_states[state]
    
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
    
    return RedirectResponse(
        REDIRECT_URL + "?scan=started"
    )

# ------------------------
# Basiq Integration
# ------------------------
@app.get("/basiq/connect")
async def connect_bank(user=Depends(get_current_user)):
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    if not user_obj["basiq_id"]:
        basiq_user = basiq.create_user(user_obj["email"], user_obj["phone"])
        add_basiq_id(user_obj["user_id"], basiq_user["id"])
        user_obj = find_user(auth0_id)
    client_token = basiq.get_client_access_token(user_obj["basiq_id"])
    consent_url = f"https://consent.basiq.io/home?token={client_token}"
    return RedirectResponse(consent_url)

@app.get("/clients/bank/transactions")
async def get_client_bank_transactions(user=Depends(get_current_user)):
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    basiq_id = user_obj.get("basiq_id")
    if not basiq_id:
        return {"transactions": []}
    connections = basiq.get_user_connections(basiq_id).get("data", [])
    if not connections:
        return {"transactions": []}
    transactions = basiq.get_user_transactions(basiq_id, active_connections=connections)
    return {"transactions": transactions}

@app.get("/brokers/client/{client_id}/bank/transactions")
async def get_broker_client_bank_transactions(client_id: int, user=Depends(get_current_user)):
    client = verify_client(client_id)
    if not client.get("brokerAccess"):
        return {"error": "Access denied"}
    client_user = get_user_from_client(client_id)
    basiq_id = client_user.get("basiq_id")
    if not basiq_id:
        return {"transactions": []}
    connections = basiq.get_user_connections(basiq_id).get("data", [])
    if not connections:
        return {"transactions": []}
    transactions = basiq.get_user_transactions(basiq_id, active_connections=connections)
    return {"transactions": transactions}

# ------------------------
# Shufti
# ------------------------
@app.post("/shufti/user_redirect")
async def shufti_redirect(user=Depends(get_current_user)):
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    client = find_client(user_obj["user_id"])
    emails = get_client_emails(client["client_id"])
    # Get user info
    user_obj = find_user(auth0_id)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Create verification request
    response = shufti_url(user_obj["email"], user_obj["user_id"])
    if not response:
        raise HTTPException(status_code=500, detail="Failed to create verification")
    
    reference = response.get("reference")

    # Store the mapping of reference to user
    verification_states_shufti[reference] = {
        "user_id": user_obj["user_id"],
        "auth0_id": auth0_id,
        "email": user_obj["email"],
        "created_at": time.time(),
        "emails": emails,
        "client_id": client["client_id"]
    }
    
    return {
        "verification_url": response["verification_url"],
        "reference": reference
    }

@app.post('/profile/notifyCallback')
async def notify_callback(request: Request):
    try:
        raw_data = await request.body()
        response_data = await request.json()
        
        # Verify signature
        SECRET_KEY = os.environ.get("SHUFTI_SECRET_KEY")
        sp_signature = request.headers.get('signature', '')
        
        if not verify_signature(raw_data, sp_signature, SECRET_KEY):
            raise HTTPException(status_code=401, detail="Invalid signature")
        
        reference = response_data.get('reference')
        event = response_data.get('event')
        
        log_callback_event(event, reference)
        
        # Find the user associated with this verification
        verification_state = get_verification_state(reference, get_verification_state)
        
        if not verification_state:
            # Still return 200 to acknowledge callback
            return {"status": "success"}
        
        # Handle different event types
        if event == 'verification.accepted':
            await handle_verification_accepted(reference, verification_state)
            del verification_states_shufti[reference]
        
        elif event == 'verification.declined':
            handle_verification_declined(verification_state["user_id"], response_data)
            del verification_states_shufti[reference]
        
        return {"status": "success"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ------------------------
# Xero Routes
# ------------------------
@app.post("/api/xero-signup")
async def xero_signup(req: XeroAuthRequest):
    """
    Exchange authorization code for tokens and get user info
    Similar to your Google signup flow
    """
    user_data = await verify_xero_auth(req.code)
    if not user_data:
        raise HTTPException(status_code=401, detail="Invalid Xero authorization")
    
    # user_data will contain email, name, xero_user_id, tenant_id
    # Create/update user in your database here
    
    return {"success": "User registered successfully"}

@app.post("/api/xero-signin")
async def xero_signin(req: XeroAuthRequest):
    """
    Same flow as signup - Xero OAuth handles both
    """
    user_data = await verify_xero_auth(req.code)
    if not user_data:
        raise HTTPException(status_code=401, detail="Invalid Xero authorization")
    
    # Look up existing user in your database
    
    return {"success": "User signed in successfully"}

@app.get("/connect/xero")
async def connect_xero(user=Depends(get_current_user)):
    """Initiate Xero OAuth flow"""
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    client = find_client(user_obj["user_id"])
    emails = get_client_emails(client["client_id"])
    hashed_email = hash_email(emails[0]["email_address"])

    # Get user info
    user_obj = find_user(auth0_id)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found")
    state = secrets.token_urlsafe(24)
    session_state[state] = {"timestamp": int(time.time()), "hashed_email": hashed_email}
    
    params = {
        "response_type": "code",
        "client_id": XERO_CLIENT_ID,
        "redirect_uri": XERO_REDIRECT_URI,
        "scope": XERO_SCOPES,
        "state": state,
    }
    
    auth_url = f"{AUTH_URL}?{urlencode(params)}"
    return {"auth_url": auth_url}

@app.get("/callback/xero")
async def callback_xero(code: str = "", state: str = ""):
    """Handle OAuth callback from Xero"""
    global tenant_id
    
    # Verify state
    if state not in session_state:
        raise HTTPException(400, "Invalid state")
    hashed_email = session_state[state]["hashed_email"]
    session_state.pop(state)
    
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
            "redirect_uri": XERO_REDIRECT_URI
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
        raise HTTPException(400, f"Failed to get connections: {connections_response.text}")
    
    connections = connections_response.json()
    if not connections:
        return RedirectResponse(
            url=REDIRECT_URL,
            status_code=303
        )
    
    # Use first organization
    tenant_id = connections[0]["tenantId"]
    org_name = connections[0]["tenantName"]
    
    # Fetch all data
    all_data = fetch_all_data(tenant_id)
    
    # Transform to preview format for PDF generation
    preview = {
        "settings": {
            "accounts": (all_data["data"].get("accounts") or [None])[0],
            "accounts_total": len(all_data["data"].get("accounts", [])),
            "accounts_list": all_data["data"].get("accounts", []),

            "tax_rates": (all_data["data"].get("tax_rates") or [None])[0],
            "tax_rates_total": len(all_data["data"].get("tax_rates", [])),
            "tax_rates_list": all_data["data"].get("tax_rates", []),

            "tracking_categories": (all_data["data"].get("tracking_categories") or [None])[0],
            "tracking_categories_total": len(all_data["data"].get("tracking_categories", [])),
            "tracking_categories_list": all_data["data"].get("tracking_categories", []),
        },
        "transactions": {
            "bank_transactions": (all_data["data"].get("bank_transactions") or [None])[0],
            "bank_transactions_total": len(all_data["data"].get("bank_transactions", [])),
            "bank_transactions_list": all_data["data"].get("bank_transactions", []),

            "payments": (all_data["data"].get("payments") or [None])[0],
            "payments_total": len(all_data["data"].get("payments", [])),
            "payments_list": all_data["data"].get("payments", []),

            "credit_notes": (all_data["data"].get("credit_notes") or [None])[0],
            "credit_notes_total": len(all_data["data"].get("credit_notes", [])),
            "credit_notes_list": all_data["data"].get("credit_notes", []),

            "manual_journals": (all_data["data"].get("manual_journals") or [None])[0],
            "manual_journals_total": len(all_data["data"].get("manual_journals", [])),
            "manual_journals_list": all_data["data"].get("manual_journals", []),

            "overpayments": (all_data["data"].get("overpayments") or [None])[0],
            "overpayments_total": len(all_data["data"].get("overpayments", [])),
            "overpayments_list": all_data["data"].get("overpayments", []),

            "prepayments": (all_data["data"].get("prepayments") or [None])[0],
            "prepayments_total": len(all_data["data"].get("prepayments", [])),
            "prepayments_list": all_data["data"].get("prepayments", []),

            "invoices": (all_data["data"].get("invoices") or [None])[0],
            "invoices_total": len(all_data["data"].get("invoices", [])),
            "invoices_list": all_data["data"].get("invoices", []),

            "bank_transfers": (all_data["data"].get("bank_transfers") or [None])[0],
            "bank_transfers_total": len(all_data["data"].get("bank_transfers", [])),
            "bank_transfers_list": all_data["data"].get("bank_transfers", []),
        },
        "payroll": {
            "employees": (all_data["data"].get("employees") or [None])[0],
            "employees_total": len(all_data["data"].get("employees", [])),
            "employees_list": all_data["data"].get("employees", []),

            "payruns": (all_data["data"].get("payruns") or [None])[0],
            "payruns_total": len(all_data["data"].get("payruns", [])),
            "payruns_list": all_data["data"].get("payruns", []),

            "payslips": (all_data["data"].get("payslips") or [None])[0],
            "payslips_total": len(all_data["data"].get("payslips", [])),
            "payslips_list": all_data["data"].get("payslips", []),
        },
        "reports": {
            "profit_loss": all_data["data"].get("profit_loss"),
            "balance_sheet": all_data["data"].get("balance_sheet"),
        }
    }
    
    result = {
        "status": "success",
        "organization": org_name,
        "tenant_id": tenant_id,
        "preview": preview
    }
    
    if all_data.get("errors"):
        result["errors"] = all_data["errors"]
    
    # Generate all PDF reports
    try:
        s3_keys = []
        s3_keys.append(generate_accounts_report(result, "xero_accounts_report.pdf", hashed_email))
        s3_keys.append(generate_transactions_report(result, "xero_transactions_report.pdf", hashed_email))
        s3_keys.append(generate_payments_report(result, "xero_payments_report.pdf", hashed_email))
        s3_keys.append(generate_credit_notes_report(result, "xero_credit_notes_report.pdf", hashed_email))
        s3_keys.append(generate_payroll_report(result, "xero_payroll_report.pdf", hashed_email))
        s3_keys.append(generate_invoices_report(result, "xero_invoices_report.pdf", hashed_email))
        s3_keys.append(generate_bank_transfers_report(result, "xero_bank_transfers_report.pdf", hashed_email))
        s3_keys.append(generate_reports_summary(result, "xero_financial_reports.pdf", hashed_email))
        
        result["pdf_reports"] = s3_keys
        result["s3_bucket"] = bucket_name
    except Exception as e:
        result["pdf_error"] = str(e)

    return RedirectResponse(
        url=REDIRECT_URL,
        status_code=303
    )
    
@app.get("/xero/connections")
async def get_xero_connections(user=Depends(get_current_user)):
    """
    Fetch all Xero connections (organizations) for the logged-in user.
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
        raise HTTPException(status_code=400, detail="No Xero access token available. Connect first.")

    # Fetch connections from Xero
    response = requests.get(
        "https://api.xero.com/connections",
        headers={"Authorization": f"Bearer {tokens['access_token']}"},
        timeout=10
    )

    if response.status_code != 200:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Failed to fetch Xero connections: {response.text}"
        )

    connections = response.json()
    return {"connections": connections}

@app.delete("/xero/connections/{connection_id}")
async def delete_xero_connection(connection_id: str, user=Depends(get_current_user)):
    """
    Delete a specific Xero connection (organization) for the logged-in user.
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
        raise HTTPException(status_code=400, detail="No Xero access token available. Connect first.")

    # Make request to Xero API to disconnect the connection
    response = requests.delete(
        f"https://api.xero.com/connections/{connection_id}",
        headers={"Authorization": f"Bearer {tokens['access_token']}"},
        timeout=10
    )

    if response.status_code not in [200, 204]:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Failed to delete Xero connection: {response.text}"
        )

    return {"status": "success", "message": f"Xero connection {connection_id} deleted"}

# ------------------------
# MYOB
# ------------------------
@app.post("/myob/user_redirect")
async def myob_redirect(user=Depends(get_current_user)):
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found")
    
    client = find_client(user_obj["user_id"])
    emails = get_client_emails(client["client_id"])
    
    state = secrets.token_urlsafe(32)
    
    url_to_redirect = build_auth_url(state=state)
    
    verification_states_myob[state] = {
        "user_id": user_obj["user_id"],
        "auth0_id": auth0_id,
        "email": user_obj["email"],
        "created_at": time.time(),
        "emails": emails,
        "client_id": client["client_id"]
    }
    
    return {
        "verification_url": url_to_redirect,
        "state": state
    }

@app.get("/myob/callback")
async def myob_callback_compilation(request: Request, background_tasks: BackgroundTasks):
    query_params = request.query_params
    code = query_params.get("code")
    code = urllib.parse.unquote(code)
    business_id = query_params.get("businessId")
    state = query_params.get("state")
    
    if not code:
        raise HTTPException(status_code=400, detail="No authorization code received")
    
    if state not in verification_states_myob:
        raise HTTPException(status_code=400, detail="Invalid state parameter")
    
    user_data = verification_states_myob[state]
    user_email = user_data.get("emails")

    if isinstance(user_email, list) and user_email:
        user_email = user_email[0]
    elif not user_email:
        raise HTTPException(status_code=400, detail="No email found for user")
    
    hashed_user_email = hash_email(user_email['email_address'])
    
    background_tasks.add_task(
        process_myob_data,
        code,
        business_id,
        state,
        hashed_user_email
    )

    return RedirectResponse(url=REDIRECT_URL)

def process_myob_data(code: str, business_id: str, state: str, hashed_user_email: str):
    try:
        tokens = get_access_token_myob(code)
        
        if not tokens:
            return
        
        access_token = tokens.get("access_token")
        # refresh_token = tokens.get("refresh_token")
        
        myob_data = retrieve_endpoints_myob(access_token, business_id)

        payroll_pdf = generate_payroll_pdf(myob_data)
        sales_pdf = generate_sales_pdf(myob_data)
        banking_pdf = generate_banking_pdf(myob_data)
        purchases_pdf = generate_purchases_pdf(myob_data)
        
        # Upload to S3
        upload_myob_pdf_to_s3(payroll_pdf, hashed_user_email, "Broker_Payroll_Summary.pdf")
        upload_myob_pdf_to_s3(sales_pdf, hashed_user_email, "Broker_Sales_Summary.pdf")
        upload_myob_pdf_to_s3(banking_pdf, hashed_user_email, "Broker_Banking_Summary.pdf")
        upload_myob_pdf_to_s3(purchases_pdf, hashed_user_email, "Broker_Purchases_Summary.pdf")
        
    except Exception as e:
        print(f"✗ Error processing MYOB data: {e}")
    finally:
        if state in verification_states_myob:
            del verification_states_myob[state]

# ------------------------
# Health Checks
# ------------------------
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "dukbill"}

@app.get("/internet/check")
async def check_internet():
    """
    Verify outbound internet from this ECS task (via NAT for IPv4).
    - DNS resolution (example.com)
    - HTTPS to ipify (returns the NAT public IP)
    - Optional HTTP (non-fatal)
    """
    import socket, time, json, urllib.request
    result = {
        "dns_ok": False,
        "https_ok": False,
        "http_ok": False,
        "public_ip": None,
        "errors": [],
        "timestamp": int(time.time()),
    }

    try:
        socket.getaddrinfo("example.com", 443, type=socket.SOCK_STREAM)
        result["dns_ok"] = True
    except Exception as e:
        result["errors"].append(f"DNS resolution failed: {e!r}")

    try:
        req = urllib.request.Request(
            "https://api.ipify.org?format=json",
            headers={"User-Agent": "dukbill-ecs-egress-check/1.0"},
        )
        with urllib.request.urlopen(req, timeout=4) as r:
            if 200 <= r.status < 300:
                body = json.loads(r.read().decode("utf-8", "replace"))
                result["public_ip"] = body.get("ip")
                result["https_ok"] = bool(result["public_ip"])
                if not result["public_ip"]:
                    result["errors"].append("HTTPS ok but missing public IP in body.")
            else:
                result["errors"].append(f"HTTPS status {r.status}")
    except Exception as e:
        result["errors"].append(f"HTTPS request failed: {e!r}")

    try:
        req = urllib.request.Request(
            "http://example.com/",
            headers={"User-Agent": "dukbill-ecs-egress-check/1.0"},
        )
        with urllib.request.urlopen(req, timeout=4) as r:
            if 200 <= r.status < 400:
                result["http_ok"] = True
    except Exception:
        pass

    return result

# ------------------------
# Run App
# ------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
