from fastapi import FastAPI, HTTPException, Depends, Body, Request, File, Form, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import RedirectResponse, JSONResponse

from pydantic import BaseModel
from basiq_api import BasiqAPI

from auth import verify_token, verify_google_token, verify_xero_auth
from users import *
from documents import *
from db_init import initialize_database
from config import AUTH0_DOMAIN
from S3_utils import *
from gmail_connect import get_google_auth_url, run_gmail_scan, exchange_code_for_tokens
from file_downloads import _first_email, _invoke_zip_lambda_for, _stream_s3_zip
from redis_utils import start_expiry_listener
from shufti import shufti_url, get_verification_status_with_proofs, download_proof_image
from id_helpers import *
from xero_helpers import *
from xero_pdf_generation import *

import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
import socket
import threading
import os
from typing import List, Dict, Any
import secrets
import time
from urllib.parse import urlencode

oauth_states = {}

# ------------------------
# FastAPI App Initialization
# ------------------------
app = FastAPI(title="Dukbill API", version="1.0.0")
basiq = BasiqAPI()

# CORS settings
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
# Startup
# ------------------------

@app.on_event("startup")
async def startup_event():
    """Start Redis expiry listener when app starts"""
    start_expiry_listener()
    print("✓ Application started with Redis expiry listener")

# ------------------------
# Shufti
# ------------------------
verification_states = {}
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
    verification_states[reference] = {
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
        verification_state = get_verification_state(reference)
        
        if not verification_state:
            # Still return 200 to acknowledge callback
            return {"status": "success"}
        
        # Handle different event types
        if event == 'verification.accepted':
            await handle_verification_accepted(reference, verification_state)
            del verification_states[reference]
        
        elif event == 'verification.declined':
            handle_verification_declined(verification_state["user_id"], response_data)
            del verification_states[reference]
        
        return {"status": "success"}
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# ------------------------
# Xero
# ------------------------

session_state = {}
SCOPES = "offline_access accounting.settings.read accounting.transactions.read accounting.contacts.read accounting.attachments.read accounting.reports.read payroll.employees.read payroll.payruns.read payroll.payslip.read"

@app.get("/connect/xero")
async def connect_xero(user=Depends(get_current_user)):
    """Initiate Xero OAuth flow"""
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    client = find_client(user_obj["user_id"])
    emails = get_client_emails(client["client_id"])
    print(emails)
    hashed_email = hash_email(emails[0]["email_address"])
    print("entered")

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
        "scope": SCOPES,
        "state": state,
    }
    print("this is state before")
    print(state)
    
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
            url="https://314dbc1f-20f1-4b30-921e-c30d6ad9036e-00-19bw6chuuv0n8.riker.replit.dev/dashboard",
            status_code=303
        )
    
    # Use first organization
    tenant_id = connections[0]["tenantId"]
    org_name = connections[0]["tenantName"]
    print('before fetch')
    # Fetch all data
    all_data = fetch_all_data(tenant_id)
    print("after fetch")
    # Transform to preview format for PDF generation
    preview = {
        "settings": {
            "accounts": all_data["data"].get("accounts", [None])[0] if all_data["data"].get("accounts") else None,
            "accounts_total": len(all_data["data"].get("accounts", [])),
            "tax_rates": all_data["data"].get("tax_rates", [None])[0] if all_data["data"].get("tax_rates") else None,
            "tax_rates_total": len(all_data["data"].get("tax_rates", [])),
            "tracking_categories": all_data["data"].get("tracking_categories", [None])[0] if all_data["data"].get("tracking_categories") else None,
            "tracking_categories_total": len(all_data["data"].get("tracking_categories", [])),
        },
        "transactions": {
            "bank_transactions": all_data["data"].get("bank_transactions", [None])[0] if all_data["data"].get("bank_transactions") else None,
            "bank_transactions_total": len(all_data["data"].get("bank_transactions", [])),
            "payments": all_data["data"].get("payments", [None])[0] if all_data["data"].get("payments") else None,
            "payments_total": len(all_data["data"].get("payments", [])),
            "credit_notes": all_data["data"].get("credit_notes", [None])[0] if all_data["data"].get("credit_notes") else None,
            "credit_notes_total": len(all_data["data"].get("credit_notes", [])),
            "manual_journals": all_data["data"].get("manual_journals", [None])[0] if all_data["data"].get("manual_journals") else None,
            "manual_journals_total": len(all_data["data"].get("manual_journals", [])),
            "overpayments": all_data["data"].get("overpayments", [None])[0] if all_data["data"].get("overpayments") else None,
            "overpayments_total": len(all_data["data"].get("overpayments", [])),
            "prepayments": all_data["data"].get("prepayments", [None])[0] if all_data["data"].get("prepayments") else None,
            "prepayments_total": len(all_data["data"].get("prepayments", [])),
            "invoices": all_data["data"].get("invoices", [None])[0] if all_data["data"].get("invoices") else None,
            "invoices_total": len(all_data["data"].get("invoices", [])),
            "bank_transfers": all_data["data"].get("bank_transfers", [None])[0] if all_data["data"].get("bank_transfers") else None,
            "bank_transfers_total": len(all_data["data"].get("bank_transfers", [])),
        },
        "payroll": {
            "employees": all_data["data"].get("employees", [None])[0] if all_data["data"].get("employees") else None,
            "employees_total": len(all_data["data"].get("employees", [])),
            "payruns": all_data["data"].get("payruns", [None])[0] if all_data["data"].get("payruns") else None,
            "payruns_total": len(all_data["data"].get("payruns", [])),
            "payslips": all_data["data"].get("payslips", [None])[0] if all_data["data"].get("payslips") else None,
            "payslips_total": len(all_data["data"].get("payslips", [])),
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
        url="https://314dbc1f-20f1-4b30-921e-c30d6ad9036e-00-19bw6chuuv0n8.riker.replit.dev/dashboard",
        status_code=303
    )

# ------------------------
# Auth / User Routes
# ------------------------
@app.post("/api/google-signup")
async def google_signup(req: GoogleTokenRequest):
    payload = verify_google_token(req.googleToken)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid Google token")
    return {"success": "User registered successfully"}

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



@app.post("/auth/client/register")
async def register(user=Depends(get_current_user)):
    claims, access_token = user
    profile = get_user_info_from_auth0(access_token)
    auth0_id = profile["sub"]
    user_obj = find_user(auth0_id)
    missing_fields = []

    if not user_obj:
        # New user — register and mark missing fields
        user_id = register_user(auth0_id, profile["email"], profile["picture"], profileComplete=False)
        missing_fields = ["name", "company", "phone"]
        return {
            "user": user_id,
            "isNewUser": True,
            "missingFields": missing_fields,
            "profileComplete": False,
        }

    if user_obj["profile_complete"]:
        return {
            "user": user_obj["user_id"],
            "isNewUser": False,
            "missingFields": [],
            "profileComplete": True,
        }

    for field in ["name", "company", "phone"]:
        if not user_obj.get(field):
            missing_fields.append(field)

    return {
        "user": user_obj["user_id"],
        "isNewUser": False,
        "missingFields": missing_fields,
        "profileComplete": False,
    }

@app.post("/auth/check-verification")
async def user_email_authentication(user=Depends(get_current_user)):
    claims, access_token = user
    profile = get_user_info_from_auth0(access_token)
    return {"email_verified": profile["email_verified"]}

# @app.get("/auth/logout")
# async def logout():
#     logout_url = (
#         f"https://{AUTH0_DOMAIN}/v2/logout?"
#         f"client_id={AUTH0_CLIENT_ID}&"
#         f"returnTo={POST_LOGOUT_REDIRECT_URI}&"
#         f"federated"
#     )
#     return RedirectResponse(url=logout_url)

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
        "https://314dbc1f-20f1-4b30-921e-c30d6ad9036e-00-19bw6chuuv0n8.riker.replit.dev/dashboard?scan=started"
    )
# ------------------------
# User Profile
# ------------------------
@app.get("/user/profile")
async def fetch_user_profile(user=Depends(get_current_user)):
    claims, access_token = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    if user_obj["isBroker"]:
        profile = find_broker(user_obj["user_id"])
        profile_id = profile["broker_id"]
        user_type = "broker"
    else:
        profile = find_client(user_obj["user_id"])
        profile_id = profile["client_id"]
        user_type = "client"
    # Need to remove email_scan from frontend
    return {"name": user_obj["name"], "id": profile_id, "picture": user_obj["picture"], "user_type": user_type, "email_scan": False}

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
    if verified_headings:
        headings.extend(verified_headings)
    if xero_verified_documents:
        headings.extend(xero_verified_documents)
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
    result = _invoke_zip_lambda_for(email)  # {"zip_key":..., "presigned_url":..., ...}
    zip_key = result["zip_key"]
    filename = f"client_{client_id}_documents.zip"
    return _stream_s3_zip(zip_key, filename)

@app.post("/brokers/client/{client_id}/verify")
async def verify_client_documents(client_id: int, user=Depends(get_current_user)):
    toggle_client_verification(client_id)
    client = verify_client(client_id)
    return {"broker_verify": int(client["broker_verify"])}
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
    
    # Check if it's a verified identity document by checking the id
    if threadid in identity_doc_types:
        delete_client_document_identity(threadid, hashed_email)
    # Check if it's a Xero report
    elif threadid in xero_report_types:
        delete_client_xero_report(threadid, hashed_email)
    else:
        delete_client_document(hashed_email, threadid)
    
    return {"status": "success"}

@app.post("/upload/document/card")
async def upload_document_card(
    category: str = Form(...),
    company: str = Form(...),
    amount: str = Form(...),
    date: str = Form(...),
    file: UploadFile = File(...),
    user=Depends(get_current_user),
):
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found")
    email = user_obj["email"]
    new_doc = await upload_client_document(email, category, company, amount, date, file)
    return {"status": "success", "uploaded_document": new_doc}


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
# Health Check
# ------------------------
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "dukbill"}

'''
# ------------------------
# IPv6 Check
# ------------------------
@app.get("/debug/network")
async def debug_network():
    """Debug endpoint to test network connectivity"""
    import socket
    results = {
        "hostname": socket.gethostname(),
        "ipv6_test": None,
        "auth0_dns": None,
        "auth0_connection": None
    }

    try:
        # Test if we have IPv6 address
        addrs = socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET6)
        results["ipv6_test"] = f"Found {len(addrs)} IPv6 addresses"
    except Exception as e:
        results["ipv6_test"] = f"Error: {str(e)}"

    try:
        # Test Auth0 DNS resolution
        addrs = socket.getaddrinfo(AUTH0_DOMAIN, 443, socket.AF_INET6)
        results["auth0_dns"] = [addr[4][0] for addr in addrs[:3]]
    except Exception as e:
        results["auth0_dns"] = f"Error: {str(e)}"

    try:
        # Test actual connection
        import requests
        response = requests.get(f"https://{AUTH0_DOMAIN}/.well-known/jwks.json", timeout=5)
        results["auth0_connection"] = f"Success: {response.status_code}"
    except Exception as e:
        results["auth0_connection"] = f"Error: {str(e)}"

    return results
'''
# ------------------------

# Internet/NAT connectivity check
# ------------------------
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

    # 1) DNS
    try:
        socket.getaddrinfo("example.com", 443, type=socket.SOCK_STREAM)
        result["dns_ok"] = True
    except Exception as e:
        result["errors"].append(f"DNS resolution failed: {e!r}")

    # 2) HTTPS + public IP (works over IPv4 NAT)
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

    # 3) Optional HTTP (some orgs block 80—non-fatal)
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

    # Exit code is only relevant if you run this as a script; for API we just return JSON
    return result

# Run App
# ------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
