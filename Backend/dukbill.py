from fastapi import FastAPI, HTTPException, Depends, Body, Request, File, Form, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import RedirectResponse

from pydantic import BaseModel
from basiq_api import BasiqAPI

from auth import verify_token, verify_google_token
from users import *
from documents import *
from db_init import initialize_database
from config import AUTH0_DOMAIN
from S3_utils import *
from gmail_connect import get_google_auth_url, run_gmail_scan, exchange_code_for_tokens

import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
import socket
import threading
import os

import secrets
import time
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
# Auth / User Routes
# ------------------------
@app.post("/api/google-signup")
async def google_signup(req: GoogleTokenRequest):
    payload = verify_google_token(req.googleToken)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid Google token")
    return {"success": "User registered successfully"}

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
    print(profile)
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
    print("this is auth0 id")
    print(auth0_id)
    user_obj = find_user(auth0_id)
    client = find_client(user_obj["user_id"])
    emails = get_client_emails(client["client_id"])
    print(emails)
    headings = get_client_dashboard(client["client_id"], emails)
    return {"headings": headings, "BrokerAccess": client["brokerAccess"]}

@app.post("/clients/category/documents")
async def get_category_documents(request: dict, user=Depends(get_current_user)):
    claims, _ = user
    auth0_id = claims["sub"]
    category = request.get("category")
    user_obj = find_user(auth0_id)
    client = find_client(user_obj["user_id"])
    emails = get_client_emails(client["client_id"])
    print(emails)
    return get_client_category_documents(client["client_id"], emails, category)

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
    emails = get_client_emails(client_user["client_id"])
    headings = get_client_dashboard(client_id, emails)
    print(emails)
    return {"headings": headings, "BrokerAccess": client["brokerAccess"]}

@app.post("/brokers/client/{client_id}/category/documents")
async def get_category_documents_broker(client_id: int, request: dict, user=Depends(get_current_user)):
    claims, _ = user
    client = verify_client(client_id)
    if not client["brokerAccess"]:
        return {"error": "Access denied"}
    category = request.get("category")
    client_user = get_user_from_client(client_id)
    emails = get_client_emails(client_user["client_id"])
    print(emails)
    return get_client_category_documents(client_id, emails, category)

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
async def edit_client_document_endpoint(updates: dict = Body(...), user=Depends(get_current_user)):
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found")
    email = user_obj["email"]
    edit_client_document(email, updates)
    return {"status": "success"}

@app.delete("/delete/document/card")
async def delete_client_document_endpoint(request: Request, user=Depends(get_current_user)):
    data = await request.json()
    threadid = data.get("id")
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found")
    delete_client_document(user_obj["email"], threadid)
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
# Run App
# ------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
