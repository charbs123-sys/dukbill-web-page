from fastapi import FastAPI, HTTPException, Depends, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import RedirectResponse

from pydantic import BaseModel
from basiq_api import BasiqAPI


from auth import verify_token, verify_google_token
from users import *
from documents import *
from db_init import initialize_database
from config import AUTH0_DOMAIN, AUTH0_CLIENT_ID, POST_LOGOUT_REDIRECT_URI
from S3_utils import *
from gmail_connect import get_google_auth_url, run_gmail_scan, exchange_code_for_tokens


from fastapi.responses import RedirectResponse
import requests
import threading

# Initialize FastAPI app
app = FastAPI(title="Dukbill API", version="1.0.0")
basiq = BasiqAPI()

# Allowed CORS origins
origins = [
    "https://314dbc1f-20f1-4b30-921e-c30d6ad9036e-00-19bw6chuuv0n8.riker.replit.dev",
    "https://api.vericare.com.au",
    "http://localhost:5000",
    "http://localhost:3000",
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

# Security dependency for extracting Bearer token
security = HTTPBearer()

# Pydantic model for Google signup
class GoogleTokenRequest(BaseModel):
    googleToken: str

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    claims = verify_token(token)
    if not claims:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return claims, token

def get_user_info_from_auth0(access_token: str):
    userinfo_url = f"https://{AUTH0_DOMAIN}/userinfo"
    response = requests.get(
        userinfo_url,
        headers={"Authorization": f"Bearer {access_token}"}
    )
    if response.status_code != 200:
        raise HTTPException(status_code=401, detail="Failed to fetch user profile from Auth0")
    return response.json()

@app.post("/api/google-signup")
async def google_signup(req: GoogleTokenRequest):
    payload = verify_google_token(req.googleToken)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid Google token")
    return {"success": "User registered successfully"}

@app.post("/auth/client/register")
async def register(user=Depends(get_current_user)):
    claims, access_token = user
    # Fetch full profile from Auth0
    profile = get_user_info_from_auth0(access_token)
    auth0_id = profile["sub"]
    
    user = find_user(auth0_id)
    missing_fields = []
    
    # handles new user
    if not user:
        user_id = register_user(auth0_id, profile["email"], profile["picture"], False)
        missing_fields = ["name", "company", "phone"]
        return {
            "user": user_id,
            "isNewUser": True,
            "missingFields": missing_fields,
            "profileComplete": False
        }   
    else:
    # existing user with complete profile 
        if user["profile_complete"]:
            return {
                "user": user["user_id"],
                "isNewUser": False,
                "missingFields": missing_fields,
                "profileComplete": user["profile_complete"]
            } 
        # existing user with incomplete profile
        else:
            fields_to_check = ["name", "company", "phone"]
            
            for field in fields_to_check:
                if user.get(field) is None: 
                    missing_fields.append(field)

            return {
                "user": user["user_id"],
                "isNewUser": False,
                "missingFields": missing_fields,
                "profileComplete": user["profile_complete"]
            }

@app.get("/auth/logout")
async def logout():
    logout_url = (
        f"https://{AUTH0_DOMAIN}/v2/logout?"
        f"client_id={AUTH0_CLIENT_ID}&"
        f"returnTo={POST_LOGOUT_REDIRECT_URI}&"
        f"federated"
    )
    return RedirectResponse(url=logout_url)

@app.patch("/users/onboarding")
async def complete_profile(profile_data: dict, user=Depends(get_current_user)):
    claims, access_token = user
    profile = get_user_info_from_auth0(access_token)
       
    auth0_id = profile["sub"]
    user_type = profile_data["user_type"]
    broker_id = profile_data.pop("broker_id", None)
    user_obj = update_profile(auth0_id, profile_data)
    validatedBroker = False

    if user_type == 'client':
        validatedBroker = bool(register_client(user_obj["user_id"], broker_id))
    elif user_type == "broker":
        register_broker(user_obj["user_id"])
        validatedBroker = True

    return {
        "user": user_obj["user_id"],
        "profileComplete": user_obj["profile_complete"],
        "missingFields": [
            field for field in ["full_name", "phone_number", "company_name"]
            if not user_obj.get(field)
        ],
        "validatedBroker": validatedBroker,
    }

@app.post("/gmail/scan")
async def gmail_scan(user=Depends(get_current_user)):
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)

    toggle_email_scan(user_obj["user_id"])

    consent_url = get_google_auth_url()

    return {"consent_url": consent_url}

@app.get("/gmail/callback")
async def gmail_callback(code: str, user=Depends(get_current_user)):
    claims, _ = user
    auth0_id = claims["sub"]
    user_obj = find_user(auth0_id)

    # Exchange authorization code for access token
    tokens = exchange_code_for_tokens(code)
    access_token = tokens.get("access_token")

    # Start Gmail scan in background
    threading.Thread(
        target=run_gmail_scan,
        args=(user_obj["email"], access_token),
        daemon=True
    ).start()

    # Redirect user to a frontend page (adjust your URL)
    return RedirectResponse("https://314dbc1f-20f1-4b30-921e-c30d6ad9036e-00-19bw6chuuv0n8.riker.replit.dev/signup?completed=true")

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

    return {
        "name": user_obj["name"],
        "id": profile_id,
        "picture": user_obj["picture"],
        "user_type": user_type,
        "email_scan": user_obj["email_scan"]
    }

@app.get("/brokers/client/list")
async def get_client_list(user=Depends(get_current_user)):
    claims, access_token = user
    auth0_id = claims["sub"]
    
    user = find_user(auth0_id)
    broker = find_broker(user["user_id"])
    clients = get_broker_clients(broker["broker_id"])

    return {"clients": clients}

@app.get("/clients/dashboard")
async def get_client_documents(user=Depends(get_current_user)):
    claims, access_token = user
    auth0_id = claims["sub"]

    user = find_user(auth0_id)
    client = find_client(user["user_id"])

    headings = get_client_dashboard(client["client_id"], user["email"])
    
    return {"headings": headings, "BrokerAccess": client["brokerAccess"]}

@app.post("/broker/access")
async def toggle_broker_access_route(user=Depends(get_current_user)):
    claims, _ = user
    auth0_id = claims["sub"]
    
    user = find_user(auth0_id)
    client = find_client(user["user_id"])
    
    toggle_broker_access(client["client_id"])
    return {"BrokerAccess": not client["brokerAccess"]}

@app.post("/clients/category/documents")
async def get_category_documents(request: dict, user=Depends(get_current_user)):  
    claims, access_token = user
    auth0_id = claims["sub"]
    
    category = request.get("category")
    user = find_user(auth0_id)
    client = find_client(user["user_id"])
    
    return get_client_category_documents(client["client_id"], user["email"], category)

@app.get("/brokers/client/{client_id}/dashboard")
async def get_client_dashboard_broker(client_id: int, user=Depends(get_current_user)):
    claims, _ = user
    auth0_id = claims["sub"]

    client = verify_client(client_id)
    client_email = get_user_from_client(client_id)
    headings = get_client_dashboard(client_id, client_email)

    return {"headings": headings, "BrokerAccess": client["brokerAccess"]}

@app.post("/brokers/client/{client_id}/category/documents")
async def get_category_documents_broker(client_id: int, request: dict, user=Depends(get_current_user)):  
    claims, access_token = user
    auth0_id = claims["sub"]

    client = verify_client(client_id)
    if not client["brokerAccess"]:
        return {"error": "Access denied"}

    category = request.get("category")
    client = get_user_from_client(client_id)
    
    return get_client_category_documents(client_id, client["email"], category)

@app.get("/basiq/connect")
async def connect_bank(user=Depends(get_current_user)):
    claims, access_token = user
    auth0_id = claims["sub"]

    user = find_user(auth0_id)
    if not user["basiq_id"]:
        basiq_user = basiq.create_user(user["email"], user["phone"])
        add_basiq_id(user["user_id"], basiq_user["id"])
        user = find_user(auth0_id)
        
    client_token = basiq.get_client_access_token(user["basiq_id"])
    consent_url = f"https://consent.basiq.io/home?token={client_token}"
    return RedirectResponse(consent_url)

@app.get("/clients/bank/transactions")
async def get_client_bank_transactions(user=Depends(get_current_user)):
    claims, _ = user
    auth0_id = claims["sub"]

    user = find_user(auth0_id)
    basiq_id = user.get("basiq_id")
    if not basiq_id:
        return {"transactions": []}

    connections_resp = basiq.get_user_connections(basiq_id)
    connections = connections_resp.get("data", [])
    if not connections:
        return {"transactions": []}

    transactions = basiq.get_user_transactions(basiq_id, active_connections=connections)
    return {"transactions": transactions}


@app.get("/brokers/client/{client_id}/bank/transactions")
async def get_broker_client_bank_transactions(client_id: int, user=Depends(get_current_user)):
    claims, _ = user

    client = verify_client(client_id)
    if not client.get("brokerAccess"):
        return {"error": "Access denied"}

    user_client = get_user_from_client(client_id)
    basiq_id = user_client.get("basiq_id")
    if not basiq_id:
        return {"transactions": []}

    connections_resp = basiq.get_user_connections(basiq_id)
    connections = connections_resp.get("data", [])
    if not connections:
        return {"transactions": []}

    transactions = basiq.get_user_transactions(basiq_id, active_connections=connections)
    return {"transactions": transactions}

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

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "dukbill"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
