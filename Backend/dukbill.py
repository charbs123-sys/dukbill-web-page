from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import requests
from auth import verify_token, verify_google_token
from users import *
from db_init import initialize_database
from config import AUTH0_DOMAIN
from S3_utils import *

# Initialize FastAPI app
app = FastAPI(title="Dukbill API", version="1.0.0")

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

# Dependency: Get current user claims from Auth0 token
def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    claims = verify_token(token)
    if not claims:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return claims, token  # return both claims and raw access token

# Helper to call Auth0 /userinfo endpoint
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
    print(payload)
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

@app.patch("/users/onboarding")
async def complete_profile(profile_data: dict, user=Depends(get_current_user)):
    claims, access_token = user
    profile = get_user_info_from_auth0(access_token)
    
    print(profile_data)
    
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
        "user_type": user_type
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
    
    category = request.get("category")
    client = verify_client(client_id)
    client_email = get_user_from_client(client_id)
    
    return get_client_category_documents(client_id, client_email, category)

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "dukbill"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
