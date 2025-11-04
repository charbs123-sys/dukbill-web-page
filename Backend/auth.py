import jwt
from jwt import PyJWKClient
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from fastapi import HTTPException
from config import AUTH0_DOMAIN, AUTH0_AUDIENCE, GOOGLE_CLIENT_ID, XERO_CLIENT_ID, XERO_CLIENT_SECRET, XERO_REDIRECT_URI
import os
import httpx

# Xero OAuth endpoints
XERO_TOKEN_URL = "https://identity.xero.com/connect/token"
XERO_USERINFO_URL = "https://api.xero.com/api.xro/2.0/Organisation"
XERO_CONNECTIONS_URL = "https://api.xero.com/connections"

# ------------------------
# Setup JWKS client for Auth0
# ------------------------
jwks_url = f"https://{AUTH0_DOMAIN}/.well-known/jwks.json"
jwks_client = PyJWKClient(jwks_url)

# ------------------------
# Auth0 JWT Verification
# ------------------------
def verify_token(token: str):
    """
    Verify an Auth0 JWT token and return the decoded payload.
    Returns None if verification fails.
    """
    try:
        # Get signing key from JWT
        signing_key = jwks_client.get_signing_key_from_jwt(token).key

        # Decode and verify JWT
        payload = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            audience=AUTH0_AUDIENCE
        )

        return payload

    except Exception as e:
        import traceback
        traceback.print_exc()
        return None

# ------------------------
# Google Token Verification
# ------------------------
def verify_google_token(token: str):
    """
    Verify a Google ID token and return the decoded payload.
    Returns None if token is invalid or email is not verified.
    """
    try:
        print(f"[AUTH] Attempting to verify Google token...")
        payload = id_token.verify_oauth2_token(token, google_requests.Request(), GOOGLE_CLIENT_ID)

        # Ensure email is verified
        if not payload.get("email_verified"):
            print("[AUTH] Google email not verified")
            return None

        print(f"[AUTH] Google token verified successfully for: {payload.get('email')}")
        return payload

    except ValueError as e:
        print(f"[AUTH] Google token verification failed: {e}")
        return None

# ------------------------
# Xero Verification
# ------------------------
async def verify_xero_auth(code: str):
    """
    Verify Xero authorization code and return user information.
    Similar to your verify_google_token function.
    """
    try:
        print(f"[AUTH] Exchanging Xero authorization code for tokens...")
        
        # Step 1: Exchange code for tokens
        async with httpx.AsyncClient() as client:
            token_response = await client.post(
                XERO_TOKEN_URL,
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": XERO_REDIRECT_URI,
                },
                auth=(XERO_CLIENT_ID, XERO_CLIENT_SECRET),
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )
            
            if token_response.status_code != 200:
                print(f"[AUTH] Token exchange failed: {token_response.text}")
                return None
            
            tokens = token_response.json()
            access_token = tokens.get("access_token")
            id_token = tokens.get("id_token")  # Contains user identity claims
            
            # Step 2: Decode ID token to get user info (email, name, etc.)
            # Note: In production, verify the signature properly
            user_claims = jwt.get_unverified_claims(id_token)
            
            # Step 3: Get Xero tenant (organization) connection
            connections_response = await client.get(
                XERO_CONNECTIONS_URL,
                headers={"Authorization": f"Bearer {access_token}"}
            )
            
            if connections_response.status_code != 200:
                print(f"[AUTH] Failed to get Xero connections")
                return None
            
            connections = connections_response.json()
            
            if not connections:
                print("[AUTH] No Xero organizations connected")
                return None
            
            # Usually take the first connection, or let user choose
            tenant_id = connections[0]["tenantId"]
            
            user_data = {
                "email": user_claims.get("email"),
                "name": user_claims.get("name"),
                "xero_user_id": user_claims.get("xero_userid"),
                "tenant_id": tenant_id,
                "access_token": access_token,  # Store securely for API calls
                "refresh_token": tokens.get("refresh_token")  # For token refresh
            }
            
            print(f"[AUTH] Xero auth verified successfully for: {user_data['email']}")
            return user_data
            
    except Exception as e:
        print(f"[AUTH] Xero auth verification failed: {e}")
        return None