import jwt
from jwt import PyJWKClient
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from fastapi import HTTPException
from config import AUTH0_DOMAIN, AUTH0_AUDIENCE, GOOGLE_CLIENT_ID

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
