import jwt
import requests
from jwt import PyJWKClient
from config import AUTH0_DOMAIN, AUTH0_AUDIENCE, GOOGLE_CLIENT_ID

from google.oauth2 import id_token
from google.auth.transport import requests

# Setup JWKS client for your Auth0 tenant
jwks_url = f"https://{AUTH0_DOMAIN}/.well-known/jwks.json"
jwks_client = PyJWKClient(jwks_url)

def verify_token(token):
    try:
        # Get signing key from token
        signing_key = jwks_client.get_signing_key_from_jwt(token).key

        # Decode and verify token
        payload = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            audience=AUTH0_AUDIENCE
        )
        return payload

    except Exception as e:
        print("Token verification failed:", e)
        return None

def verify_google_token(token: str):
    try:
        # Verify the token with Google
        payload = id_token.verify_oauth2_token(token, requests.Request(), GOOGLE_CLIENT_ID)

        # Optional: check that email is verified
        if not payload.get("email_verified"):
            print("Email not verified")
            return None

        # Token is valid
        return payload

    except ValueError as e:
        # Token is invalid
        print(f"Token verification failed: {e}")
        return None

