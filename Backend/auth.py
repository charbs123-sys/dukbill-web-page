import jwt
from jwt import PyJWKClient
import requests
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from fastapi import HTTPException
from config import AUTH0_DOMAIN, AUTH0_AUDIENCE, GOOGLE_CLIENT_ID
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
import socket

# ------------------------
# IPv6 Adapter for Requests
# ------------------------
class IPv6Adapter(HTTPAdapter):
    """Transport adapter that forces IPv6 connections."""
    def init_poolmanager(self, *args, **kwargs):
        kwargs['socket_options'] = [
            (socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        ]
        self.poolmanager = PoolManager(*args, **kwargs)

# ------------------------
# Setup JWKS client using IPv6-enabled requests session
# ------------------------
session = requests.Session()
session.mount("https://", IPv6Adapter())
jwks_url = f"https://{AUTH0_DOMAIN}/.well-known/jwks.json"
jwks_client = PyJWKClient(jwks_url, session=session)

# ------------------------
# Auth0 JWT Verification
# ------------------------
def verify_token(token):
    try:
        print(f"[AUTH] Attempting to verify token...")
        signing_key = jwks_client.get_signing_key_from_jwt(token).key
        payload = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            audience=AUTH0_AUDIENCE
        )
        print(f"[AUTH] Token verified successfully for user: {payload.get('sub')}")
        return payload
    except Exception as e:
        print(f"[AUTH] Token verification failed: {e}")
        import traceback
        traceback.print_exc()
        return None

# ------------------------
# Google Token Verification
# ------------------------
def verify_google_token(token: str):
    try:
        print(f"[AUTH] Attempting to verify Google token...")
        payload = id_token.verify_oauth2_token(token, google_requests.Request(), GOOGLE_CLIENT_ID)

        if not payload.get("email_verified"):
            print("[AUTH] Google email not verified")
            return None

        print(f"[AUTH] Google token verified successfully for: {payload.get('email')}")
        return payload
    except ValueError as e:
        print(f"[AUTH] Google token verification failed: {e}")
        return None
