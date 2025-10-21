import socket
import os

# Monkey patch socket.getaddrinfo to prefer IPv6 - MUST be first
_original_getaddrinfo = socket.getaddrinfo

def ipv6_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    """Force IPv6 resolution"""
    # Always request IPv6
    return _original_getaddrinfo(host, port, socket.AF_INET6, type, proto, flags)

socket.getaddrinfo = ipv6_getaddrinfo

# Now import everything else
import jwt
import requests
from jwt import PyJWKClient
from config import AUTH0_DOMAIN, AUTH0_AUDIENCE, GOOGLE_CLIENT_ID
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests

print(f"[AUTH] Initializing with IPv6-only mode for domain: {AUTH0_DOMAIN}")

# Setup JWKS client for your Auth0 tenant
jwks_url = f"https://{AUTH0_DOMAIN}/.well-known/jwks.json"
jwks_client = PyJWKClient(jwks_url)

def verify_token(token):
    try:
        print(f"[AUTH] Attempting to verify token...")
        # Get signing key from token
        signing_key = jwks_client.get_signing_key_from_jwt(token).key
        print(f"[AUTH] Successfully retrieved signing key")

        # Decode and verify token
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

def verify_google_token(token: str):
    try:
        print(f"[AUTH] Attempting to verify Google token...")
        # Verify the token with Google
        payload = id_token.verify_oauth2_token(token, google_requests.Request(), GOOGLE_CLIENT_ID)

        # Optional: check that email is verified
        if not payload.get("email_verified"):
            print("[AUTH] Email not verified")
            return None

        print(f"[AUTH] Google token verified successfully for: {payload.get('email')}")
        # Token is valid
        return payload

    except ValueError as e:
        # Token is invalid
        print(f"[AUTH] Google token verification failed: {e}")
        return None