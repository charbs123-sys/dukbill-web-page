import requests
import jwt
from jwt import PyJWKClient
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from fastapi import HTTPException


AUTH0_DOMAIN = "dev-fg1hwnn3wmqamynb.au.auth0.com"
token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlgzTzJUZFNlMmtVX25NX1hzS0pHZiJ9.eyJpc3MiOiJodHRwczovL2Rldi1mZzFod25uM3dtcWFteW5iLmF1LmF1dGgwLmNvbS8iLCJzdWIiOiJhdXRoMHw2OGY5ODMxNmMyYTg2MDhmZjU1ZDUzMDYiLCJhdWQiOlsiaHR0cHM6Ly9hcGkuZHVrYmlsbC5jb20iLCJodHRwczovL2Rldi1mZzFod25uM3dtcWFteW5iLmF1LmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE3NjExODM0NjQsImV4cCI6MTc2MTI2OTg2NCwic2NvcGUiOiJvcGVuaWQgcHJvZmlsZSBlbWFpbCBwaG9uZSIsImF6cCI6InhJZWE4bU5BQXlrYVd3bDNTVVlNYmJNVUVYN2U2OTQxIn0.irCW2dh0n4fZ7HOSXHSkvDZJ2cocd5S3q49Iu0w0clciuv8lPVsC51nWORzN9qarkRWrNd4QpOWMr2ckY--iDRdW3KN82rNezHH03Aev5Vmpkb_rArVKQSCWeCLqDXbGgx-a1duDxH3KAPtGLzNGpZGQSV2osemyMl1dWd544c3CnVFSNhN2fawYcOFZXW2t7Q5eKnAIrj0eKg3OvKGBxMUSzaRtxtuq3-dqo7kl1XbLaBgc4VnD6tM5-TrgRGFEzi9j4CpF7GCjbpznQ6PgdmJDct9q8Oi7vZ-K4ZtWHNvbFgtM49O5lPDUXPd6JRAw9qH9qYrWXHJptWye4D-oKA"
r = requests.get(f"https://{AUTH0_DOMAIN}/userinfo",
                 headers={"Authorization": f"Bearer {token}"},
                 timeout=10)
print(r.status_code, r.headers.get("content-type"), r.text)

jwks_url = f"https://{AUTH0_DOMAIN}/.well-known/jwks.json"
jwks_client = PyJWKClient(jwks_url)

def verify_token(token: str):
    """
    Verify an Auth0 JWT token and return the decoded payload.
    Returns None if verification fails.
    """
    try:
        print(f"[AUTH] Attempting to verify Auth0 token...")
        # Get signing key from JWT
        signing_key = jwks_client.get_signing_key_from_jwt(token).key

        # Decode and verify JWT
        payload = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            audience="https://api.dukbill.com"
        )

        print(f"[AUTH] Token verified successfully for user: {payload.get('sub')}")
        return payload
    except Exception as e:
        print(f"[AUTH] Auth0 token verification failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    
verify_token(token)