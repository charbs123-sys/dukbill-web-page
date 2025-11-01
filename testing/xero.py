# xero_sso_min.py
"""
Minimal Xero SSO (OIDC) with FastAPI.
Requires env:
  XERO_CLIENT_ID=...
  XERO_CLIENT_SECRET=...
  XERO_REDIRECT_URI=https://your.api.com/callback/xero

Install:
  pip install fastapi uvicorn requests python-jose[cryptography] python-dotenv
Run:
  uvicorn xero_sso_min:app --host 0.0.0.0 --port 8080 --reload
"""
import os
import time
import base64
import secrets
from typing import Dict, Any, Optional
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from jose import jwt

load_dotenv()

# --- Required env (keep names EXACTLY as requested) ---
XERO_CLIENT_ID = os.environ["XERO_CLIENT_ID"]
XERO_CLIENT_SECRET = os.environ["XERO_CLIENT_SECRET"]
XERO_REDIRECT_URI = os.environ["XERO_REDIRECT_URI"]

# --- OpenID Discovery (once at startup) ---
DISCOVERY_URL = "https://identity.xero.com/.well-known/openid-configuration"
_disc = requests.get(DISCOVERY_URL, timeout=10).json()
AUTHORIZATION_ENDPOINT = _disc["authorization_endpoint"]
TOKEN_ENDPOINT = _disc["token_endpoint"]
JWKS_URI = _disc["jwks_uri"]
ISSUER = _disc["issuer"]

# --- Minimal in-memory stores (replace later with Redis/DB if needed) ---
SESS: Dict[str, Dict[str, Any]] = {}   # sid -> {state, nonce, ts}
JWKS_CACHE: Dict[str, Any] = {}
JWKS_TS = 0

# --- App ---
app = FastAPI(title="Dukbill ↔ Xero SSO (Minimal)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],     # tighten in prod
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

SCOPES = "openid profile email"


def _basic_auth() -> str:
    return base64.b64encode(f"{XERO_CLIENT_ID}:{XERO_CLIENT_SECRET}".encode()).decode()


def _get_jwks() -> Dict[str, Any]:
    global JWKS_CACHE, JWKS_TS
    if not JWKS_CACHE or (time.time() - JWKS_TS) > 6 * 3600:
        JWKS_CACHE = requests.get(JWKS_URI, timeout=10).json()
        JWKS_TS = time.time()
    return JWKS_CACHE


def _pick_jwk_for_kid(jwks: Dict[str, Any], kid: Optional[str]) -> Dict[str, Any]:
    keys = jwks.get("keys", [])
    for k in keys:
        if k.get("kid") == kid:
            return k
    raise HTTPException(401, "Signing key not found for token kid")

SESS = {}
@app.get("/health")
def health():
    return {"ok": True}


@app.get("/signin/xero")
def signin_xero():
    state = secrets.token_urlsafe(24)
    nonce = secrets.token_urlsafe(24)
    SESS[state] = {"nonce": nonce, "ts": int(time.time())}

    params = {
        "response_type": "code",
        "client_id": XERO_CLIENT_ID,
        "redirect_uri": XERO_REDIRECT_URI,
        "scope": SCOPES,
        "state": state,
        "nonce": nonce,
    }
    return RedirectResponse(f"{AUTHORIZATION_ENDPOINT}?{urlencode(params)}", status_code=302)

@app.get("/callback/xero")
def callback_xero(code: str = "", state: str = ""):
    sess = SESS.pop(state, None)
    if not sess:
        raise HTTPException(400, "Invalid session/state")

    # exchange code
    tok = requests.post(
        TOKEN_ENDPOINT,
        headers={
            "Authorization": "Basic " + base64.b64encode(
                f"{XERO_CLIENT_ID}:{XERO_CLIENT_SECRET}".encode()
            ).decode(),
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": XERO_REDIRECT_URI,
        },
        timeout=10,
    )

    if tok.status_code != 200:
        raise HTTPException(400, f"Token exchange failed: {tok.text}")
    token_set = tok.json()
    print("this is token")
    print(token_set)
    id_token = token_set.get("id_token")
    if not id_token:
        raise HTTPException(400, "No ID token in response")

    # verify ID token (JWKS)
    unverified = jwt.get_unverified_header(id_token)
    kid = unverified.get("kid")
    jwks = requests.get(JWKS_URI, timeout=10).json()
    jwk = next((k for k in jwks.get("keys", []) if k.get("kid") == kid), None)
    if not jwk:
        raise HTTPException(401, "Signing key not found")

    claims = jwt.decode(
        id_token,
        jwk,
        algorithms=["RS256"],
        audience=XERO_CLIENT_ID,
        issuer=ISSUER,
        options={"verify_at_hash": False},
    )

    # nonce check
    if claims.get("nonce") != sess["nonce"]:
        raise HTTPException(401, "Nonce mismatch")

    identity = {
        "sub": claims.get("sub"),
        "email": claims.get("email"),
        "given_name": claims.get("given_name"),
        "family_name": claims.get("family_name"),
    }
    return JSONResponse({"status": "sso_ok", "identity": identity})


# add near your other imports/utilities
CONNECTIONS = {}  # user_id -> [{ id, tenantId, tenantType, tenantName, ... }]
TOKENS = {}       # user_id -> { access_token, refresh_token, expires_at, ... }

SCOPES_CONNECT = "offline_access accounting.settings.read"  # add more later if needed

def _basic_auth():
    import base64, os
    return base64.b64encode(f"{XERO_CLIENT_ID}:{XERO_CLIENT_SECRET}".encode()).decode()

def _save_tokens(user_id, token_set):
    import time
    TOKENS[user_id] = {
        "access_token": token_set["access_token"],
        "refresh_token": token_set.get("refresh_token"),
        "expires_at": int(time.time()) + int(token_set.get("expires_in", 1800)),
        "token_type": token_set.get("token_type", "Bearer"),
        "scope": token_set.get("scope", ""),
    }

def _ensure_access_token(user_id):
    import time, requests
    t = TOKENS.get(user_id)
    if not t:
        raise HTTPException(401, "No Xero tokens stored for user")
    if time.time() < t["expires_at"] - 30:
        return t["access_token"]
    # refresh
    resp = requests.post(
        TOKEN_ENDPOINT,
        headers={"Authorization": f"Basic {_basic_auth()}",
                 "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "refresh_token", "refresh_token": t["refresh_token"]},
        timeout=10,
    )
    if resp.status_code != 200:
        raise HTTPException(401, f"Refresh failed: {resp.text}")
    _save_tokens(user_id, resp.json())
    return TOKENS[user_id]["access_token"]

@app.get("/xero/connect")
def xero_connect(user_id: str):
    import secrets
    state = secrets.token_urlsafe(24)
    SESS[state] = {"user_id": user_id, "ts": int(time.time())}
    params = {
        "response_type": "code",
        "client_id": XERO_CLIENT_ID,
        "redirect_uri": XERO_REDIRECT_URI,  # you can reuse the same callback OR make a second one
        "scope": SCOPES_CONNECT,
        "state": state,
        "prompt": "consent",
    }
    from urllib.parse import urlencode
    return RedirectResponse(f"{AUTHORIZATION_ENDPOINT}?{urlencode(params)}", status_code=302)

# Run App
# ------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
