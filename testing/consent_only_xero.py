# xero_consent_only.py
import os, time, base64, secrets
from typing import Dict, Any, List
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse, JSONResponse

load_dotenv()

XERO_CLIENT_ID = os.environ["XERO_CLIENT_ID"]
XERO_CLIENT_SECRET = os.environ["XERO_CLIENT_SECRET"]
XERO_REDIRECT_URI = os.environ["XERO_REDIRECT_URI"]  # e.g. http://localhost:8080/callback/xero

DISCOVERY_URL = "https://identity.xero.com/.well-known/openid-configuration"
disc = requests.get(DISCOVERY_URL, timeout=10).json()
AUTHORIZATION_ENDPOINT = disc["authorization_endpoint"]
TOKEN_ENDPOINT = disc["token_endpoint"]

# In-memory (swap for DB in prod)
SESS: Dict[str, Dict[str, Any]] = {}           # state -> {"user_id":..., "ts":...}
TOKENS: Dict[str, Dict[str, Any]] = {}         # user_id -> token set
CONNECTIONS: Dict[str, List[Dict[str, Any]]] = {}  # user_id -> list of tenants

SCOPES = "offline_access accounting.settings.read accounting.transactions.read"  # add more org scopes as needed

app = FastAPI()

def _basic_auth() -> str:
    return base64.b64encode(f"{XERO_CLIENT_ID}:{XERO_CLIENT_SECRET}".encode()).decode()

def _save_tokens(user_id: str, token_set: Dict[str, Any]):
    TOKENS[user_id] = {
        "access_token": token_set["access_token"],
        "refresh_token": token_set.get("refresh_token"),
        "expires_at": int(time.time()) + int(token_set.get("expires_in", 1800)),
        "token_type": token_set.get("token_type", "Bearer"),
        "scope": token_set.get("scope", ""),
    }

def _ensure_access_token(user_id: str) -> str:
    t = TOKENS.get(user_id)
    if not t:
        raise HTTPException(401, "No Xero tokens stored for user")
    if time.time() < t["expires_at"] - 30:
        return t["access_token"]
    # refresh
    resp = requests.post(
        TOKEN_ENDPOINT,
        headers={
            "Authorization": f"Basic {_basic_auth()}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={"grant_type": "refresh_token", "refresh_token": t["refresh_token"]},
        timeout=10,
    )
    if resp.status_code != 200:
        raise HTTPException(401, f"Refresh failed: {resp.text}")
    _save_tokens(user_id, resp.json())
    return TOKENS[user_id]["access_token"]

@app.get("/xero/connect")
def xero_connect(user_id: str = Query(..., description="Your internal user id")):
    state = secrets.token_urlsafe(24)
    SESS[state] = {"user_id": user_id, "ts": int(time.time())}
    params = {
        "response_type": "code",
        "client_id": XERO_CLIENT_ID,
        "redirect_uri": XERO_REDIRECT_URI,  # must exactly match Xero app config
        "scope": SCOPES,
        "state": state,
        "prompt": "consent",                # ensures refresh_token on reconsent
    }
    return RedirectResponse(f"{AUTHORIZATION_ENDPOINT}?{urlencode(params)}", status_code=302)

@app.get("/callback/xero")
def callback_xero(code: str = "", state: str = ""):
    sess = SESS.pop(state, None)
    if not sess:
        raise HTTPException(400, "Invalid session/state")
    user_id = sess["user_id"]

    # 1) Exchange code for access/refresh tokens (no ID token expected in consent-only flow)
    tok = requests.post(
        TOKEN_ENDPOINT,
        headers={
            "Authorization": f"Basic {_basic_auth()}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={"grant_type": "authorization_code", "code": code, "redirect_uri": XERO_REDIRECT_URI},
        timeout=10,
    )
    if tok.status_code != 200:
        raise HTTPException(400, f"Token exchange failed: {tok.text}")
    token_set = tok.json()
    _save_tokens(user_id, token_set)

    # 2) Discover tenant connections
    access_token = TOKENS[user_id]["access_token"]
    conns = requests.get(
        "https://api.xero.com/connections",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=10,
    )
    if conns.status_code != 200:
        raise HTTPException(400, f"Fetch connections failed: {conns.text}")
    CONNECTIONS[user_id] = conns.json()
    if not CONNECTIONS[user_id]:
        return JSONResponse({"status": "connected", "connections": [], "bankTransactions": []})

    # 3) Pick the first tenant and fetch bank transactions immediately
    tenant_id = CONNECTIONS[user_id][0]["tenantId"]
    # Optional filters; comment out or change as you like:
    # where = 'Status=="AUTHORISED"'
    # order = "Date DESC"
    where = None
    order = None

    try:
        bank_tx = fetch_bank_transactions(user_id, tenant_id, where=where, order=order)
    except HTTPException as e:
        # Surface fetch error but still return connections so you can debug scopes
        return JSONResponse(
            {
                "status": "connected",
                "connections": CONNECTIONS[user_id],
                "bankTransactionsError": {"status": e.status_code, "detail": e.detail},
            },
            status_code=200,
        )

    # 4) Return connections + transactions
    return JSONResponse(
        {
            "status": "connected",
            "connections": CONNECTIONS[user_id],
            "tenantIdUsed": tenant_id,
            "bankTransactions": bank_tx,
        }
    )


@app.get("/xero/accounts")
def xero_accounts(user_id: str, tenant_id: str):
    """
    Read chart of accounts (requires accounting.settings.read).
    """
    token = _ensure_access_token(user_id)
    r = requests.get(
        "https://api.xero.com/api.xro/2.0/Accounts",
        headers={
            "Authorization": f"Bearer {token}",
            "xero-tenant-id": tenant_id,
            "Accept": "application/json",
        },
        timeout=15,
    )
    print(r.json())
    if r.status_code != 200:
        raise HTTPException(r.status_code, r.text)
    return r.json().get("Accounts", [])

from typing import Optional
"to get banking transactions"
def fetch_bank_transactions(
    user_id: str,
    tenant_id: str,
    where: Optional[str] = None,
    order: Optional[str] = None,
):
    token = _ensure_access_token(user_id)
    params = {}
    if where:
        params["where"] = where
    if order:
        params["order"] = order

    resp = requests.get(
        "https://api.xero.com/api.xro/2.0/BankTransactions",
        headers={
            "Authorization": f"Bearer {token}",
            "xero-tenant-id": tenant_id,
            "Accept": "application/json",
        },
        params=params,
        timeout=20,
    )
    print(resp.json())
    if resp.status_code == 401:
        # Most common cause: missing accounting.transactions.read scope
        raise HTTPException(401, f"Unauthorized (check scopes): {resp.text}")
    if resp.status_code != 200:
        raise HTTPException(resp.status_code, resp.text)
    return resp.json().get("BankTransactions", [])

# Run App
# ------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
