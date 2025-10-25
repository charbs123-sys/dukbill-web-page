import os
import json
import time
import requests
import uuid
from typing import Optional, List
from datetime import datetime, UTC  # timezone-aware
from urllib.parse import urlencode
from fastapi import BackgroundTasks
from config import CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, SCOPES, SEARCH_QUERY
import urllib.request
import urllib.error

from helper import get_email_domain
from users import client_add_email

AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
GMAIL_THREADS_URL = "https://gmail.googleapis.com/gmail/v1/users/me/threads"
TOKENS_FILE = "tokens.json"

SCOPES = "https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/script.external_request https://www.googleapis.com/auth/userinfo.email"
USERINFO_V2 = "https://www.googleapis.com/oauth2/v2/userinfo"

# ===== Token helpers =====
def save_tokens(tokens: dict) -> None:
    with open(TOKENS_FILE, "w", encoding="utf-8") as f:
        json.dump(tokens, f, indent=2)

def load_tokens() -> Optional[dict]:
    if not os.path.exists(TOKENS_FILE):
        return None
    with open(TOKENS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def refresh_access_token(refresh_token: str) -> dict:
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    r = requests.post(TOKEN_URL, data=data, timeout=20)
    r.raise_for_status()
    new_payload = r.json()
    tokens = load_tokens() or {}
    tokens.update(new_payload)
    tokens["expires_at"] = int(time.time()) + int(new_payload.get("expires_in", 3600)) - 30
    save_tokens(tokens)
    return tokens


def get_valid_access_token() -> Optional[str]:
    tokens = load_tokens()
    if not tokens:
        return None
    if int(time.time()) >= int(tokens.get("expires_at", 0)):
        rt = tokens.get("refresh_token")
        if not rt:
            return None
        tokens = refresh_access_token(rt)
    return tokens.get("access_token")

def fetch_authorized_email(access_token: str) -> str | None:
    """Returns the Google account email tied to this access_token. 
    Why: Identify which user granted consent."""
    req = urllib.request.Request(
        USERINFO_V2,
        headers={"Authorization": f"Bearer {access_token}"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.load(resp)
            # data: {"id": "...", "email": "...", "verified_email": true, ...}
            return data.get("email")
    except urllib.error.HTTPError as e:
        # 401 usually means token missing/expired or insufficient scopes
        print(f"ℹ️  UserInfo HTTP {e.code}: {e.reason}")
    except Exception as e:
        print(f"ℹ️  UserInfo error: {e}")
    return None

# ===== Gmail query =====
def list_all_thread_ids(access_token: str, query: str, max_results: int = 500) -> List[str]:
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"q": query, "maxResults": max_results}
    thread_ids: List[str] = []

    page = 1
    while True:
        r = requests.get(GMAIL_THREADS_URL, headers=headers, params=params, timeout=30)
        if r.status_code == 401:
            tokens = load_tokens() or {}
            if "refresh_token" in tokens:
                refresh_access_token(tokens["refresh_token"])
                headers["Authorization"] = f"Bearer {get_valid_access_token()}"
                r = requests.get(GMAIL_THREADS_URL, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        tids = [t["id"] for t in data.get("threads", [])]
        thread_ids.extend(tids)

        page_token = data.get("nextPageToken")
        if not page_token:
            break
        params["pageToken"] = page_token
        page += 1

    return sorted(set(thread_ids))

# ===== Post batches =====
def post_batches_to_api(
    url: str,
    thread_ids: List[str],
    access_token: str,
    refresh_token: Optional[str],
    user_email: str,
    warnings: List[str],
    is_complete: bool,
) -> None:
    # Why chunking: avoid oversized payloads/timeouts downstream.
    job_id = str(uuid.uuid4())
    max_batches = 3
    total_threads = len(thread_ids)
    batch_size = max(1, total_threads // max_batches)
    total_batches = min(max_batches, (total_threads + batch_size - 1) // batch_size)

    headers = {
        "Authorization": "aef391b3d4pg56tf",  # replace if needed
        "Content-Type": "application/json",
        "X-User-Email": user_email,
    }

    for i in range(0, total_threads, batch_size):
        batch = thread_ids[i : i + batch_size]
        batch_number = (i // batch_size) + 1
        payload = {
            "thread_ids": batch,
            "user_token": access_token,
            "refresh_token": refresh_token,  # ✅ now included
            "user_email": user_email,
            "job_metadata": {
                "job_id": job_id,
                "batch_number": batch_number,
                "total_batches": total_batches,
                "total_threads": total_threads,
                "search_complete": is_complete,
                "warnings": warnings,
                "search_timestamp": datetime.now(UTC).isoformat(),
            },
        }
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        print(f"Batch {batch_number}/{total_batches} -> {r.status_code}")
        if r.status_code >= 400:
            print("  ❌ Error:", r.text)
        time.sleep(1)

def exchange_code_for_tokens(code: str) -> dict:
    """Exchange the Google OAuth code for access & refresh tokens."""
    data = {
        "code": code,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
    }
    r = requests.post(TOKEN_URL, data=data, timeout=20)
    r.raise_for_status()
    tokens = r.json()
    tokens["expires_at"] = int(time.time()) + int(tokens.get("expires_in", 3600)) - 30
    # save_tokens(tokens)  # optional persistence
    return tokens  # includes 'refresh_token' when Google returns it

# ===== Gmail scan =====
def run_gmail_scan(client_id: str, user_email: str, access_token: str, refresh_token: Optional[str]) -> None:
    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError("Missing CLIENT_ID/CLIENT_SECRET")
    if not access_token:
        raise RuntimeError("Missing access token")

    thread_ids = list_all_thread_ids(access_token, SEARCH_QUERY, max_results=500)
    user_email = fetch_authorized_email(access_token) if access_token else None

    print(f"✅ Found {len(thread_ids)} threads for {user_email}")
    client_add_email(client_id, get_email_domain(user_email), user_email)
    
    warnings: List[str] = []
    is_complete = True
    url = "https://z1c3olnck5.execute-api.ap-southeast-2.amazonaws.com/Prod/"
    post_batches_to_api(url, thread_ids, access_token, refresh_token, user_email, warnings, is_complete)

# ===== OAuth redirect URL =====
def get_google_auth_url(state):  # ← Add state parameter
    params = {
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": SCOPES,
        "access_type": "offline",
        "include_granted_scopes": "false",
        "prompt": "consent",
        "state": state,  # ← Add this line
    }
    return f"{AUTH_URL}?{urlencode(params)}"
