import os
import sys
import json
import time
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlencode, urlparse, parse_qs

import requests
from dotenv import load_dotenv

import uuid
from datetime import datetime


# ===== Config from .env =====
load_dotenv()
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:8765/callback")
SCOPE = os.getenv("SCOPE", "https://www.googleapis.com/auth/gmail.readonly")
SEARCH_QUERY = os.getenv("SEARCH_QUERY", "has:attachment newer_than:2y")  # change if needed

AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
GMAIL_THREADS_URL = "https://gmail.googleapis.com/gmail/v1/users/me/threads"
TOKENS_FILE = "tokens.json"
THREADS_OUT = "threads_2y.json"

oauth = {"code": None, "error": None}
callback_event = threading.Event()


# ===== Local callback server =====
class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlparse(self.path)
        if url.path != "/callback":
            self.send_response(404); self.end_headers(); return

        qs = parse_qs(url.query)
        if "error" in qs:
            oauth["error"] = qs["error"][0]
            msg = f"OAuth error: {oauth['error']}"
        elif "code" in qs:
            oauth["code"] = qs["code"][0]
            msg = "✅ Success! You can close this tab and return to the terminal."
        else:
            msg = "No code parameter received."

        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(msg.encode("utf-8"))
        callback_event.set()

    def log_message(self, *args, **kwargs):
        # silence default logs
        pass


def start_server():
    server = HTTPServer(("127.0.0.1", 8765), CallbackHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server


# ===== Token helpers =====
def save_tokens(tokens: dict):
    with open(TOKENS_FILE, "w") as f:
        json.dump(tokens, f, indent=2)


def load_tokens() -> dict | None:
    if not os.path.exists(TOKENS_FILE):
        return None
    with open(TOKENS_FILE, "r") as f:
        return json.load(f)


def exchange_code_for_tokens(code: str) -> dict:
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
    }
    r = requests.post(TOKEN_URL, data=data, timeout=20)
    r.raise_for_status()
    payload = r.json()
    payload["expires_at"] = int(time.time()) + int(payload.get("expires_in", 3600)) - 30
    save_tokens(payload)
    return payload


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


def get_valid_access_token() -> str | None:
    tokens = load_tokens()
    if not tokens:
        return None
    if int(time.time()) >= int(tokens.get("expires_at", 0)):
        rt = tokens.get("refresh_token")
        if not rt:
            return None
        tokens = refresh_access_token(rt)
    return tokens.get("access_token")


# ===== Gmail query (pagination, no result cap) =====
def list_all_thread_ids(access_token: str, query: str, max_results=500):
    """
    Returns a list of thread IDs that match the query.
    Uses pageToken to iterate all pages (Gmail API allows up to 500 per page).
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"q": query, "maxResults": max_results}
    thread_ids = []

    page = 1
    while True:
        r = requests.get(GMAIL_THREADS_URL, headers=headers, params=params, timeout=30)
        # If token expired mid-loop, try one refresh
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
        print(f"Page {page}: {len(tids)} threads (running total: {len(thread_ids)})")

        token = data.get("nextPageToken")
        if not token:
            break
        params["pageToken"] = token
        page += 1

    # Dedup just in case (shouldn’t be necessary, but harmless)
    return sorted(set(thread_ids))

# ===== API POST Section (same as your Apps Script pattern) =====

def post_batches_to_api(
    url: str,
    thread_ids: list[str],
    access_token: str,
    user_email: str,
    warnings: list[str],
    is_complete: bool
):
    job_id = str(uuid.uuid4())
    max_batches = 3
    batch_size = max(1, len(thread_ids) // max_batches)
    total_batches = min(max_batches, (len(thread_ids) + batch_size - 1) // batch_size)

    print(f"\n🚀 Sending {len(thread_ids)} threads to {url}")
    print(f"Dividing into {total_batches} batches of ~{batch_size} threads each\n")

    for i in range(0, len(thread_ids), batch_size):
        batch = thread_ids[i : i + batch_size]
        batch_number = (i // batch_size) + 1

        payload = {
            "thread_ids": batch,
            "user_token": access_token,
            "user_email": user_email,
            "job_metadata": {
                "job_id": job_id,
                "batch_number": batch_number,
                "total_batches": total_batches,
                "total_threads": len(thread_ids),
                "search_complete": is_complete,
                "warnings": warnings,
                "search_timestamp": datetime.utcnow().isoformat() + "Z",
            },
        }

        headers = {
            "Authorization": "aef391b3d4pg56tf",  # replace if needed
            "Content-Type": "application/json",
            "X-User-Email": user_email,
        }
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        print(f"Batch {batch_number}/{total_batches} -> {r.status_code}")
        if r.status_code >= 400:
            print("  ❌ Error:", r.text)
        time.sleep(1)

    print(f"\n✅ All {total_batches} batches sent for job {job_id}\n")


def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("❌ Missing CLIENT_ID/CLIENT_SECRET in .env")
        sys.exit(1)

    # 1) Ensure we have a valid access token
    access_token = get_valid_access_token()
    if not access_token:
        server = start_server()
        params = {
            "client_id": CLIENT_ID,
            "redirect_uri": REDIRECT_URI,
            "response_type": "code",
            "scope": SCOPE,
            "access_type": "offline",
            "include_granted_scopes": "true",
            "prompt": "consent",  # helps get a refresh_token during testing
        }
        url = f"{AUTH_URL}?{urlencode(params)}"
        print("\nOpen this URL to authorize Gmail access:\n", url, "\n")
        webbrowser.open(url, new=1)

        print("Waiting for callback on", REDIRECT_URI, "… (Ctrl+C to abort)")
        callback_event.wait(timeout=300)
        server.shutdown()

        if oauth["error"]:
            print("OAuth error:", oauth["error"])
            sys.exit(1)
        if not oauth["code"]:
            print("No authorization code received. Exiting.")
            sys.exit(1)

        tokens = exchange_code_for_tokens(oauth["code"])
        access_token = tokens.get("access_token")

    # 2) Pull ALL thread IDs for past 2 years with attachments
    print(f"\nSearching Gmail with query: {SEARCH_QUERY!r}")
    thread_ids = list_all_thread_ids(access_token, SEARCH_QUERY, max_results=500)
    print(f"\n✅ Found {len(thread_ids)} unique thread IDs for query: {SEARCH_QUERY}")

    # 3) Save results
    output = {
        "query": SEARCH_QUERY,
        "thread_ids": thread_ids,
        "count": len(thread_ids),
        "retrieved_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    # 4) Show token info for your webapp handoff (be careful where you log this)
    tokens = load_tokens() or {}
    print("\n--- Token snapshot (store securely) ---")
    print(json.dumps({
        "access_token": tokens.get("access_token"),
        "expires_at_unix": tokens.get("expires_at"),
        "has_refresh_token": bool(tokens.get("refresh_token")),
    }, indent=2))

    user_email = input("Enter the Gmail address of the authorized user: ").strip()
    warnings = []  # optionally collect any issues during search
    is_complete = True

    url = "https://z1c3olnck5.execute-api.ap-southeast-2.amazonaws.com/Prod/"
    post_batches_to_api(url, thread_ids, access_token, user_email, warnings, is_complete)

    #print(f"Results written to: {THREADS_OUT}")
    #print(f"Tokens saved to:    {TOKENS_FILE}")


if __name__ == "__main__":
    main()
