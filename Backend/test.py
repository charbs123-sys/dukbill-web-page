import requests
from fastapi import FastAPI, HTTPException, Depends, Body, Request, File, Form, UploadFile
import requests
from fastapi import HTTPException
from json import JSONDecodeError


access_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlgzTzJUZFNlMmtVX25NX1hzS0pHZiJ9.eyJpc3MiOiJodHRwczovL2Rldi1mZzFod25uM3dtcWFteW5iLmF1LmF1dGgwLmNvbS8iLCJzdWIiOiJnb29nbGUtb2F1dGgyfDExNzc1MTI5MjE2MTQyNjQyNzcxMSIsImF1ZCI6WyJodHRwczovL2FwaS5kdWtiaWxsLmNvbSIsImh0dHBzOi8vZGV2LWZnMWh3bm4zd21xYW15bmIuYXUuYXV0aDAuY29tL3VzZXJpbmZvIl0sImlhdCI6MTc2MTEwNTI1OCwiZXhwIjoxNzYxMTkxNjU4LCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIHBob25lIiwiYXpwIjoieEllYThtTkFBeWthV3dsM1NVWU1iYk1VRVg3ZTY5NDEifQ.ZpLpMW0unsEIneyPXu_9Ys5-El7hl74rLYNI1HDmTbtxQk-VxWlPwYLW4WnDFPnchL242zEu0Jh23PQidjQhUHjl1DO5R1mQcZLa4U1DBdwG_eBO6DBcICzjJMtPKjv7iZDvmaBjPDkiX0x9N5hTgB2_C2NrnY8etgOHsCrrdVP-UukIegp4e1tB9gymQU4fK1WcSsHVEYhLwC-dtCDB86Ho02pYX13BSY72za3paadqnd_TyBkoLFpLiEHSQBe2CsoPZNZ5YaQN2GzKAvVVokTUqbW_mcuClYBn2jRbsk438QFP9yLDCqSnHYsC3Ru1uPNtha5D4nzCXkBo5hBQrQ"
userinfo_url = "https://dev-fg1hwnn3wmqamynb.au.auth0.com/userinfo"
session = requests.Session()

try:
    resp = session.get(
        userinfo_url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        },
        timeout=10,
    )
except requests.RequestException as e:
    raise HTTPException(status_code=503, detail=f"Auth0 request failed: {e!s}")

status = resp.status_code
ctype = resp.headers.get("content-type", "")
body_bytes = resp.content or b""
body_len = len(body_bytes)

# Helpful debug prints while you’re local
print("status:", status)
print("content-type:", ctype)
print("body-bytes:", body_len)

def try_json(r: requests.Response):
    # Only attempt JSON if content-type is JSON and body isn’t empty
    if "application/json" in (r.headers.get("content-type") or "").lower() and r.content:
        try:
            return r.json()
        except JSONDecodeError:
            pass
    return None

if 200 <= status < 300:
    data = try_json(resp)
    if data is not None:
        print("userinfo:", data)
    else:
        # Success but not JSON (unexpected for /userinfo)
        preview = (resp.text or "").strip()
        if len(preview) > 500:
            preview = preview[:500] + "…"
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Auth0 /userinfo returned non-JSON or empty body on success.",
                "status": status,
                "content_type": ctype,
                "body_preview": preview,
            },
        )
else:
    # Error path: show JSON if available, otherwise text preview
    err_json = try_json(resp)
    if err_json is not None:
        raise HTTPException(status_code=status, detail=err_json)
    else:
        preview = (resp.text or "").strip()
        if len(preview) > 500:
            preview = preview[:500] + "…"
        raise HTTPException(
            status_code=status if 400 <= status < 600 else 502,
            detail={
                "message": "Failed to fetch user profile from Auth0",
                "status": status,
                "content_type": ctype,
                "body_preview": preview,
            },
        )