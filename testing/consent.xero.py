import os
import time
import base64
import secrets
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse

load_dotenv()

XERO_CLIENT_ID = os.environ["XERO_CLIENT_ID"]
XERO_CLIENT_SECRET = os.environ["XERO_CLIENT_SECRET"]
XERO_REDIRECT_URI = os.environ["XERO_REDIRECT_URI"]  # e.g. http://localhost:8080/callback

SCOPES = "offline_access accounting.settings.read accounting.transactions.read accounting.contacts.read accounting.attachments.read payroll.employees.read payroll.payruns.read payroll.payslip.read"

# Get Xero endpoints
DISCOVERY_URL = "https://identity.xero.com/.well-known/openid-configuration"
disc = requests.get(DISCOVERY_URL, timeout=10).json()
AUTH_URL = disc["authorization_endpoint"]
TOKEN_URL = disc["token_endpoint"]

# Simple in-memory storage
session_state = {}
tokens = {}
tenant_id = None

app = FastAPI()


def get_basic_auth():
    """Create Basic Auth header for token exchange"""
    credentials = f"{XERO_CLIENT_ID}:{XERO_CLIENT_SECRET}"
    return base64.b64encode(credentials.encode()).decode()


def get_access_token():
    """Get valid access token, refresh if needed"""
    if not tokens:
        raise HTTPException(401, "Not connected to Xero")
    
    # Check if token is still valid
    if time.time() < tokens["expires_at"] - 30:
        return tokens["access_token"]
    
    # Refresh token
    resp = requests.post(
        TOKEN_URL,
        headers={
            "Authorization": f"Basic {get_basic_auth()}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "refresh_token",
            "refresh_token": tokens["refresh_token"]
        },
        timeout=10,
    )
    
    if resp.status_code != 200:
        raise HTTPException(401, f"Token refresh failed: {resp.text}")
    
    # Save new tokens
    new_tokens = resp.json()
    tokens["access_token"] = new_tokens["access_token"]
    tokens["refresh_token"] = new_tokens.get("refresh_token", tokens["refresh_token"])
    tokens["expires_at"] = int(time.time()) + int(new_tokens.get("expires_in", 1800))
    
    return tokens["access_token"]


def fetch_xero_data_paginated(endpoint: str, data_key: str, params: dict = None):
    """
    Helper function to fetch paginated data from Xero API
    
    endpoint: API endpoint path (e.g., "Accounts", "BankTransactions")
    data_key: JSON key containing the data array (e.g., "Accounts", "BankTransactions")
    params: Optional query parameters
    """
    if not tenant_id:
        raise HTTPException(400, "Not connected. Visit /connect first")
    
    token = get_access_token()
    all_data = []
    page = 1
    params = params or {}
    
    while True:
        params["page"] = page
        
        response = requests.get(
            f"https://api.xero.com/api.xro/2.0/{endpoint}",
            headers={
                "Authorization": f"Bearer {token}",
                "xero-tenant-id": tenant_id,
                "Accept": "application/json",
            },
            params=params,
            timeout=20,
        )
        
        if response.status_code != 200:
            raise HTTPException(response.status_code, response.text)
        
        data = response.json()
        records = data.get(data_key, [])
        
        if not records:
            break
            
        all_data.extend(records)
        
        # Xero returns 100 records per page by default
        if len(records) < 100:
            break
            
        page += 1
    
    return all_data


def fetch_payroll_data_paginated(endpoint: str, data_key: str, params: dict = None, api_version: str = "1.0"):
    """
    Helper function to fetch paginated data from Xero Payroll API (AU)
    
    endpoint: Payroll API endpoint path (e.g., "Employees", "PayRuns", "Payslips")
    data_key: JSON key containing the data array (e.g., "Employees", "PayRuns", "Payslips")
    params: Optional query parameters
    api_version: API version to use (1.0 or 2.0) - defaults to 1.0
    """
    if not tenant_id:
        raise HTTPException(400, "Not connected. Visit /connect first")
    
    token = get_access_token()
    all_data = []
    page = 1
    params = params or {}
    
    while True:
        params["page"] = page
        
        response = requests.get(
            f"https://api.xero.com/payroll.xro/{api_version}/{endpoint}",
            headers={
                "Authorization": f"Bearer {token}",
                "xero-tenant-id": tenant_id,
                "Accept": "application/json",
            },
            params=params,
            timeout=20,
        )
        
        if response.status_code != 200:
            # Return detailed error message
            error_detail = f"{response.status_code}: {response.text if response.text else 'No error details'}"
            raise HTTPException(response.status_code, error_detail)
        
        data = response.json()
        records = data.get(data_key, [])
        
        if not records:
            break
            
        all_data.extend(records)
        
        # Xero returns 100 records per page by default
        if len(records) < 100:
            break
            
        page += 1
    
    return all_data


@app.get("/")
def home():
    """Landing page"""
    return {"message": "Xero Integration API", "connect": "/connect"}


@app.get("/connect")
def connect():
    """Initiate Xero OAuth flow"""
    state = secrets.token_urlsafe(24)
    session_state[state] = {"timestamp": int(time.time())}
    
    params = {
        "response_type": "code",
        "client_id": XERO_CLIENT_ID,
        "redirect_uri": XERO_REDIRECT_URI,
        "scope": SCOPES,
        "state": state,
    }
    
    auth_url = f"{AUTH_URL}?{urlencode(params)}"
    return RedirectResponse(auth_url, status_code=302)


@app.get("/callback/xero")
def callback(code: str = "", state: str = ""):
    """Handle OAuth callback from Xero"""
    global tenant_id
    
    # Verify state
    if state not in session_state:
        raise HTTPException(400, "Invalid state")
    session_state.pop(state)
    
    # Exchange code for tokens
    token_response = requests.post(
        TOKEN_URL,
        headers={
            "Authorization": f"Basic {get_basic_auth()}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": XERO_REDIRECT_URI
        },
        timeout=10,
    )
    
    if token_response.status_code != 200:
        raise HTTPException(400, f"Token exchange failed: {token_response.text}")
    
    # Save tokens
    token_data = token_response.json()
    tokens["access_token"] = token_data["access_token"]
    tokens["refresh_token"] = token_data.get("refresh_token")
    tokens["expires_at"] = int(time.time()) + int(token_data.get("expires_in", 1800))
    tokens["scope"] = token_data.get("scope", "")
    
    # Get tenant/organization connections
    connections_response = requests.get(
        "https://api.xero.com/connections",
        headers={"Authorization": f"Bearer {tokens['access_token']}"},
        timeout=10,
    )
    
    if connections_response.status_code != 200:
        raise HTTPException(400, f"Failed to get connections: {connections_response.text}")
    
    connections = connections_response.json()
    if not connections:
        return JSONResponse({
            "status": "connected",
            "message": "No Xero organizations found"
        })
    
    # Use first organization
    tenant_id = connections[0]["tenantId"]
    org_name = connections[0]["tenantName"] #name of the tenant that has been connected
    
    # Fetch one sample from each endpoint for preview
    preview = {
        "settings": {},
        "transactions": {},
        "contacts": {},
        "payroll": {}
    }
    errors = {}
    
    # Settings
    try:
        accounts = get_accounts()
        preview["settings"]["accounts"] = accounts[0] if accounts else None
        preview["settings"]["accounts_total"] = len(accounts)
    except Exception as e:
        errors["accounts"] = str(e)
    
    try:
        tax_rates = get_tax_rates()
        preview["settings"]["tax_rates"] = tax_rates[0] if tax_rates else None
        preview["settings"]["tax_rates_total"] = len(tax_rates)
    except Exception as e:
        errors["tax_rates"] = str(e)
    
    try:
        tracking_categories = get_tracking_categories()
        preview["settings"]["tracking_categories"] = tracking_categories[0] if tracking_categories else None
        preview["settings"]["tracking_categories_total"] = len(tracking_categories)
    except Exception as e:
        errors["tracking_categories"] = str(e)
    
    try:
        organisation = get_organisation()
        preview["settings"]["organisation"] = organisation
    except Exception as e:
        errors["organisation"] = str(e)
    
    # Transactions
    try:
        bank_transactions = get_bank_transactions()
        preview["transactions"]["bank_transactions"] = bank_transactions[0] if bank_transactions else None
        preview["transactions"]["bank_transactions_total"] = len(bank_transactions)
    except Exception as e:
        errors["bank_transactions"] = str(e)
    
    try:
        payments = get_payments()
        preview["transactions"]["payments"] = payments[0] if payments else None
        preview["transactions"]["payments_total"] = len(payments)
    except Exception as e:
        errors["payments"] = str(e)
    
    try:
        credit_notes = get_credit_notes()
        preview["transactions"]["credit_notes"] = credit_notes[0] if credit_notes else None
        preview["transactions"]["credit_notes_total"] = len(credit_notes)
    except Exception as e:
        errors["credit_notes"] = str(e)
    
    try:
        overpayments = get_overpayments()
        preview["transactions"]["overpayments"] = overpayments[0] if overpayments else None
        preview["transactions"]["overpayments_total"] = len(overpayments)
    except Exception as e:
        errors["overpayments"] = str(e)
    
    try:
        prepayments = get_prepayments()
        preview["transactions"]["prepayments"] = prepayments[0] if prepayments else None
        preview["transactions"]["prepayments_total"] = len(prepayments)
    except Exception as e:
        errors["prepayments"] = str(e)
    
    try:
        manual_journals = get_manual_journals()
        preview["transactions"]["manual_journals"] = manual_journals[0] if manual_journals else None
        preview["transactions"]["manual_journals_total"] = len(manual_journals)
    except Exception as e:
        errors["manual_journals"] = str(e)
    
    # Contacts
    try:
        contacts = get_contacts()
        preview["contacts"]["contacts"] = contacts[0] if contacts else None
        preview["contacts"]["contacts_total"] = len(contacts)
    except Exception as e:
        errors["contacts"] = str(e)
    
    # Payroll (may not be available)
    try:
        employees = get_employees()
        preview["payroll"]["employees"] = employees[0] if employees else None
        preview["payroll"]["employees_total"] = len(employees)
    except Exception as e:
        errors["employees"] = str(e)
    
    try:
        payruns = get_payruns()
        preview["payroll"]["payruns"] = payruns[0] if payruns else None
        preview["payroll"]["payruns_total"] = len(payruns)
        
        # Try to fetch payslips using the first payrun ID
        if payruns and len(payruns) > 0:
            try:
                first_payrun_id = payruns[0].get("PayRunID")
                payslips = get_payslips(payrun_id=first_payrun_id)
                preview["payroll"]["payslips"] = payslips[0] if payslips else None
                preview["payroll"]["payslips_total"] = len(payslips)
            except Exception as e:
                errors["payslips"] = str(e)
    except Exception as e:
        errors["payruns"] = str(e)
    
    result = {
        "status": "success",
        "organization": org_name,
        "tenant_id": tenant_id,
        "preview": preview
    }
    
    if errors:
        result["errors"] = errors
    
    return JSONResponse(result)


@app.get("/accounts")
def get_accounts():
    """Fetch chart of accounts (with pagination support)"""
    return fetch_xero_data_paginated("Accounts", "Accounts")


@app.get("/transactions")
def get_bank_transactions():
    """Fetch bank transactions (with pagination support)"""
    return fetch_xero_data_paginated("BankTransactions", "BankTransactions")


# ============================================================================
# ACCOUNTING SETTINGS ENDPOINTS (accounting.settings.read)
# ============================================================================

@app.get("/tax-rates")
def get_tax_rates():
    """Fetch tax rates (GST logic)"""
    return fetch_xero_data_paginated("TaxRates", "TaxRates")


@app.get("/tracking-categories")
def get_tracking_categories():
    """Fetch tracking categories (cost centres, jobs, departments)"""
    return fetch_xero_data_paginated("TrackingCategories", "TrackingCategories")


@app.get("/organisation")
def get_organisation():
    """Fetch organisation details (name, currency, country, tax settings)"""
    orgs = fetch_xero_data_paginated("Organisation", "Organisations")
    return orgs[0] if orgs else {}


# ============================================================================
# ACCOUNTING TRANSACTIONS ENDPOINTS (accounting.transactions.read)
# ============================================================================

@app.get("/payments")
def get_payments():
    """Fetch payments (links to invoices/credit notes)"""
    return fetch_xero_data_paginated("Payments", "Payments")


@app.get("/credit-notes")
def get_credit_notes():
    """Fetch credit notes (refunds, returns)"""
    return fetch_xero_data_paginated("CreditNotes", "CreditNotes")


@app.get("/overpayments")
def get_overpayments():
    """Fetch overpayments (excess payments requiring allocation)"""
    return fetch_xero_data_paginated("Overpayments", "Overpayments")


@app.get("/prepayments")
def get_prepayments():
    """Fetch prepayments (advance payments)"""
    return fetch_xero_data_paginated("Prepayments", "Prepayments")


@app.get("/manual-journals")
def get_manual_journals():
    """Fetch manual journals (adjustments not in bank transactions)"""
    return fetch_xero_data_paginated("ManualJournals", "ManualJournals")


# ============================================================================
# CONTACTS ENDPOINT (accounting.contacts.read)
# ============================================================================

@app.get("/contacts")
def get_contacts():
    """Fetch contacts (customers, suppliers with ABN/tax details) - with pagination"""
    return fetch_xero_data_paginated("Contacts", "Contacts")


# ============================================================================
# PAYROLL ENDPOINTS (Australian Payroll API v1.0)
# ============================================================================

@app.get("/payroll/employees")
def get_employees():
    """
    Fetch employees (for income verification)
    Returns: EmployeeID, names, DOB, addresses, employment status, bank accounts
    
    Uses Payroll API v1.0 (compatible with most Xero accounts)
    """
    return fetch_payroll_data_paginated("Employees", "Employees", api_version="1.0")


@app.get("/payroll/payruns")
def get_payruns():
    """
    Fetch pay runs (pay periods & totals)
    Returns: PayRunID, period start/end, calendar, totals, status
    
    Uses Payroll API v1.0 (compatible with most Xero accounts)
    """
    return fetch_payroll_data_paginated("PayRuns", "PayRuns", api_version="1.0")


@app.get("/payroll/payslips")
def get_payslips(payrun_id: str = None):
    """
    Fetch payslips (income details per employee per pay run)
    
    Query params:
    - payrun_id: Required for v1.0 - specific pay run ID
    
    Returns: Gross, net, tax withheld, super, YTD figures, earnings lines
    
    Uses Payroll API v1.0
    Note: v1.0 requires PayRunID to fetch payslips
    """
    if not payrun_id:
        # If no payrun_id provided, get all payruns and fetch payslips for the most recent one
        try:
            payruns = get_payruns()
            if payruns and len(payruns) > 0:
                # Get the most recent payrun
                payrun_id = payruns[0].get("PayRunID")
        except:
            pass
    
    if not payrun_id:
        raise HTTPException(400, "payrun_id is required for v1.0 API. Get PayRunID from /payroll/payruns first")
    
    if not tenant_id:
        raise HTTPException(400, "Not connected. Visit /connect first")
    
    token = get_access_token()
    
    # v1.0 uses a different endpoint structure: GET /PayRuns/{PayRunID}
    # This returns the PayRun with Payslips embedded
    response = requests.get(
        f"https://api.xero.com/payroll.xro/1.0/PayRuns/{payrun_id}",
        headers={
            "Authorization": f"Bearer {token}",
            "xero-tenant-id": tenant_id,
            "Accept": "application/json",
        },
        timeout=20,
    )
    
    if response.status_code != 200:
        error_detail = f"{response.status_code}: {response.text if response.text else 'No error details'}"
        raise HTTPException(response.status_code, error_detail)
    
    data = response.json()
    
    # Extract payslips from the PayRuns array
    payruns_data = data.get("PayRuns", [])
    if payruns_data and len(payruns_data) > 0:
        return payruns_data[0].get("Payslips", [])
    
    return []


# ============================================================================
# ATTACHMENTS ENDPOINTS (accounting.attachments.read)
# ============================================================================

@app.get("/attachments/{endpoint}/{guid}")
def get_attachments(endpoint: str, guid: str):
    """
    Fetch attachments for a specific entity
    
    endpoint: BankTransactions, Invoices, PurchaseOrders, etc.
    guid: The GUID of the specific record
    
    Example: /attachments/BankTransactions/abc-123-def
    """
    if not tenant_id:
        raise HTTPException(400, "Not connected. Visit /connect first")
    
    token = get_access_token()
    
    response = requests.get(
        f"https://api.xero.com/api.xro/2.0/{endpoint}/{guid}/Attachments",
        headers={
            "Authorization": f"Bearer {token}",
            "xero-tenant-id": tenant_id,
            "Accept": "application/json",
        },
        timeout=20,
    )
    
    if response.status_code != 200:
        raise HTTPException(response.status_code, response.text)
    
    return response.json().get("Attachments", [])


@app.get("/attachment-file/{endpoint}/{guid}/{filename}")
def download_attachment(endpoint: str, guid: str, filename: str):
    """
    Download a specific attachment file
    
    endpoint: BankTransactions, Invoices, etc.
    guid: The GUID of the record
    filename: The attachment filename
    
    Example: /attachment-file/BankTransactions/abc-123/receipt.pdf
    """
    if not tenant_id:
        raise HTTPException(400, "Not connected. Visit /connect first")
    
    token = get_access_token()
    
    response = requests.get(
        f"https://api.xero.com/api.xro/2.0/{endpoint}/{guid}/Attachments/{filename}",
        headers={
            "Authorization": f"Bearer {token}",
            "xero-tenant-id": tenant_id,
        },
        timeout=30,
    )
    
    if response.status_code != 200:
        raise HTTPException(response.status_code, response.text)
    
    # Return the file content with appropriate headers
    from fastapi.responses import Response
    return Response(
        content=response.content,
        media_type=response.headers.get("Content-Type", "application/octet-stream"),
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        }
    )


@app.get("/status")
def status():
    """Check connection status"""
    return {
        "connected": bool(tokens),
        "has_tenant": tenant_id is not None,
        "tenant_id": tenant_id,
        "token_scopes": tokens.get("scope") if tokens else None
    }


@app.get("/fetch-all")
def fetch_all_data():
    """
    Fetch all available data from Xero in one call
    Useful for initial sync or comprehensive data pull
    """
    if not tenant_id:
        raise HTTPException(400, "Not connected. Visit /connect first")
    
    data = {}
    errors = {}
    
    # Settings
    try:
        data["accounts"] = get_accounts()
    except Exception as e:
        errors["accounts"] = str(e)
    
    try:
        data["tax_rates"] = get_tax_rates()
    except Exception as e:
        errors["tax_rates"] = str(e)
    
    try:
        data["tracking_categories"] = get_tracking_categories()
    except Exception as e:
        errors["tracking_categories"] = str(e)
    
    try:
        data["organisation"] = get_organisation()
    except Exception as e:
        errors["organisation"] = str(e)
    
    # Transactions
    try:
        data["bank_transactions"] = get_bank_transactions()
    except Exception as e:
        errors["bank_transactions"] = str(e)
    
    try:
        data["payments"] = get_payments()
    except Exception as e:
        errors["payments"] = str(e)
    
    try:
        data["credit_notes"] = get_credit_notes()
    except Exception as e:
        errors["credit_notes"] = str(e)
    
    try:
        data["overpayments"] = get_overpayments()
    except Exception as e:
        errors["overpayments"] = str(e)
    
    try:
        data["prepayments"] = get_prepayments()
    except Exception as e:
        errors["prepayments"] = str(e)
    
    try:
        data["manual_journals"] = get_manual_journals()
    except Exception as e:
        errors["manual_journals"] = str(e)
    
    # Contacts
    try:
        data["contacts"] = get_contacts()
    except Exception as e:
        errors["contacts"] = str(e)
    
    # Payroll
    try:
        data["employees"] = get_employees()
    except Exception as e:
        errors["employees"] = str(e)
    
    try:
        data["payruns"] = get_payruns()
    except Exception as e:
        errors["payruns"] = str(e)
    
    try:
        data["payslips"] = get_payslips()
    except Exception as e:
        errors["payslips"] = str(e)
    
    result = {"data": data}
    if errors:
        result["errors"] = errors
    
    return result


@app.get("/endpoints")
def list_endpoints():
    """List all available API endpoints"""
    return {
        "connection": {
            "/": "Home page",
            "/connect": "Start OAuth flow",
            "/callback": "OAuth callback (automatic)",
            "/status": "Check connection status",
        },
        "settings": {
            "/accounts": "Chart of accounts (codes/names)",
            "/tax-rates": "Tax rates (GST logic)",
            "/tracking-categories": "Tracking categories (cost centres, jobs)",
            "/organisation": "Organisation details (name, currency, country)",
        },
        "transactions": {
            "/transactions": "Bank transactions",
            "/payments": "Payments (linked to invoices/credit notes)",
            "/credit-notes": "Credit notes (refunds, returns)",
            "/overpayments": "Overpayments (excess payments)",
            "/prepayments": "Prepayments (advance payments)",
            "/manual-journals": "Manual journals (adjustments)",
        },
        "contacts": {
            "/contacts": "Contacts (customers/suppliers with ABN/tax details)",
        },
        "payroll": {
            "/payroll/employees": "Employees (names, DOB, bank accounts, employment status)",
            "/payroll/payruns": "Pay runs (pay periods, totals, status)",
            "/payroll/payslips": "Payslips (gross, net, tax, super, YTD) - supports ?pay_run_id= or ?employee_id= filters",
        },
        "attachments": {
            "/attachments/{endpoint}/{guid}": "List attachments for a record",
            "/attachment-file/{endpoint}/{guid}/{filename}": "Download attachment file",
        },
        "bulk": {
            "/fetch-all": "Fetch all data in one call",
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)