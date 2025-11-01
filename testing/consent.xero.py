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

SCOPES = "offline_access accounting.settings.read accounting.transactions.read accounting.contacts.read accounting.attachments.read accounting.reports.read payroll.employees.read payroll.payruns.read payroll.payslip.read"
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
    org_name = connections[0]["tenantName"]
    
    # Fetch one sample from each endpoint for preview
    preview = {
        "settings": {},
        "transactions": {},
        "contacts": {},
        "payroll": {},
        "attachments": {}
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
        
        # Try to fetch attachments for the first bank transaction
        if bank_transactions and len(bank_transactions) > 0:
            try:
                first_transaction = bank_transactions[0]
                transaction_id = first_transaction.get("BankTransactionID")
                
                if transaction_id:
                    attachments = get_attachments("BankTransactions", transaction_id)
                    preview["attachments"]["bank_transaction_attachments"] = attachments
                    preview["attachments"]["bank_transaction_attachments_total"] = len(attachments)
                    
                    # If there's an attachment, provide download info
                    if attachments and len(attachments) > 0:
                        first_attachment = attachments[0]
                        attachment_filename = first_attachment.get("FileName")
                        preview["attachments"]["example_download"] = {
                            "transaction_id": transaction_id,
                            "filename": attachment_filename,
                            "download_url": f"/attachment-file/BankTransactions/{transaction_id}/{attachment_filename}",
                            "note": "Use this URL to download the attachment file"
                        }
            except Exception as e:
                errors["bank_transaction_attachments"] = str(e)
                
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
    

    try:
        invoices = get_invoices()
        preview["transactions"]["invoices"] = invoices[0] if invoices else None
        preview["transactions"]["invoices_total"] = len(invoices)
    except Exception as e:
        errors["invoices"] = str(e)

    # Bank Transfers
    try:
        bank_transfers = get_bank_transfers()
        preview["transactions"]["bank_transfers"] = bank_transfers[0] if bank_transfers else None
        preview["transactions"]["bank_transfers_total"] = len(bank_transfers)
    except Exception as e:
        errors["bank_transfers"] = str(e)

    # Financial Reports
    preview["reports"] = {}
    try:
        pl_report = get_profit_loss()
        preview["reports"]["profit_loss"] = pl_report
    except Exception as e:
        errors["profit_loss"] = str(e)

    try:
        bs_report = get_balance_sheet()
        preview["reports"]["balance_sheet"] = bs_report
    except Exception as e:
        errors["balance_sheet"] = str(e)




    result = {
        "status": "success",
        "organization": org_name,
        "tenant_id": tenant_id,
        "preview": preview
    }
    
    if errors:
        result["errors"] = errors
    
    try:
        generate_accounts_report(result, "xero_accounts_report.pdf")
        generate_transactions_report(result, "xero_transactions_report.pdf")
        generate_payments_report(result, "xero_payments_report.pdf")
        generate_credit_notes_report(result, "xero_credit_notes_report.pdf")
        generate_payroll_report(result, "xero_payroll_report.pdf")
        generate_invoices_report(result, "xero_invoices_report.pdf")
        generate_bank_transfers_report(result, "xero_bank_transfers_report.pdf")
        generate_reports_summary(result, "xero_financial_reports.pdf")
        
        result["pdf_reports"] = [
            "xero_accounts_report.pdf",
            "xero_transactions_report.pdf",
            "xero_payments_report.pdf",
            "xero_credit_notes_report.pdf",
            "xero_payroll_report.pdf",
            "xero_invoices_report.pdf",
            "xero_bank_transfers_report.pdf",
            "xero_financial_reports.pdf"
        ]
    except Exception as e:
        result["pdf_error"] = str(e)

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
# INVOICES ENDPOINT (accounting.transactions.read)
# ============================================================================

@app.get("/invoices")
def get_invoices():
    """Fetch invoices (for income verification)"""
    return fetch_xero_data_paginated("Invoices", "Invoices")


# ============================================================================
# REPORTS ENDPOINTS (accounting.reports.read) - requires additional scope
# ============================================================================

@app.get("/reports/profit-loss")
def get_profit_loss(from_date: str = None, to_date: str = None):
    """
    Fetch Profit & Loss statement
    
    Query params:
    - from_date: Start date (YYYY-MM-DD)
    - to_date: End date (YYYY-MM-DD)
    """
    if not tenant_id:
        raise HTTPException(400, "Not connected. Visit /connect first")
    
    token = get_access_token()
    params = {}
    if from_date:
        params["fromDate"] = from_date
    if to_date:
        params["toDate"] = to_date
    
    response = requests.get(
        "https://api.xero.com/api.xro/2.0/Reports/ProfitAndLoss",
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
    
    return response.json()


@app.get("/reports/balance-sheet")
def get_balance_sheet(date: str = None):
    """
    Fetch Balance Sheet
    
    Query params:
    - date: Report date (YYYY-MM-DD)
    """
    if not tenant_id:
        raise HTTPException(400, "Not connected. Visit /connect first")
    
    token = get_access_token()
    params = {}
    if date:
        params["date"] = date
    
    response = requests.get(
        "https://api.xero.com/api.xro/2.0/Reports/BalanceSheet",
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
    
    return response.json()


@app.get("/bank-transfers")
def get_bank_transfers():
    """Fetch bank transfers (fund movements between accounts)"""
    return fetch_xero_data_paginated("BankTransfers", "BankTransfers")

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
            "/invoices": "Invoices (income verification)",
            "/bank-transfers": "Bank transfers",
            "/payments": "Payments (linked to invoices/credit notes)",
            "/credit-notes": "Credit notes (refunds, returns)",
            "/overpayments": "Overpayments (excess payments)",
            "/prepayments": "Prepayments (advance payments)",
            "/manual-journals": "Manual journals (adjustments)",
        },
        "reports": {
            "/reports/profit-loss": "P&L statement (supports ?from_date= and ?to_date=)",
            "/reports/balance-sheet": "Balance sheet (supports ?date=)",
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

#!/usr/bin/env python3
"""
Generate PDF reports from Xero callback output
Creates separate PDFs for:
- Accounts
- Transactions
- Payments
- Credit Notes
- Payroll (combined employees, payruns, payslips)
"""

import json
from datetime import datetime
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT


def format_date(date_str):
    """Format Xero date string to readable format"""
    if not date_str:
        return "N/A"
    if isinstance(date_str, str) and "/Date(" in date_str:
        # Extract timestamp from /Date(timestamp)/
        timestamp = date_str.split("(")[1].split(")")[0]
        if "+" in timestamp or "-" in timestamp:
            timestamp = timestamp.split("+")[0].split("-")[0]
        try:
            dt = datetime.fromtimestamp(int(timestamp) / 1000)
            return dt.strftime("%Y-%m-%d")
        except:
            return date_str
    return date_str


def format_currency(amount):
    """Format currency amounts"""
    if amount is None:
        return "$0.00"
    try:
        return f"${float(amount):,.2f}"
    except:
        return str(amount)


def create_header(title, org_name):
    """Create a header section for the report"""
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Title'],
        fontSize=24,
        textColor=colors.HexColor('#1a5490'),
        spaceAfter=6,
    )
    subtitle_style = ParagraphStyle(
        'CustomSubtitle',
        parent=styles['Normal'],
        fontSize=12,
        textColor=colors.grey,
        spaceAfter=20,
    )
    
    story = []
    story.append(Paragraph(title, title_style))
    story.append(Paragraph(f"Organization: {org_name}", subtitle_style))
    story.append(Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", subtitle_style))
    story.append(Spacer(1, 0.2*inch))
    
    return story


def generate_accounts_report(data, output_file):
    """Generate Accounts PDF report"""
    doc = SimpleDocTemplate(output_file, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()
    
    # Header
    org_name = data.get('organization', 'Unknown Organization')
    story.extend(create_header("Chart of Accounts Report", org_name))
    
    # Summary
    accounts_total = data['preview']['settings'].get('accounts_total', 0)
    story.append(Paragraph(f"<b>Total Accounts:</b> {accounts_total}", styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    
    # Sample account details
    sample_account = data['preview']['settings'].get('accounts')
    if sample_account:
        story.append(Paragraph("<b>Sample Account Details:</b>", styles['Heading2']))
        story.append(Spacer(1, 0.1*inch))
        
        account_data = [
            ['Field', 'Value'],
            ['Account Code', sample_account.get('Code', 'N/A')],
            ['Account Name', sample_account.get('Name', 'N/A')],
            ['Type', sample_account.get('Type', 'N/A')],
            ['Class', sample_account.get('Class', 'N/A')],
            ['Status', sample_account.get('Status', 'N/A')],
            ['Tax Type', sample_account.get('TaxType', 'N/A')],
            ['Currency', sample_account.get('CurrencyCode', 'N/A')],
            ['Bank Account Number', sample_account.get('BankAccountNumber', 'N/A')],
            ['Has Attachments', str(sample_account.get('HasAttachments', False))],
        ]
        
        table = Table(account_data, colWidths=[2.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ]))
        
        story.append(table)
    
    # Tax rates summary
    story.append(Spacer(1, 0.3*inch))
    tax_rates_total = data['preview']['settings'].get('tax_rates_total', 0)
    story.append(Paragraph(f"<b>Tax Rates Available:</b> {tax_rates_total}", styles['Normal']))
    
    sample_tax = data['preview']['settings'].get('tax_rates')
    if sample_tax:
        story.append(Spacer(1, 0.1*inch))
        story.append(Paragraph(f"Sample: {sample_tax.get('Name', 'N/A')} - {sample_tax.get('DisplayTaxRate', 0)}%", styles['Normal']))
    
    # Tracking categories
    story.append(Spacer(1, 0.3*inch))
    tracking_total = data['preview']['settings'].get('tracking_categories_total', 0)
    story.append(Paragraph(f"<b>Tracking Categories:</b> {tracking_total}", styles['Normal']))
    
    sample_tracking = data['preview']['settings'].get('tracking_categories')
    if sample_tracking:
        story.append(Spacer(1, 0.1*inch))
        story.append(Paragraph(f"Sample: {sample_tracking.get('Name', 'N/A')}", styles['Normal']))
        options = sample_tracking.get('Options', [])
        if options:
            story.append(Paragraph(f"Options: {', '.join([opt.get('Name', '') for opt in options])}", styles['Normal']))
    
    doc.build(story)
    print(f"✓ Generated: {output_file}")


def generate_transactions_report(data, output_file):
    """Generate Bank Transactions PDF report"""
    doc = SimpleDocTemplate(output_file, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()
    
    # Header
    org_name = data.get('organization', 'Unknown Organization')
    story.extend(create_header("Bank Transactions Report", org_name))
    
    # Summary
    transactions_total = data['preview']['transactions'].get('bank_transactions_total', 0)
    story.append(Paragraph(f"<b>Total Bank Transactions:</b> {transactions_total}", styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    
    # Sample transaction
    sample_txn = data['preview']['transactions'].get('bank_transactions')
    if sample_txn:
        story.append(Paragraph("<b>Sample Transaction Details:</b>", styles['Heading2']))
        story.append(Spacer(1, 0.1*inch))
        
        txn_data = [
            ['Field', 'Value'],
            ['Transaction ID', sample_txn.get('BankTransactionID', 'N/A')[:20] + '...'],
            ['Type', sample_txn.get('Type', 'N/A')],
            ['Status', sample_txn.get('Status', 'N/A')],
            ['Date', format_date(sample_txn.get('Date'))],
            ['Reference', sample_txn.get('Reference', 'N/A')],
            ['Contact', sample_txn.get('Contact', {}).get('Name', 'N/A')],
            ['Subtotal', format_currency(sample_txn.get('SubTotal'))],
            ['Total Tax', format_currency(sample_txn.get('TotalTax'))],
            ['Total', format_currency(sample_txn.get('Total'))],
            ['Currency', sample_txn.get('CurrencyCode', 'N/A')],
            ['Reconciled', str(sample_txn.get('IsReconciled', False))],
            ['Has Attachments', str(sample_txn.get('HasAttachments', False))],
        ]
        
        table = Table(txn_data, colWidths=[2.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ]))
        
        story.append(table)
        
        # Line items
        line_items = sample_txn.get('LineItems', [])
        if line_items:
            story.append(Spacer(1, 0.2*inch))
            story.append(Paragraph("<b>Line Items:</b>", styles['Heading3']))
            story.append(Spacer(1, 0.1*inch))
            
            for idx, item in enumerate(line_items, 1):
                story.append(Paragraph(f"<b>Item {idx}:</b>", styles['Normal']))
                story.append(Paragraph(f"Description: {item.get('Description', 'N/A')}", styles['Normal']))
                story.append(Paragraph(f"Amount: {format_currency(item.get('LineAmount'))}", styles['Normal']))
                story.append(Paragraph(f"Account Code: {item.get('AccountCode', 'N/A')}", styles['Normal']))
                story.append(Spacer(1, 0.1*inch))
    
    # Other transaction types summary
    story.append(Spacer(1, 0.3*inch))
    story.append(Paragraph("<b>Other Transaction Types:</b>", styles['Heading2']))
    story.append(Spacer(1, 0.1*inch))
    
    other_txns = [
        ('Manual Journals', data['preview']['transactions'].get('manual_journals_total', 0)),
        ('Overpayments', data['preview']['transactions'].get('overpayments_total', 0)),
        ('Prepayments', data['preview']['transactions'].get('prepayments_total', 0)),
    ]
    
    for txn_type, count in other_txns:
        story.append(Paragraph(f"{txn_type}: {count}", styles['Normal']))
    
    doc.build(story)
    print(f"✓ Generated: {output_file}")


def generate_payments_report(data, output_file):
    """Generate Payments PDF report"""
    doc = SimpleDocTemplate(output_file, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()
    
    # Header
    org_name = data.get('organization', 'Unknown Organization')
    story.extend(create_header("Payments Report", org_name))
    
    # Summary
    payments_total = data['preview']['transactions'].get('payments_total', 0)
    story.append(Paragraph(f"<b>Total Payments:</b> {payments_total}", styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    
    # Sample payment
    sample_payment = data['preview']['transactions'].get('payments')
    if sample_payment:
        story.append(Paragraph("<b>Sample Payment Details:</b>", styles['Heading2']))
        story.append(Spacer(1, 0.1*inch))
        
        payment_data = [
            ['Field', 'Value'],
            ['Payment ID', sample_payment.get('PaymentID', 'N/A')[:20] + '...'],
            ['Date', format_date(sample_payment.get('Date'))],
            ['Amount', format_currency(sample_payment.get('Amount'))],
            ['Bank Amount', format_currency(sample_payment.get('BankAmount'))],
            ['Reference', sample_payment.get('Reference', 'N/A')],
            ['Payment Type', sample_payment.get('PaymentType', 'N/A')],
            ['Status', sample_payment.get('Status', 'N/A')],
            ['Reconciled', str(sample_payment.get('IsReconciled', False))],
        ]
        
        # Add invoice details if present
        invoice = sample_payment.get('Invoice', {})
        if invoice:
            contact = invoice.get('Contact', {})
            payment_data.extend([
                ['Invoice Contact', contact.get('Name', 'N/A')],
                ['Invoice Type', invoice.get('Type', 'N/A')],
            ])
        
        table = Table(payment_data, colWidths=[2.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ]))
        
        story.append(table)
    
    doc.build(story)
    print(f"✓ Generated: {output_file}")


def generate_credit_notes_report(data, output_file):
    """Generate Credit Notes PDF report"""
    doc = SimpleDocTemplate(output_file, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()
    
    # Header
    org_name = data.get('organization', 'Unknown Organization')
    story.extend(create_header("Credit Notes Report", org_name))
    
    # Summary
    credit_notes_total = data['preview']['transactions'].get('credit_notes_total', 0)
    story.append(Paragraph(f"<b>Total Credit Notes:</b> {credit_notes_total}", styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    
    # Sample credit note
    sample_cn = data['preview']['transactions'].get('credit_notes')
    if sample_cn:
        story.append(Paragraph("<b>Sample Credit Note Details:</b>", styles['Heading2']))
        story.append(Spacer(1, 0.1*inch))
        
        cn_data = [
            ['Field', 'Value'],
            ['Credit Note ID', sample_cn.get('CreditNoteID', 'N/A')[:20] + '...'],
            ['Credit Note Number', sample_cn.get('CreditNoteNumber', 'N/A')],
            ['Type', sample_cn.get('Type', 'N/A')],
            ['Status', sample_cn.get('Status', 'N/A')],
            ['Date', format_date(sample_cn.get('Date'))],
            ['Due Date', format_date(sample_cn.get('DueDate'))],
            ['Contact', sample_cn.get('Contact', {}).get('Name', 'N/A')],
            ['Reference', sample_cn.get('Reference', 'N/A') or 'None'],
            ['Subtotal', format_currency(sample_cn.get('SubTotal'))],
            ['Total Tax', format_currency(sample_cn.get('TotalTax'))],
            ['Total', format_currency(sample_cn.get('Total'))],
            ['Remaining Credit', format_currency(sample_cn.get('RemainingCredit'))],
            ['Currency', sample_cn.get('CurrencyCode', 'N/A')],
            ['Has Attachments', str(sample_cn.get('HasAttachments', False))],
        ]
        
        table = Table(cn_data, colWidths=[2.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ]))
        
        story.append(table)
        
        # Line items
        line_items = sample_cn.get('LineItems', [])
        if line_items:
            story.append(Spacer(1, 0.2*inch))
            story.append(Paragraph("<b>Line Items:</b>", styles['Heading3']))
            story.append(Spacer(1, 0.1*inch))
            
            for idx, item in enumerate(line_items, 1):
                story.append(Paragraph(f"<b>Item {idx}:</b>", styles['Normal']))
                story.append(Paragraph(f"Description: {item.get('Description', 'N/A')}", styles['Normal']))
                story.append(Paragraph(f"Amount: {format_currency(item.get('LineAmount'))}", styles['Normal']))
                story.append(Paragraph(f"Account Code: {item.get('AccountCode', 'N/A')}", styles['Normal']))
                story.append(Spacer(1, 0.1*inch))
        
        # Allocations
        allocations = sample_cn.get('Allocations', [])
        if allocations:
            story.append(Spacer(1, 0.2*inch))
            story.append(Paragraph("<b>Allocations:</b>", styles['Heading3']))
            story.append(Spacer(1, 0.1*inch))
            
            for idx, alloc in enumerate(allocations, 1):
                story.append(Paragraph(f"Allocation {idx}: {format_currency(alloc.get('Amount'))} on {format_date(alloc.get('Date'))}", styles['Normal']))
    
    doc.build(story)
    print(f"✓ Generated: {output_file}")


def generate_payroll_report(data, output_file):
    """Generate combined Payroll PDF report"""
    doc = SimpleDocTemplate(output_file, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()
    
    # Header
    org_name = data.get('organization', 'Unknown Organization')
    story.extend(create_header("Payroll Report", org_name))
    
    # EMPLOYEES SECTION
    story.append(Paragraph("<b>EMPLOYEES</b>", styles['Heading1']))
    story.append(Spacer(1, 0.1*inch))
    
    employees_total = data['preview']['payroll'].get('employees_total', 0)
    story.append(Paragraph(f"<b>Total Employees:</b> {employees_total}", styles['Normal']))
    story.append(Spacer(1, 0.2*inch))
    
    sample_employee = data['preview']['payroll'].get('employees')
    if sample_employee:
        story.append(Paragraph("<b>Sample Employee:</b>", styles['Heading2']))
        story.append(Spacer(1, 0.1*inch))
        
        emp_data = [
            ['Field', 'Value'],
            ['Employee ID', sample_employee.get('EmployeeID', 'N/A')[:20] + '...'],
            ['Name', f"{sample_employee.get('FirstName', '')} {sample_employee.get('LastName', '')}"],
            ['Email', sample_employee.get('Email', 'N/A')],
            ['Status', sample_employee.get('Status', 'N/A')],
            ['Date of Birth', format_date(sample_employee.get('DateOfBirth'))],
            ['Gender', sample_employee.get('Gender', 'N/A')],
            ['Phone', sample_employee.get('Phone', 'N/A')],
            ['Mobile', sample_employee.get('Mobile', 'N/A')],
            ['Start Date', format_date(sample_employee.get('StartDate'))],
        ]
        
        table = Table(emp_data, colWidths=[2.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ]))
        
        story.append(table)
    
    # PAY RUNS SECTION
    story.append(Spacer(1, 0.4*inch))
    story.append(Paragraph("<b>PAY RUNS</b>", styles['Heading1']))
    story.append(Spacer(1, 0.1*inch))
    
    payruns_total = data['preview']['payroll'].get('payruns_total', 0)
    story.append(Paragraph(f"<b>Total Pay Runs:</b> {payruns_total}", styles['Normal']))
    story.append(Spacer(1, 0.2*inch))
    
    sample_payrun = data['preview']['payroll'].get('payruns')
    if sample_payrun:
        story.append(Paragraph("<b>Sample Pay Run:</b>", styles['Heading2']))
        story.append(Spacer(1, 0.1*inch))
        
        payrun_data = [
            ['Field', 'Value'],
            ['Pay Run ID', sample_payrun.get('PayRunID', 'N/A')[:20] + '...'],
            ['Status', sample_payrun.get('PayRunStatus', 'N/A')],
            ['Period Start', format_date(sample_payrun.get('PayRunPeriodStartDate'))],
            ['Period End', format_date(sample_payrun.get('PayRunPeriodEndDate'))],
            ['Payment Date', format_date(sample_payrun.get('PaymentDate'))],
            ['Wages', format_currency(sample_payrun.get('Wages'))],
            ['Deductions', format_currency(sample_payrun.get('Deductions'))],
            ['Tax', format_currency(sample_payrun.get('Tax'))],
            ['Super', format_currency(sample_payrun.get('Super'))],
            ['Net Pay', format_currency(sample_payrun.get('NetPay'))],
        ]
        
        table = Table(payrun_data, colWidths=[2.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ]))
        
        story.append(table)
    
    # PAYSLIPS SECTION
    story.append(Spacer(1, 0.4*inch))
    story.append(Paragraph("<b>PAYSLIPS</b>", styles['Heading1']))
    story.append(Spacer(1, 0.1*inch))
    
    payslips_total = data['preview']['payroll'].get('payslips_total', 0)
    story.append(Paragraph(f"<b>Total Payslips (from sample pay run):</b> {payslips_total}", styles['Normal']))
    story.append(Spacer(1, 0.2*inch))
    
    sample_payslip = data['preview']['payroll'].get('payslips')
    if sample_payslip:
        story.append(Paragraph("<b>Sample Payslip:</b>", styles['Heading2']))
        story.append(Spacer(1, 0.1*inch))
        
        payslip_data = [
            ['Field', 'Value'],
            ['Payslip ID', sample_payslip.get('PayslipID', 'N/A')[:20] + '...'],
            ['Employee', f"{sample_payslip.get('FirstName', '')} {sample_payslip.get('LastName', '')}"],
            ['Employee ID', sample_payslip.get('EmployeeID', 'N/A')[:20] + '...'],
            ['Wages', format_currency(sample_payslip.get('Wages'))],
            ['Deductions', format_currency(sample_payslip.get('Deductions'))],
            ['Tax', format_currency(sample_payslip.get('Tax'))],
            ['Super', format_currency(sample_payslip.get('Super'))],
            ['Reimbursements', format_currency(sample_payslip.get('Reimbursements'))],
            ['Net Pay', format_currency(sample_payslip.get('NetPay'))],
        ]
        
        table = Table(payslip_data, colWidths=[2.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ]))
        
        story.append(table)
    
    doc.build(story)
    print(f"✓ Generated: {output_file}")

def generate_invoices_report(data, output_file):
    """Generate Invoices PDF report"""
    doc = SimpleDocTemplate(output_file, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()
    
    # Header
    org_name = data.get('organization', 'Unknown Organization')
    story.extend(create_header("Invoices Report", org_name))
    
    # Summary
    invoices_total = data['preview']['transactions'].get('invoices_total', 0)
    story.append(Paragraph(f"<b>Total Invoices:</b> {invoices_total}", styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    
    # Sample invoice
    sample_invoice = data['preview']['transactions'].get('invoices')
    if sample_invoice:
        story.append(Paragraph("<b>Sample Invoice Details:</b>", styles['Heading2']))
        story.append(Spacer(1, 0.1*inch))
        
        invoice_data = [
            ['Field', 'Value'],
            ['Invoice ID', sample_invoice.get('InvoiceID', 'N/A')[:20] + '...'],
            ['Invoice Number', sample_invoice.get('InvoiceNumber', 'N/A')],
            ['Type', sample_invoice.get('Type', 'N/A')],
            ['Status', sample_invoice.get('Status', 'N/A')],
            ['Date', format_date(sample_invoice.get('Date'))],
            ['Due Date', format_date(sample_invoice.get('DueDate'))],
            ['Contact', sample_invoice.get('Contact', {}).get('Name', 'N/A')],
            ['Reference', sample_invoice.get('Reference', 'N/A') or 'None'],
            ['Subtotal', format_currency(sample_invoice.get('SubTotal'))],
            ['Total Tax', format_currency(sample_invoice.get('TotalTax'))],
            ['Total', format_currency(sample_invoice.get('Total'))],
            ['Amount Due', format_currency(sample_invoice.get('AmountDue'))],
            ['Amount Paid', format_currency(sample_invoice.get('AmountPaid'))],
            ['Currency', sample_invoice.get('CurrencyCode', 'N/A')],
        ]
        
        table = Table(invoice_data, colWidths=[2.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ]))
        
        story.append(table)
        
        # Line items
        line_items = sample_invoice.get('LineItems', [])
        if line_items:
            story.append(Spacer(1, 0.2*inch))
            story.append(Paragraph("<b>Line Items:</b>", styles['Heading3']))
            story.append(Spacer(1, 0.1*inch))
            
            for idx, item in enumerate(line_items, 1):
                story.append(Paragraph(f"<b>Item {idx}:</b>", styles['Normal']))
                story.append(Paragraph(f"Description: {item.get('Description', 'N/A')}", styles['Normal']))
                story.append(Paragraph(f"Quantity: {item.get('Quantity', 'N/A')}", styles['Normal']))
                story.append(Paragraph(f"Unit Amount: {format_currency(item.get('UnitAmount'))}", styles['Normal']))
                story.append(Paragraph(f"Line Amount: {format_currency(item.get('LineAmount'))}", styles['Normal']))
                story.append(Spacer(1, 0.1*inch))
    
    doc.build(story)
    print(f"✓ Generated: {output_file}")


def generate_reports_summary(data, output_file):
    """Generate Financial Reports Summary PDF"""
    doc = SimpleDocTemplate(output_file, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()
    
    # Header
    org_name = data.get('organization', 'Unknown Organization')
    story.extend(create_header("Financial Reports Summary", org_name))
    
    # P&L Summary
    pl_data = data['preview'].get('reports', {}).get('profit_loss')
    if pl_data:
        story.append(Paragraph("<b>PROFIT & LOSS</b>", styles['Heading1']))
        story.append(Spacer(1, 0.1*inch))
        
        reports = pl_data.get('Reports', [])
        if reports:
            report = reports[0]
            story.append(Paragraph(f"Report Title: {report.get('ReportTitles', ['N/A'])[0]}", styles['Normal']))
            story.append(Paragraph(f"Report Date: {report.get('ReportDate', 'N/A')}", styles['Normal']))
            story.append(Spacer(1, 0.2*inch))
            
            # Show key sections
            rows = report.get('Rows', [])
            for row in rows[:5]:  # Show first 5 sections
                if row.get('RowType') == 'Header':
                    story.append(Paragraph(f"<b>{row.get('Title', '')}</b>", styles['Heading3']))
                elif row.get('RowType') == 'Section':
                    story.append(Paragraph(row.get('Title', ''), styles['Normal']))
                    cells = row.get('Cells', [])
                    if cells:
                        value = cells[0].get('Value', 'N/A')
                        story.append(Paragraph(f"Amount: {format_currency(value)}", styles['Normal']))
                story.append(Spacer(1, 0.1*inch))
    else:
        story.append(Paragraph("<b>PROFIT & LOSS</b>", styles['Heading1']))
        story.append(Paragraph("No P&L data available", styles['Normal']))
    
    story.append(Spacer(1, 0.3*inch))
    
    # Balance Sheet Summary
    bs_data = data['preview'].get('reports', {}).get('balance_sheet')
    if bs_data:
        story.append(Paragraph("<b>BALANCE SHEET</b>", styles['Heading1']))
        story.append(Spacer(1, 0.1*inch))
        
        reports = bs_data.get('Reports', [])
        if reports:
            report = reports[0]
            story.append(Paragraph(f"Report Title: {report.get('ReportTitles', ['N/A'])[0]}", styles['Normal']))
            story.append(Paragraph(f"Report Date: {report.get('ReportDate', 'N/A')}", styles['Normal']))
            story.append(Spacer(1, 0.2*inch))
            
            # Show key sections
            rows = report.get('Rows', [])
            for row in rows[:5]:  # Show first 5 sections
                if row.get('RowType') == 'Header':
                    story.append(Paragraph(f"<b>{row.get('Title', '')}</b>", styles['Heading3']))
                elif row.get('RowType') == 'Section':
                    story.append(Paragraph(row.get('Title', ''), styles['Normal']))
                    cells = row.get('Cells', [])
                    if cells:
                        value = cells[0].get('Value', 'N/A')
                        story.append(Paragraph(f"Amount: {format_currency(value)}", styles['Normal']))
                story.append(Spacer(1, 0.1*inch))
    else:
        story.append(Paragraph("<b>BALANCE SHEET</b>", styles['Heading1']))
        story.append(Paragraph("No Balance Sheet data available", styles['Normal']))
    
    doc.build(story)
    print(f"✓ Generated: {output_file}")


def generate_bank_transfers_report(data, output_file):
    """Generate Bank Transfers PDF report"""
    doc = SimpleDocTemplate(output_file, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()
    
    # Header
    org_name = data.get('organization', 'Unknown Organization')
    story.extend(create_header("Bank Transfers Report", org_name))
    
    # Summary
    transfers_total = data['preview']['transactions'].get('bank_transfers_total', 0)
    story.append(Paragraph(f"<b>Total Bank Transfers:</b> {transfers_total}", styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    
    # Sample transfer
    sample_transfer = data['preview']['transactions'].get('bank_transfers')
    if sample_transfer:
        story.append(Paragraph("<b>Sample Bank Transfer Details:</b>", styles['Heading2']))
        story.append(Spacer(1, 0.1*inch))
        
        transfer_data = [
            ['Field', 'Value'],
            ['Transfer ID', sample_transfer.get('BankTransferID', 'N/A')[:20] + '...'],
            ['Date', format_date(sample_transfer.get('Date'))],
            ['Amount', format_currency(sample_transfer.get('Amount'))],
            ['From Account', sample_transfer.get('FromBankAccount', {}).get('Name', 'N/A')],
            ['To Account', sample_transfer.get('ToBankAccount', {}).get('Name', 'N/A')],
            ['Reference', sample_transfer.get('Reference', 'N/A') or 'None'],
            ['Currency', sample_transfer.get('CurrencyRate', 'N/A')],
        ]
        
        table = Table(transfer_data, colWidths=[2.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ]))
        
        story.append(table)
    
    doc.build(story)
    print(f"✓ Generated: {output_file}")


def main():
    """Main function to generate all reports"""
    import sys
    
    # Check if input file is provided
    if len(sys.argv) < 2:
        print("Usage: python generate_xero_reports.py <callback_output.json>")
        print("\nThis script generates PDF reports from Xero callback JSON output.")
        print("It creates 5 separate PDF files:")
        print("  - xero_accounts_report.pdf")
        print("  - xero_transactions_report.pdf")
        print("  - xero_payments_report.pdf")
        print("  - xero_credit_notes_report.pdf")
        print("  - xero_payroll_report.pdf")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    # Load the JSON data
    try:
        with open(input_file, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: File '{input_file}' is not valid JSON.")
        sys.exit(1)
    
    print(f"\nGenerating PDF reports from: {input_file}\n")
    
    # Generate each report
    try:
        generate_accounts_report(data, "xero_accounts_report.pdf")
        generate_transactions_report(data, "xero_transactions_report.pdf")
        generate_payments_report(data, "xero_payments_report.pdf")
        generate_credit_notes_report(data, "xero_credit_notes_report.pdf")
        generate_payroll_report(data, "xero_payroll_report.pdf")
        
        print("\n✓ All reports generated successfully!")
        print("\nGenerated files:")
        print("  - xero_accounts_report.pdf")
        print("  - xero_transactions_report.pdf")
        print("  - xero_payments_report.pdf")
        print("  - xero_credit_notes_report.pdf")
        print("  - xero_payroll_report.pdf")
        
    except Exception as e:
        print(f"\n✗ Error generating reports: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)