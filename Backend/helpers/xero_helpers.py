import os
import base64
from fastapi import FastAPI, HTTPException
import time
import requests
from External_APIs.xero_pdf_generation import *

tokens = {}
XERO_CLIENT_ID = os.environ.get("XERO_CLIENT_ID")
XERO_CLIENT_SECRET = os.environ.get("XERO_CLIENT_SECRET")
XERO_REDIRECT_URI = os.environ.get("XERO_REDIRECT_URI")  # e.g. http://localhost:8080/callback

# Get Xero endpoints
DISCOVERY_URL = "https://identity.xero.com/.well-known/openid-configuration"
disc = requests.get(DISCOVERY_URL, timeout=10).json()
AUTH_URL = disc["authorization_endpoint"]
TOKEN_URL = disc["token_endpoint"]

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


def fetch_xero_data_paginated(endpoint: str, data_key: str, tenant_id: str, params: dict = None):
    """
    Helper function to fetch paginated data from Xero API
    
    endpoint: API endpoint path (e.g., "Accounts", "BankTransactions")
    data_key: JSON key containing the data array (e.g., "Accounts", "BankTransactions")
    tenant_id: Xero tenant ID
    params: Optional query parameters
    """
    if not tenant_id:
        raise HTTPException(400, "Not connected. tenant_id is required")
    
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


def fetch_payroll_data_paginated(endpoint: str, data_key: str, tenant_id: str, params: dict = None, api_version: str = "1.0"):
    """
    Helper function to fetch paginated data from Xero Payroll API (AU)
    
    endpoint: Payroll API endpoint path (e.g., "Employees", "PayRuns", "Payslips")
    data_key: JSON key containing the data array (e.g., "Employees", "PayRuns", "Payslips")
    tenant_id: Xero tenant ID
    params: Optional query parameters
    api_version: API version to use (1.0 or 2.0) - defaults to 1.0
    """
    if not tenant_id:
        raise HTTPException(400, "Not connected. tenant_id is required")
    
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

def fetch_all_data(tenant_id: str):
    """
    Fetch all available data from Xero in one call
    Useful for initial sync or comprehensive data pull
    """
    if not tenant_id:
        raise HTTPException(400, "Not connected. tenant_id is required")
    
    data = {}
    errors = {}
    
    # Settings
    try:
        data["accounts"] = get_accounts(tenant_id)
    except Exception as e:
        errors["accounts"] = str(e)
    
    try:
        data["tax_rates"] = get_tax_rates(tenant_id)
    except Exception as e:
        errors["tax_rates"] = str(e)
    
    try:
        data["tracking_categories"] = get_tracking_categories(tenant_id)
    except Exception as e:
        errors["tracking_categories"] = str(e)
    
    # Transactions
    try:
        data["bank_transactions"] = get_bank_transactions(tenant_id)
    except Exception as e:
        errors["bank_transactions"] = str(e)
    
    try:
        data["payments"] = get_payments(tenant_id)
    except Exception as e:
        errors["payments"] = str(e)
    
    try:
        data["credit_notes"] = get_credit_notes(tenant_id)
    except Exception as e:
        errors["credit_notes"] = str(e)
    
    try:
        data["overpayments"] = get_overpayments(tenant_id)
    except Exception as e:
        errors["overpayments"] = str(e)
    
    try:
        data["prepayments"] = get_prepayments(tenant_id)
    except Exception as e:
        errors["prepayments"] = str(e)
    
    try:
        data["manual_journals"] = get_manual_journals(tenant_id)
    except Exception as e:
        errors["manual_journals"] = str(e)
    
    try:
        data["invoices"] = get_invoices(tenant_id)
    except Exception as e:
        errors["invoices"] = str(e)
    
    try:
        data["bank_transfers"] = get_bank_transfers(tenant_id)
    except Exception as e:
        errors["bank_transfers"] = str(e)
    
    # Payroll
    try:
        data["employees"] = get_employees(tenant_id)
    except Exception as e:
        errors["employees"] = str(e)
    
    try:
        data["payruns"] = get_payruns(tenant_id)
    except Exception as e:
        errors["payruns"] = str(e)
    
    try:
        payruns = data.get("payruns", [])
        if payruns and len(payruns) > 0:
            first_payrun_id = payruns[0].get("PayRunID")
            data["payslips"] = get_payslips(tenant_id, payrun_id=first_payrun_id)
        else:
            data["payslips"] = []
    except Exception as e:
        errors["payslips"] = str(e)
    
    # Reports
    try:
        data["profit_loss"] = get_profit_loss(tenant_id)
    except Exception as e:
        errors["profit_loss"] = str(e)
    
    try:
        data["balance_sheet"] = get_balance_sheet(tenant_id)
    except Exception as e:
        errors["balance_sheet"] = str(e)
    
    result = {"data": data}
    if errors:
        result["errors"] = errors
    
    return result


# ============================================================================
# ACCOUNTING SETTINGS ENDPOINTS (accounting.settings.read)
# ============================================================================

def get_accounts(tenant_id: str):
    """Fetch chart of accounts (with pagination support)"""
    return fetch_xero_data_paginated("Accounts", "Accounts", tenant_id)


def get_tax_rates(tenant_id: str):
    """Fetch tax rates (GST logic)"""
    return fetch_xero_data_paginated("TaxRates", "TaxRates", tenant_id)


def get_tracking_categories(tenant_id: str):
    """Fetch tracking categories (cost centres, jobs, departments)"""
    return fetch_xero_data_paginated("TrackingCategories", "TrackingCategories", tenant_id)


def get_organisation(tenant_id: str):
    """Fetch organisation details (name, currency, country, tax settings)"""
    orgs = fetch_xero_data_paginated("Organisation", "Organisations", tenant_id)
    return orgs[0] if orgs else {}


# ============================================================================
# ACCOUNTING TRANSACTIONS ENDPOINTS (accounting.transactions.read)
# ============================================================================

def get_bank_transactions(tenant_id: str):
    """Fetch bank transactions (with pagination support)"""
    return fetch_xero_data_paginated("BankTransactions", "BankTransactions", tenant_id)


def get_payments(tenant_id: str):
    """Fetch payments (links to invoices/credit notes)"""
    return fetch_xero_data_paginated("Payments", "Payments", tenant_id)


def get_credit_notes(tenant_id: str):
    """Fetch credit notes (refunds, returns)"""
    return fetch_xero_data_paginated("CreditNotes", "CreditNotes", tenant_id)


def get_overpayments(tenant_id: str):
    """Fetch overpayments (excess payments requiring allocation)"""
    return fetch_xero_data_paginated("Overpayments", "Overpayments", tenant_id)


def get_prepayments(tenant_id: str):
    """Fetch prepayments (advance payments)"""
    return fetch_xero_data_paginated("Prepayments", "Prepayments", tenant_id)


def get_manual_journals(tenant_id: str):
    """Fetch manual journals (adjustments not in bank transactions)"""
    return fetch_xero_data_paginated("ManualJournals", "ManualJournals", tenant_id)


def get_invoices(tenant_id: str):
    """Fetch invoices (for income verification)"""
    return fetch_xero_data_paginated("Invoices", "Invoices", tenant_id)


def get_bank_transfers(tenant_id: str):
    """Fetch bank transfers (fund movements between accounts)"""
    return fetch_xero_data_paginated("BankTransfers", "BankTransfers", tenant_id)


# ============================================================================
# CONTACTS ENDPOINT (accounting.contacts.read)
# ============================================================================

def get_contacts(tenant_id: str):
    """Fetch contacts (customers, suppliers with ABN/tax details) - with pagination"""
    return fetch_xero_data_paginated("Contacts", "Contacts", tenant_id)


# ============================================================================
# PAYROLL ENDPOINTS (Australian Payroll API v1.0)
# ============================================================================

def get_employees(tenant_id: str):
    """
    Fetch employees (for income verification)
    Returns: EmployeeID, names, DOB, addresses, employment status, bank accounts
    
    Uses Payroll API v1.0 (compatible with most Xero accounts)
    """
    return fetch_payroll_data_paginated("Employees", "Employees", tenant_id, api_version="1.0")


def get_payruns(tenant_id: str):
    """
    Fetch pay runs (pay periods & totals)
    Returns: PayRunID, period start/end, calendar, totals, status
    
    Uses Payroll API v1.0 (compatible with most Xero accounts)
    """
    return fetch_payroll_data_paginated("PayRuns", "PayRuns", tenant_id, api_version="1.0")


def get_payslips(tenant_id: str, payrun_id: str = None):
    """
    Fetch payslips (income details per employee per pay run)
    
    Query params:
    - tenant_id: Required - Xero tenant ID
    - payrun_id: Required for v1.0 - specific pay run ID
    
    Returns: Gross, net, tax withheld, super, YTD figures, earnings lines
    
    Uses Payroll API v1.0
    Note: v1.0 requires PayRunID to fetch payslips
    """
    if not payrun_id:
        # If no payrun_id provided, get all payruns and fetch payslips for the most recent one
        try:
            payruns = get_payruns(tenant_id)
            if payruns and len(payruns) > 0:
                # Get the most recent payrun
                payrun_id = payruns[0].get("PayRunID")
        except:
            pass
    
    if not payrun_id:
        raise HTTPException(400, "payrun_id is required for v1.0 API. Get PayRunID from /payroll/payruns first")
    
    if not tenant_id:
        raise HTTPException(400, "Not connected. tenant_id is required")
    
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
# REPORTS ENDPOINTS (accounting.reports.read) - requires additional scope
# ============================================================================

def get_profit_loss(tenant_id: str, from_date: str = None, to_date: str = None):
    """
    Fetch Profit & Loss statement
    
    Query params:
    - tenant_id: Required - Xero tenant ID
    - from_date: Start date (YYYY-MM-DD)
    - to_date: End date (YYYY-MM-DD)
    """
    if not tenant_id:
        raise HTTPException(400, "Not connected. tenant_id is required")
    
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


def get_balance_sheet(tenant_id: str, date: str = None):
    """
    Fetch Balance Sheet
    
    Query params:
    - tenant_id: Required - Xero tenant ID
    - date: Report date (YYYY-MM-DD)
    """
    if not tenant_id:
        raise HTTPException(400, "Not connected. tenant_id is required")
    
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


# ============================================================================
# ATTACHMENTS ENDPOINTS (accounting.attachments.read)
# ============================================================================

def get_attachments(tenant_id: str, endpoint: str, guid: str):
    """
    Fetch attachments for a specific entity
    
    tenant_id: Xero tenant ID
    endpoint: BankTransactions, Invoices, PurchaseOrders, etc.
    guid: The GUID of the specific record
    
    Example: /attachments/BankTransactions/abc-123-def
    """
    if not tenant_id:
        raise HTTPException(400, "Not connected. tenant_id is required")
    
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