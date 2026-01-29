# helpers/xero_helpers.py
import base64
import os
import time
from datetime import datetime, timedelta
import requests
from fastapi import HTTPException
from External_APIs.xero_pdf_generation import (
    generate_accounts_report,
    generate_bank_transfers_report,
    generate_credit_notes_report,
    generate_invoices_report,
    generate_payments_report,
    generate_payroll_report,
    generate_reports_summary,
    generate_transactions_report,
)

# Configuration
XERO_CLIENT_ID = os.environ.get("XERO_CLIENT_ID")
XERO_CLIENT_SECRET = os.environ.get("XERO_CLIENT_SECRET")
XERO_REDIRECT_URI = os.environ.get("XERO_REDIRECT_URI")

# Scopes
XERO_SCOPES = (
    "offline_access "
    "accounting.settings.read "
    "accounting.transactions.read "
    "accounting.contacts.read "
    "accounting.attachments.read "
    "accounting.reports.read "
    "payroll.employees.read "
    "payroll.payruns.read "
    "payroll.payslip.read"
)

# Discovery
try:
    DISCOVERY_URL = "https://identity.xero.com/.well-known/openid-configuration"
    disc = requests.get(DISCOVERY_URL, timeout=10).json()
    AUTH_URL = disc["authorization_endpoint"]
    TOKEN_URL = disc["token_endpoint"]
except Exception:
    AUTH_URL = "https://login.xero.com/identity/connect/authorize"
    TOKEN_URL = "https://identity.xero.com/connect/token"

tokens = {}


def get_basic_auth():
    credentials = f"{XERO_CLIENT_ID}:{XERO_CLIENT_SECRET}"
    return base64.b64encode(credentials.encode()).decode()


def get_valid_access_token():
    if not tokens or "access_token" not in tokens:
        raise HTTPException(401, "Not connected to Xero")

    if time.time() < tokens.get("expires_at", 0) - 30:
        return tokens["access_token"]

    try:
        resp = requests.post(
            TOKEN_URL,
            headers={
                "Authorization": f"Basic {get_basic_auth()}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "refresh_token",
                "refresh_token": tokens["refresh_token"],
            },
            timeout=10,
        )
        resp.raise_for_status()
        
        new_tokens = resp.json()
        tokens["access_token"] = new_tokens["access_token"]
        if "refresh_token" in new_tokens:
            tokens["refresh_token"] = new_tokens["refresh_token"]
        tokens["expires_at"] = int(time.time()) + int(
            new_tokens.get("expires_in", 1800)
        )
        
        return tokens["access_token"]
    except Exception as e:
        raise HTTPException(401, "Failed to refresh Xero token")


def get_date_filter_iso():
    one_year_ago = datetime.now() - timedelta(days=365)
    return one_year_ago.strftime("%Y-%m-%dT00:00:00")


def get_date_filter_simple():
    one_year_ago = datetime.now() - timedelta(days=365)
    return one_year_ago.strftime("%Y-%m-%d")


def safe_json_response(response, endpoint_name):
    """
    Helper to safely parse JSON and print raw text if it fails.
    """
    try:
        return response.json()
    except Exception:
        return None


def fetch_xero_data_paginated(
    endpoint: str,
    data_key: str,
    tenant_id: str,
    access_token: str,
    params: dict = None
):
    if not params:
        params = {}

    all_data = []
    page = 1
    base_url = "https://api.xero.com/api.xro/2.0"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "xero-tenant-id": tenant_id,
        "Accept": "application/json",
    }

    while True:
        params["page"] = page
        try:
            response = requests.get(
                f"{base_url}/{endpoint}",
                headers=headers,
                params=params,
                timeout=30,
            )
            
            if response.status_code != 200:
                break

            data = safe_json_response(response, endpoint)
            if not data:
                break
            
            if page == 1:
                records = data.get(data_key, [])
            
            records = data.get(data_key, [])
            if not records:
                break

            all_data.extend(records)
            if len(records) < 100:
                break
            page += 1
            
        except Exception as e:
            break

    return all_data


def fetch_payroll_data(
    endpoint: str,
    data_key: str,
    tenant_id: str,
    access_token: str,
    api_version: str = "1.0"
):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "xero-tenant-id": tenant_id,
        "Accept": "application/json",
    }
    
    url = f"https://api.xero.com/payroll.xro/{api_version}/{endpoint}"
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = safe_json_response(response, f"Payroll-{endpoint}")
            records = data.get(data_key, []) if data else []
            return records
        
        elif response.status_code == 404 and api_version == "1.0":
            url_v2 = f"https://api.xero.com/payroll.xro/2.0/{endpoint}"
            response = requests.get(url_v2, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = safe_json_response(response, f"Payroll-{endpoint}")
                records = data.get(data_key, []) if data else []
                
                return records
        
        if response.status_code in [404, 501, 503]:
             return []
        else:
            if "not been provisioned" in response.text:
                return []
            return []
            
    except Exception as e:
        return []


def fetch_all_data(tenant_id: str, access_token: str = None):
    if not access_token:
        access_token = get_valid_access_token()

    data = {}
    errors = {}
    
    date_str = get_date_filter_iso().split("T")[0].replace("-", ",")
    where_filter = f"Date>=DateTime({date_str})"
    
    base_headers = {
        "Authorization": f"Bearer {access_token}", 
        "xero-tenant-id": tenant_id,
        "Accept": "application/json"
    }

    def run_paginated(key, endpoint, d_key, params=None): 
        try:
            data[key] = fetch_xero_data_paginated(
                endpoint, d_key, tenant_id, access_token, params=params
            )
        except Exception as e:
            errors[key] = str(e)

    # --- SETTINGS ---
    run_paginated("accounts", "Accounts", "Accounts")
    run_paginated("tax_rates", "TaxRates", "TaxRates")
    run_paginated(
        "tracking_categories", "TrackingCategories", "TrackingCategories"
    )

    # --- TRANSACTIONS ---
    run_paginated(
        "bank_transactions",
        "BankTransactions",
        "BankTransactions",
        params={"where": where_filter}
    )
    run_paginated(
        "invoices", "Invoices", "Invoices", params={"where": where_filter}
    )
    run_paginated(
        "payments", "Payments", "Payments", params={"where": where_filter}
    )
    run_paginated(
        "credit_notes",
        "CreditNotes",
        "CreditNotes",
        params={"where": where_filter}
    )
    run_paginated(
        "bank_transfers",
        "BankTransfers",
        "BankTransfers",
        params={"where": where_filter}
    )
        
    # --- COUNTS ---
    for endpoint in ["ManualJournals", "Overpayments", "Prepayments"]:
        try:
            res = requests.get(
                f"https://api.xero.com/api.xro/2.0/{endpoint}",
                headers=base_headers,
                params={"page": 1}
            )
            
            key_lower = endpoint.lower()
            
            if res.status_code == 200:
                json_data = safe_json_response(res, endpoint)
                if json_data:
                    items = json_data.get(endpoint, [])
                    data[f"{key_lower}_count"] = len(items)
                    data[key_lower] = items
                    
                else:
                    data[f"{key_lower}_count"] = 0
                    data[key_lower] = []
            else:
                data[f"{key_lower}_count"] = 0
                data[key_lower] = []
                
        except Exception as e:
            errors[endpoint] = str(e)

    # --- REPORTS ---
    try:
        pl_url = "https://api.xero.com/api.xro/2.0/Reports/ProfitAndLoss"
        res = requests.get(
            pl_url,
            headers=base_headers,
            params={
                "fromDate": get_date_filter_simple(),
                "toDate": datetime.now().strftime("%Y-%m-%d")
            }
        )
        if res.status_code == 200:
            pl_data = safe_json_response(res, "ProfitAndLoss") or {}
            data["profit_loss"] = pl_data
            
        else:
            data["profit_loss"] = {}
    except Exception as e:
        errors["profit_loss"] = str(e)

    try:
        bs_url = "https://api.xero.com/api.xro/2.0/Reports/BalanceSheet"
        res = requests.get(
            bs_url,
            headers=base_headers,
            params={"date": datetime.now().strftime("%Y-%m-%d")}
        )
        if res.status_code == 200:
            bs_data = safe_json_response(res, "BalanceSheet") or {}
            data["balance_sheet"] = bs_data
            
        else:
            data["balance_sheet"] = {}
    except Exception as e:
        errors["balance_sheet"] = str(e)

    # --- PAYROLL ---
    try:
        data["employees"] = fetch_payroll_data(
            "Employees", "Employees", tenant_id, access_token
        )
    except Exception as e:
        errors["employees"] = str(e)

    try:
        data["payruns"] = fetch_payroll_data(
            "PayRuns", "PayRuns", tenant_id, access_token
        )
    except Exception as e:
        errors["payruns"] = str(e)

    # Payslips
    try:
        if data.get("payruns"):
            latest_id = data["payruns"][0].get("PayRunID")
            
            res = requests.get(
                f"https://api.xero.com/payroll.xro/1.0/PayRuns/{latest_id}",
                headers=base_headers
            )
            
            if res.status_code == 200:
                json_data = safe_json_response(res, "Payslips")
                if json_data:
                    payruns_wrappers = json_data.get("PayRuns", [])
                    payslips = (
                        payruns_wrappers[0].get("Payslips", []) 
                        if payruns_wrappers else []
                    )
                    data["payslips"] = payslips
                    
                else:
                    data["payslips"] = []
            else:
                 data["payslips"] = []
        else:
            data["payslips"] = []
    except Exception as e:
        errors["payslips"] = str(e)

    return {"data": data, "errors": errors}


def generate_xero_preview(all_data: dict) -> dict:
    fetched = all_data.get("data", {})
    
    def get_list(key, limit=100):
        items = fetched.get(key, [])
        return items[:limit], len(items)

    def get_count(key):
        return fetched.get(f"{key}_count", 0)

    acc_list, acc_total = get_list("accounts")
    tr_list, tr_total = get_list("tax_rates")
    tc_list, tc_total = get_list("tracking_categories")
    bt_list, bt_total = get_list("bank_transactions")
    inv_list, inv_total = get_list("invoices")
    pay_list, pay_total = get_list("payments")
    cn_list, cn_total = get_list("credit_notes")
    btr_list, btr_total = get_list("bank_transfers")
    emp_list, emp_total = get_list("employees")
    pr_list, pr_total = get_list("payruns")
    ps_list, ps_total = get_list("payslips")

    preview = {
        "organization": "Xero Organization", 
        "settings": {
            "accounts_list": acc_list,
            "accounts_total": acc_total,
            "tax_rates_list": tr_list,
            "tax_rates_total": tr_total,
            "tracking_categories_list": tc_list,
            "tracking_categories_total": tc_total,
        },
        "transactions": {
            "bank_transactions_list": bt_list,
            "bank_transactions_total": bt_total,
            "invoices_list": inv_list,
            "invoices_total": inv_total,
            "payments_list": pay_list,
            "payments_total": pay_total,
            "credit_notes_list": cn_list,
            "credit_notes_total": cn_total,
            "bank_transfers_list": btr_list,
            "bank_transfers_total": btr_total,
            "manual_journals_total": get_count("manualjournals"),
            "overpayments_total": get_count("overpayments"),
            "prepayments_total": get_count("prepayments"),
        },
        "payroll": {
            "employees_list": emp_list,
            "employees_total": emp_total,
            "payruns_list": pr_list,
            "payruns_total": pr_total,
            "payslips_list": ps_list,
            "payslips_total": ps_total,
        },
        "reports": {
            "profit_loss": fetched.get("profit_loss", {}),
            "balance_sheet": fetched.get("balance_sheet", {}),
        },
    }
    return preview


def generate_all_reports_xero(
    result: dict, hashed_email: str, org_name: str = "MyCompany"
) -> list:
    s3_keys = []
    
    safe_org_name = "".join(
        c for c in org_name if c.isalnum() or c in (' ', '_')
    ).replace(" ", "_")
    
    generators = [
        (
            generate_accounts_report,
            f"{safe_org_name}_xero_accounts_report.pdf"
        ),
        (
            generate_transactions_report,
            f"{safe_org_name}_xero_transactions_report.pdf"
        ),
        (
            generate_payments_report,
            f"{safe_org_name}_xero_payments_report.pdf"
        ),
        (
            generate_credit_notes_report,
            f"{safe_org_name}_xero_credit_notes_report.pdf"
        ),
        (
            generate_payroll_report,
            f"{safe_org_name}_xero_payroll_report.pdf"
        ),
        (
            generate_invoices_report,
            f"{safe_org_name}_xero_invoices_report.pdf"
        ),
        (
            generate_bank_transfers_report,
            f"{safe_org_name}_xero_bank_transfers_report.pdf"
        ),
        (
            generate_reports_summary,
            f"{safe_org_name}_xero_financial_reports.pdf"
        ),
    ]

    for func, filename in generators:
        try:
            key = func(result, filename, hashed_email)
            if key:
                s3_keys.append(key)
        except Exception as e:
            pass

    return s3_keys