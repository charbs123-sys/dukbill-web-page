import urllib
import os
import requests
from External_APIs.myob_pdf_generation import (
    generate_payroll_pdf,
    generate_sales_pdf,
    generate_banking_pdf,
    generate_purchases_pdf
)
from Database.S3_utils import upload_myob_pdf_to_s3

API_KEY = os.environ.get("MYOB_API_KEY")
API_SECRET = os.environ.get("MYOB_SECRET")
MYOB_REDIRECT_URI = os.environ.get("MYOB_REDIRECT_URL", "http://localhost:8080/myob/callback")
SCOPE = "sme-banking sme-purchases sme-sales sme-payroll sme-company-file sme-contacts-customer sme-contacts-supplier"

def build_auth_url(state: str) -> str:
    '''
    Generate the url to redirect user to MYOB OAuth consent screen
    '''
    params = {
        'client_id': API_KEY,
        'redirect_uri': MYOB_REDIRECT_URI,
        'response_type': 'code',
        'scope': SCOPE,
        'prompt': 'consent',
        'state': state
    }
    base_url = "https://secure.myob.com/oauth2/account/authorize"
    return f"{base_url}?{urllib.parse.urlencode(params)}"

def get_access_token_myob(code: str) -> dict:
    """Exchange authorization code for access token"""
    
    token_url = "https://secure.myob.com/oauth2/v1/authorize"
    
    data = {
        'client_id': API_KEY,
        'client_secret': API_SECRET,
        'code': code,
        'redirect_uri': MYOB_REDIRECT_URI,
        'grant_type': 'authorization_code'
    }
    
    response = requests.post(token_url, data=data)
    
    if response.status_code == 200:
        tokens = response.json()
        print("\n✓ Successfully received access token!")
        print(f"Access Token: {tokens['access_token'][:30]}...")
        print(f"Expires in: {tokens['expires_in']} seconds")
        print(f"Refresh Token: {tokens['refresh_token'][:30]}...")
        return tokens
    else:
        print(f"\n✗ Error getting access token:")
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")
        return None
    

def make_api_call(access_token, api_key, business_id, endpoint):
    """
    Make a generic API call to MYOB
    
    Args:
        access_token: Your access token from OAuth
        api_key: Your MYOB API Key
        business_id: Your business/company file ID
        endpoint: The endpoint path (e.g., 'Customer', 'Invoice', 'Supplier')
    """
    # Clean the business_id
    business_id = business_id.split('&')[0] if '&' in business_id else business_id
    
    url = f"https://api.myob.com/accountright/{business_id}/{endpoint}"
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'x-myobapi-key': api_key,
        'x-myobapi-version': 'v2',
        'Accept': 'application/json'
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None


def retrieve_endpoints_myob(access_token, business_id):
    endpoints = [
        # --- Sales / Income Verification ---
        "Sale/Invoice",
        "Sale/Invoice/Service",
        "Sale/Invoice/Professional",
        "Sale/CustomerPayment",
        "Sale/CreditRefund",

        # --- Banking / Statements & Assets ---
        "Banking/BankAccount",
        "Banking/Statement",
        "Banking/ReceiveMoneyTxn",
        "Banking/SpendMoneyTxn",

        # --- Purchases / Liabilities & Expenses ---
        "Purchase/Bill",
        "Purchase/Bill/Item",
        "Purchase/Bill/Service",
        "Purchase/Bill/Professional",
        "Purchase/Bill/Item/{Bill_UID}/Attachment",
        "Purchase/Bill/Service/{Bill_UID}/Attachment",
        "Purchase/Bill/Professional/{Bill_UID}/Attachment",
        "Purchase/SupplierPayment/RecordWithDiscountsAndFees",

        # --- Payroll / Income, Employment, Superannuation ---
        "Payroll/Timesheet",
        "Report/Payroll/EmployeePayrollAdvice",
        "Report/PayrollCategorySummary",
        "Payroll/PayrollCategory/Wage",
        "Payroll/PayrollCategory/Superannuation",
        "Payroll/PayrollCategory/Tax",
        "Payroll/EmploymentClassification",
        "Payroll/SuperannuationFund",
        "Payroll/PayrollCategory/Entitlement",
        "Payroll/PayrollCategory/Deduction",
        "Payroll/PayrollCategory/Expense"
    ]


    all_results = []
    for endpoint in endpoints:
        print(f"Fetching data from endpoint: {endpoint}")
        data = make_api_call(access_token, API_KEY, business_id, endpoint)
        if data:
            all_results.append({
                "endpoint": endpoint,  # ✅ Add this
                "data": data
            })
        else:
            print(f"✗ No data returned for {endpoint}")
    return all_results


def process_myob_data(code: str, business_id: str, state: str, hashed_user_email: str):
    '''
    Process MYOB data after OAuth callback

    code (str): The authorization code from MYOB OAuth.
    business_id (str): The business/company file ID.
    state (str): The state parameter to verify the request.
    hashed_user_email (str): The hashed email identifier for S3 storage.

    Returns:
        None
    '''
    try:
        tokens = get_access_token_myob(code)
        
        if not tokens:
            return
        
        access_token = tokens.get("access_token")
        # refresh_token = tokens.get("refresh_token")
        
        myob_data = retrieve_endpoints_myob(access_token, business_id)
        
        payroll_pdf = generate_payroll_pdf(myob_data)
        sales_pdf = generate_sales_pdf(myob_data)
        banking_pdf = generate_banking_pdf(myob_data)
        purchases_pdf = generate_purchases_pdf(myob_data)
        
        # Upload to S3 -> this should probably be a general function
        upload_myob_pdf_to_s3(payroll_pdf, hashed_user_email, "Broker_Payroll_Summary.pdf")
        upload_myob_pdf_to_s3(sales_pdf, hashed_user_email, "Broker_Sales_Summary.pdf")
        upload_myob_pdf_to_s3(banking_pdf, hashed_user_email, "Broker_Banking_Summary.pdf")
        upload_myob_pdf_to_s3(purchases_pdf, hashed_user_email, "Broker_Purchases_Summary.pdf")
        
    except Exception as e:
        print(f"✗ Error processing MYOB data: {e}")