"""
MYOB API - Making API Calls with Access Tokens
"""

import requests
import json
import os
from dotenv import load_dotenv
# Load environment variables
load_dotenv()


def load_tokens_from_file(filename='access_tokens.txt'):
    """Load tokens from file"""
    try:
        with open(filename, 'r') as f:
            content = f.read()
            tokens = eval(content)
            return tokens
    except Exception as e:
        print(f"Error loading tokens: {e}")
        return None


def get_customers(access_token, api_key, business_id):
    """Get list of customers"""
    # Clean the business_id - remove any query parameters
    business_id = business_id.split('&')[0] if '&' in business_id else business_id

    url = f"https://api.myob.com/accountright/{business_id}/Sale/Invoice"

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


# Example usage:
if __name__ == "__main__":
    # Load API key from .env
    api_key = os.getenv('MYOB_API_KEY')
    
    # Load business_id from .env (add MYOB_BUSINESS_ID to your .env file)
    business_id = os.getenv('MYOB_BUSINESS_ID')

    if not api_key:
        print("Error: MYOB_API_KEY must be set in .env file")
        exit(1)
    
    if not business_id:
        print("Error: MYOB_BUSINESS_ID must be set in .env file")
        print("Add this line to your .env: MYOB_BUSINESS_ID=c086f5e8-c459-4a49-88b2-bf7a6c82213e")
        exit(1)
    
    # Load tokens from file
    print("Loading tokens from access_tokens.txt...")
    tokens = load_tokens_from_file('access_tokens.txt')
    
    if not tokens:
        print("Error: Could not load tokens from access_tokens.txt")
        exit(1)
    
    access_token = tokens.get('access_token')

    if not access_token:
        print("Error: No access_token found in access_tokens.txt")
        exit(1)
    
    # Get customers
    #print("Fetching customers...")
    #customers = get_customers(access_token, api_key, business_id)
    
    #if customers:
    #    print(json.dumps(customers, indent=2))
    # --- Relevant MYOB endpoints for Dukbill ---
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


    # --- Directory to save the results ---
    output_dir = "myob_outputs"
    os.makedirs(output_dir, exist_ok=True)

    # --- Loop through each endpoint and save result ---
    for endpoint in endpoints:
        print(f"Fetching data from endpoint: {endpoint}")
        data = make_api_call(access_token, api_key, business_id, endpoint)
        
        if data:
            filename = os.path.join(output_dir, f"{endpoint.replace('/', '_')}.json")
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            print(f"✓ Saved output to {filename}")
        else:
            print(f"✗ No data returned for {endpoint}")