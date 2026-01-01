"""
Simple MYOB OAuth2.0 Authentication
"""

import urllib.parse
import webbrowser
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Step 1: Configure your credentials from .env file
API_KEY = os.getenv("MYOB_API_KEY")
API_SECRET = os.getenv("MYOB_SECRET")
REDIRECT_URI = "http://localhost:8080/callback"  # Must match registration


SCOPE = (
    "sme-banking"
    "sme-purchases"
    "sme-sales"
    "sme-payroll"
    "sme-company-file"
    "sme-contacts-customer"
    "sme-contacts-supplier"
)

def build_auth_url(api_key, redirect_uri, scope):
    """Step 2: Build the authorization URL"""
    params = {
        'client_id': api_key,
        'redirect_uri': redirect_uri,
        'response_type': 'code',
        'scope': scope,
        'prompt': 'consent'  # REQUIRED for new API keys (March 2025+)
    }
    base_url = "https://secure.myob.com/oauth2/account/authorize"
    return f"{base_url}?{urllib.parse.urlencode(params)}"


def get_access_token(code):
    """Step 4: Exchange authorization code for access token"""
    import requests
    
    token_url = "https://secure.myob.com/oauth2/v1/authorize"
    
    data = {
        'client_id': API_KEY,
        'client_secret': API_SECRET,
        'code': code,
        'redirect_uri': REDIRECT_URI,
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
        print("\n✗ Error getting access token:")
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")
        return None


def start_auth():
    """Step 3: Open browser for user to authenticate"""
    if not API_KEY or not API_SECRET:
        print("Error: MYOB_API_KEY and MYOB_SECRET must be set in .env file")
        return
    
    auth_url = build_auth_url(API_KEY, REDIRECT_URI, SCOPE)
    print("Opening browser for authentication...")
    print(f"Authorization URL: {auth_url}")
    webbrowser.open(auth_url)
    
    print(f"\nUser will be redirected to: {REDIRECT_URI}")
    print("IMPORTANT: Copy BOTH 'code' AND 'businessId' from the redirect URL!")


if __name__ == "__main__":
    # Step 3: Start authentication
    start_auth()
    
    # Step 4: After getting the code, exchange it for an access token
    print("\n" + "="*60)
    code = input("Enter the authorization code from the URL: ").strip()
    
    if code:
        code = urllib.parse.unquote(code)
        tokens = get_access_token(code)
        if tokens:
            print("\n" + "="*60)
            print("Save these tokens securely!")
            print("="*60)
            print(tokens)