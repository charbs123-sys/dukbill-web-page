"""
MYOB Token Refresh
"""

import requests
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def load_tokens_from_file(filename='access_tokens.txt'):
    """Load tokens from file"""
    try:
        with open(filename, 'r') as f:
            content = f.read()
            # Use eval to parse the dictionary string
            tokens = eval(content)
            return tokens
    except Exception as e:
        print(f"Error loading tokens: {e}")
        return None


def save_tokens_to_file(tokens, filename='access_tokens.txt'):
    """Save tokens to file"""
    try:
        with open(filename, 'w') as f:
            f.write(str(tokens))
        print(f"\n✓ Tokens saved to {filename}")
    except Exception as e:
        print(f"Error saving tokens: {e}")


def refresh_access_token(client_id, client_secret, refresh_token):
    """
    Refresh an expired access token using the refresh token
    
    Args:
        client_id: Your MYOB API Key
        client_secret: Your MYOB API Secret
        refresh_token: Your refresh token from the original authentication
    
    Returns:
        dict: New tokens including access_token and refresh_token
    """
    token_url = "https://secure.myob.com/oauth2/v1/authorize"
    
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token'
    }
    
    response = requests.post(token_url, data=data)
    print(response.json())
    if response.status_code == 200:
        tokens = response.json()
        print("\n✓ Successfully refreshed access token!")
        print(f"New Access Token: {tokens['access_token'][:30]}...")
        print(f"Expires in: {tokens['expires_in']} seconds")
        print(f"New Refresh Token: {tokens['refresh_token'][:30]}...")
        print(f"\nTimestamp: {datetime.now()}")
        return tokens
    else:
        print("\n✗ Error refreshing token:")
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")
        return None


if __name__ == "__main__":
    # Load credentials from .env
    client_id = os.getenv('MYOB_API_KEY')
    client_secret = os.getenv('MYOB_SECRET')
    
    if not client_id or not client_secret:
        print("Error: MYOB_API_KEY and MYOB_SECRET must be set in .env file")
        exit(1)
    
    # Load existing tokens from file
    print("Loading tokens from access_tokens.txt...")
    old_tokens = load_tokens_from_file('access_tokens.txt')
    
    if not old_tokens:
        print("Error: Could not load tokens from access_tokens.txt")
        exit(1)
    
    refresh_token = old_tokens.get('refresh_token')
    
    if not refresh_token:
        print("Error: No refresh_token found in access_tokens.txt")
        exit(1)
    
    print(f"Old access token: {old_tokens['access_token'][:30]}...")
    
    # Refresh the token
    new_tokens = refresh_access_token(client_id, client_secret, refresh_token)
    
    if new_tokens:
        print("\n" + "="*60)
        print("Token refresh successful!")
        print("="*60)
        
        # Save new tokens back to file
        save_tokens_to_file(new_tokens, 'access_tokens.txt')
        
        print("\nNew tokens saved. You can now use the updated access_token for API calls.")