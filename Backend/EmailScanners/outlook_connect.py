import os
import urllib
from configparser import SectionProxy

import requests
from azure.identity import DeviceCodeCredential
from msgraph import GraphServiceClient

OUTLOOK_CLIENT_ID = os.getenv("OUTLOOK_CLIENT_ID")
OUTLOOK_CLIENT_SECRET = os.getenv("OUTLOOK_CLIENT_SECRET")
# Must match exactly what is in Azure Portal
OUTLOOK_REDIRECT_URI = "http://localhost:8000/outlook/callback"
# Use 'common' for multi-tenant, or your specific Tenant ID
OUTLOOK_AUTHORITY = "https://login.microsoftonline.com/common"


class Graph:
    settings: SectionProxy
    device_code_credential: DeviceCodeCredential
    user_client: GraphServiceClient

    def __init__(self, config: SectionProxy):
        self.settings = config
        client_id = self.settings["clientId"]
        tenant_id = self.settings["tenantId"]
        graph_scopes = self.settings["graphUserScopes"].split()

        self.device_code_credential = DeviceCodeCredential(
            client_id=client_id, tenant_id=tenant_id
        )
        self.user_client = GraphServiceClient(self.device_code_credential, graph_scopes)

    async def get_user_token(self):
        scopes = self.settings["graphUserScopes"].split()
        token = self.device_code_credential.get_token(*scopes)
        return token.token


def get_outlook_auth_url(state: str):
    """Generates the Microsoft login URL."""
    base_url = f"{OUTLOOK_AUTHORITY}/oauth2/v2.0/authorize"
    params = {
        "client_id": OUTLOOK_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": OUTLOOK_REDIRECT_URI,
        "response_mode": "query",
        # 'offline_access' is required to get a refresh_token
        "scope": "User.Read Mail.Read offline_access",
        "state": state,
    }
    return f"{base_url}?{urllib.parse.urlencode(params)}"


def exchange_outlook_code_for_tokens(code: str):
    """Exchanges the auth code for access and refresh tokens."""
    token_url = f"{OUTLOOK_AUTHORITY}/oauth2/v2.0/token"
    data = {
        "client_id": OUTLOOK_CLIENT_ID,
        "scope": "User.Read Mail.Read offline_access",
        "code": code,
        "redirect_uri": OUTLOOK_REDIRECT_URI,
        "grant_type": "authorization_code",
        "client_secret": OUTLOOK_CLIENT_SECRET,
    }

    response = requests.post(token_url, data=data)
    response.raise_for_status()  # Raise error if exchange failed
    return response.json()


LAMBDA_ENDPOINT = (
    "https://gtqktd45m5rgjbpxsflqt2nj2y0ducar.lambda-url.ap-southeast-2.on.aws/"
)


# outlook scan
def run_outlook_scan(client_id, email, access_token, refresh_token):
    """
    Sends the user credentials to an external Lambda/Function for processing.
    """
    print(f"Triggering remote Outlook scan for: {email}")

    payload = {
        "client_id": client_id,
        "email": email,
        "access_token": access_token,
        "refresh_token": refresh_token,
    }

    try:
        # Send the data to your Lambda function
        response = requests.post(
            LAMBDA_ENDPOINT,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,  # Good practice to set a timeout
        )

        if response.status_code == 200:
            print(f"Successfully triggered scan for {email}. Response: {response.text}")
        else:
            print(
                f"Failed to trigger scan. Status: {response.status_code}, Body: {response.text}"
            )

    except requests.exceptions.RequestException as e:
        print(f"Network error triggering scan for {email}: {e}")
