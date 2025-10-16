import requests
import datetime
import time
from config import BASIQ_API_KEY, BASIQ_BASE_URL


class BasiqAPI:
    def __init__(self):
        self.dukbill_token = None
        self.dukbill_token_expiry = None

    def get_dukbill_token(self):
        if self.dukbill_token and self.dukbill_token_expiry and self.dukbill_token_expiry > time.time():
            return self.dukbill_token

        url = f"{BASIQ_BASE_URL}/token"
        headers = {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {BASIQ_API_KEY}",
            "basiq-version": "3.0"
        }

        data = {"scope": "SERVER_ACCESS"}

        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        resp_json = response.json()
        self.dukbill_token = resp_json["access_token"]
        self.dukbill_token_expiry = time.time() + resp_json["expires_in"] - 300
        return self.dukbill_token

    def create_user(self, email: str, mobile: str):
        token = self.get_dukbill_token()
        url = f"{BASIQ_BASE_URL}/users"
        
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        
        user_body = {"email": email, "mobile": mobile}
        response = requests.post(url, headers=headers, json=user_body)
        response.raise_for_status()
        # Need to Store ID
        return response.json()

    def get_client_access_token(self, user_id: str):

        url = f"{BASIQ_BASE_URL}/token"
        headers = {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {BASIQ_API_KEY}",
            "basiq-version": "3.0"
        }

        data = {
            "scope": "CLIENT_ACCESS",
            "userId": user_id
            }
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        return response.json()["access_token"]

    def get_user_connections(self, user_id: str):
        token = self.get_dukbill_token()
        url = f"{BASIQ_BASE_URL}/users/{user_id}/connections"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "accept": "application/json"
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def get_user_transactions(self, user_id: str, years: int = 2, active_connections: list = None):
        token = self.get_dukbill_token()
        to_date = datetime.date.today()
        from_date = to_date - datetime.timedelta(days=years * 365)

        if not active_connections:
            return [], []  # return empty lists if no connections

        bank_transactions = []
        testing_transactions = []  # new array for all raw data

        for conn in active_connections:
            provider = conn.get("provider") or {}
            bank_name = provider.get("name", "Unknown Bank")
            conn_id = conn.get("id")
            if not conn_id:
                continue

            url = f"{BASIQ_BASE_URL}/users/{user_id}/transactions?from={from_date}&to={to_date}&limit=500&connectionIds[]={conn_id}"
            transactions = []

            while url:
                r = requests.get(url, headers={"Authorization": f"Bearer {token}", "accept": "application/json"})
                r.raise_for_status()
                data = r.json()

                testing_transactions.extend(data.get("data", []))

                for tx in data.get("data", []):
                    transactions.append({
                        "date": tx.get("postDate"),
                        "description": tx.get("description"),
                        "amount": tx.get("amount")
                    })

                url = data.get("links", {}).get("next")

            bank_transactions.append({
                "bank": bank_name,
                "transactions": transactions
            })

        return bank_transactions



