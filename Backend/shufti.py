'''
import base64, requests, json, hashlib
from random import randint
import os

from dotenv import load_dotenv
load_dotenv()
REDIRECT_URI = os.environ.get("SHUFTI_REDIRECT_URI", "https://api.vericare.com.au/profile/notifyCallback")

def shufti_url(user_email: str, user_id: int):
    """
    Create verification request with user context
    """
    url = 'https://api.shuftipro.com/'
    client_id = os.environ.get("SHUFTI_CLIENTID")
    secret_key = os.environ.get("SHUFTI_SECRET_KEY")

    # Generate reference with user_id for easier tracking
    reference = f'ref-{user_id}-{randint(10000, 99999)}'

    verification_request = {
        "reference": reference,
        "callback_url": REDIRECT_URI,
        "email": user_email,  # Use actual user email
        "country": "AU",
        "language": "EN",
        "redirect_url": "https://dukbillapp.com/dashboard",
        "verification_mode": "image_only",
        "allow_offline": "1",
        "allow_online": "1",
        "show_privacy_policy": "1",
        "show_results": "1",
        "show_consent": "1",
        "show_feedback_form": "0"
    }

    verification_request['document'] = {
        'name': "",
        'dob': "",
        'gender': "",
        'place_of_issue': "",
        'document_number': "",
        'expiry_date': "",
        'issue_date': "",
        'fetch_enhanced_data': "1",
        'supported_types': ['id_card', 'passport', 'driving_license']
    }

    # ... rest of your existing code
    auth = '{}:{}'.format(client_id, secret_key)
    b64Val = base64.b64encode(auth.encode()).decode()
    response = requests.post(
        url,
        headers={"Authorization": "Basic %s" % b64Val, "Content-Type": "application/json"},
        data=json.dumps(verification_request)
    )

    secret_key_new = hashlib.sha256(secret_key.encode()).hexdigest()
    calculated_signature = hashlib.sha256(
        f"{response.content.decode()}{secret_key_new}".encode()
    ).hexdigest()
    sp_signature = response.headers.get('Signature', "")
    json_response = json.loads((response.content))

    if sp_signature == calculated_signature:
        return json_response
    else:
        print(f'Invalid signature: {response.content}')
        return None

def get_verification_status_with_proofs(reference: str):
    """Call Status API to get proof URLs and access token"""
    url = 'https://api.shuftipro.com/status'
    client_id = os.environ.get("SHUFTI_CLIENTID")
    secret_key = os.environ.get("SHUFTI_SECRET_KEY")

    payload = {
        "reference": reference
    }

    auth = f'{client_id}:{secret_key}'
    b64Val = base64.b64encode(auth.encode()).decode()

    response = requests.post(
        url,
        headers={
            "Authorization": f"Basic {b64Val}",
            "Content-Type": "application/json"
        },
        data=json.dumps(payload)
    )

    # Verify signature
    secret_key_hash = hashlib.sha256(secret_key.encode()).hexdigest()
    calculated_signature = hashlib.sha256(
        f"{response.content.decode()}{secret_key_hash}".encode()
    ).hexdigest()
    sp_signature = response.headers.get('Signature', "")

    if sp_signature == calculated_signature:
        return response.json()
    else:
        print(f"Invalid signature in status response")
        return None


def download_proof_image(proof_url: str, access_token: str):
    """Download proof image using access token"""
    payload = {
        "access_token": access_token
    }

    response = requests.post(
        proof_url,
        json=payload,
        headers={"Content-Type": "application/json"}
    )

    if response.status_code == 200:
        return response.content
    else:
        print(f"Failed to download proof: {response.status_code}")
        return None


'''
