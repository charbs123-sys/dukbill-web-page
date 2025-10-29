import base64, requests, json, hashlib
from random import randint
import os
from dotenv import load_dotenv
load_dotenv()

def shufti_url():
    url = 'https://api.shuftipro.com/'
    client_id = os.environ.get("SHUFTI_CLIENTID")
    secret_key = os.environ.get("SHUFTI_SECRET_KEY")
    
    reference = f'ref-{randint(10000, 99999)}-{randint(10000, 99999)}'
    
    verification_request = {
        "reference": reference,
        "callback_url": "https://api.vericare.com.au/profile/notifyCallback",
        "email": "johndoe@example.com", 
        "country": "AU",
        "language": "EN",
        #"redirect_url": "https://yourdomain.com/verification-complete",
        "verification_mode": "image_only",
        "allow_offline": "1",
        "allow_online": "1",
        "show_privacy_policy": "1",
        "show_results": "1",
        "show_consent": "1",
        "show_feedback_form": "0",
        "return_document_images": "1"
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