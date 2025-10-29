
"""
If you run into error: 'ImportError: No module named requests'
Please run
- pip install requests
"""
"""
Python 2
import urllib2

Python 3
import urllib.request
urllib.request.urlopen(url).read()
"""
#import required libraries
import base64, requests, json, hashlib
import urllib.request
from random import randint
import os
from dotenv import load_dotenv
load_dotenv()

#Shufti api base url
url = 'https://api.shuftipro.com/'

# Your Shufti account Client ID
client_id	= os.environ.get("SHUFTI_CLIENTID")

# Your Shufti account Secret Key
secret_key = os.environ.get("SHUFTI_SECRET_KEY")
print(secret_key)
verification_request = {
        #your unique request reference
        "reference":'ref-{}{}'.format(randint(1000, 9999), randint(1000, 9999)),
        #URL where you will receive the webhooks from Shufti
        "callback_url":"https://api.vericare.com.au/profile/notifyCallback",
        #end-user email
        "email":"johndoe@example.com", 
        #end-user country
        "country":"",
        #select ISO2 Code for your desired language on verification screen
        "language":"EN",
        #URL where end-user will be redirected after verification completed
        "redirect_url":"",
        #what kind of proofs will be provided to Shufti for verification?
        "verification_mode":"image_only",
        #allow end-user to upload verification proofs if the webcam is not accessible
        "allow_offline":"1",
        #allow end-user to upload real-time or already captured proofs
        "allow_online":"1",
        #privacy policy screen will be shown to end-user
        "show_privacy_policy":"1",
        #verification results screen will be shown to end-user
        "show_results":"1",
        #consent screen will be shown to end-user
        "show_consent":"1",
        #User cannot send Feedback
        "show_feedback_form":"0"
}
#document onsite verification with OCR
verification_request['document'] =  {
        'name'							:	"",
        'dob'								:	"",
        'gender'						 :	"",
        'place_of_issue'		 :	"",
        'document_number'	:  "",
        'expiry_date'					:	"",
        'issue_date'					:	"",
        'fetch_enhanced_data'			 :	"1",
        'supported_types'		:	['id_card','passport']
}

# calling Shufti request API using python request
auth = '{}:{}'.format(client_id, secret_key)
b64Val = base64.b64encode(auth.encode()).decode()
response = requests.post(url,
                                headers={"Authorization": "Basic %s" % b64Val, "Content-Type": "application/json"},
                                data=json.dumps(verification_request))

# calculating signature for verification
secret_key_new = hashlib.sha256(secret_key.encode()).hexdigest()
calculated_signature = hashlib.sha256(f"{response.content.decode()}{secret_key_new}".encode()).hexdigest()

# get Shufti Signature
sp_signature = response.headers.get('Signature',"")

# Convert json string to json object
json_response = json.loads((response.content))

if sp_signature == calculated_signature:
        print(json_response)
else:
        print('Invalid signature: {}'.format(response.content))

