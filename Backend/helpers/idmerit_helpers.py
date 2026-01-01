import base64
import io
import time
import uuid

import requests
from config import IDMERIT_CLIENT_ID, IDMERIT_SECRET_KEY
from Database.db_utils import (
    delete_row_from_requestid,
    fetch_clientid_from_requestid,
    save_verification_request_to_db,
)
from Database.S3_utils import upload_id_to_s3
from PIL import Image

API_URL = "https://sandbox.idmvalidate.com"


class IDMeritTokenManager:
    def __init__(self):
        self._access_token = None
        self._token_expiry_timestamp = 0
        self._safety_buffer = 60

    def get_valid_token(self) -> str:
        """
        Returns a valid access token.
        Refreshes it if it is missing or expired.
        """
        current_time = time.time()

        # Check if token exists and if we are within the valid window
        if self._access_token is None or current_time >= (
            self._token_expiry_timestamp - self._safety_buffer
        ):
            print("Token expired or missing. Refreshing...")
            self._refresh_token()
        else:
            pass

        return self._access_token

    def _refresh_token(self):
        """
        Internal method to perform the HTTP request to IDMerit
        """
        credentials = f"{IDMERIT_CLIENT_ID}:{IDMERIT_SECRET_KEY}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()

        url = f"{API_URL}/token"
        payload = {"grant_type": "client_credentials", "scope": "idmvalidate"}
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {encoded_credentials}",
        }

        response = requests.post(url, headers=headers, data=payload)

        if response.status_code == 200:
            data = response.json()
            self._access_token = data.get("access_token")
            expires_in = data.get("expires_in", 3600)
            self._token_expiry_timestamp = time.time() + int(expires_in)
        else:
            # Handle error appropriately (logging, raising exception)
            print(f"Error fetching token: {response.text}")
            raise Exception("Failed to refresh IDMerit token")


idmerit_manager = IDMeritTokenManager()


def send_idmerit_verification_message(
    client_id: str,
    phone_number: str,
    name: str,
    country: str,
    dob: str,
    redirectURL: str,
    callbackURL="http://localhost",
) -> dict:
    access_token = idmerit_manager.get_valid_token()
    requestID = str(uuid.uuid4())
    save_verification_request_to_db(requestID, client_id)

    url = f"{API_URL}/verify"
    payload = {
        "mobile": phone_number,
        "name": name,
        "country": country,
        "requestID": requestID,
        "dateOfBirth": dob,
        "callbackURL": callbackURL,
        "redirectURL": redirectURL,
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
    }
    response = requests.request("POST", url, headers=headers, json=payload)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error sending verification message: {response.text}")
        return None


def convert_base64_jpg_to_pdf_bytes(base64_string: str) -> io.BytesIO:
    """
    Helper function to clean base64 string, decode it,
    load it as an Image, and save it as a PDF stream.
    """
    try:
        # 1. Clean the base64 string if it contains headers (e.g., "data:image/jpeg;base64,...")
        if "," in base64_string:
            base64_string = base64_string.split(",")[1]

        # 2. Decode bytes
        image_data = base64.b64decode(base64_string)

        # 3. Open image using Pillow
        image = Image.open(io.BytesIO(image_data))

        # 4. Convert to RGB (required for saving as PDF)
        if image.mode != "RGB":
            image = image.convert("RGB")

        # 5. Save to BytesIO object as PDF
        pdf_stream = io.BytesIO()
        image.save(pdf_stream, format="PDF")
        pdf_stream.seek(0)  # Reset pointer to start of stream

        return pdf_stream
    except Exception as e:
        print(f"Error converting image to PDF: {e}")
        return None


def upload_idmerit_user_image_s3(
    front_image: str, back_image: str, hashed_email: str, document_type: str
) -> bool:
    if front_image:
        front_pdf_stream = convert_base64_jpg_to_pdf_bytes(front_image)
        if not front_pdf_stream:
            return False
        if document_type == "passport":
            front_key = f"{hashed_email}/idmerit_docs/idmerit_passport.pdf"
        else:
            front_key = f"{hashed_email}/idmerit_docs/idmerit_front_id.pdf"
        front_upload_success = upload_id_to_s3(front_pdf_stream, front_key)
    if back_image:
        back_pdf_stream = convert_base64_jpg_to_pdf_bytes(back_image)
        if not back_pdf_stream:
            return False
        back_key = f"{hashed_email}/idmerit_docs/idmerit_back_id.pdf"
        back_upload_success = upload_id_to_s3(back_pdf_stream, back_key)

    return front_upload_success or back_upload_success


def idmerit_fetch_clientid(request_id: str) -> dict:
    client_info = fetch_clientid_from_requestid(request_id)
    delete_row_from_requestid(request_id)
    return client_info
