import hashlib
#from dukbill import verification_states_shufti
from shufti import shufti_url, get_verification_status_with_proofs, download_proof_image
from helpers.helper import jpg_to_pdf_simple, hash_email
from Documents.documents import *

def verify_signature(raw_data: bytes, sp_signature: str, secret_key: str) -> bool:
    """Verify the callback signature matches the expected hash."""
    secret_key_hash = hashlib.sha256(secret_key.encode()).hexdigest()
    calculated_signature = hashlib.sha256(
        raw_data + secret_key_hash.encode()
    ).hexdigest()
    return sp_signature == calculated_signature


def log_callback_event(event: str, reference: str):
    """Log callback event details."""
    print(f"\n{'='*60}")
    print(f"Callback received: {event} for {reference}")
    print(f"{'='*60}")


def get_verification_state(reference: str, verification_states_shufti) -> dict:
    """Retrieve verification state for a reference."""
    verification_state = verification_states_shufti.get(reference)
    
    if not verification_state:
        print(f"⚠️ No user found for reference: {reference}")
        return None
    
    print(f"User ID: {verification_state['user_id']}, Auth0 ID: {verification_state['auth0_id']}")
    return verification_state


async def process_proof_image(image_url: str, access_token: str, 
                              hashed_email: str, doc_type: str, 
                              side: str) -> str:
    """Download, convert, and upload a proof image."""
    image_jpg = download_proof_image(image_url, access_token)
    if not image_jpg:
        return None
    
    image_pdf = jpg_to_pdf_simple(image_jpg)
    if not image_pdf:
        return None
    
    s3_key = f"{hashed_email}/verified_ids/{doc_type}_{side}.pdf"
    s3_url = await upload_bytes_to_s3(image_pdf, s3_key)
    
    if s3_url:
        print(f"✅ {side.capitalize()} image uploaded: {s3_url}")
    
    return s3_url


async def handle_verification_accepted(reference: str, verification_state: dict):
    """Handle accepted verification by fetching and storing proof images."""
    print("Fetching proof images from Status API...")
    
    status_response = get_verification_status_with_proofs(reference)
    if not status_response:
        print("⚠️ Failed to get status response")
        return
    
    proofs = status_response.get('proofs', {})
    access_token = proofs.get('access_token')
    
    if not proofs or not access_token:
        print("⚠️ No proofs or access_token in status response")
        return
    
    # Extract user info
    user_id = verification_state["user_id"]
    user_email = verification_state["email"]
    hashed_user_email = hash_email(user_email)
    
    # Get document type and proof URLs
    doc_type = status_response["verification_data"]["document"]["selected_type"][0]
    document_proofs = proofs.get('document', {})
    front_url = document_proofs.get('proof')
    back_url = document_proofs.get('additional_proof')
    
    print(f"Front proof URL: {front_url}")
    print(f"Back proof URL: {back_url}")
    
    # Process front image
    if front_url:
        await process_proof_image(front_url, access_token, hashed_user_email, 
                                 doc_type, 'front')
    
    # Process back image
    if back_url:
        await process_proof_image(back_url, access_token, hashed_user_email, 
                                doc_type, 'back')
    
    # Store verification data in database
    # TODO: Save to database linked to user_id
    
    print(f"✅ Verification complete for user {user_id}")


def handle_verification_declined(user_id: str, response_data: dict):
    """Handle declined verification."""
    print(f"❌ Verification declined for user {user_id}")
    declined_reason = response_data.get('declined_reason', 'Unknown')
    print(f"Reason: {declined_reason}")
    # TODO: Update database with declined status
