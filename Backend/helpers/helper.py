import hashlib
import io
from datetime import datetime
from io import BytesIO

import phonenumbers
from PIL import Image
from pypdf import PdfReader, PdfWriter


def parse_amount(amount):
    if isinstance(amount, str):
        cleaned = amount.replace("$", "").replace(",", "")
        try:
            return float(cleaned)
        except ValueError:
            return 0
    elif isinstance(amount, (int, float)):
        return float(amount)
    return 0


def normalize_date(date_str):
    if not date_str:
        return None
    try:
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(date_str, fmt).date().isoformat()
            except ValueError:
                continue
        return None
    except Exception:
        return None


def format_phonenumber(phonenumber):
    parsed_number = phonenumbers.parse(phonenumber, "AU")
    formatted_number = phonenumbers.format_number(
        parsed_number, phonenumbers.PhoneNumberFormat.E164
    )
    return formatted_number


def truncate_pdf(file_bytes: bytes) -> bytes:
    """Return a PDF containing only the first page of the original PDF."""
    reader = PdfReader(io.BytesIO(file_bytes))
    writer = PdfWriter()
    if len(reader.pages) > 0:
        writer.add_page(reader.pages[0])
        output = io.BytesIO()
        writer.write(output)
        return output.getvalue()
    return b""


def get_email_domain(email: str):
    """
    Get domain from email address
    """
    try:
        return email.split("@")[1]
    except IndexError:
        raise ValueError(f"Invalid email address: {email}")


def hash_email(email):
    """
    Turn email into 256 hash representation
    """
    return hashlib.sha256(email.encode("utf-8")).hexdigest()


def jpg_to_pdf_simple(image_bytes: bytes) -> bytes:
    """
    Simpler conversion using PIL's built-in PDF support

    Args:
        image_bytes: JPG image as bytes

    Returns:
        bytes: PDF file as bytes
    """
    try:
        # Open image
        img = Image.open(BytesIO(image_bytes))

        # Convert to RGB
        if img.mode != "RGB":
            img = img.convert("RGB")

        # Save as PDF to BytesIO
        pdf_buffer = BytesIO()
        img.save(pdf_buffer, "PDF", resolution=100.0)

        return pdf_buffer.getvalue()

    except Exception as e:
        print(f"❌ Error converting JPG to PDF: {e}")
        return None
