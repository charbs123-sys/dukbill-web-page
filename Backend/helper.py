from datetime import datetime
from pypdf import PdfReader, PdfWriter
import phonenumbers
import io

def parse_amount(amount):
    """
    Safely converts an amount to float.
    Works if amount is a string with $/comma or a number.
    """
    if isinstance(amount, str):
        # Remove $ and commas if present
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
    formatted_number = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
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
    return b''  # empty PDF if no pages
