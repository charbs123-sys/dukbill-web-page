from datetime import datetime

def parse_amount(amount_str):
    if not amount_str:
        return 0.0
    try:
        cleaned = amount_str.replace("$", "").replace(",", "")
        return float(cleaned)
    except ValueError:
        return 0.0

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