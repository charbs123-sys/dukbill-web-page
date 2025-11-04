# generate_sales_summary_final.py
# Creates Broker_Sales_Summary.pdf — visually identical to Payroll & Purchases
import json
from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

# ---------- CONFIG ----------
BASE = Path("./myob_outputs")
FILES = {
    "invoice_generic":      BASE / "Sale_Invoice.json",
    "invoice_professional": BASE / "Sale_Invoice_Professional.json",
    "invoice_service":      BASE / "Sale_Invoice_Service.json",
    "customer_payment":     BASE / "Sale_CustomerPayment.json",
    "credit_refund":        BASE / "Sale_CreditRefund.json",
}
OUTPUT = BASE / "Broker_Sales_Summary.pdf"

# ---------- HELPERS ----------
def load_json(p: Path):
    if not p.exists():
        print(f"⚠️ Missing file: {p.name}")
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def safe_date(s): 
    return (s or "")[:10]

def money(x):
    try: 
        return f"{float(x):,.2f}"
    except: 
        return "0.00"

def short(s, n=50):
    s = str(s or "")
    return s if len(s) <= n else s[:n-1] + "…"

# ---------- PDF STYLES & TABLE (MATCH PAYROLL) ----------
styles = getSampleStyleSheet()
styles.add(ParagraphStyle(name="caption", fontSize=8.5, textColor=colors.HexColor("#666")))

def add_table(story, title, rows, columns, col_widths=None):
    if not rows:
        return
    story.append(Paragraph(title, styles["Heading2"]))
    data = [columns] + rows
    tbl = Table(data, colWidths=col_widths, hAlign="LEFT")
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#f2f2f2")),
        ("TEXTCOLOR", (0,0), (-1,0), colors.HexColor("#333")),
        ("GRID", (0,0), (-1,-1), 0.25, colors.HexColor("#ccc")),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTNAME", (0,1), (-1,-1), "Helvetica"),
        ("FONTSIZE", (0,0), (-1,-1), 9),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#fafafa")]),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 12))

# ---------- EXTRACTORS ----------
def collect_invoices():
    all_inv = []
    for path in [FILES["invoice_generic"], FILES["invoice_professional"], FILES["invoice_service"]]:
        data = load_json(path)
        for it in (data.get("Items") or []):
            cust = it.get("Customer") or {}
            terms = it.get("Terms") or {}
            inv = {
                "Date": safe_date(it.get("Date")),
                "InvoiceNumber": it.get("Number") or "",
                "CustomerName": cust.get("Name", "Unknown"),
                "Status": it.get("Status") or "Unknown",
                "Subtotal": float(it.get("Subtotal") or 0),
                "Total": float(it.get("TotalAmount") or it.get("Subtotal") or 0),
                "BalanceDue": float(it.get("BalanceDueAmount") or 0),
                "DueDate": safe_date(terms.get("DueDate")),
            }
            all_inv.append(inv)
    all_inv.sort(key=lambda x: x["Date"] or "", reverse=True)
    return all_inv[:100]

def collect_payments():
    data = load_json(FILES["customer_payment"])
    payments = []
    for it in (data.get("Items") or []):
        cust = it.get("Customer") or {}
        payments.append({
            "Date": safe_date(it.get("Date")),
            "CustomerName": cust.get("Name", "Unknown"),
            "Amount": float(it.get("AmountReceived") or 0),
            "Memo": short(it.get("Memo") or "", 60),
            "PaymentMethod": it.get("PaymentMethod") or "",
        })
    payments.sort(key=lambda x: x["Date"], reverse=True)
    return payments[:100]

def collect_credit_refunds():
    data = load_json(FILES["credit_refund"])
    refunds = []
    for it in (data.get("Items") or []):
        cust = it.get("Customer") or {}
        inv = it.get("Invoice") or {}
        acct = it.get("Account") or {}
        refunds.append({
            "Date": safe_date(it.get("Date")),
            "CustomerName": cust.get("Name", "Unknown"),
            "Amount": float(it.get("Amount") or 0),
            "InvoiceNumber": inv.get("Number") or "",
            "BankAccount": acct.get("Name") or "Unknown",
        })
    refunds.sort(key=lambda x: x["Date"], reverse=True)
    return refunds[:100]

# ---------- BUILD PDF ----------
def build_pdf(invoices, payments, refunds):
    doc = SimpleDocTemplate(
        str(OUTPUT),
        pagesize=A4,
        leftMargin=36, rightMargin=36,
        topMargin=30, bottomMargin=30
    )
    story = []

    # Header
    story.append(Paragraph("Dukbill — Sales Summary (Broker Essentials)", styles["Heading1"]))
    story.append(Paragraph(
        "Income verification pack: recent invoices, payments received, and credit refunds. "
        "Sorted newest first. All amounts in AUD.",
        styles["BodyText"]
    ))
    story.append(Spacer(1, 12))

    # === INVOICES TABLE ===
    inv_rows = []
    for inv in invoices:
        inv_rows.append([
            inv["Date"],
            inv["InvoiceNumber"],
            short(inv["CustomerName"], 32),
            inv["Status"],
            money(inv["Total"]),
            money(inv["BalanceDue"]),
            inv["DueDate"] or "-",
        ])

    add_table(
        story,
        "Invoices — Overview",
        inv_rows,
        ["Date", "#", "Customer", "Status", "Total", "Balance", "Due"],
        col_widths=[65, 70, 120, 65, 70, 70, 70]
    )
    story.append(Paragraph(
        "Most recent 100 invoices. Balance = Total - Paid.",
        styles["caption"]
    ))
    story.append(Spacer(1, 6))

    # === PAYMENTS TABLE ===
    pay_rows = []
    for p in payments:
        pay_rows.append([
            p["Date"],
            short(p["CustomerName"], 35),
            money(p["Amount"]),
            p["Memo"]#,
            #p["PaymentMethod"],
        ])

    add_table(
        story,
        "Customer Payments Received",
        pay_rows,
        ["Date", "Customer", "Amount", "Memo"],
        col_widths=[70, 150, 80, 180]
    )
    story.append(Paragraph(
        "Last 100 payments. Supports income stability proof.",
        styles["caption"]
    ))
    story.append(Spacer(1, 6))

    # === REFUNDS TABLE ===
    ref_rows = []
    for r in refunds:
        ref_rows.append([
            r["Date"],
            short(r["CustomerName"], 35),
            money(r["Amount"]),
            r["InvoiceNumber"],
            short(r["BankAccount"], 40),
        ])

    add_table(
        story,
        "Credit Refunds Issued",
        ref_rows,
        ["Date", "Customer", "Amount", "Invoice #", "Bank Account"],
        col_widths=[70, 150, 80, 80, 140]
    )
    story.append(Paragraph(
        "Refunds reduce net income. Shown for transparency.",
        styles["caption"]
    ))

    # Fallback
    if not (invoices or payments or refunds):
        story.append(Paragraph("No sales data available.", styles["BodyText"]))

    # Build
    doc.build(story)
    print(f"✅ PDF created successfully: {OUTPUT}")

# ---------- RUN ----------
if __name__ == "__main__":
    invoices = collect_invoices()
    payments = collect_payments()
    refunds = collect_credit_refunds()
    build_pdf(invoices, payments, refunds)