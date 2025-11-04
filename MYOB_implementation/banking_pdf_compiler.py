# generate_banking_summary_broker_only_all.py
# Creates Broker_Banking_Summary.pdf — matches Payroll, Purchases, Sales
import json
from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

# ----------------------------
# CONFIG
# ----------------------------
base = Path("./myob_outputs")
files = {
    "bank_accounts": base / "Banking_BankAccount.json",
    "statements": base / "Banking_Statement.json",
    "receive_money": base / "Banking_ReceiveMoneyTxn.json",
    "spend_money": base / "Banking_SpendMoneyTxn.json",
}
output_pdf = base / "Broker_Banking_Summary.pdf"

# ----------------------------
# UTILS
# ----------------------------
def load_json(path: Path):
    if not path.exists():
        print(f"⚠️ Missing file: {path.name}")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def mask(val, show_last=4):
    val = str(val or "")
    return ("*" * max(0, len(val) - show_last)) + val[-show_last:] if len(val) > show_last else val

def signed_amount(is_credit, amount):
    try:
        amt = float(amount or 0)
    except:
        amt = 0.0
    return f"{amt if is_credit else -amt:,.2f}"

def short(s, n=70):
    s = str(s or "")
    return s if len(s) <= n else s[:n-1] + "…"

# ----------------------------
# PDF STYLES & TABLE (MATCH PAYROLL)
# ----------------------------
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

# ----------------------------
# LOAD DATA
# ----------------------------
J = {k: load_json(v) for k, v in files.items()}

# ----------------------------
# PDF SETUP
# ----------------------------
doc = SimpleDocTemplate(
    str(output_pdf),
    pagesize=A4,
    leftMargin=36, rightMargin=36,
    topMargin=30, bottomMargin=30
)
story = []

story.append(Paragraph("Dukbill — Banking Summary (Broker Essentials)", styles["Heading1"]))

# ----------------------------
# LINKED BANK ACCOUNTS
# ----------------------------
accounts = J.get("bank_accounts", {}).get("Items", [])
if accounts:
    rows = []
    for acc in accounts:
        rows.append([
            acc.get("BankAccountName", "Unknown"),
            mask(acc.get("BankAccountNumber", "")),
            acc.get("FinancialInstitution", "Unknown"),
            (acc.get("LastReconciledDate") or "")[:10],
        ])
    add_table(
        story,
        "Linked Bank Accounts",
        rows,
        ["Account Name", "Account No.", "Institution", "Last Reconciled"],
        col_widths=[160, 120, 120, 100]
    )
    story.append(Paragraph(
        "Account numbers masked for security. Full details in MYOB.",
        styles["caption"]
    ))
    story.append(Spacer(1, 6))

# ----------------------------
# FULL STATEMENT HISTORY
# ----------------------------
stmts = J.get("statements", {}).get("Items", [])
if stmts:
    rows = []
    for ln in stmts:
        rows.append([
            (ln.get("Date") or "")[:10],
            short(ln.get("Description", ""), 70),
            signed_amount(ln.get("IsCredit"), ln.get("Amount")),
        ])
    add_table(
        story,
        "Bank Statement — All Transactions",
        rows,
        ["Date", "Description", "Amount (±)"],
        col_widths=[70, 360, 80]
    )
    story.append(Paragraph(
        "Full history shown. Positive = deposit, Negative = withdrawal.",
        styles["caption"]
    ))
    story.append(Spacer(1, 6))

# ----------------------------
# RECEIVED PAYMENTS
# ----------------------------
recv = J.get("receive_money", {}).get("Items", [])
if recv:
    rows = []
    for txn in recv:
        contact = (txn.get("Contact") or {}).get("Name", "Unknown")
        rows.append([
            (txn.get("Date") or "")[:10],
            short(contact, 50),
            f"{float(txn.get('AmountReceived', 0) or 0):,.2f}",
            txn.get("PaymentMethod", "Unknown"),
        ])
    add_table(
        story,
        "Incoming Payments",
        rows,
        ["Date", "From", "Amount", "Method"],
        col_widths=[70, 200, 80, 100]
    )
    story.append(Paragraph(
        "All money received into accounts. Supports income proof.",
        styles["caption"]
    ))
    story.append(Spacer(1, 6))

# ----------------------------
# OUTGOING PAYMENTS
# ----------------------------
spend = J.get("spend_money", {}).get("Items", [])
if spend:
    rows = []
    for txn in spend:
        contact = (txn.get("Contact") or {}).get("Name", "Unknown")
        rows.append([
            (txn.get("Date") or "")[:10],
            short(contact, 50),
            f"{float(txn.get('AmountPaid', 0) or 0):,.2f}",
            txn.get("PaymentMethod", "Unknown"),
        ])
    add_table(
        story,
        "Outgoing Payments",
        rows,
        ["Date", "To", "Amount", "Method"],
        col_widths=[70, 200, 80, 100]
    )
    story.append(Paragraph(
        "All payments made. Helps verify expenses and cash flow.",
        styles["caption"]
    ))

# ----------------------------
# FALLBACK
# ----------------------------
if len(story) <= 3:
    story.append(Paragraph("No banking data found in provided files.", styles["BodyText"]))

# ----------------------------
# BUILD PDF
# ----------------------------
doc.build(story)
print(f"✅ PDF created successfully: {output_pdf}")