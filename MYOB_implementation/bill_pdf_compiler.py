# generate_purchases_summary_v3.py
# Creates Broker_Purchases_Summary.pdf from MYOB Purchase Bill JSONs
# Matches payroll summary aesthetics exactly.

import json
from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

# ----------------------------
# CONFIG
# ----------------------------
BASE = Path("./myob_outputs")
FILES = {
    "generic":      BASE / "Purchase_Bill.json",
    "item":         BASE / "Purchase_Bill_Item.json",
    "professional": BASE / "Purchase_Bill_Professional.json",
    "service":      BASE / "Purchase_Bill_Service.json",
}
OUTPUT_PDF = BASE / "Broker_Purchases_Summary.pdf"

MAX_BILLS = 50
MAX_LINES = 5

# ----------------------------
# HELPERS
# ----------------------------
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
    except Exception: 
        return "0.00"

def short(s, n=35):
    s = str(s or "")
    return s if len(s) <= n else s[:n-1] + "…"

# ----------------------------
# PDF STYLES & TABLE HELPER (MATCH PAYROLL)
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
# EXTRACTOR
# ----------------------------
def collect_bills():
    all_bills = []
    for path in FILES.values():
        data = load_json(path)
        for it in (data.get("Items") or []):
            supplier = it.get("Supplier") or {}
            terms = it.get("Terms") or {}
            bill = {
                "Date": safe_date(it.get("Date")),
                "SupplierName": supplier.get("Name", "Unknown"),
                "Status": it.get("Status") or "Unknown",
                "TotalAmount": float(it.get("TotalAmount") or 0),
                "AppliedToDate": float(it.get("AppliedToDate") or 0),
                "BalanceDueAmount": float(it.get("BalanceDueAmount") or 0),
                "DueDate": safe_date(terms.get("DueDate")),
                "Lines": [],
            }
            for ln in (it.get("Lines") or [])[:MAX_LINES]:
                acct = ln.get("Account") or {}
                item = ln.get("Item") or {}
                desc = ln.get("Description") or item.get("Name") or ""
                bill["Lines"].append({
                    "Description": short(desc, 50),
                    "Qty": ln.get("BillQuantity") or ln.get("UnitCount") or 0,
                    "UnitPrice": float(ln.get("UnitPrice") or 0),
                    "LineTotal": float(ln.get("Total") or 0),
                    "TaxCode": ((ln.get("TaxCode") or {}).get("Code")) or "",
                    "AccountName": acct.get("Name") or "",
                })
            all_bills.append(bill)
    all_bills.sort(key=lambda b: b["Date"] or "", reverse=True)
    return all_bills[:MAX_BILLS]

# ----------------------------
# BUILD PDF
# ----------------------------
def build_pdf(bills):
    doc = SimpleDocTemplate(
        str(OUTPUT_PDF),
        pagesize=A4,
        leftMargin=36, rightMargin=36,
        topMargin=30, bottomMargin=30
    )
    story = []

    # Title & Intro
    story.append(Paragraph("Dukbill — Purchases Summary (Broker Essentials)", styles["Heading1"]))
    story.append(Paragraph(
        "Broker-focused view: supplier, dates, status, totals, balance due. "
        "Line items capped at 5 per bill for clarity.",
        styles["BodyText"]
    ))
    story.append(Spacer(1, 12))

    # === BILLS OVERVIEW TABLE ===
    overview_rows = []
    for b in bills:
        overview_rows.append([
            b["Date"],
            short(b["SupplierName"], 35),
            b["Status"],
            money(b["TotalAmount"]),
            money(b["AppliedToDate"]),
            money(b["BalanceDueAmount"]),
            b["DueDate"] or "-",
        ])

    add_table(
        story,
        "Bills — Overview",
        overview_rows,
        ["Date", "Supplier", "Status", "Total", "Paid", "Balance", "Due"],
        col_widths=[70, 120, 70, 70, 65, 70, 60]
    )

    # Optional caption
    if bills:
        story.append(Paragraph(
            "Most recent 50 bills shown. Sorted by date (newest first). "
            "Balance Due = Total - Paid. All amounts in AUD.",
            styles["caption"]
        ))
        story.append(Spacer(1, 6))

    # === DETAILED LINE ITEMS (SAMPLE) ===
    line_rows = []
    for b in bills[:10]:  # Limit to avoid overflow
        for ln in b["Lines"]:
            line_rows.append([
                b["Date"],
                short(b["SupplierName"], 25),
                ln["Description"],
                f"{ln['Qty']:.2f}",
                money(ln['UnitPrice']),
                money(ln['LineTotal']),
                ln["TaxCode"],
            ])

    if line_rows:
        add_table(
            story,
            "Line Items — Sample (First 10 Bills)",
            line_rows,
            ["Date", "Supplier", "Description", "Qty", "Unit $", "Line Total", "Tax"],
            col_widths=[60, 90, 150, 45, 60, 70, 45]
        )
        story.append(Paragraph(
            "Truncated for readability. Full details available in MYOB exports.",
            styles["caption"]
        ))

    # Fallback
    if not bills:
        story.append(Paragraph("No purchase bill data available.", styles["BodyText"]))

    # Build
    doc.build(story)
    print(f"✅ PDF created successfully: {OUTPUT_PDF}")

# ----------------------------
# RUN
# ----------------------------
if __name__ == "__main__":
    bills = collect_bills()
    build_pdf(bills)