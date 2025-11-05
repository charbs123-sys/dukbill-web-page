# xero_pdf_generators_aesthetic.py
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from Database.S3_utils import upload_pdf_to_s3

# ---------- Shared aesthetic (matches MYOB) ----------

def setup_styles():
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="caption", fontSize=8.5, textColor=colors.HexColor("#666")))
    # Cell style for tables: smaller font, tighter leading, and wrapping enabled
    styles.add(ParagraphStyle(
        name="cell",
        parent=styles["BodyText"],
        fontSize=9,
        leading=11,
        wordWrap="CJK"  # robust wrapping (incl. long tokens)
    ))
    return styles

def add_table(story, title, rows, columns, col_widths, styles):
    if not rows:
        return
    story.append(Paragraph(title, styles["Heading2"]))

    # Wrap all data cells in Paragraphs so rows expand vertically as needed
    def _wrap_row(row):
        return [Paragraph("" if c is None else str(c), styles["cell"]) for c in row]

    data = [columns] + [_wrap_row(r) for r in rows]

    tbl = Table(data, colWidths=col_widths, hAlign="LEFT", repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#f2f2f2")),
        ("TEXTCOLOR", (0,0), (-1,0), colors.HexColor("#333")),
        ("GRID", (0,0), (-1,-1), 0.25, colors.HexColor("#ccc")),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTNAME", (0,1), (-1,-1), "Helvetica"),
        ("FONTSIZE", (0,0), (-1,-1), 9),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#fafafa")]),
        ("VALIGN", (0,0), (-1,-1), "TOP"),  # make cells grow downward
    ]))
    story.append(tbl)
    story.append(Spacer(1, 12))

def safe_date(s):
    if not s:
        return ""
    if isinstance(s, str) and "/Date(" in s:
        ts = s.split("(")[1].split(")")[0]
        if "+" in ts or "-" in ts:
            ts = ts.split("+")[0].split("-")[0]
        try:
            import datetime
            dt = datetime.datetime.utcfromtimestamp(int(ts) / 1000)
            return dt.strftime("%Y-%m-%d")
        except:
            return s[:10]
    return str(s)[:10]

def money(x):
    try:
        return f"{float(x):,.2f}"
    except:
        return "0.00"

def short(s, n=50):
    s = str(s or "")
    return s if len(s) <= n else s[:n-1] + "…"

def mask(val, show_last=4):
    val = str(val or "")
    return ("*" * max(0, len(val) - show_last)) + val[-show_last:] if len(val) > show_last else val

def _get(d, path, default=None):
    cur = d
    for k in path.split("/"):
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def _new_doc(buffer):
    return SimpleDocTemplate(
        buffer, pagesize=A4,
        leftMargin=36, rightMargin=36, topMargin=30, bottomMargin=30
    )

def _as_list(maybe_list_or_item):
    if maybe_list_or_item is None:
        return []
    if isinstance(maybe_list_or_item, list):
        return maybe_list_or_item
    return [maybe_list_or_item]

# ---------- 1) ACCOUNTS ----------

def generate_accounts_report(data, output_file, hashed_email):
    buffer = BytesIO()
    doc = _new_doc(buffer)
    styles = setup_styles()
    story = []
    org_name = data.get('organization', 'Unknown Organization')

    story.append(Paragraph("Dukbill – Chart of Accounts (Broker Essentials)", styles["Heading1"]))
    story.append(Paragraph(f"Organization: {org_name}", styles["BodyText"]))
    story.append(Spacer(1, 12))

    accounts = _get(data, "preview/settings/accounts_list", [])
    if not accounts:
        accounts = _as_list(_get(data, "preview/settings/accounts"))

    rows = []
    for a in accounts:
        if not a:
            continue
        rows.append([
            a.get("Code","N/A"),
            a.get("Name","N/A"),
            a.get("Class","N/A"),     # keep
            a.get("TaxType","N/A"),   # keep
        ])
    add_table(
        story,
        "Accounts",
        rows,
        ["Code","Name","Class","Tax Type"],
        [70,260,80,120],
        styles
    )

    # Tax Rates (unchanged)
    tax_rates = _get(data, "preview/settings/tax_rates_list", [])
    if not tax_rates:
        tax_rates = _as_list(_get(data, "preview/settings/tax_rates"))
    tr_rows = []
    for t in tax_rates:
        if not t:
            continue
        tr_rows.append([
            t.get("Name","N/A"),
            t.get("DisplayTaxRate", 0),
            t.get("TaxType","N/A"),
            str(t.get("ReportTaxType","") or ""),
        ])
    add_table(
        story,
        "Tax Rates",
        tr_rows,
        ["Name","Rate %","Tax Type","Report Type"],
        [220,60,120,120],
        styles
    )

    # Tracking Categories (unchanged)
    trk = _get(data, "preview/settings/tracking_categories_list", [])
    if not trk:
        trk = _as_list(_get(data, "preview/settings/tracking_categories"))
    tc_rows = []
    for tc in trk:
        if not tc:
            continue
        options = ", ".join([o.get("Name","") for o in (tc.get("Options") or [])])
        tc_rows.append([tc.get("Name","N/A"), options])
    add_table(
        story,
        "Tracking Categories",
        tc_rows,
        ["Category","Options"],
        [180,350],
        styles
    )

    doc.build(story)
    return upload_pdf_to_s3(buffer, hashed_email, output_file)



# ---------- 2) TRANSACTIONS (Bank Transactions + other counts) ----------

def generate_transactions_report(data, output_file, hashed_email):
    buffer = BytesIO()
    doc = _new_doc(buffer)
    styles = setup_styles()
    story = []
    org_name = data.get('organization', 'Unknown Organization')

    story.append(Paragraph("Dukbill – Bank Transactions (Broker Essentials)", styles["Heading1"]))
    story.append(Paragraph(f"Organization: {org_name}", styles["BodyText"]))
    story.append(Spacer(1, 12))

    txns = _get(data, "preview/transactions/bank_transactions_list", [])
    if not txns:
        txns = _as_list(_get(data, "preview/transactions/bank_transactions"))

    rows = []
    for t in txns:
        if not t:
            continue
        desc = t.get("Reference") or _get(t, "Contact/Name", "N/A")  # no truncation; allow wrap
        rows.append([
            safe_date(t.get("Date")),
            desc,
            money(t.get("Total")),
            t.get("Status","") or "",
            t.get("CurrencyCode","") or "",
            str(t.get("IsReconciled", False)),
        ])
    add_table(
        story,
        "Bank Transactions – All",
        rows,
        ["Date","Description","Total","Status","CCY","Reconciled"],
        [70,150,70,70,45,65],   # Description ~150
        styles
    )

    other = [
        ("Manual Journals", _get(data, "preview/transactions/manual_journals_total", 0)),
        ("Overpayments",   _get(data, "preview/transactions/overpayments_total", 0)),
        ("Prepayments",    _get(data, "preview/transactions/prepayments_total", 0)),
    ]
    add_table(
        story, "Other Transaction Types (Counts)",
        [[k, v] for k, v in other],
        ["Type", "Count"], [250, 260], styles
    )

    doc.build(story)
    return upload_pdf_to_s3(buffer, hashed_email, output_file)



# ---------- 3) PAYMENTS ----------

def generate_payments_report(data, output_file, hashed_email):
    buffer = BytesIO()
    doc = _new_doc(buffer)
    styles = setup_styles()
    story = []
    org_name = data.get('organization', 'Unknown Organization')

    story.append(Paragraph("Dukbill – Payments (Broker Essentials)", styles["Heading1"]))
    story.append(Paragraph(f"Organization: {org_name}", styles["BodyText"]))
    story.append(Spacer(1, 12))

    payments = _get(data, "preview/transactions/payments_list", [])
    if not payments:
        payments = _as_list(_get(data, "preview/transactions/payments"))

    rows = []
    for p in payments:
        if not p:
            continue
        rows.append([
            safe_date(p.get("Date")),
            short(_get(p, "Invoice/Contact/Name", "N/A"), 35),
            money(p.get("Amount")),
            p.get("Status","") or "",
            str(p.get("IsReconciled", False))
        ])
    add_table(
        story, "Payments – All",
        rows,
        ["Date","Contact","Amount","Status","Reconciled"],
        [70,160,80,70,70],
        styles
    )

    doc.build(story)
    return upload_pdf_to_s3(buffer, hashed_email, output_file)


# ---------- 4) CREDIT NOTES ----------

def generate_credit_notes_report(data, output_file, hashed_email):
    buffer = BytesIO()
    doc = _new_doc(buffer)
    styles = setup_styles()
    story = []
    org_name = data.get('organization', 'Unknown Organization')

    story.append(Paragraph("Dukbill – Credit Notes (Broker Essentials)", styles["Heading1"]))
    story.append(Paragraph(f"Organization: {org_name}", styles["BodyText"]))
    story.append(Spacer(1, 12))

    credit_notes = _get(data, "preview/transactions/credit_notes_list", [])
    if not credit_notes:
        credit_notes = _as_list(_get(data, "preview/transactions/credit_notes"))

    rows = []
    for cn in credit_notes:
        if not cn:
            continue
        rows.append([
            safe_date(cn.get("Date")),
            short(_get(cn, "Contact/Name", "Unknown"), 35),
            cn.get("Status","Unknown"),
            money(cn.get("Total")),
            money(cn.get("RemainingCredit")),
            cn.get("CurrencyCode","") or "",
        ])
    add_table(
        story, "Credit Notes – All",
        rows,
        ["Date","Customer","Status","Total","Remaining","CCY"],
        [70,160,70,70,75,40],
        styles
    )

    doc.build(story)
    return upload_pdf_to_s3(buffer, hashed_email, output_file)

# ---------- 5) PAYROLL (Employees, Pay Runs, Payslips) ----------

def generate_payroll_report(data, output_file, hashed_email):
    buffer = BytesIO()
    doc = _new_doc(buffer)
    styles = setup_styles()
    story = []
    org_name = data.get('organization', 'Unknown Organization')

    story.append(Paragraph("Dukbill – Payroll (Broker Essentials)", styles["Heading1"]))
    story.append(Paragraph(f"Organization: {org_name}", styles["BodyText"]))
    story.append(Spacer(1, 12))

    # Employees
    story.append(Paragraph("EMPLOYEES", styles["Heading2"]))
    employees = _get(data, "preview/payroll/employees_list", [])
    if not employees:
        employees = _as_list(_get(data, "preview/payroll/employees"))
    emp_rows = []
    for e in employees:
        if not e: 
            continue
        emp_rows.append([
            f"{e.get('FirstName','')} {e.get('LastName','')}".strip(),
            e.get("Email","") or "-",
            safe_date(e.get("StartDate")),
            e.get("Status","Unknown"),
            e.get("Gender","") or "",
            safe_date(e.get("DateOfBirth")),
        ])
    add_table(
        story, "Employees – All",
        emp_rows,
        ["Name","Email","Start","Status","Gender","DOB"],
        [150,160,55,55,55,55], styles
    )

    # Pay Runs
    story.append(Paragraph("PAY RUNS", styles["Heading2"]))
    payruns = _get(data, "preview/payroll/payruns_list", [])
    if not payruns:
        payruns = _as_list(_get(data, "preview/payroll/payruns"))
    pr_rows = []
    for pr in payruns:
        if not pr:
            continue
        pr_rows.append([
            safe_date(pr.get("PayRunPeriodStartDate")),
            safe_date(pr.get("PayRunPeriodEndDate")),
            safe_date(pr.get("PaymentDate")),
            money(pr.get("Wages")),
            money(pr.get("Tax")),
            money(pr.get("Super")),
            money(pr.get("NetPay")),
            pr.get("PayRunStatus","Unknown"),
        ])
    add_table(
        story, "Pay Runs – All",
        pr_rows,
        ["Start","End","Paid","Wages","Tax","Super","Net","Status"],
        [55,55,55,60,55,55,60,65], styles
    )

    # Payslips
    story.append(Paragraph("PAYSLIPS", styles["Heading2"]))
    payslips = _get(data, "preview/payroll/payslips_list", [])
    if not payslips:
        payslips = _as_list(_get(data, "preview/payroll/payslips"))
    ps_rows = []
    for ps in payslips:
        if not ps:
            continue
        ps_rows.append([
            f"{ps.get('FirstName','')} {ps.get('LastName','')}".strip(),
            money(ps.get("Wages")),
            money(ps.get("Deductions")),
            money(ps.get("Tax")),
            money(ps.get("Super")),
            money(ps.get("Reimbursements")),
            money(ps.get("NetPay")),
        ])
    add_table(
        story, "Payslips – All",
        ps_rows,
        ["Employee","Wages","Deductions","Tax","Super","Reimb.","Net"],
        [140,60,70,55,55,60,60], styles
    )

    doc.build(story)
    return upload_pdf_to_s3(buffer, hashed_email, output_file)

# ---------- 6) INVOICES ----------

def generate_invoices_report(data, output_file, hashed_email):
    buffer = BytesIO()
    doc = _new_doc(buffer)
    styles = setup_styles()
    story = []
    org_name = data.get('organization', 'Unknown Organization')

    story.append(Paragraph("Dukbill – Invoices (Broker Essentials)", styles["Heading1"]))
    story.append(Paragraph(f"Organization: {org_name}", styles["BodyText"]))
    story.append(Spacer(1, 12))

    invoices = _get(data, "preview/transactions/invoices_list", [])
    if not invoices:
        invoices = _as_list(_get(data, "preview/transactions/invoices"))

    inv_rows = []
    for inv in invoices:
        if not inv:
            continue
        inv_rows.append([
            safe_date(inv.get("Date")),
            _get(inv, "Contact/Name", "Unknown"),  # let the cell wrap
            inv.get("Status","Unknown"),
            money(inv.get("Total")),
            money(inv.get("AmountPaid")),
            money(inv.get("AmountDue")),
            safe_date(inv.get("DueDate")) or "-",
        ])
    add_table(
        story, "Invoices – All",
        inv_rows,
        ["Date","Customer","Status","Total","Paid","Balance","Due"],
        [65,140,75,70,60,70,70],
        styles
    )

    doc.build(story)
    return upload_pdf_to_s3(buffer, hashed_email, output_file)



# ---------- 7) FINANCIAL REPORTS SUMMARY (shows all top-level rows) ----------

def generate_reports_summary(data, output_file, hashed_email):
    buffer = BytesIO()
    doc = _new_doc(buffer)
    styles = setup_styles()
    story = []
    org_name = data.get('organization', 'Unknown Organization')

    story.append(Paragraph("Dukbill – Financial Reports Summary", styles["Heading1"]))
    story.append(Paragraph(f"Organization: {org_name}", styles["BodyText"]))
    story.append(Spacer(1, 12))

    # P&L
    pl = _get(data, "preview/reports/profit_loss", {})
    story.append(Paragraph("PROFIT & LOSS", styles["Heading2"]))
    if pl and isinstance(pl.get("Reports"), list) and pl["Reports"]:
        rp = pl["Reports"][0]
        story.append(Paragraph(f"Title: {(rp.get('ReportTitles') or [''])[0]}", styles["BodyText"]))
        story.append(Paragraph(f"Report Date: {rp.get('ReportDate','')}", styles["BodyText"]))
        story.append(Spacer(1, 6))
        # Flatten one level of sections into a table if possible
        rows = []
        for row in (rp.get("Rows") or []):
            if row.get("RowType") == "Section":
                title = row.get("Title","")
                cells = row.get("Cells") or []
                val = cells[0].get("Value") if cells else None
                rows.append([short(title, 60), money(val)])
        add_table(story, "P&L – Sections", rows, ["Section", "Amount"], [350,160], styles)
    else:
        story.append(Paragraph("No P&L data available.", styles["BodyText"]))

    story.append(Spacer(1, 12))

    # Balance Sheet
    bs = _get(data, "preview/reports/balance_sheet", {})
    story.append(Paragraph("BALANCE SHEET", styles["Heading2"]))
    if bs and isinstance(bs.get("Reports"), list) and bs["Reports"]:
        rp = bs["Reports"][0]
        story.append(Paragraph(f"Title: {(rp.get('ReportTitles') or [''])[0]}", styles["BodyText"]))
        story.append(Paragraph(f"Report Date: {rp.get('ReportDate','')}", styles["BodyText"]))
        story.append(Spacer(1, 6))
        rows = []
        for row in (rp.get("Rows") or []):
            if row.get("RowType") == "Section":
                title = row.get("Title","")
                cells = row.get("Cells") or []
                val = cells[0].get("Value") if cells else None
                rows.append([short(title, 60), money(val)])
        add_table(story, "Balance Sheet – Sections", rows, ["Section", "Amount"], [350,160], styles)
    else:
        story.append(Paragraph("No Balance Sheet data available.", styles["BodyText"]))

    doc.build(story)
    return upload_pdf_to_s3(buffer, hashed_email, output_file)

# ---------- 8) BANK TRANSFERS ----------

def generate_bank_transfers_report(data, output_file, hashed_email):
    buffer = BytesIO()
    doc = _new_doc(buffer)
    styles = setup_styles()
    story = []
    org_name = data.get('organization', 'Unknown Organization')

    story.append(Paragraph("Dukbill – Bank Transfers (Broker Essentials)", styles["Heading1"]))
    story.append(Paragraph(f"Organization: {org_name}", styles["BodyText"]))
    story.append(Spacer(1, 12))

    transfers = _get(data, "preview/transactions/bank_transfers_list", [])
    if not transfers:
        transfers = _as_list(_get(data, "preview/transactions/bank_transfers"))

    rows = []
    for bt in transfers:
        if not bt:
            continue
        rows.append([
            safe_date(bt.get("Date")),
            short(_get(bt, "FromBankAccount/Name","Unknown"), 30),
            short(_get(bt, "ToBankAccount/Name","Unknown"), 30),
            money(bt.get("Amount")),
            short(bt.get("Reference") or "None", 40),
        ])
    add_table(
        story, "Bank Transfers – All",
        rows,
        ["Date","From","To","Amount","Reference"],
        [65,150,150,70,90], styles
    )

    doc.build(story)
    return upload_pdf_to_s3(buffer, hashed_email, output_file)
