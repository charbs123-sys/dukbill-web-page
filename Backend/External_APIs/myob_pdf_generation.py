from io import BytesIO

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

# ==================== SHARED UTILITIES ====================


def safe_date(s):
    return (s or "")[:10]


def money(x):
    try:
        return f"{float(x):,.2f}"
    except Exception as e:
        print(f"Error formatting money value {x}: {e}")
        return "0.00"


def short(s, n=50):
    s = str(s or "")
    return s if len(s) <= n else s[: n - 1] + "…"


def mask(val, show_last=4):
    val = str(val or "")
    return (
        ("*" * max(0, len(val) - show_last)) + val[-show_last:]
        if len(val) > show_last
        else val
    )


def signed_amount(is_credit, amount):
    try:
        amt = float(amount or 0)
    except Exception as e:
        print(f"Error parsing amount {amount}: {e}")
        amt = 0.0
    return f"{amt if is_credit else -amt:,.2f}"


def setup_styles():
    styles = getSampleStyleSheet()
    styles.add(
        ParagraphStyle(name="caption", fontSize=8.5, textColor=colors.HexColor("#666"))
    )
    return styles


def add_table(story, title, rows, columns, col_widths, styles):
    if not rows:
        return
    story.append(Paragraph(title, styles["Heading2"]))
    data = [columns] + rows
    tbl = Table(data, colWidths=col_widths, hAlign="LEFT")
    tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f2f2f2")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#333")),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#ccc")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                (
                    "ROWBACKGROUNDS",
                    (0, 1),
                    (-1, -1),
                    [colors.white, colors.HexColor("#fafafa")],
                ),
            ]
        )
    )
    story.append(tbl)
    story.append(Spacer(1, 12))


def find_data_by_endpoint(all_results, endpoint_suffix):
    """Find data matching endpoint suffix (e.g., 'Payroll/Timesheet')"""
    for item in all_results:
        if isinstance(item, dict) and "endpoint" in item:
            if item["endpoint"].endswith(endpoint_suffix):
                return item.get("data", {})
    return {}


# ==================== PAYROLL PDF ====================


def generate_payroll_pdf(all_results):
    """
    Generate Payroll Summary PDF from MYOB data
    Returns: PDF as bytes
    """
    buffer = BytesIO()
    styles = setup_styles()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=36,
        rightMargin=36,
        topMargin=30,
        bottomMargin=30,
    )
    story = []

    # Header
    story.append(
        Paragraph(
            "Dukbill – Payroll Summary (Broker Essentials Only)", styles["Heading1"]
        )
    )
    story.append(
        Paragraph(
            "This summary includes only information a broker needs for income verification: payslip totals and, if applicable, "
            "supporting timesheet hours. Internal payroll setup details are intentionally excluded.",
            styles["BodyText"],
        )
    )
    story.append(Spacer(1, 12))

    # Get payroll advice data
    advice_data = find_data_by_endpoint(
        all_results, "Report/Payroll/EmployeePayrollAdvice"
    )

    if advice_data.get("Items"):
        rows = []
        for it in advice_data["Items"]:
            emp = it.get("Employee") or {}
            lines = it.get("Lines") or []

            hours = 0.0
            payg = 0.0
            super_amt = 0.0
            for ln in lines:
                cat = (ln.get("PayrollCategory") or {}).get("Type", "")
                amt = float(ln.get("Amount") or 0)
                if cat == "Wage":
                    hours += float(ln.get("Hours") or 0)
                elif cat == "Tax":
                    payg += amt
                elif cat == "Superannuation":
                    super_amt += amt

            rows.append(
                [
                    emp.get("Name", ""),
                    safe_date(it.get("PayPeriodStartDate")),
                    safe_date(it.get("PayPeriodEndDate")),
                    safe_date(it.get("PaymentDate")),
                    money(hours),
                    money(it.get("GrossPay")),
                    money(payg),
                    money(super_amt),
                    money(it.get("NetPay")),
                ]
            )

        add_table(
            story,
            "Payslip Summary",
            rows,
            [
                "Employee",
                "Start",
                "End",
                "Paid",
                "Hours",
                "Gross",
                "PAYG",
                "Super",
                "Net",
            ],
            [90, 60, 60, 60, 45, 60, 55, 55, 55],
            styles,
        )
        story.append(
            Paragraph(
                "Notes: Hours are summed from wage lines only. PAYG may appear negative in source data; totals shown are absolute values.",
                styles["caption"],
            )
        )
        story.append(Spacer(1, 6))

    # Get timesheet data
    timesheet_data = find_data_by_endpoint(all_results, "Payroll/Timesheet")

    if timesheet_data.get("Items"):
        rows = []
        for item in timesheet_data["Items"][:20]:
            emp = item.get("Employee", {}) or {}
            total_hours = 0.0
            for ln in item.get("Lines") or []:
                try:
                    total_hours += float(ln.get("Hours") or 0)
                except Exception as e:
                    print(f"Error parsing hours value {ln.get('Hours')}: {e}")
                    pass

            rows.append(
                [
                    emp.get("Name", ""),
                    safe_date(item.get("StartDate")),
                    safe_date(item.get("EndDate")),
                    money(total_hours),
                ]
            )

        add_table(
            story,
            "Timesheets – Period Hours (Sample)",
            rows,
            ["Employee", "Start", "End", "Total Hours"],
            [180, 70, 70, 70],
            styles,
        )
        story.append(
            Paragraph(
                "Shown for hourly/casual roles to corroborate income patterns. Detailed task-level rows are omitted.",
                styles["caption"],
            )
        )

    if len(story) <= 3:
        story.append(Paragraph("No payroll data available.", styles["BodyText"]))

    doc.build(story)
    buffer.seek(0)
    return buffer.getvalue()


# ==================== SALES PDF ====================


def generate_sales_pdf(all_results):
    """
    Generate Sales Summary PDF from MYOB data
    Returns: PDF as bytes
    """
    buffer = BytesIO()
    styles = setup_styles()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=36,
        rightMargin=36,
        topMargin=30,
        bottomMargin=30,
    )
    story = []

    # Header
    story.append(
        Paragraph("Dukbill – Sales Summary (Broker Essentials)", styles["Heading1"])
    )
    story.append(
        Paragraph(
            "Income verification pack: recent invoices, payments received, and credit refunds. "
            "Sorted newest first. All amounts in AUD.",
            styles["BodyText"],
        )
    )
    story.append(Spacer(1, 12))

    # Collect invoices from multiple endpoints
    all_invoices = []
    for suffix in ["Sale/Invoice", "Sale/Invoice/Professional", "Sale/Invoice/Service"]:
        data = find_data_by_endpoint(all_results, suffix)
        for it in data.get("Items") or []:
            cust = it.get("Customer") or {}
            terms = it.get("Terms") or {}
            all_invoices.append(
                {
                    "Date": safe_date(it.get("Date")),
                    "Number": it.get("Number") or "",
                    "CustomerName": cust.get("Name", "Unknown"),
                    "Status": it.get("Status") or "Unknown",
                    "Total": float(it.get("TotalAmount") or it.get("Subtotal") or 0),
                    "BalanceDue": float(it.get("BalanceDueAmount") or 0),
                    "DueDate": safe_date(terms.get("DueDate")),
                }
            )

    all_invoices.sort(key=lambda x: x["Date"] or "", reverse=True)
    all_invoices = all_invoices[:100]

    # Invoices table
    if all_invoices:
        rows = [
            [
                inv["Date"],
                inv["Number"],
                short(inv["CustomerName"], 32),
                inv["Status"],
                money(inv["Total"]),
                money(inv["BalanceDue"]),
                inv["DueDate"] or "-",
            ]
            for inv in all_invoices
        ]

        add_table(
            story,
            "Invoices – Overview",
            rows,
            ["Date", "#", "Customer", "Status", "Total", "Balance", "Due"],
            [65, 70, 120, 65, 70, 70, 70],
            styles,
        )
        story.append(
            Paragraph(
                "Most recent 100 invoices. Balance = Total - Paid.", styles["caption"]
            )
        )
        story.append(Spacer(1, 6))

    # Customer payments
    payment_data = find_data_by_endpoint(all_results, "Sale/CustomerPayment")
    if payment_data.get("Items"):
        payments = []
        for it in payment_data["Items"]:
            cust = it.get("Customer") or {}
            payments.append(
                {
                    "Date": safe_date(it.get("Date")),
                    "CustomerName": cust.get("Name", "Unknown"),
                    "Amount": float(it.get("AmountReceived") or 0),
                    "Memo": short(it.get("Memo") or "", 60),
                }
            )
        payments.sort(key=lambda x: x["Date"], reverse=True)
        payments = payments[:100]

        rows = [
            [p["Date"], short(p["CustomerName"], 35), money(p["Amount"]), p["Memo"]]
            for p in payments
        ]

        add_table(
            story,
            "Customer Payments Received",
            rows,
            ["Date", "Customer", "Amount", "Memo"],
            [70, 150, 80, 180],
            styles,
        )
        story.append(
            Paragraph(
                "Last 100 payments. Supports income stability proof.", styles["caption"]
            )
        )
        story.append(Spacer(1, 6))

    # Credit refunds
    refund_data = find_data_by_endpoint(all_results, "Sale/CreditRefund")
    if refund_data.get("Items"):
        refunds = []
        for it in refund_data["Items"]:
            cust = it.get("Customer") or {}
            inv = it.get("Invoice") or {}
            acct = it.get("Account") or {}
            refunds.append(
                {
                    "Date": safe_date(it.get("Date")),
                    "CustomerName": cust.get("Name", "Unknown"),
                    "Amount": float(it.get("Amount") or 0),
                    "InvoiceNumber": inv.get("Number") or "",
                    "BankAccount": acct.get("Name") or "Unknown",
                }
            )
        refunds.sort(key=lambda x: x["Date"], reverse=True)
        refunds = refunds[:100]

        rows = [
            [
                r["Date"],
                short(r["CustomerName"], 35),
                money(r["Amount"]),
                r["InvoiceNumber"],
                short(r["BankAccount"], 40),
            ]
            for r in refunds
        ]

        add_table(
            story,
            "Credit Refunds Issued",
            rows,
            ["Date", "Customer", "Amount", "Invoice #", "Bank Account"],
            [70, 150, 80, 80, 140],
            styles,
        )
        story.append(
            Paragraph(
                "Refunds reduce net income. Shown for transparency.", styles["caption"]
            )
        )

    if len(story) <= 3:
        story.append(Paragraph("No sales data available.", styles["BodyText"]))

    doc.build(story)
    buffer.seek(0)
    return buffer.getvalue()


# ==================== BANKING PDF ====================


def generate_banking_pdf(all_results):
    """
    Generate Banking Summary PDF from MYOB data
    Returns: PDF as bytes
    """
    buffer = BytesIO()
    styles = setup_styles()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=36,
        rightMargin=36,
        topMargin=30,
        bottomMargin=30,
    )
    story = []

    story.append(
        Paragraph("Dukbill – Banking Summary (Broker Essentials)", styles["Heading1"])
    )
    story.append(Spacer(1, 12))

    # Bank accounts
    accounts_data = find_data_by_endpoint(all_results, "Banking/BankAccount")
    if accounts_data.get("Items"):
        rows = [
            [
                acc.get("BankAccountName", "Unknown"),
                mask(acc.get("BankAccountNumber", "")),
                acc.get("FinancialInstitution", "Unknown"),
                (acc.get("LastReconciledDate") or "")[:10],
            ]
            for acc in accounts_data["Items"]
        ]

        add_table(
            story,
            "Linked Bank Accounts",
            rows,
            ["Account Name", "Account No.", "Institution", "Last Reconciled"],
            [160, 120, 120, 100],
            styles,
        )
        story.append(
            Paragraph(
                "Account numbers masked for security. Full details in MYOB.",
                styles["caption"],
            )
        )
        story.append(Spacer(1, 6))

    # Statements
    stmt_data = find_data_by_endpoint(all_results, "Banking/Statement")
    if stmt_data.get("Items"):
        rows = [
            [
                (ln.get("Date") or "")[:10],
                short(ln.get("Description", ""), 70),
                signed_amount(ln.get("IsCredit"), ln.get("Amount")),
            ]
            for ln in stmt_data["Items"]
        ]

        add_table(
            story,
            "Bank Statement – All Transactions",
            rows,
            ["Date", "Description", "Amount (±)"],
            [70, 360, 80],
            styles,
        )
        story.append(
            Paragraph(
                "Full history shown. Positive = deposit, Negative = withdrawal.",
                styles["caption"],
            )
        )
        story.append(Spacer(1, 6))

    # Received money
    recv_data = find_data_by_endpoint(all_results, "Banking/ReceiveMoneyTxn")
    if recv_data.get("Items"):
        rows = [
            [
                (txn.get("Date") or "")[:10],
                short((txn.get("Contact") or {}).get("Name", "Unknown"), 50),
                money(txn.get("AmountReceived", 0)),
                txn.get("PaymentMethod", "Unknown"),
            ]
            for txn in recv_data["Items"]
        ]

        add_table(
            story,
            "Incoming Payments",
            rows,
            ["Date", "From", "Amount", "Method"],
            [70, 200, 80, 100],
            styles,
        )
        story.append(
            Paragraph(
                "All money received into accounts. Supports income proof.",
                styles["caption"],
            )
        )
        story.append(Spacer(1, 6))

    # Spent money
    spend_data = find_data_by_endpoint(all_results, "Banking/SpendMoneyTxn")
    if spend_data.get("Items"):
        rows = [
            [
                (txn.get("Date") or "")[:10],
                short((txn.get("Contact") or {}).get("Name", "Unknown"), 50),
                money(txn.get("AmountPaid", 0)),
                txn.get("PaymentMethod", "Unknown"),
            ]
            for txn in spend_data["Items"]
        ]

        add_table(
            story,
            "Outgoing Payments",
            rows,
            ["Date", "To", "Amount", "Method"],
            [70, 200, 80, 100],
            styles,
        )
        story.append(
            Paragraph(
                "All payments made. Helps verify expenses and cash flow.",
                styles["caption"],
            )
        )

    if len(story) <= 2:
        story.append(Paragraph("No banking data found.", styles["BodyText"]))

    doc.build(story)
    buffer.seek(0)
    return buffer.getvalue()


# ==================== PURCHASES PDF ====================


def generate_purchases_pdf(all_results):
    """
    Generate Purchases Summary PDF from MYOB data
    Returns: PDF as bytes
    """
    buffer = BytesIO()
    styles = setup_styles()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=36,
        rightMargin=36,
        topMargin=30,
        bottomMargin=30,
    )
    story = []

    story.append(
        Paragraph("Dukbill – Purchases Summary (Broker Essentials)", styles["Heading1"])
    )
    story.append(
        Paragraph(
            "Broker-focused view: supplier, dates, status, totals, balance due. "
            "Line items capped at 5 per bill for clarity.",
            styles["BodyText"],
        )
    )
    story.append(Spacer(1, 12))

    # Collect bills from multiple endpoints
    all_bills = []
    for suffix in [
        "Purchase/Bill",
        "Purchase/Bill/Item",
        "Purchase/Bill/Professional",
        "Purchase/Bill/Service",
    ]:
        data = find_data_by_endpoint(all_results, suffix)
        for it in data.get("Items") or []:
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
            for ln in (it.get("Lines") or [])[:5]:
                # acct = ln.get("Account") or {}
                item = ln.get("Item") or {}
                desc = ln.get("Description") or item.get("Name") or ""
                bill["Lines"].append(
                    {
                        "Description": short(desc, 50),
                        "Qty": ln.get("BillQuantity") or ln.get("UnitCount") or 0,
                        "UnitPrice": float(ln.get("UnitPrice") or 0),
                        "LineTotal": float(ln.get("Total") or 0),
                        "TaxCode": ((ln.get("TaxCode") or {}).get("Code")) or "",
                    }
                )
            all_bills.append(bill)

    all_bills.sort(key=lambda b: b["Date"] or "", reverse=True)
    all_bills = all_bills[:50]

    # Bills overview
    if all_bills:
        rows = [
            [
                b["Date"],
                short(b["SupplierName"], 35),
                b["Status"],
                money(b["TotalAmount"]),
                money(b["AppliedToDate"]),
                money(b["BalanceDueAmount"]),
                b["DueDate"] or "-",
            ]
            for b in all_bills
        ]

        add_table(
            story,
            "Bills – Overview",
            rows,
            ["Date", "Supplier", "Status", "Total", "Paid", "Balance", "Due"],
            [70, 120, 70, 70, 65, 70, 60],
            styles,
        )
        story.append(
            Paragraph(
                "Most recent 50 bills shown. Sorted by date (newest first). Balance Due = Total - Paid. All amounts in AUD.",
                styles["caption"],
            )
        )
        story.append(Spacer(1, 6))

        # Line items sample
        line_rows = []
        for b in all_bills[:10]:
            for ln in b["Lines"]:
                line_rows.append(
                    [
                        b["Date"],
                        short(b["SupplierName"], 25),
                        ln["Description"],
                        f"{ln['Qty']:.2f}",
                        money(ln["UnitPrice"]),
                        money(ln["LineTotal"]),
                        ln["TaxCode"],
                    ]
                )

        if line_rows:
            add_table(
                story,
                "Line Items – Sample (First 10 Bills)",
                line_rows,
                [
                    "Date",
                    "Supplier",
                    "Description",
                    "Qty",
                    "Unit $",
                    "Line Total",
                    "Tax",
                ],
                [60, 90, 150, 45, 60, 70, 45],
                styles,
            )
            story.append(
                Paragraph(
                    "Truncated for readability. Full details available in MYOB exports.",
                    styles["caption"],
                )
            )

    if len(story) <= 3:
        story.append(Paragraph("No purchase bill data available.", styles["BodyText"]))

    doc.build(story)
    buffer.seek(0)
    return buffer.getvalue()
