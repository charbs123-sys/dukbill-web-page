# generate_payroll_summary_broker_only.py
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
    # Core broker-relevant sources:
    "advice": base / "Report_Payroll_EmployeePayrollAdvice.json",  # payslip-style
    "timesheet": base / "Payroll_Timesheet.json",                  # optional support evidence for casual/hourly
}
output_pdf = base / "Broker_Payroll_Summary.pdf"

# ----------------------------
# UTILS
# ----------------------------
def load_json(path: Path):
    if not path.exists():
        print(f"⚠️ Missing file: {path.name}")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def safe_date(s):
    return (s or "")[:10]

def f2(x):
    try:
        return f"{float(x):,.2f}"
    except Exception:
        return "0.00"

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
# PDF SETUP
# ----------------------------
styles = getSampleStyleSheet()
styles.add(ParagraphStyle(name="caption", fontSize=8.5, textColor=colors.HexColor("#666")))
doc = SimpleDocTemplate(str(output_pdf), pagesize=A4, leftMargin=36, rightMargin=36, topMargin=30, bottomMargin=30)
story = []

story.append(Paragraph("Dukbill — Payroll Summary (Broker Essentials Only)", styles["Heading1"]))
story.append(Paragraph(
    "This summary includes only information a broker needs for income verification: payslip totals and, if applicable, "
    "supporting timesheet hours. Internal payroll setup details are intentionally excluded.",
    styles["BodyText"]
))
story.append(Spacer(1, 12))

# ----------------------------
# LOAD JSON DATA
# ----------------------------
J = {k: load_json(v) for k, v in files.items()}

# ----------------------------
# EMPLOYEE PAYROLL ADVICE (Payslips)
# ----------------------------
ad = J.get("advice", {})
if ad.get("Items"):
    rows = []
    for it in ad["Items"]:
        emp = it.get("Employee") or {}
        lines = it.get("Lines") or []

        # Derive Hours (sum of Wage lines), PAYG (sum of Tax lines), Super (sum of Super lines)
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

        rows.append([
            emp.get("Name",""),
            safe_date(it.get("PayPeriodStartDate")),
            safe_date(it.get("PayPeriodEndDate")),
            safe_date(it.get("PaymentDate")),
            f2(hours),
            f2(it.get("GrossPay")),
            f2(payg),
            f2(super_amt),
            f2(it.get("NetPay")),
        ])

    # Tight, A4-safe widths
    add_table(
        story,
        "Payslip Summary",
        rows,
        ["Employee", "Start", "End", "Paid", "Hours", "Gross", "PAYG", "Super", "Net"],
        col_widths=[90,60,60,60,45,60,55,55,55]
    )
    story.append(Paragraph(
        "Notes: Hours are summed from wage lines only. PAYG may appear negative in source data; totals shown are absolute values.",
        styles["caption"]
    ))
    story.append(Spacer(1, 6))

# ----------------------------
# TIMESHEETS (optional supporting evidence)
# ----------------------------
ts = J.get("timesheet", {})
if ts.get("Items"):
    rows = []
    for item in ts["Items"][:20]:
        emp = item.get("Employee", {}) or {}
        # Sum hours across lines for the period to avoid page-wide tables
        total_hours = 0.0
        for ln in (item.get("Lines") or []):
            try:
                total_hours += float(ln.get("Hours") or 0)
            except Exception:
                pass

        rows.append([
            emp.get("Name",""),
            safe_date(item.get("StartDate")),
            safe_date(item.get("EndDate")),
            f2(total_hours),
        ])

    add_table(
        story,
        "Timesheets — Period Hours (Sample)",
        rows,
        ["Employee", "Start", "End", "Total Hours"],
        col_widths=[180,70,70,70]
    )
    story.append(Paragraph(
        "Shown for hourly/casual roles to corroborate income patterns. Detailed task-level rows are omitted.",
        styles["caption"]
    ))
    story.append(Spacer(1, 6))

# ----------------------------
# FINALIZE PDF
# ----------------------------
if len(story) <= 3:
    story.append(Paragraph("No payroll data available.", styles["BodyText"]))

doc.build(story)
print(f"✅ PDF created successfully: {output_pdf}")
