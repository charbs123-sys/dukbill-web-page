import json
from datetime import datetime
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
import io
from datetime import datetime
from reportlab.lib.pagesizes import letter, A4
import os

from Backend.Database.S3_init import s3
from Backend.Database.S3_utils import upload_pdf_to_s3

s3_client = s3

def format_date(date_str):
    """Format Xero date string to readable format"""
    if not date_str:
        return "N/A"
    if isinstance(date_str, str) and "/Date(" in date_str:
        timestamp = date_str.split("(")[1].split(")")[0]
        if "+" in timestamp or "-" in timestamp:
            timestamp = timestamp.split("+")[0].split("-")[0]
        try:
            dt = datetime.fromtimestamp(int(timestamp) / 1000)
            return dt.strftime("%Y-%m-%d")
        except:
            return date_str
    return date_str


def format_currency(amount):
    """Format currency amounts"""
    if amount is None:
        return "$0.00"
    try:
        return f"${float(amount):,.2f}"
    except:
        return str(amount)


def create_header(title, org_name):
    """Create a header section for the report"""
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Title'],
        fontSize=24,
        textColor=colors.HexColor('#1a5490'),
        spaceAfter=6,
    )
    subtitle_style = ParagraphStyle(
        'CustomSubtitle',
        parent=styles['Normal'],
        fontSize=12,
        textColor=colors.grey,
        spaceAfter=20,
    )
    
    story = []
    story.append(Paragraph(title, title_style))
    story.append(Paragraph(f"Organization: {org_name}", subtitle_style))
    story.append(Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", subtitle_style))
    story.append(Spacer(1, 0.2*inch))
    
    return story


def generate_accounts_report(data, output_file, hashed_email):
    """Generate Accounts PDF report and save to S3"""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()
    
    org_name = data.get('organization', 'Unknown Organization')
    story.extend(create_header("Chart of Accounts Report", org_name))
    
    accounts_total = data['preview']['settings'].get('accounts_total', 0)
    story.append(Paragraph(f"<b>Total Accounts:</b> {accounts_total}", styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    
    sample_account = data['preview']['settings'].get('accounts')
    if sample_account:
        story.append(Paragraph("<b>Sample Account Details:</b>", styles['Heading2']))
        story.append(Spacer(1, 0.1*inch))
        
        account_data = [
            ['Field', 'Value'],
            ['Account Code', sample_account.get('Code', 'N/A')],
            ['Account Name', sample_account.get('Name', 'N/A')],
            ['Type', sample_account.get('Type', 'N/A')],
            ['Class', sample_account.get('Class', 'N/A')],
            ['Status', sample_account.get('Status', 'N/A')],
            ['Tax Type', sample_account.get('TaxType', 'N/A')],
            ['Currency', sample_account.get('CurrencyCode', 'N/A')],
            ['Bank Account Number', sample_account.get('BankAccountNumber', 'N/A')],
            ['Has Attachments', str(sample_account.get('HasAttachments', False))],
        ]
        
        table = Table(account_data, colWidths=[2.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ]))
        story.append(table)
    
    story.append(Spacer(1, 0.3*inch))
    tax_rates_total = data['preview']['settings'].get('tax_rates_total', 0)
    story.append(Paragraph(f"<b>Tax Rates Available:</b> {tax_rates_total}", styles['Normal']))
    
    sample_tax = data['preview']['settings'].get('tax_rates')
    if sample_tax:
        story.append(Spacer(1, 0.1*inch))
        story.append(Paragraph(f"Sample: {sample_tax.get('Name', 'N/A')} - {sample_tax.get('DisplayTaxRate', 0)}%", styles['Normal']))
    
    story.append(Spacer(1, 0.3*inch))
    tracking_total = data['preview']['settings'].get('tracking_categories_total', 0)
    story.append(Paragraph(f"<b>Tracking Categories:</b> {tracking_total}", styles['Normal']))
    
    sample_tracking = data['preview']['settings'].get('tracking_categories')
    if sample_tracking:
        story.append(Spacer(1, 0.1*inch))
        story.append(Paragraph(f"Sample: {sample_tracking.get('Name', 'N/A')}", styles['Normal']))
        options = sample_tracking.get('Options', [])
        if options:
            story.append(Paragraph(f"Options: {', '.join([opt.get('Name', '') for opt in options])}", styles['Normal']))
    
    doc.build(story)
    return upload_pdf_to_s3(buffer, hashed_email, output_file)


def generate_transactions_report(data, output_file, hashed_email):
    """Generate Bank Transactions PDF report and save to S3"""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()
    
    org_name = data.get('organization', 'Unknown Organization')
    story.extend(create_header("Bank Transactions Report", org_name))
    
    transactions_total = data['preview']['transactions'].get('bank_transactions_total', 0)
    story.append(Paragraph(f"<b>Total Bank Transactions:</b> {transactions_total}", styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    
    sample_txn = data['preview']['transactions'].get('bank_transactions')
    if sample_txn:
        story.append(Paragraph("<b>Sample Transaction Details:</b>", styles['Heading2']))
        story.append(Spacer(1, 0.1*inch))
        
        txn_data = [
            ['Field', 'Value'],
            ['Transaction ID', sample_txn.get('BankTransactionID', 'N/A')[:20] + '...'],
            ['Type', sample_txn.get('Type', 'N/A')],
            ['Status', sample_txn.get('Status', 'N/A')],
            ['Date', format_date(sample_txn.get('Date'))],
            ['Reference', sample_txn.get('Reference', 'N/A')],
            ['Contact', sample_txn.get('Contact', {}).get('Name', 'N/A')],
            ['Subtotal', format_currency(sample_txn.get('SubTotal'))],
            ['Total Tax', format_currency(sample_txn.get('TotalTax'))],
            ['Total', format_currency(sample_txn.get('Total'))],
            ['Currency', sample_txn.get('CurrencyCode', 'N/A')],
            ['Reconciled', str(sample_txn.get('IsReconciled', False))],
            ['Has Attachments', str(sample_txn.get('HasAttachments', False))],
        ]
        
        table = Table(txn_data, colWidths=[2.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ]))
        story.append(table)
        
        line_items = sample_txn.get('LineItems', [])
        if line_items:
            story.append(Spacer(1, 0.2*inch))
            story.append(Paragraph("<b>Line Items:</b>", styles['Heading3']))
            story.append(Spacer(1, 0.1*inch))
            
            for idx, item in enumerate(line_items, 1):
                story.append(Paragraph(f"<b>Item {idx}:</b>", styles['Normal']))
                story.append(Paragraph(f"Description: {item.get('Description', 'N/A')}", styles['Normal']))
                story.append(Paragraph(f"Amount: {format_currency(item.get('LineAmount'))}", styles['Normal']))
                story.append(Paragraph(f"Account Code: {item.get('AccountCode', 'N/A')}", styles['Normal']))
                story.append(Spacer(1, 0.1*inch))
    
    story.append(Spacer(1, 0.3*inch))
    story.append(Paragraph("<b>Other Transaction Types:</b>", styles['Heading2']))
    story.append(Spacer(1, 0.1*inch))
    
    other_txns = [
        ('Manual Journals', data['preview']['transactions'].get('manual_journals_total', 0)),
        ('Overpayments', data['preview']['transactions'].get('overpayments_total', 0)),
        ('Prepayments', data['preview']['transactions'].get('prepayments_total', 0)),
    ]
    
    for txn_type, count in other_txns:
        story.append(Paragraph(f"{txn_type}: {count}", styles['Normal']))
    
    doc.build(story)
    return upload_pdf_to_s3(buffer, hashed_email, output_file)


def generate_payments_report(data, output_file, hashed_email):
    """Generate Payments PDF report and save to S3"""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()
    
    org_name = data.get('organization', 'Unknown Organization')
    story.extend(create_header("Payments Report", org_name))
    
    payments_total = data['preview']['transactions'].get('payments_total', 0)
    story.append(Paragraph(f"<b>Total Payments:</b> {payments_total}", styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    
    sample_payment = data['preview']['transactions'].get('payments')
    if sample_payment:
        story.append(Paragraph("<b>Sample Payment Details:</b>", styles['Heading2']))
        story.append(Spacer(1, 0.1*inch))
        
        payment_data = [
            ['Field', 'Value'],
            ['Payment ID', sample_payment.get('PaymentID', 'N/A')[:20] + '...'],
            ['Date', format_date(sample_payment.get('Date'))],
            ['Amount', format_currency(sample_payment.get('Amount'))],
            ['Bank Amount', format_currency(sample_payment.get('BankAmount'))],
            ['Reference', sample_payment.get('Reference', 'N/A')],
            ['Payment Type', sample_payment.get('PaymentType', 'N/A')],
            ['Status', sample_payment.get('Status', 'N/A')],
            ['Reconciled', str(sample_payment.get('IsReconciled', False))],
        ]
        
        invoice = sample_payment.get('Invoice', {})
        if invoice:
            contact = invoice.get('Contact', {})
            payment_data.extend([
                ['Invoice Contact', contact.get('Name', 'N/A')],
                ['Invoice Type', invoice.get('Type', 'N/A')],
            ])
        
        table = Table(payment_data, colWidths=[2.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ]))
        story.append(table)
    
    doc.build(story)
    return upload_pdf_to_s3(buffer, hashed_email, output_file)


def generate_credit_notes_report(data, output_file, hashed_email):
    """Generate Credit Notes PDF report and save to S3"""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()
    
    org_name = data.get('organization', 'Unknown Organization')
    story.extend(create_header("Credit Notes Report", org_name))
    
    credit_notes_total = data['preview']['transactions'].get('credit_notes_total', 0)
    story.append(Paragraph(f"<b>Total Credit Notes:</b> {credit_notes_total}", styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    
    sample_cn = data['preview']['transactions'].get('credit_notes')
    if sample_cn:
        story.append(Paragraph("<b>Sample Credit Note Details:</b>", styles['Heading2']))
        story.append(Spacer(1, 0.1*inch))
        
        cn_data = [
            ['Field', 'Value'],
            ['Credit Note ID', sample_cn.get('CreditNoteID', 'N/A')[:20] + '...'],
            ['Credit Note Number', sample_cn.get('CreditNoteNumber', 'N/A')],
            ['Type', sample_cn.get('Type', 'N/A')],
            ['Status', sample_cn.get('Status', 'N/A')],
            ['Date', format_date(sample_cn.get('Date'))],
            ['Due Date', format_date(sample_cn.get('DueDate'))],
            ['Contact', sample_cn.get('Contact', {}).get('Name', 'N/A')],
            ['Reference', sample_cn.get('Reference', 'N/A') or 'None'],
            ['Subtotal', format_currency(sample_cn.get('SubTotal'))],
            ['Total Tax', format_currency(sample_cn.get('TotalTax'))],
            ['Total', format_currency(sample_cn.get('Total'))],
            ['Remaining Credit', format_currency(sample_cn.get('RemainingCredit'))],
            ['Currency', sample_cn.get('CurrencyCode', 'N/A')],
            ['Has Attachments', str(sample_cn.get('HasAttachments', False))],
        ]
        
        table = Table(cn_data, colWidths=[2.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ]))
        story.append(table)
        
        line_items = sample_cn.get('LineItems', [])
        if line_items:
            story.append(Spacer(1, 0.2*inch))
            story.append(Paragraph("<b>Line Items:</b>", styles['Heading3']))
            story.append(Spacer(1, 0.1*inch))
            
            for idx, item in enumerate(line_items, 1):
                story.append(Paragraph(f"<b>Item {idx}:</b>", styles['Normal']))
                story.append(Paragraph(f"Description: {item.get('Description', 'N/A')}", styles['Normal']))
                story.append(Paragraph(f"Amount: {format_currency(item.get('LineAmount'))}", styles['Normal']))
                story.append(Paragraph(f"Account Code: {item.get('AccountCode', 'N/A')}", styles['Normal']))
                story.append(Spacer(1, 0.1*inch))
        
        allocations = sample_cn.get('Allocations', [])
        if allocations:
            story.append(Spacer(1, 0.2*inch))
            story.append(Paragraph("<b>Allocations:</b>", styles['Heading3']))
            story.append(Spacer(1, 0.1*inch))
            
            for idx, alloc in enumerate(allocations, 1):
                story.append(Paragraph(f"Allocation {idx}: {format_currency(alloc.get('Amount'))} on {format_date(alloc.get('Date'))}", styles['Normal']))
    
    doc.build(story)
    return upload_pdf_to_s3(buffer, hashed_email, output_file)


def generate_payroll_report(data, output_file, hashed_email):
    """Generate combined Payroll PDF report and save to S3"""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()
    
    org_name = data.get('organization', 'Unknown Organization')
    story.extend(create_header("Payroll Report", org_name))
    
    # EMPLOYEES
    story.append(Paragraph("<b>EMPLOYEES</b>", styles['Heading1']))
    story.append(Spacer(1, 0.1*inch))
    
    employees_total = data['preview']['payroll'].get('employees_total', 0)
    story.append(Paragraph(f"<b>Total Employees:</b> {employees_total}", styles['Normal']))
    story.append(Spacer(1, 0.2*inch))
    
    sample_employee = data['preview']['payroll'].get('employees')
    if sample_employee:
        story.append(Paragraph("<b>Sample Employee:</b>", styles['Heading2']))
        story.append(Spacer(1, 0.1*inch))
        
        emp_data = [
            ['Field', 'Value'],
            ['Employee ID', sample_employee.get('EmployeeID', 'N/A')[:20] + '...'],
            ['Name', f"{sample_employee.get('FirstName', '')} {sample_employee.get('LastName', '')}"],
            ['Email', sample_employee.get('Email', 'N/A')],
            ['Status', sample_employee.get('Status', 'N/A')],
            ['Date of Birth', format_date(sample_employee.get('DateOfBirth'))],
            ['Gender', sample_employee.get('Gender', 'N/A')],
            ['Phone', sample_employee.get('Phone', 'N/A')],
            ['Mobile', sample_employee.get('Mobile', 'N/A')],
            ['Start Date', format_date(sample_employee.get('StartDate'))],
        ]
        
        table = Table(emp_data, colWidths=[2.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ]))
        story.append(table)
    
    # PAY RUNS
    story.append(Spacer(1, 0.4*inch))
    story.append(Paragraph("<b>PAY RUNS</b>", styles['Heading1']))
    story.append(Spacer(1, 0.1*inch))
    
    payruns_total = data['preview']['payroll'].get('payruns_total', 0)
    story.append(Paragraph(f"<b>Total Pay Runs:</b> {payruns_total}", styles['Normal']))
    story.append(Spacer(1, 0.2*inch))
    
    sample_payrun = data['preview']['payroll'].get('payruns')
    if sample_payrun:
        story.append(Paragraph("<b>Sample Pay Run:</b>", styles['Heading2']))
        story.append(Spacer(1, 0.1*inch))
        
        payrun_data = [
            ['Field', 'Value'],
            ['Pay Run ID', sample_payrun.get('PayRunID', 'N/A')[:20] + '...'],
            ['Status', sample_payrun.get('PayRunStatus', 'N/A')],
            ['Period Start', format_date(sample_payrun.get('PayRunPeriodStartDate'))],
            ['Period End', format_date(sample_payrun.get('PayRunPeriodEndDate'))],
            ['Payment Date', format_date(sample_payrun.get('PaymentDate'))],
            ['Wages', format_currency(sample_payrun.get('Wages'))],
            ['Deductions', format_currency(sample_payrun.get('Deductions'))],
            ['Tax', format_currency(sample_payrun.get('Tax'))],
            ['Super', format_currency(sample_payrun.get('Super'))],
            ['Net Pay', format_currency(sample_payrun.get('NetPay'))],
        ]
        
        table = Table(payrun_data, colWidths=[2.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ]))
        story.append(table)
    
    # PAYSLIPS
    story.append(Spacer(1, 0.4*inch))
    story.append(Paragraph("<b>PAYSLIPS</b>", styles['Heading1']))
    story.append(Spacer(1, 0.1*inch))
    
    payslips_total = data['preview']['payroll'].get('payslips_total', 0)
    story.append(Paragraph(f"<b>Total Payslips:</b> {payslips_total}", styles['Normal']))
    story.append(Spacer(1, 0.2*inch))
    
    sample_payslip = data['preview']['payroll'].get('payslips')
    if sample_payslip:
        story.append(Paragraph("<b>Sample Payslip:</b>", styles['Heading2']))
        story.append(Spacer(1, 0.1*inch))
        
        payslip_data = [
            ['Field', 'Value'],
            ['Payslip ID', sample_payslip.get('PayslipID', 'N/A')[:20] + '...'],
            ['Employee', f"{sample_payslip.get('FirstName', '')} {sample_payslip.get('LastName', '')}"],
            ['Employee ID', sample_payslip.get('EmployeeID', 'N/A')[:20] + '...'],
            ['Wages', format_currency(sample_payslip.get('Wages'))],
            ['Deductions', format_currency(sample_payslip.get('Deductions'))],
            ['Tax', format_currency(sample_payslip.get('Tax'))],
            ['Super', format_currency(sample_payslip.get('Super'))],
            ['Reimbursements', format_currency(sample_payslip.get('Reimbursements'))],
            ['Net Pay', format_currency(sample_payslip.get('NetPay'))],
        ]
        
        table = Table(payslip_data, colWidths=[2.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ]))
        story.append(table)
    
    doc.build(story)
    return upload_pdf_to_s3(buffer, hashed_email, output_file)


def generate_invoices_report(data, output_file, hashed_email):
    """Generate Invoices PDF report and save to S3"""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()
    
    org_name = data.get('organization', 'Unknown Organization')
    story.extend(create_header("Invoices Report", org_name))
    
    invoices_total = data['preview']['transactions'].get('invoices_total', 0)
    story.append(Paragraph(f"<b>Total Invoices:</b> {invoices_total}", styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    
    sample_invoice = data['preview']['transactions'].get('invoices')
    if sample_invoice:
        story.append(Paragraph("<b>Sample Invoice Details:</b>", styles['Heading2']))
        story.append(Spacer(1, 0.1*inch))
        
        invoice_data = [
            ['Field', 'Value'],
            ['Invoice ID', sample_invoice.get('InvoiceID', 'N/A')[:20] + '...'],
            ['Invoice Number', sample_invoice.get('InvoiceNumber', 'N/A')],
            ['Type', sample_invoice.get('Type', 'N/A')],
            ['Status', sample_invoice.get('Status', 'N/A')],
            ['Date', format_date(sample_invoice.get('Date'))],
            ['Due Date', format_date(sample_invoice.get('DueDate'))],
            ['Contact', sample_invoice.get('Contact', {}).get('Name', 'N/A')],
            ['Reference', sample_invoice.get('Reference', 'N/A') or 'None'],
            ['Subtotal', format_currency(sample_invoice.get('SubTotal'))],
            ['Total Tax', format_currency(sample_invoice.get('TotalTax'))],
            ['Total', format_currency(sample_invoice.get('Total'))],
            ['Amount Due', format_currency(sample_invoice.get('AmountDue'))],
            ['Amount Paid', format_currency(sample_invoice.get('AmountPaid'))],
            ['Currency', sample_invoice.get('CurrencyCode', 'N/A')],
        ]
        
        table = Table(invoice_data, colWidths=[2.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ]))
        story.append(table)
        
        line_items = sample_invoice.get('LineItems', [])
        if line_items:
            story.append(Spacer(1, 0.2*inch))
            story.append(Paragraph("<b>Line Items:</b>", styles['Heading3']))
            story.append(Spacer(1, 0.1*inch))
            
            for idx, item in enumerate(line_items, 1):
                story.append(Paragraph(f"<b>Item {idx}:</b>", styles['Normal']))
                story.append(Paragraph(f"Description: {item.get('Description', 'N/A')}", styles['Normal']))
                story.append(Paragraph(f"Quantity: {item.get('Quantity', 'N/A')}", styles['Normal']))
                story.append(Paragraph(f"Unit Amount: {format_currency(item.get('UnitAmount'))}", styles['Normal']))
                story.append(Paragraph(f"Line Amount: {format_currency(item.get('LineAmount'))}", styles['Normal']))
                story.append(Spacer(1, 0.1*inch))
    
    doc.build(story)
    return upload_pdf_to_s3(buffer, hashed_email, output_file)


def generate_reports_summary(data, output_file, hashed_email):
    """Generate Financial Reports Summary PDF and save to S3"""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()
    
    org_name = data.get('organization', 'Unknown Organization')
    story.extend(create_header("Financial Reports Summary", org_name))
    
    # P&L Summary
    pl_data = data['preview'].get('reports', {}).get('profit_loss')
    if pl_data:
        story.append(Paragraph("<b>PROFIT & LOSS</b>", styles['Heading1']))
        story.append(Spacer(1, 0.1*inch))
        
        reports = pl_data.get('Reports', [])
        if reports:
            report = reports[0]
            story.append(Paragraph(f"Report Title: {report.get('ReportTitles', ['N/A'])[0]}", styles['Normal']))
            story.append(Paragraph(f"Report Date: {report.get('ReportDate', 'N/A')}", styles['Normal']))
            story.append(Spacer(1, 0.2*inch))
            
            rows = report.get('Rows', [])
            for row in rows[:5]:
                if row.get('RowType') == 'Header':
                    story.append(Paragraph(f"<b>{row.get('Title', '')}</b>", styles['Heading3']))
                elif row.get('RowType') == 'Section':
                    story.append(Paragraph(row.get('Title', ''), styles['Normal']))
                    cells = row.get('Cells', [])
                    if cells:
                        value = cells[0].get('Value', 'N/A')
                        story.append(Paragraph(f"Amount: {format_currency(value)}", styles['Normal']))
                story.append(Spacer(1, 0.1*inch))
    else:
        story.append(Paragraph("<b>PROFIT & LOSS</b>", styles['Heading1']))
        story.append(Paragraph("No P&L data available", styles['Normal']))
    
    story.append(Spacer(1, 0.3*inch))
    
    # Balance Sheet Summary
    bs_data = data['preview'].get('reports', {}).get('balance_sheet')
    if bs_data:
        story.append(Paragraph("<b>BALANCE SHEET</b>", styles['Heading1']))
        story.append(Spacer(1, 0.1*inch))
        
        reports = bs_data.get('Reports', [])
        if reports:
            report = reports[0]
            story.append(Paragraph(f"Report Title: {report.get('ReportTitles', ['N/A'])[0]}", styles['Normal']))
            story.append(Paragraph(f"Report Date: {report.get('ReportDate', 'N/A')}", styles['Normal']))
            story.append(Spacer(1, 0.2*inch))
            
            rows = report.get('Rows', [])
            for row in rows[:5]:
                if row.get('RowType') == 'Header':
                    story.append(Paragraph(f"<b>{row.get('Title', '')}</b>", styles['Heading3']))
                elif row.get('RowType') == 'Section':
                    story.append(Paragraph(row.get('Title', ''), styles['Normal']))
                    cells = row.get('Cells', [])
                    if cells:
                        value = cells[0].get('Value', 'N/A')
                        story.append(Paragraph(f"Amount: {format_currency(value)}", styles['Normal']))
                story.append(Spacer(1, 0.1*inch))
    else:
        story.append(Paragraph("<b>BALANCE SHEET</b>", styles['Heading1']))
        story.append(Paragraph("No Balance Sheet data available", styles['Normal']))
    
    doc.build(story)
    return upload_pdf_to_s3(buffer, hashed_email, output_file)


def generate_bank_transfers_report(data, output_file, hashed_email):
    """Generate Bank Transfers PDF report and save to S3"""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()
    
    org_name = data.get('organization', 'Unknown Organization')
    story.extend(create_header("Bank Transfers Report", org_name))
    
    transfers_total = data['preview']['transactions'].get('bank_transfers_total', 0)
    story.append(Paragraph(f"<b>Total Bank Transfers:</b> {transfers_total}", styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    
    sample_transfer = data['preview']['transactions'].get('bank_transfers')
    if sample_transfer:
        story.append(Paragraph("<b>Sample Bank Transfer Details:</b>", styles['Heading2']))
        story.append(Spacer(1, 0.1*inch))
        
        transfer_data = [
            ['Field', 'Value'],
            ['Transfer ID', sample_transfer.get('BankTransferID', 'N/A')[:20] + '...'],
            ['Date', format_date(sample_transfer.get('Date'))],
            ['Amount', format_currency(sample_transfer.get('Amount'))],
            ['From Account', sample_transfer.get('FromBankAccount', {}).get('Name', 'N/A')],
            ['To Account', sample_transfer.get('ToBankAccount', {}).get('Name', 'N/A')],
            ['Reference', sample_transfer.get('Reference', 'N/A') or 'None'],
            ['Currency', sample_transfer.get('CurrencyRate', 'N/A')],
        ]
        
        table = Table(transfer_data, colWidths=[2.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ]))
        story.append(table)
    
    doc.build(story)
    return upload_pdf_to_s3(buffer, hashed_email, output_file)