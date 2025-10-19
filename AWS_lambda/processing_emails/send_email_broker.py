import base64
import zipfile
import io
from reportlab.lib.utils import ImageReader
from reportlab.lib.units import inch
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable, Image, PageBreak
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from collections import defaultdict
import urllib.request

category_instructions = {
    # Income Documents
    "Payslips": (
        "Steps to access Payslips:\n"
        "1. Log into your employer's payroll portal (e.g., Workday, ADP).\n"
        "2. Navigate to 'Payslips' or 'Pay History'.\n"
        "3. Download the most recent 2–3 payslips as PDFs.\n"
        "4. Ensure they include your name, pay period, and employer details."
    ),
    "PAYG Summary": (
        "Steps to access PAYG Summary:\n"
        "1. Log into your myGov account linked with the ATO.\n"
        "2. Go to 'Income Statements and PAYG Summaries'.\n"
        "3. Download the summary for the most recent financial year as a PDF."
    ),
    "Tax Return": (
        "Steps to access Tax Return:\n"
        "1. Log into your myGov account linked to the ATO.\n"
        "2. Navigate to 'Tax' > 'Lodged Returns'.\n"
        "3. Download the most recent tax return lodged as a PDF."
    ),
    "Notice of Assessment": (
        "Steps to access Notice of Assessment:\n"
        "1. Log into your myGov account linked to the ATO.\n"
        "2. Go to 'Tax' > 'View Notices of Assessment'.\n"
        "3. Download the most recent assessment notice as a PDF."
    ),
    "Employment Contract": (
        "Steps to access Employment Contract:\n"
        "1. Check your onboarding email or HR system (e.g., Employment Hero).\n"
        "2. Download or request a copy of the contract from your employer.\n"
        "3. Ensure it includes position, start date, and salary details.\n"
        "4. Save the document as a PDF."
    ),
    "Employment Letter": (
        "Steps to access Employment Letter:\n"
        "1. Request a letter from your employer or HR department.\n"
        "2. Ensure it’s signed, dated, and includes your position and salary.\n"
        "3. Save or scan the letter clearly as a PDF."
    ),

    # Bank & Financial Documents
    "Bank Statements": (
        "Steps to access Bank Statements:\n"
        "1. Log into your bank’s online banking portal (e.g., CBA, ANZ).\n"
        "2. Navigate to 'Statements' or 'Documents'.\n"
        "3. Download the last 3 months of statements as PDFs."
    ),
    "Credit Card Statements": (
        "Steps to access Credit Card Statements:\n"
        "1. Log into your online credit card account.\n"
        "2. Find the 'Statements' or 'Billing History' section.\n"
        "3. Download the most recent 1–3 statements as PDFs."
    ),
    "Loan Statements": (
        "Steps to access Loan Statements:\n"
        "1. Log into your loan provider’s portal (e.g., for car, home, or personal loans).\n"
        "2. Navigate to 'Statements' or 'Repayment History'.\n"
        "3. Download the current statement showing remaining balance."
    ),
    "ATO Debt Statement": (
        "Steps to access ATO Debt Statement:\n"
        "1. Log into your myGov account linked to the ATO.\n"
        "2. Go to 'Tax' > 'Account Balance'.\n"
        "3. Download a statement of account or summary showing your debt."
    ),
    "HECS/HELP Debt": (
        "Steps to access HECS/HELP Debt:\n"
        "1. Log into your myGov account linked with the ATO.\n"
        "2. Select 'Loan Accounts' to view your HELP balance.\n"
        "3. Download or screenshot your current loan balance."
    ),

    # ID & Verification Documents
    "Driver’s Licence": (
        "Steps to access Driver’s Licence:\n"
        "1. Locate your physical licence.\n"
        "2. Scan or photograph both sides clearly.\n"
        "3. Save the image as a PDF or JPEG with full name and expiry visible."
    ),
    "Passport": (
        "Steps to access Passport:\n"
        "1. Locate your physical passport.\n"
        "2. Scan the photo ID page clearly.\n"
        "3. Save as a PDF or JPEG including your name, photo, and passport number."
    ),
    "Medicare Card": (
        "Steps to access Medicare Card:\n"
        "1. Locate your physical Medicare card.\n"
        "2. Alternatively, log into myGov and access your digital card.\n"
        "3. Screenshot or save the card image clearly."
    ),
    "Birth Certificate": (
        "Steps to access Birth Certificate:\n"
        "1. Locate your original or certified copy of the birth certificate.\n"
        "2. If lost, order a replacement from your state’s registry (e.g., NSW Registry of Births).\n"
        "3. Scan and save as a clear PDF."
    ),
    "Citizenship Certificate": (
        "Steps to access Citizenship Certificate:\n"
        "1. Locate your original document.\n"
        "2. If needed, request a replacement from the Department of Home Affairs.\n"
        "3. Scan and save as a clear PDF."
    ),
    "VOI Certificate": (
        "Steps to access VOI Certificate:\n"
        "1. Retrieve your VOI certificate from providers like IDme or ZipID.\n"
        "2. Scan or download the full PDF version.\n"
        "3. Ensure it includes your verified photo and reference number."
    ),

    # Property-Related Documents
    "Contract of Sale": (
        "Steps to access Contract of Sale:\n"
        "1. Request a copy from your real estate agent or conveyancer.\n"
        "2. Ensure all pages are included and legible.\n"
        "3. Save as a complete PDF."
    ),
    "Building Contract": (
        "Steps to access Building Contract:\n"
        "1. Request a signed copy from your builder.\n"
        "2. Ensure it includes cost breakdown, scope, and signatures.\n"
        "3. Save as a PDF document."
    ),
    "Plans and Specifications": (
        "Steps to access Plans and Specifications:\n"
        "1. Request plans from your architect or builder.\n"
        "2. Include site plans, elevations, and specifications.\n"
        "3. Save all documents clearly as PDFs."
    ),
    "Council Approval": (
        "Steps to access Council Approval:\n"
        "1. Obtain a copy of your DA or CDC approval from your builder or council.\n"
        "2. Include stamped plans and approval letters.\n"
        "3. Save all as a consolidated PDF."
    ),
    "Deposit Receipt": (
        "Steps to access Deposit Receipt:\n"
        "1. Obtain a receipt from your conveyancer or bank showing deposit payment.\n"
        "2. Ensure it shows amount, date, and reference number.\n"
        "3. Save as a PDF."
    ),
    "Transfer Document": (
        "Steps to access Transfer Document:\n"
        "1. Request a title transfer document from your solicitor or conveyancer.\n"
        "2. Alternatively, conduct a title search via state land registry.\n"
        "3. Save or scan the final document as PDF."
    ),
    "Valuation Report": (
        "Steps to access Valuation Report:\n"
        "1. Ask your broker or lender for the valuation report.\n"
        "2. Ensure it includes property details and final valuation.\n"
        "3. Save as a clear PDF."
    ),
    "Insurance Certificate": (
        "Steps to access Insurance Certificate:\n"
        "1. Log into your insurer’s portal (e.g., NRMA, AAMI).\n"
        "2. Download the Certificate of Currency or Policy Summary.\n"
        "3. Save as PDF."
    ),
    "Rates Notice": (
        "Steps to access Rates Notice:\n"
        "1. Log in to your local council website.\n"
        "2. Download the latest rates or water notice.\n"
        "3. Ensure your name and address are visible.\n"
        "4. Save as a PDF."
    ),
    "Rental Appraisal": (
        "Steps to access Rental Appraisal:\n"
        "1. Request an appraisal letter from a licensed property manager.\n"
        "2. Ensure it includes expected rental income and date of issue.\n"
        "3. Save the document as a PDF."
    ),
    "Tenancy Agreement": (
        "Steps to access Tenancy Agreement:\n"
        "1. Retrieve your rental agreement from your property manager or landlord.\n"
        "2. Include all signed pages.\n"
        "3. Save as a single PDF file."
    ),
    "Rental Statement": (
        "Steps to access Rental Statement:\n"
        "1. Log into your property manager’s portal.\n"
        "2. Download the most recent rental income statement.\n"
        "3. Save as a PDF showing date, amount, and property address."
    ),

    # Other Supporting Documents
    "Gift Letter": (
        "Steps to access Gift Letter:\n"
        "1. Ask your family member or gift giver to write a signed letter.\n"
        "2. It should declare the amount and confirm no repayment is expected.\n"
        "3. Save the letter as a scanned PDF."
    ),
    "Guarantor Documents": (
        "Steps to access Guarantor Documents:\n"
        "1. Request signed guarantor forms from your lender.\n"
        "2. Ensure the guarantor has completed and signed all sections.\n"
        "3. Save scanned or digital versions as PDFs."
    ),
    "Superannuation Statement": (
        "Steps to access Superannuation Statement:\n"
        "1. Log into your super fund portal (e.g., AustralianSuper, Hostplus).\n"
        "2. Navigate to 'Statements'.\n"
        "3. Download the most recent annual statement as a PDF."
    ),
    "Utility Bills": (
        "Steps to access Utility Bills:\n"
        "1. Log into your utility provider’s website (e.g., Origin, AGL, Telstra).\n"
        "2. Navigate to 'Billing' or 'Statements'.\n"
        "3. Download a recent bill with your name and address as a PDF."
    ),

    # Unclassified
    "Miscellaneous or Unclassified": (
        "Steps to assess Miscellaneous or Unclassified documents:\n"
        "1. Review the document to determine if it fits another category.\n"
        "2. If uncertain, contact your broker or support team.\n"
        "3. Save it clearly labeled in PDF format for further review."
    )
}


def generate_no_results_html_broker(unused_categories):
    """
    Generate static HTML content informing the user that no relevant emails
    were found, and include instructions for retrieving missing document categories.

    Args:
        unused_categories (list): List of broker document categories with no matches.

    Returns:
        str: HTML content for the email.
    """
    guide_sections = ""
    for cat in unused_categories:
        if cat in category_instructions:
            guide_sections += f"<h3>{cat}</h3><p style='white-space: pre-line'>{category_instructions[cat]}</p><br/>"

    return f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333;">
        <h2>Your Dukbill Summary</h2>
        <p>Thank you for using Dukbill to scan your inbox.</p>
        <p>We've completed scanning your email within the date range you selected, but found no relevant invoices or summary emails.</p>
        <p>If you believe something is missing or have any pending issues, please contact our support team at <a href="mailto:support@dukbill.com.au">support@dukbill.com.au</a>.</p>
        <p>If available, we've attached a PDF summary from your previous scan for your reference.</p>
        <br/>
        <h2>Helpful Steps to Retrieve Missing Documents</h2>
        {guide_sections}
        <p>Best regards,<br/>The Dukbill Team</p>
      </body>
    </html>
    """

def generate_no_findings_html_broker():
    """
    Generate static HTML content informing the user that no relevant emails
    were found, and include instructions for retrieving missing document categories.

    Args:
        unused_categories (list): List of broker document categories with no matches.

    Returns:
        str: HTML content for the email.
    """
    return f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333;">
        <h2>Your Dukbill Summary</h2>
        <p>Thank you for using Dukbill to scan your inbox.</p>
        <p>We've completed scanning your email within the date range you selected, but found no relevant invoices or summary emails.</p>
        <p>If you believe something is missing or have any pending issues, please contact our support team at <a href="mailto:support@dukbill.com.au">support@dukbill.com.au</a>.</p>
        <p>If available, we've attached a PDF summary from your previous scan for your reference.</p>
        <p>Best regards,<br/>The Dukbill Team</p>
      </body>
    </html>
    """



def generate_pdf_broker(unused_categories):
    """
    Generate static HTML content informing the user that no relevant emails
    were found, and include instructions for retrieving missing document categories.

    Args:
        unused_categories (list): List of broker document categories with no matches.

    Returns:
        str: HTML content for the email.
    """
    guide_sections = ""
    for cat in unused_categories:
        if cat in category_instructions:
            guide_sections += f"<h3>{cat}</h3><p style='white-space: pre-line'>{category_instructions[cat]}</p><br/>"

    return f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333;">
        <h2>Your Dukbill Summary</h2>
        <p>Thank you for using Dukbill to scan your inbox.</p>
        <p>We've completed scanning your email within the date range you selected.</p>
        <p>If you believe something is missing or have any pending issues, please contact our support team at <a href="mailto:support@dukbill.com.au">support@dukbill.com.au</a>.</p>
        <p>If available, we've attached a PDF summary for your reference.</p>
        <br/>
        <h2>Helpful Steps to Retrieve Missing Documents</h2>
        {guide_sections}
        <p>Best regards,<br/>The Dukbill Team</p>
      </body>
    </html>
    """



def generate_no_email_found_html():
    """
    Generate HTML content to inform the user that no emails were found in the scanned date range.

    Returns:
        str: HTML content for the email
    """
    return """
    <html>
      <body style="font-family: Arial, sans-serif; color: #333;">
        <h2>Your Dukbill Scan Results</h2>
        <p>We’ve completed scanning your inbox for the selected date range, but no emails were found to process.</p>
        <p>If you believe this is incorrect or have any questions, please reach out to our support team at <a href="mailto:support@dukbill.com.au">support@dukbill.com.au</a>.</p>
        <p>If available, we've attached a PDF summary from your previous scan for your reference.</p>
        <br/>
        <p>Best regards,<br/>The Dukbill Team</p>
      </body>
    </html>
    """



def zip_all_files(raw_emails, output_path=None):
    # Create a zip file in memory or write to disk
    zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
        pdf_count = 0
        
        # Iterate through each thread ID in the raw_emails dictionary
        for thread_id, messages in raw_emails.items():
            # Each thread contains a list of messages
            for message_idx, message in enumerate(messages):
                # Check if this message has PDF data
                if 'pdfencoded' in message and isinstance(message['pdfencoded'], list):
                    # Get corresponding PDF filenames if available
                    pdf_names = message.get('pdfs', [])
                    
                    # Process each encoded PDF
                    for pdf_idx, encoded_pdf in enumerate(message['pdfencoded']):
                        try:
                            pdf_data = base64.b64decode(encoded_pdf)
                            
                            # Use original filename if available, otherwise generate one
                            if pdf_idx < len(pdf_names):
                                filename = pdf_names[pdf_idx]
                            else:
                                filename = f"document_{thread_id}_{message_idx}_{pdf_idx}.pdf"
                            
                            # Ensure unique filenames in case of duplicates
                            counter = 1
                            original_filename = filename
                            while filename in [info.filename for info in zipf.filelist]:
                                name, ext = original_filename.rsplit('.', 1) if '.' in original_filename else (original_filename, '')
                                filename = f"{name}_{counter}.{ext}" if ext else f"{name}_{counter}"
                                counter += 1
                            
                            zipf.writestr(filename, pdf_data)
                            pdf_count += 1
                            print(f"[Zip Success] Added PDF: {filename}")
                            
                        except Exception as e:
                            print(f"[Zip Error] Failed to add PDF from thread {thread_id}, message {message_idx}, PDF {pdf_idx}: {e}")
        
        print(f"[Zip Complete] Total PDFs added: {pdf_count}")
    
    # If output_path is provided, write to disk; otherwise return the bytes
    if output_path:
        zip_buffer.seek(0)
        with open(output_path, "wb") as f:
            f.write(zip_buffer.getvalue())
        print(f"[Zip Saved] Zip file saved to: {output_path}")
    else:
        zip_buffer.seek(0)
        return zip_buffer.getvalue()

def create_pdf_from_final_json_broker(final_json, filename, raw_emails):
    # Filter out invalid entries
    filtered_data = [
        entry for entry in final_json 
        if 'broker_document_category' in entry
        and entry["broker_document_category"] != "Miscellaneous or Unclassified"
        and entry["broker_document_category"] != "NA"
    ]
    
    # Group by broker_document_category
    grouped_data = defaultdict(list)
    for entry in filtered_data:
        grouped_data[entry.get('broker_document_category')].append(entry)

    doc = SimpleDocTemplate(filename, pagesize=A4,
                            rightMargin=40, leftMargin=40,
                            topMargin=40, bottomMargin=40)

    styles = getSampleStyleSheet()
    title_style = styles['Title']
    title_style.fontName = 'Helvetica-Bold'
    title_style.textColor = colors.HexColor("#000000")
    title_style.fontSize = 24
    title_style.alignment = 0  # Left-aligned

    subtitle_style = styles['Normal']
    subtitle_style.fontSize = 12
    subtitle_style.leading = 14

    # Style for document category - larger and bold
    category_style = styles['Normal']
    category_style.fontName = 'Helvetica-Bold'
    category_style.fontSize = 16
    category_style.textColor = colors.HexColor("#000000")
    category_style.leading = 18

    # Style for thread ID and date
    thread_style = styles['Normal']
    thread_style.fontName = 'Helvetica-Bold'
    thread_style.fontSize = 11
    thread_style.textColor = colors.HexColor("#333333")

    # Style for email summary
    summary_style = styles['Normal']
    summary_style.fontSize = 10
    summary_style.leading = 12
    summary_style.leftIndent = 20  # Indent the summary text

    elements = []

    # Download and load logo with aspect-ratio preservation
    logo_url = "https://raw.githubusercontent.com/charbs123-sys/zoopi-assets/main/Screenshot%20from%202025-07-05%2021-38-28.png"
    logo_path = "/tmp/dukbill_logo.png"
    
    try:
        urllib.request.urlretrieve(logo_url, logo_path)
        # Preserve aspect ratio
        logo_reader = ImageReader(logo_path)
        original_width, original_height = logo_reader.getSize()
        desired_width = 2.0 * inch  # Reduced from 2.5 to ensure it fits
        aspect_ratio = original_height / float(original_width)
        desired_height = desired_width * aspect_ratio
        logo_available = True
    except Exception as e:
        print(f"Failed to download logo: {e}")
        logo_available = False
        desired_width = 0
        desired_height = 0

    for i, (category, items) in enumerate(grouped_data.items()):
        if i > 0:
            elements.append(PageBreak())

        # Create title paragraphs
        title_paragraph = [
            Paragraph("Dukbill Broker", title_style),
            Paragraph("Intelligence Summary", title_style)
        ]

        # Calculate safe column widths
        # doc.width is the available width after margins
        available_width = doc.width  # This is typically A4 width - left margin - right margin
        
        if logo_available:
            # Create header with logo
            logo = Image(logo_path, width=desired_width, height=desired_height)
            
            # Calculate column widths more carefully
            # Small spacer | Title text | Logo
            spacer_width = 10
            logo_col_width = desired_width + 10  # Logo width plus some padding
            text_col_width = available_width - spacer_width - logo_col_width
            
            # Ensure we don't have negative widths
            if text_col_width < 100:  # Minimum reasonable text width
                # Adjust logo size if needed
                logo_col_width = 100
                text_col_width = available_width - spacer_width - logo_col_width
                desired_width = 90  # Smaller logo
                aspect_ratio = original_height / float(original_width)
                desired_height = desired_width * aspect_ratio
                logo = Image(logo_path, width=desired_width, height=desired_height)
            
            header_data = [
                [Spacer(width=spacer_width, height=1), title_paragraph, logo]
            ]
            
            header_table = Table(header_data, colWidths=[spacer_width, text_col_width, logo_col_width])
        else:
            # Create header without logo if download failed
            header_data = [
                [title_paragraph]
            ]
            header_table = Table(header_data, colWidths=[available_width])

        header_table.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('ALIGN', (2, 0), (2, 0), 'RIGHT') if logo_available else ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('LEFTPADDING', (0, 0), (0, 0), 0),
            ('RIGHTPADDING', (-1, 0), (-1, 0), 0),
        ]))

        elements.append(header_table)

        # Horizontal line separating title and subtitle
        elements.append(Spacer(1, 6))
        elements.append(HRFlowable(width="100%", thickness=1, color=colors.black))
        elements.append(Spacer(1, 12))

        # Subtitle for category
        elements.append(Paragraph(f"Document Category: {category}", category_style))
        elements.append(Spacer(1, 20))

        # Sort items by date for better organization
        sorted_items = sorted(items, key=lambda x: x.get('date', ''))
   
        # Add email summaries for this category
        for item in sorted_items:
            thread_id = item.get("threadid", None)
            if thread_id: #and thread_id in raw_emails:
                # Get the subject from raw emails
                #raw_thread_data = raw_emails.get(thread_id, [])
                #if raw_thread_data and len(raw_thread_data) > 0:
                #    subject = raw_thread_data[0].get('subject', 'No subject')
                #else:
                #    subject = 'No subject'
                subject = 'No subject'
                for thread in final_json:
                    if thread["threadid"] == thread_id:
                        subject = thread.get('subject', 'No subject')
                        break

                # Thread ID and Date header
                date_str = item.get('date', 'No date')
                thread_header = f"{subject} - {date_str}"
                elements.append(Paragraph(thread_header, thread_style))
                elements.append(Spacer(1, 6))
                
                # Email summary content
                email_summary = item.get('email_summary', 'No summary available')
                # Clean up the summary text to avoid PDF rendering issues
                email_summary = str(email_summary) if email_summary else 'No summary available'
                elements.append(Paragraph(email_summary, summary_style))
                elements.append(Spacer(1, 15))  # Space between entries

    # Build the PDF
    try:
        doc.build(elements)
        print(f"PDF created successfully at {filename}")
    except Exception as e:
        print(f"Error building PDF: {e}")
        # Create a simple fallback PDF
        elements = [Paragraph("Error creating detailed PDF. Please check the data.", styles['Normal'])]
        doc.build(elements)
