import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from email.mime.application import MIMEApplication

def send_email(to_email, subject, html_content, old=True, pdf_path=None, zip_path=None):
    from_email = os.getenv("EMAIL_ADDRESS")
    from_password = os.getenv("EMAIL_PASSWORD")
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email
    
    # HTML body
    html_part = MIMEText(html_content, "html")
    msg.attach(html_part)
    # PDF attachment\
    if pdf_path:  # Local file
        print("attempt to attach pdf")
        try:
            print("attaching pdf")
            with open(pdf_path, "rb") as f:
                part = MIMEApplication(f.read(), _subtype="pdf")
                part.add_header("Content-Disposition", "attachment", filename="bill_summary.pdf")
                msg.attach(part)
            print("pdf added succesfully")
        except Exception as e:
            print(f"[Attachment Error] Failed to attach PDF: {e}")
    
    # ZIP attachment
    if zip_path:
        try:
            with open(zip_path, "rb") as f:
                zip_part = MIMEApplication(f.read(), _subtype="zip")
                zip_part.add_header("Content-Disposition", "attachment", filename="documents.zip")
                msg.attach(zip_part)
        except Exception as e:
            print(f"[Attachment Error] Failed to attach ZIP file: {e}")
    
    # Send email
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(from_email, from_password)
            server.sendmail(from_email, to_email, msg.as_string())
        print(f"Email sent to {to_email}")
    except Exception as e:
        print(f"Failed to send email: {e}")
