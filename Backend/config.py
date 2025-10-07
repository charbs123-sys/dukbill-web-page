import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": 3306
}

AUTH0_DOMAIN = os.getenv("AUTH0_DOMAIN")
AUTH0_AUDIENCE = os.getenv("AUTH0_AUDIENCE")

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")

S3_CONFIG = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "AWS_REGION": os.getenv("AWS_REGION"),
    "S3_BUCKET_NAME": os.getenv("S3_BUCKET_NAME")
}

DOCUMENT_CATEGORIES = [
    # Income & Employment Documents
    'Payslips',
    'PAYG Summary',
    'Tax Return',
    'Notice of Assessment',
    'Employment Contract',
    'Employment Letter',

    # Bank & Financial Documents
    'Bank Statements',
    'Credit Card Statements',
    'Loan Statements',
    'ATO Debt Statement',
    'HECS/HELP Debt',

    # ID & Verification Documents
    'Driver’s Licence',
    'Passport',
    'Medicare Card',
    'Birth Certificate',
    'Citizenship Certificate',
    'VOI Certificate',

    # Property-Related Documents
    'Contract of Sale',
    'Building Contract',
    'Plans and Specifications',
    'Council Approval',
    'Deposit Receipt',
    'Transfer Document',
    'Valuation Report',
    'Insurance Certificate',
    'Rates Notice',
    'Rental Appraisal',
    'Tenancy Agreement',
    'Rental Statement',

    # Other Supporting Documents
    'Gift Letter',
    'Guarantor Documents',
    'Superannuation Statement',
    'Utility Bills',
    'Miscellaneous or Unclassified'
]