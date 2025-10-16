import os
from dotenv import load_dotenv

load_dotenv()

# mySQL Credentials
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": 3306
}

# Auth0 Credentials
AUTH0_DOMAIN = os.getenv("AUTH0_DOMAIN")
AUTH0_AUDIENCE = os.getenv("AUTH0_AUDIENCE")
AUTH0_CLIENT_ID = os.getenv("AUTH0_CLIENT_ID")
POST_LOGOUT_REDIRECT_URI = os.getenv("POST_LOGOUT_REDIRECT_URI")

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")

# S3 Credentials
S3_CONFIG = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "AWS_REGION": os.getenv("AWS_REGION"),
    "S3_BUCKET_NAME": os.getenv("S3_BUCKET_NAME")
}

# Basiq API Credentials
BASIQ_API_KEY = os.getenv("BASIQ_API_KEY")
BASIQ_BASE_URL = os.getenv("BASIQ_BASE_URL")

# Gmail Connection
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:8765/callback")
SCOPE = os.getenv("SCOPE", "https://www.googleapis.com/auth/gmail.readonly")
SEARCH_QUERY = os.getenv("SEARCH_QUERY", "has:attachment newer_than:2y")


DOCUMENT_CATEGORIES = {
    "Income & Employment Documents": [
        "Payslips",
        "PAYG Summary",
        "Tax Return",
        "Notice of Assessment",
        "Employment Contract",
        "Employment Letter"
    ],
    "Bank & Financial Documents": [
        "Bank Statements",
        "Credit Card Statements",
        "Loan Statements",
        "ATO Debt Statement",
        "HECS/HELP Debt"
    ],
    "ID & Verification Documents": [
        "Driver’s Licence",
        "Passport",
        "Medicare Card",
        "Birth Certificate",
        "Citizenship Certificate",
        "VOI Certificate"
    ],
    "Property-Related Documents": [
        "Contract of Sale",
        "Building Contract",
        "Plans and Specifications",
        "Council Approval",
        "Deposit Receipt",
        "Transfer Document",
        "Valuation Report",
        "Insurance Certificate",
        "Rates Notice",
        "Rental Appraisal",
        "Tenancy Agreement",
        "Rental Statement"
    ],
    "Other Supporting Documents": [
        "Gift Letter",
        "Guarantor Documents",
        "Superannuation Statement",
        "Utility Bills"
        # 'Miscellaneous or Unclassified'
    ]
}
