import os

# No load_dotenv() in production - environment variables come from ECS
DB_CONFIG = {
    "host": os.environ.get("DB_HOST"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "database": os.environ.get("DB_NAME", "dukbill"),
    "port": int(os.environ.get("DB_PORT", 3306))
}

AUTH0_DOMAIN = os.environ.get("AUTH0_DOMAIN")
AUTH0_AUDIENCE = os.environ.get("AUTH0_AUDIENCE")
AUTH0_CLIENT_ID = os.environ.get("AUTH0_CLIENT_ID")
POST_LOGOUT_REDIRECT_URI = os.environ.get("POST_LOGOUT_REDIRECT_URI")


GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")

S3_CONFIG = {
    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY"),
    "AWS_REGION": os.environ.get("AWS_REGION", "ap-southeast-2"),
    "S3_BUCKET_NAME": os.environ.get("S3_BUCKET_NAME")
}

CLOUDFRONT_DOMAIN = os.environ.get("CLOUDFRONT_DOMAIN")

BASIQ_API_KEY = os.environ.get("BASIQ_API_KEY")
BASIQ_BASE_URL = os.environ.get("BASIQ_BASE_URL")

CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET= os.environ.get("CLIENT_SECRET")
REDIRECT_URI= os.environ.get("REDIRECT_URI")
SCOPES = os.environ.get("SCOPES")
SEARCH_QUERY = os.environ.get("SEARCH_QUERY", "has:attachment newer_than:2y")

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
        "Driver's Licence",
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
    ]
}
