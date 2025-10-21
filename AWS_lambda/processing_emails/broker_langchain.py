from typing import Optional, List
from pydantic import BaseModel, Field, model_validator
from langchain_core.prompts import ChatPromptTemplate
import asyncio
from openai import RateLimitError
from datetime import datetime, timedelta
import time

class Invoice(BaseModel):
    '''Information about the invoice potentially contained in the email contents'''

    company: Optional[str] = Field(default = "NA", description = "The Company name the document is sent from")
    amount: Optional[str] = Field(default = "NA", description = "The amount billed in the contents of the document")
    date: Optional[str] = Field(default = "NA", description = "The date which the document is due")
    threadid: Optional[str] = Field(default = None, description = "The threadid of the set of emails")
    subject: Optional[str] = Field(default = None, description = "The subject of the email")
    broker_document_category: Optional[str] = Field(
        default="NA",
        description=(
            "The broker document category this document belongs to. Choose the most appropriate label from:\n"
            "[\n"
            "  'Payslips', "
            "  'PAYG Summary', "        
            "  'Tax Return',    "            
            "  'Notice of Assessment',        "
            "  'Employment Contract',         "
            "  'Employment Letter',             "
            "  'Bank Statements',             "
            "  'Credit Card Statements',         "
            "  'Loan Statements',              "
            "  'ATO Debt Statement',           "
            "  'HECS/HELP Debt',              "
            "  'Driver’s Licence',             "
            "  'Passport',                "
            "  'Medicare Card',           "
            "  'Birth Certificate',           "
            "  'Citizenship Certificate',      "
            "  'VOI Certificate',             "
            "  'Contract of Sale',             "
            "  'Building Contract',           "
            "  'Plans and Specifications',     "
            "  'Council Approval',        "
            "  'Deposit Receipt',   "
            "  'Transfer Document',  "    
            "  'Valuation Report',    " 
            "  'Insurance Certificate',"    
            "  'Rates Notice',          " 
            "  'Rental Appraisal',       "  
            "  'Tenancy Agreement',       "
            "  'Rental Statement',       "
            "  'Gift Letter',             " 
            "  'Guarantor Documents',    "
            "  'Superannuation Statement',"
            "  'Utility Bills',             " 
            " 'Invoices',"
            " Bills,"
            "  'Miscellaneous or Unclassified    # Any document which cannot be classified into the above"
            "]"
        )
    )
    
    email_summary: Optional[str] = Field(
        default = "NA",
        description = (
            "In a single sentence explain your reasoning for the document categorisation"
        )
    )

    @model_validator(mode="before")
    def fill_empty_with_na(cls, values: dict) -> dict:
        """
        Before validating, ensure that any missing or None fields
        get the literal "NA" so that downstream you never see None.
        """
        for k in ("company", "amount", "date", "threadid", "email_summary", "broker_document_category", "subject"): 
            if values.get(k) is None:
                values[k] = "NA"
        return values
    

class BrokerData(BaseModel):
    """Extracted data about invoices."""

    invoices: List[Invoice]

'''
def create_batch_prompt():
    """Create a prompt template for processing multiple emails in one go"""
    return ChatPromptTemplate.from_messages([
        (
            "system",
            "You are an expert extraction algorithm. "
            "Process multiple emails and extract relevant information from each. "
            "Return results as a JSON array with one object per email. "
            "Only extract relevant information from the text. "
            "If you do not know the value of an attribute asked to extract, "
            "return NA for the attribute's value. "
            "Ignore duplicate entries. "
            "Ignore entries related to job opportunities, job applications, or potential salaries/pay offers. "
            "Classify each entry into one of the following categories: "
            "'Payslips', 'PAYG Summary', 'Tax Return', 'Notice of Assessment', 'Employment Contract', "
            "'Employment Letter', 'Bank Statements', 'Credit Card Statements', 'Loan Statements', "
            "'ATO Debt Statement', 'HECS/HELP Debt', 'Driver's Licence', 'Passport', 'Medicare Card', "
            "'Birth Certificate', 'Citizenship Certificate', 'VOI Certificate', 'Contract of Sale', "
            "'Building Contract', 'Plans and Specifications', 'Council Approval', 'Deposit Receipt', "
            "'Transfer Document', 'Valuation Report', 'Insurance Certificate', 'Rates Notice', "
            "'Rental Appraisal', 'Tenancy Agreement', 'Rental Statement', 'Gift Letter', "
            "'Guarantor Documents', 'Superannuation Statement', 'Utility Bills', Invoices, Bills"
            "'Miscellaneous or Unclassified'. "
            "You MUST classify into one of the following categories above with the exact same spelling I have provided!!!"
            "DO NOT provide your own classification!!!"
            "If an entry has no useful information just include the threadid and let all other entries be NA. "
            "If an entry is classified as Miscellaneous or Unclassified then just include the threadid and let all other entries be NA. "
            "If there is no due date then return None for all entries even though there is a company name. "
            "If you extract a date, format it as YYYY-MM-DD (e.g., 2025-06-20). "
            "If you extract an amount, format it as $XX.XX (e.g., $125.60). "
            "You need to be very pedantic about how emails should be categorised—only match to a category if you can guarantee "
            "the email will be relevant for the broker."
            "Note that emails are seperated by ---Email n--- where n is some integer representing the number of the email."
            "You MUST consider each email independent of each other - important"
            "The beggining of every email starts with the email number and following that immediately the threadid, this threadid"
            "should be classified as the threadid. If an email has multiple things to extract from it, the threadid will be the same"
            "for all items extracted."
            "YOU MUST produce a classification for every single email present."
            "EVEN IF AN EMAIL DOES NOT HAVE AN AMOUNT OR ANY CATEGORY SPECIFIED AS LONG AS THE CONTENTS ARE RELEVANT TO THE PDF"
            "MAKE SURE TO CLASSIFY IT AS LONG AS IT IS RELEVANT TO THE BROKER DOCUMENT CATEGORY"
        ),
        ("user", "{emails_batch}")
    ])
'''
def create_batch_prompt():
    cats = (
        "Payslips,PAYG Summary,Tax Return,Notice of Assessment,Employment Contract,"
        "Employment Letter,Bank Statements,Credit Card Statements,Loan Statements,"
        "ATO Debt Statement,HECS/HELP Debt,Driver’s Licence,Passport,Medicare Card,"
        "Birth Certificate,Citizenship Certificate,VOI Certificate,Contract of Sale,"
        "Building Contract,Plans and Specifications,Council Approval,Deposit Receipt,"
        "Transfer Document,Valuation Report,Insurance Certificate,Rates Notice,"
        "Rental Appraisal,Tenancy Agreement,Rental Statement,Gift Letter,"
        "Guarantor Documents,Superannuation Statement,Utility Bills, Invoices, Miscellaneous or Unclassified,"
        "Bills"
    )

    cues = (
        "CUES: BankStmt→bank+ABN,'Statement/Transaction',BSB/Acct, rows Date|Details|Amount|Balance; "
        "Rates→council+ABN, rating period, lot/DP, BPAY; "
        "LoanStmt→'Discharge/Refinance/Loan', acct no., security addr; "
        "CreditReport→Equifax/score/RHI; "
        "DLic→name,DOB,addr,lic#,class,expiry; "
        "TaxReturn→'Profit and Loss/Tax Return', FY 'year ended 30 June'; "
        "Invoice→'INVOICE', supplier+ABN, inv#/date, lines, GST, total; "
        "InsCert→policy#, start, insured addr, cover, 'Certificate of Currency'; "
        "Valuation→firm+API, property summary, market value; "
        "Payslip→period,gross,tax,net,YTD,super; "
        "PAYG→ATO, FY, employer ABN, gross, tax; "
        "VOI→'Verification of Identity Certificate'; "
        "RentStmt→agency, period, 'Money In/Out', rent, fees."
        "Bills-Recurring statements, pending amounts, phone bills"
    )

    sys = (
        "You extract one JSON object per email.\n"
        "RULES:\n"
        "1) Output array of length N (one per email), fields: "
        "{{company, amount, date, threadid, subject, broker_document_category, email_summary}}.\n"
        "2) If unknown, use 'NA'. Dates→YYYY-MM-DD. Amounts→$XX.XX.\n"
        "3) Category must be EXACTLY one of: [" + cats + "]. Do NOT invent labels. " + cues + "\n"
        "4) Treat emails independently. Use the threadid provided in each block.\n"
        "5) If 'Miscellaneous or Unclassified' or no useful info: include threadid; set others to 'NA'.\n"
        "6) Be conservative: only assign a category when cues strongly match broker docs.\n"
        "7) You received only the beginning of text/PDF; infer carefully but avoid hallucinating.\n"
        "8) Output JSON only. No extra text."
    )

    return ChatPromptTemplate.from_messages([("system", sys), ("user", "{emails_batch}")])



def process_true_batch(email_batch, structured_llm, max_retries=5):
    """Process multiple emails in a single LLM call"""
    batch_prompt = create_batch_prompt()
    
    emails_text = ""
    for i, email in enumerate(email_batch):
        subject_text = as_text(email.get("subject")) or "no subject present"
        emails_text += f"THIS IS THE BEGGINING OF EMAIL {i + 1} -- classify this email accordingly"
        #emails_text += f"\n--- Email {i+1} has the following properties:---\n"
        emails_text += f"threadid: {email['threadid']}\n"
        emails_text += f"from: {email['from_']}\n"
        emails_text += f"subject: {subject_text}\n"
        emails_text += f"pdf_contents: {email['pdf_contents'][:250]}\n"
        #emails_text += f"email_text: {email['email_text']}\n"
        emails_text += f"THIS IS THE END OF EMAIL {i + 1}"
    
    print(f"Prompting with {len(emails_text)} characters")
    prompt = batch_prompt.invoke({"emails_batch": emails_text})
    
    for attempt in range(max_retries):
        try:
            return structured_llm.invoke(prompt)
        except RateLimitError as e:
            wait_time = 2 ** attempt
            print(f"[Retry {attempt + 1}] RateLimitError: Waiting {wait_time}s")
            time.sleep(wait_time)
        except Exception as e:
            error_str = str(e)
            
            # Check if it's a token limit error
            if "length limit" in error_str or "16384" in error_str:
                print(f"[Error] Output token limit exceeded for batch of {len(email_batch)} emails")
                return {
                    "error": "Output token limit exceeded", 
                    "batch_size": len(email_batch),
                    "details": error_str
                }
            
            print(f"[Error] Unexpected error on batch processing: {e}")
            if attempt == max_retries - 1:
                break
            time.sleep(2 ** attempt)
    
    return {
        "error": "Failed after retries", 
        "batch_size": len(email_batch)
    }


def chunked_emails_true_batch(email_data_list, structured_llm, start_time, encoding, already_batched=False):
    """Process emails with dynamic batching to minimize API calls
    
    Args:
        email_data_list: List of emails or pre-batched emails
        structured_llm: LLM instance
        start_time: Processing start time
        encoding: Token encoding
        already_batched: If True, email_data_list is already a list of batches
    """
    
    if already_batched:
        # email_data_list is already in the form of batched_emails
        batched_emails = email_data_list
        print(f"[BATCH] Using {len(batched_emails)} pre-existing batches")
    else:
        # First, categorize emails by size
        small_emails = []  # < 5k tokens
        medium_emails = []  # 5k-15k tokens
        large_emails = []  # > 15k tokens
        
        for email in email_data_list:
            email_tokens = 0
            if email["pdf_contents"]:
                email_tokens += len(encoding.encode(email["pdf_contents"][:500]))
            
            email['_token_count'] = email_tokens  # Store for later
            
            if email_tokens < 5000:
                small_emails.append(email)
            elif email_tokens < 15000:
                medium_emails.append(email)
            else:
                large_emails.append(email)
        
        print(f"[CATEGORIZE] Small: {len(small_emails)}, Medium: {len(medium_emails)}, Large: {len(large_emails)}")
        
        # Create batches with SMALLER sizes for faster responses
        batched_emails = []
        
        # Small emails: REDUCED to 10 per batch
        current_batch = []
        current_tokens = 0
        for email in small_emails:
            if current_tokens + email['_token_count'] > 10000 or len(current_batch) >= 10:
                if current_batch:
                    batched_emails.append(current_batch)
                current_batch = [email]
                current_tokens = email['_token_count']
            else:
                current_batch.append(email)
                current_tokens += email['_token_count']
        if current_batch:
            batched_emails.append(current_batch)
        
        # Medium emails: REDUCED to 3 per batch
        current_batch = []
        current_tokens = 0
        for email in medium_emails:
            if current_tokens + email['_token_count'] > 40000 or len(current_batch) >= 3:
                if current_batch:
                    batched_emails.append(current_batch)
                current_batch = [email]
                current_tokens = email['_token_count']
            else:
                current_batch.append(email)
                current_tokens += email['_token_count']
        if current_batch:
            batched_emails.append(current_batch)
        
        # Large emails: process individually
        for email in large_emails:
            batched_emails.append([email])
        
        print(f"[BATCH] Created {len(batched_emails)} optimized batches")
    
    # Rest of processing remains the same...
    all_results = []
    rest_of_emails = []
    
    for i, batch in enumerate(batched_emails):
        elapsed = datetime.now() - start_time
        minutes, seconds = divmod(elapsed.total_seconds(), 60)
        batch_tokens = sum(e.get('_token_count', 0) for e in batch)  # Use .get() for safety
        print(f"[BATCH {i+1}/{len(batched_emails)}] {len(batch)} emails, ~{batch_tokens:,} tokens | Time: {int(minutes):02d}:{int(seconds):02d}")
        
        if datetime.now() - start_time >= timedelta(minutes=10):
            for remaining_batch in batched_emails[i:]:
                rest_of_emails.extend(remaining_batch)
            break
        
        batch_result = process_true_batch(batch, structured_llm)
        
        if isinstance(batch_result, dict) and "error" in batch_result:
            print(f"[BATCH {i+1}] FAILED - adding {len(batch)} emails to retry queue")
            rest_of_emails.extend(batch)
        else:
            invoice_count = len(batch_result.invoices) if hasattr(batch_result, 'invoices') else 0
            print(f"[BATCH {i+1}] SUCCESS - {invoice_count} invoices")
            all_results.append(batch_result)
    
    return all_results, rest_of_emails

def combine_chatgpt_responses_broker(chatgpt_response):
    invoice_list = []
    print(chatgpt_response)
    for email in chatgpt_response:
        if isinstance(email, dict):
            if "error" in email:
                print(f"[Warning] Skipping failed response: {email.get('error')}")
            else:
                print("[Warning] Unexpected dict object:", email)
            continue

        for invoice in email.invoices:  # Loop through the list of Invoice objects
            invoice_combined = {
                "company": invoice.company,
                "amount": invoice.amount,
                "date": invoice.date,
                "threadid": invoice.threadid,
                "broker_document_category": invoice.broker_document_category,
                "email_summary": invoice.email_summary,
                "subject": invoice.subject
            }
            invoice_list.append(invoice_combined)

    return invoice_list

def combine_response_for_async(gmail_1):
    response = []
    for index, key in enumerate(gmail_1.thread_keys):

        email_data = {
            "threadid": key,
            "from_": gmail_1.threads[key][0]["from_"],
            "subject": gmail_1.threads[key][0]["subject"],
            #modify pdf contents later to upload pdf instead of decoding
            "pdf_contents": gmail_1.pdf_text_list[index],
            "email_text": gmail_1.combined_bodies[key]
        }
        response.append(email_data)
    
    return response

def as_text(value) -> str:
    """Return a safe str for tokenization."""
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if not isinstance(value, str):
        return str(value)
    return value