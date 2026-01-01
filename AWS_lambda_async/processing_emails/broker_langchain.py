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
            "  'Payslips', 'PAYG Summary', 'Tax Return', 'Notice of Assessment', 'Employment Contract', "
            "  'Employment Letter', 'Bank Statements', 'Credit Card Statements', 'Loan Statements', "
            "  'ATO Debt Statement', 'HECS/HELP Debt', 'Driver's Licence', 'Passport', 'Medicare Card', "
            "  'Birth Certificate', 'Citizenship Certificate', 'VOI Certificate', 'Contract of Sale', "
            "  'Building Contract', 'Plans and Specifications', 'Council Approval', 'Deposit Receipt', "
            "  'Transfer Document', 'Valuation Report', 'Insurance Certificate', 'Rates Notice', "
            "  'Rental Appraisal', 'Tenancy Agreement', 'Rental Statement', 'Gift Letter', "
            "  'Guarantor Documents', 'Superannuation Statement', 'Utility Bills', 'Invoices', 'Bills', "
            "  'Miscellaneous or Unclassified'\n"
            "]"
        )
    )
    
    email_summary: Optional[str] = Field(
        default = "NA",
        description = "In a single sentence explain your reasoning for the document categorisation"
    )

    @model_validator(mode="before")
    def fill_empty_with_na(cls, values: dict) -> dict:
        for k in ("company", "amount", "date", "threadid", "email_summary", "broker_document_category", "subject"): 
            if values.get(k) is None:
                values[k] = "NA"
        return values
    

class BrokerData(BaseModel):
    """Extracted data about invoices."""
    invoices: List[Invoice]


def create_batch_prompt():
    """Create a prompt template for processing multiple emails in one go"""
    cats = (
        "Payslips,PAYG Summary,Tax Return,Notice of Assessment,Employment Contract,"
        "Employment Letter,Bank Statements,Credit Card Statements,Loan Statements,"
        "ATO Debt Statement,HECS/HELP Debt,Driver's Licence,Passport,Medicare Card,"
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


async def process_true_batch_async(email_batch, structured_llm, max_retries=5):
    """
    ASYNC: Process multiple emails in a single LLM call using async invoke.
    
    Args:
        email_batch: List of email data dictionaries
        structured_llm: ChatOpenAI instance with structured output
        max_retries: Maximum number of retry attempts
        
    Returns:
        BrokerData object or error dict
    """
    batch_prompt = create_batch_prompt()
    
    # Format batch text
    emails_text = ""
    for i, email in enumerate(email_batch):
        subject_text = as_text(email.get("subject")) or "no subject present"
        emails_text += f"THIS IS THE BEGGINING OF EMAIL {i + 1} -- classify this email accordingly"
        emails_text += f"threadid: {email['threadid']}\n"
        emails_text += f"from: {email['from_']}\n"
        emails_text += f"subject: {subject_text}\n"
        emails_text += f"pdf_contents: {email['pdf_contents'][:250]}\n"
        emails_text += f"THIS IS THE END OF EMAIL {i + 1}"
    
    print(f"[ASYNC API] Prompting with {len(emails_text)} characters for {len(email_batch)} emails")
    prompt = batch_prompt.invoke({"emails_batch": emails_text})
    
    for attempt in range(max_retries):
        try:
            start_time = time.time()
            print(f"[ASYNC API Call] Starting at {time.strftime('%H:%M:%S')}")
            
            # ASYNC CALL using ainvoke
            result = await structured_llm.ainvoke(prompt)
            
            elapsed = time.time() - start_time
            print(f"[ASYNC API Call] Completed in {elapsed:.2f}s ({elapsed/len(email_batch):.2f}s per email)")
            
            return result
            
        except RateLimitError:
            wait_time = 2 ** attempt
            print(f"[ASYNC Retry {attempt + 1}] RateLimitError: Waiting {wait_time}s")
            await asyncio.sleep(wait_time)  # Async sleep
            
        except Exception as e:
            error_str = str(e)
            
            # Check if it's a token limit error
            if "length limit" in error_str or "16384" in error_str:
                print(f"[ASYNC Error] Output token limit exceeded for batch of {len(email_batch)} emails")
                return {
                    "error": "Output token limit exceeded", 
                    "batch_size": len(email_batch),
                    "details": error_str
                }
            
            print(f"[ASYNC Error] Unexpected error on batch processing: {e}")
            if attempt == max_retries - 1:
                break
            await asyncio.sleep(2 ** attempt)  # Async sleep
    
    return {
        "error": "Failed after retries", 
        "batch_size": len(email_batch)
    }


# Keep synchronous version for backward compatibility
def process_true_batch(email_batch, structured_llm, max_retries=5):
    """Synchronous wrapper for backward compatibility"""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    return loop.run_until_complete(process_true_batch_async(email_batch, structured_llm, max_retries))


async def chunked_emails_true_batch_async(email_data_list, structured_llm, start_time, encoding, already_batched=False):
    """
    ASYNC: Process emails with dynamic batching to minimize API calls.
    Uses concurrent async calls for parallel processing.
    
    Args:
        email_data_list: List of emails or pre-batched emails
        structured_llm: LLM instance
        start_time: Processing start time
        encoding: Token encoding
        already_batched: If True, email_data_list is already a list of batches
    
    Returns:
        Tuple of (all_results, rest_of_emails)
    """
    
    if already_batched:
        batched_emails = email_data_list
        print(f"[ASYNC BATCH] Using {len(batched_emails)} pre-existing batches")
    else:
        # Categorize emails by size
        small_emails = []
        medium_emails = []
        large_emails = []
        
        for email in email_data_list:
            email_tokens = 0
            if email["pdf_contents"]:
                email_tokens += len(encoding.encode(email["pdf_contents"][:500]))
            
            email['_token_count'] = email_tokens
            
            if email_tokens < 5000:
                small_emails.append(email)
            elif email_tokens < 15000:
                medium_emails.append(email)
            else:
                large_emails.append(email)
        
        print(f"[ASYNC CATEGORIZE] Small: {len(small_emails)}, Medium: {len(medium_emails)}, Large: {len(large_emails)}")
        
        # Create batches
        batched_emails = []
        
        # Small emails: 10 per batch
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
        
        # Medium emails: 3 per batch
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
        
        print(f"[ASYNC BATCH] Created {len(batched_emails)} optimized batches")
    
    # Process batches with async concurrency
    all_results = []
    rest_of_emails = []
    
    # Process batches concurrently with semaphore to limit concurrency
    MAX_CONCURRENT = 5  # Limit concurrent API calls
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    
    async def process_with_semaphore(batch, batch_idx):
        """Process a single batch with semaphore control"""
        async with semaphore:
            elapsed = datetime.now() - start_time
            minutes, seconds = divmod(elapsed.total_seconds(), 60)
            batch_tokens = sum(e.get('_token_count', 0) for e in batch)
            print(f"[ASYNC BATCH {batch_idx+1}/{len(batched_emails)}] {len(batch)} emails, ~{batch_tokens:,} tokens | Time: {int(minutes):02d}:{int(seconds):02d}")
            
            # Check time limit
            if datetime.now() - start_time >= timedelta(minutes=10):
                return {'timeout': True, 'batch': batch, 'batch_idx': batch_idx}
            
            result = await process_true_batch_async(batch, structured_llm)
            
            return {
                'timeout': False,
                'result': result,
                'batch': batch,
                'batch_idx': batch_idx
            }
    
    # Create tasks for all batches
    tasks = [process_with_semaphore(batch, i) for i, batch in enumerate(batched_emails)]
    
    # Process all batches concurrently
    results = await asyncio.gather(*tasks)
    
    # Process results
    timeout_reached = False
    for res in results:
        if res['timeout']:
            timeout_reached = True
            # Add all remaining batches to rest_of_emails
            rest_of_emails.extend(res['batch'])
            continue
        
        if timeout_reached:
            # Already timed out, add to rest
            rest_of_emails.extend(res['batch'])
            continue
        
        batch_result = res['result']
        batch = res['batch']
        batch_idx = res['batch_idx']
        
        if isinstance(batch_result, dict) and "error" in batch_result:
            print(f"[ASYNC BATCH {batch_idx+1}] FAILED - adding {len(batch)} emails to retry queue")
            rest_of_emails.extend(batch)
        else:
            invoice_count = len(batch_result.invoices) if hasattr(batch_result, 'invoices') else 0
            print(f"[ASYNC BATCH {batch_idx+1}] SUCCESS - {invoice_count} invoices")
            all_results.append(batch_result)
    
    total_input = len(email_data_list) if not already_batched else sum(len(b) for b in batched_emails)
    total_classified = sum(len(r.invoices) for r in all_results if hasattr(r, 'invoices'))
    total_pending = len(rest_of_emails)
    
    print("\n[ASYNC SUMMARY]")
    print(f"  Input emails: {total_input}")
    print(f"  Classified: {total_classified}")
    print(f"  Pending retry: {total_pending}")
    
    return all_results, rest_of_emails


# Synchronous wrapper for backward compatibility
def chunked_emails_true_batch(email_data_list, structured_llm, start_time, encoding, already_batched=False):
    """Synchronous wrapper for backward compatibility"""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    return loop.run_until_complete(
        chunked_emails_true_batch_async(email_data_list, structured_llm, start_time, encoding, already_batched)
    )


def combine_chatgpt_responses_broker(chatgpt_response):
    """Extract invoice data from LLM responses"""
    invoice_list = []
    
    for email in chatgpt_response:
        if isinstance(email, dict):
            if "error" in email:
                print(f"[Warning] Skipping failed response: {email.get('error')}")
            else:
                print("[Warning] Unexpected dict object:", email)
            continue

        for invoice in email.invoices:
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
    """Prepare email data for classification"""
    response = []
    for index, key in enumerate(gmail_1.thread_keys):
        email_data = {
            "threadid": key,
            "from_": gmail_1.threads[key][0]["from_"],
            "subject": gmail_1.threads[key][0]["subject"],
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