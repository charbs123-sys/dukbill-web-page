from typing import Optional, List
from pydantic import BaseModel, Field, model_validator, field_validator
from langchain_core.prompts import ChatPromptTemplate
from openai import RateLimitError
from datetime import datetime, timedelta
import time

class Relevant(BaseModel):
    threadid: Optional[str] = Field(default="NA", description="Thread id for the subject line")
    is_relevant: bool = Field(default="0", description="1 if subject relates to a broker document and 0 otherwise")

    @field_validator("is_relevant", mode="before")
    @classmethod
    def coerce_bool(cls, v):
        if isinstance(v, str):
            return v.strip() == "1"
        return bool(v)

    @model_validator(mode="before")
    def fill_empty(cls, values: dict) -> dict:
        values.setdefault("threadid", "NA")
        return values


class RelevantList(BaseModel):
    """Extracted data about invoices."""
    subject_individual: List[Relevant]


def subject_batch_prompt():
    categories = (
        '"Payslips","PAYG Summary","Tax Return","Notice of Assessment","Employment Contract",'
        '"Employment Letter","Bank Statements","Credit Card Statements","Loan Statements",'
        '"ATO Debt Statement","HECS/HELP Debt","Drivers Licence","Passport","Medicare Card",'
        '"Birth Certificate","Citizenship Certificate","VOI Certificate","Contract of Sale",'
        '"Building Contract","Plans and Specifications","Council Approval","Deposit Receipt",'
        '"Transfer Document","Valuation Report","Insurance Certificate","Rates Notice", Bills,'
        '"Rental Appraisal","Tenancy Agreement","Rental Statement","Gift Letter", "Invoices",'
        '"Guarantor Documents","Superannuation Statement","Utility Bills","Miscellaneous or Unclassified"'
    )

    cues = (
        "[CATEGORY_CUES - hints only]\n"
        "Bank Statements: bank name+ABN/licence; 'Statement/Transaction Summary'; BSB+Acct No; rows Date|Details|Amount|Balance.\n"
        "Rates Notice: council name+ABN; rating period; lot/DP; assessment no.; itemised rates; BPAY; instalments.\n"
        "Loan Statements: 'Discharge/Refinance/Loan Statement'; loan acct no.; security address; payout/refi.\n"
        "Credit Report: Equifax/score/enquiries/defaults/RHI.\n"
        "Driver's Licence: name,DOB,address,lic no.,class,expiry.\n"
        "Tax Return: P&L/Tax Return; FY; income/expenses/profit; ABN.\n"
        "Invoices: INVOICE; supplier+ABN; invoice/date/no.; lines; GST; total.\n"
        "Insurance Certificate: policy no.; start; insured address; cover; insurer.\n"
        "Valuation Report: API/valuation firm; property summary; risk; market value.\n"
        "Payslips: pay period; gross/tax/net; YTD; super.\n"
        "PAYG Summary: ATO; FY; employer ABN/branch; gross; tax withheld; super.\n"
        "VOI Certificate: VOI; acceptable certifiers; ID categories.\n"
        "Rental Statement: agency; period; Money In/Out; rent; fees; EFT.\n"
        "Bills-Recurring statements, pending amounts, phone bills\n"
    )

    sys_msg = (
        "You are an expert subject-line relevance classifier for broker documents. "
        "Return one JSON object per subject with fields: threadid (string), is_relevant (1 or 0). "
        "A subject is relevant (1) if its subject and/or included PDF snippet indicates a strong match to any of the broker document categories. "
        "Otherwise return 0.\n"
        + cues +
        f"Valid categories are: [{categories}]. Use these only as cues; DO NOT output the category here.\n"
        "Rules:\n"
        "- You MUST output exactly one result per subject provided; never skip.\n"
        "- Consider each subject independently.\n"
        "- Use the PDF snippet only as a hint to decide relevance.\n"
        "- Output strictly 1 or 0 for is_relevant.\n"
        "Consider the following example:\n"
        "This is entry 1 which you must classify:\n"
        "threadid: 1aef3s1\n"
        "subject: commbank loan documents\n"
        "pdf_content: some information related to a commbank loan document\n"
        "output -> {{'threadid': '1aef3s1', 'is_relevant': '1'}}\n"
        "Multiple subjects will be presented one after each other, create a seperate classification for every single one of them"
    )

    return ChatPromptTemplate.from_messages([("system", sys_msg), ("user", "{subject_batch}")])


def process_subject_batch(subject_batch, structured_llm, max_retries=5):
    """Process multiple emails in a single LLM call - SYNCHRONOUS for Lambda"""
    batch_prompt = subject_batch_prompt()
    
    # Format all emails in the batch into a single string
    total_subjects = len(subject_batch)
    emails_text = f"TOTAL SUBJECTS TO CLASSIFY: {total_subjects}\n"
    emails_text += f"YOU MUST RETURN EXACTLY {total_subjects} CLASSIFICATIONS.\n"
    emails_text += "="*60 + "\n\n"

    for i, email in enumerate(subject_batch):
        subject_text = as_text(email.get("subject")) or "no subject present"

        emails_text += f"This is entry {i} which you must classify:\n"
        emails_text += f"threadid: {email['threadid']}\n"
        emails_text += f"subject: {subject_text}\n"
        emails_text += f"pdf_content: {email['body']}\n\n"
    
    print(f"Prompting with {len(emails_text)} characters for {total_subjects} subjects")
    prompt = batch_prompt.invoke({"subject_batch": emails_text})
    
    for attempt in range(max_retries):
        try:
            start_time = time.time()
            print(f"[API Call] Starting at {time.strftime('%H:%M:%S')}")
            
            # SYNCHRONOUS CALL - no await!
            result = structured_llm.invoke(prompt)
            
            elapsed = time.time() - start_time
            print(f"[API Call] Completed in {elapsed:.2f}s ({elapsed/total_subjects:.2f}s per subject)")
            
            return result
            
        except RateLimitError as e:
            wait_time = 2 ** attempt
            print(f"[Retry {attempt + 1}] RateLimitError: Waiting {wait_time}s")
            time.sleep(wait_time)
        except Exception as e:
            print(f"[Error] Unexpected error on batch processing: {e}")
            import traceback
            traceback.print_exc()
            break
    
    print("Batch processing failed after retries")
    return {"error": "Failed after retries", "batch_size": len(subject_batch)}


def chunked_subject_batch(email_data_list, structured_llm, start_time, encoding):
    """Process subjects in batches - SYNCHRONOUS for Lambda"""
    
    MAX_INPUT_TOKENS = 100000
    MAX_SUBJECTS_PER_BATCH = 20  # Reduced for faster responses
    
    batched_emails = []
    current_batch = []
    current_tokens = 0
    
    for email in email_data_list:
        print(email.get("subject"))
        subject_text = as_text(email.get("subject")) or "no subject present"
        email_tokens = (
            len(encoding.encode(subject_text)) + 
            len(encoding.encode(email["body"]))
        )
        
        should_split = (
            (current_tokens + email_tokens > MAX_INPUT_TOKENS and current_batch) or
            (len(current_batch) >= MAX_SUBJECTS_PER_BATCH)
        )
        
        if should_split:
            batched_emails.append(current_batch)
            current_batch = [email]
            current_tokens = email_tokens
        else:
            current_batch.append(email)
            current_tokens += email_tokens
    
    if current_batch:
        batched_emails.append(current_batch)
    
    print(f"[SUBJECT] Created {len(batched_emails)} batches from {len(email_data_list)} subjects")
    
    all_results = []
    rest_of_emails = []
    
    for i, batch in enumerate(batched_emails):
        elapsed = datetime.now() - start_time
        minutes, seconds = divmod(elapsed.total_seconds(), 60)
        print(f"[BATCH {i+1}/{len(batched_emails)}] {len(batch)} subjects | Time: {int(minutes):02d}:{int(seconds):02d}")
        
        # Check time limit
        if datetime.now() - start_time >= timedelta(minutes=13):
            print(f"[TIME LIMIT] Stopping at batch {i+1}, saving remaining batches")
            for remaining_batch in batched_emails[i:]:
                rest_of_emails.extend(remaining_batch)
            break
        
        # SYNCHRONOUS call - no await!
        batch_result = process_subject_batch(batch, structured_llm)
        
        if isinstance(batch_result, dict) and "error" in batch_result:
            print(f"[BATCH {i+1}] FAILED - adding {len(batch)} subjects to retry")
            rest_of_emails.extend(batch)
        else:
            result_count = len(batch_result.subject_individual) if hasattr(batch_result, 'subject_individual') else 0
            expected_count = len(batch)
            
            print(f"[BATCH {i+1}] SUCCESS - {result_count}/{expected_count} classifications")
            
            if result_count < expected_count:
                print(f"[WARNING] Missing {expected_count - result_count} classifications")
            
            all_results.append(batch_result)
    
    # Summary
    total_input = len(email_data_list)
    total_classified = sum(len(r.subject_individual) for r in all_results if hasattr(r, 'subject_individual'))
    total_pending = len(rest_of_emails)
    
    print(f"\n[SUMMARY]")
    print(f"  Input subjects: {total_input}")
    print(f"  Classified: {total_classified}")
    print(f"  Pending retry: {total_pending}")
    
    return all_results, rest_of_emails


def combine_subject_chatgpt_responses_broker(chatgpt_response):
    """Extract subject classifications from LLM response"""
    subject_class_list = []
    
    for email in chatgpt_response:
        if isinstance(email, dict):
            if "error" in email:
                print(f"[Warning] Skipping failed response: {email.get('error')}")
            else:
                print("[Warning] Unexpected dict object:", email)
            continue

        # Correct attribute: subject_individual, not invoices
        for subject_item in email.subject_individual:
            invoice_combined = {
                "classification": subject_item.is_relevant,  # Boolean or 0/1
                "threadid": subject_item.threadid
            }
            subject_class_list.append(invoice_combined)

    return subject_class_list


def combine_subject_response_for_async(gmail_1):
    """Prepare subject data for classification"""
    response = []
    MAX_PDF_CHARS = 100
    
    for index, key in enumerate(gmail_1.thread_keys):
        pdf_text_list = gmail_1.text.get(key, [])
        
        # Get first PDF's text only
        if pdf_text_list and len(pdf_text_list) > 0:
            truncated_pdf = pdf_text_list[0][:MAX_PDF_CHARS]
        else:
            truncated_pdf = ""
        
        email_data = {
            "subject": gmail_1.threads[key][0]["subject"],
            "threadid": key,
            "body": truncated_pdf
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