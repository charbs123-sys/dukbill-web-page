import boto3
import os
import base64
import tempfile
import fitz
from botocore.exceptions import ClientError
import concurrent.futures
from threading import Lock

class Person():
    """
    Class where person contains multiple threads of emails, the purpose of this text is to process the emails within each thread and simplify operations
    """
    def __init__(self, threads_json: dict, max_pdf_chars: int = 3000, max_email_body_chars: int = 2000, 
                 use_parallel_textract: bool = True, max_textract_workers: int = 6,
                 smart_textract: bool = False, textract_only_if_empty: bool = False) -> None:
        self.threads = threads_json
        self.thread_keys = list(threads_json.keys())
        self.textract_client = boto3.client('textract')
        self.textract_usage = {'pages': 0, 'calls': 0}
        self.textract_lock = Lock()  # Thread-safe counter for parallel processing
        
        # Token/character limits for LLM efficiency
        self.max_pdf_chars = max_pdf_chars  # ~750 tokens per PDF
        self.max_email_body_chars = max_email_body_chars  # ~500 tokens per email body
        self.max_combined_chars = 5000  # ~1250 tokens total per thread (safety limit)
        
        # Parallel processing settings
        self.use_parallel_textract = use_parallel_textract
        self.max_textract_workers = max_textract_workers
        
        # Smart Textract settings
        self.smart_textract = smart_textract  # Only use Textract if fitz gets <5 chars
        self.textract_only_if_empty = textract_only_if_empty  # Extreme mode: only if completely empty
    
    def remove_body_forward(self) -> None:
        for key in self.thread_keys:
            for index, email in enumerate(self.threads[key]):
                body = email.get("body", "")
                if "---------- Forwarded message ---------" in body:
                    self.threads[key][index]["body"] = self.threads[key][index]["body"].split("---------- Forwarded message ---------")[0].strip()

    def store_unique_pdf(self) -> None:
        pdfs = {}
        for key in self.thread_keys:
            encoded_list = []
            for email in self.threads[key]:
                encoded_pdf = email.get("pdfencoded")
                if isinstance(encoded_pdf, list):
                    encoded_list.extend(encoded_pdf)
            # Store unique PDFs for each thread
            pdfs[key] = list(set(encoded_list)) if encoded_list else []
        
        self.unique_pdfs = pdfs

    def _extract_first_page_to_bytes(self, pdf_bytes: bytes) -> tuple[bytes, bool]:
        """
        Extract only the first page of a PDF and return as clean single-page PDF bytes.
        
        Args:
            pdf_bytes: Original PDF file as bytes
            
        Returns:
            Tuple of (single_page_pdf_bytes, success)
        """
        temp_input = None
        temp_output = None
        
        try:
            # Save original PDF to temp file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_in:
                temp_in.write(pdf_bytes)
                temp_input = temp_in.name
            
            # Open original PDF
            doc = fitz.open(temp_input)
            
            if len(doc) == 0:
                doc.close()
                return None, False
            
            # Create new document with only first page
            new_doc = fitz.open()
            new_doc.insert_pdf(doc, from_page=0, to_page=0)  # Only page 0 (first page)
            
            # Save to temp file with clean options
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_out:
                temp_output = temp_out.name
            
            new_doc.save(
                temp_output,
                garbage=4,        # Maximum garbage collection
                deflate=True,     # Compress
                clean=True        # Clean up
            )
            
            new_doc.close()
            doc.close()
            
            # Read the single-page PDF as bytes
            with open(temp_output, 'rb') as f:
                single_page_bytes = f.read()
            
            print(f"[Single-Page Extract] Created {len(single_page_bytes):,} byte single-page PDF")
            return single_page_bytes, True
            
        except Exception as e:
            print(f"[Single-Page Extract Error]: {e}")
            return None, False
            
        finally:
            # Clean up temp files
            for temp_file in [temp_input, temp_output]:
                if temp_file:
                    try:
                        os.unlink(temp_file)
                    except:
                        pass

    def _extract_with_textract(self, pdf_bytes: bytes, max_chars: int = None, thread_safe: bool = True) -> str:
        """
        Extract text from FIRST PAGE ONLY of PDF using AWS Textract.
        Automatically converts multi-page PDFs to single-page before sending to Textract.
        Output is optimized for LLM classification.
        
        Args:
            pdf_bytes: PDF file as bytes
            max_chars: Maximum characters to extract (uses self.max_pdf_chars if None)
            thread_safe: Whether to use thread-safe counter updates (default: True)
            
        Returns:
            Clean extracted text string from first page only, formatted for LLM consumption
        """
        if max_chars is None:
            max_chars = self.max_pdf_chars
            
        try:
            # Step 1: Extract first page to single-page PDF
            single_page_bytes, extract_success = self._extract_first_page_to_bytes(pdf_bytes)
            
            if not extract_success or single_page_bytes is None:
                print(f"[Textract] Failed to extract first page")
                return "[Error: Could not extract first page from PDF]"
            
            # Use the single-page PDF for Textract
            pdf_to_process = single_page_bytes
            
            # Step 2: Validate the single-page PDF
            if len(pdf_to_process) > 10 * 1024 * 1024:
                return "[Error: PDF exceeds Textract's 10MB limit for synchronous processing]"
            
            if not pdf_to_process.startswith(b'%PDF-'):
                print(f"[Textract Validation Failed]: File does not have valid PDF header")
                return "[Error: Invalid PDF format - missing PDF header]"
            
            if len(pdf_to_process) < 100:
                print(f"[Textract Validation Failed]: PDF file too small ({len(pdf_to_process)} bytes)")
                return "[Error: PDF file too small to be valid]"
            
            # Step 3: Send to Textract
            response = self.textract_client.detect_document_text(
                Document={'Bytes': pdf_to_process}
            )
            
            # Thread-safe counter update
            if thread_safe:
                with self.textract_lock:
                    self.textract_usage['calls'] += 1
            else:
                self.textract_usage['calls'] += 1
            
            # Step 4: Extract text from response
            extracted_lines = []
            char_count = 0
            
            for block in response.get('Blocks', []):
                # Extract LINE blocks (no need to check pages since it's single-page)
                if block['BlockType'] == 'LINE':
                    text = block.get('Text', '').strip()
                    
                    if not text:  # Skip empty lines
                        continue
                    
                    # Check character limit
                    if char_count + len(text) + 1 > max_chars:  # +1 for newline
                        remaining_chars = max_chars - char_count
                        if remaining_chars > 0:
                            extracted_lines.append(text[:remaining_chars])
                        extracted_lines.append(f"\n[TRUNCATED at {max_chars} chars for LLM efficiency]")
                        break
                    
                    extracted_lines.append(text)
                    char_count += len(text) + 1  # +1 for newline
            
            # Thread-safe page counter update
            if thread_safe:
                with self.textract_lock:
                    self.textract_usage['pages'] += 1
            else:
                self.textract_usage['pages'] += 1
            
            # Join lines with newlines for clean LLM-readable format
            result = '\n'.join(extracted_lines)
            
            if result:
                print(f"[Textract ✓] Extracted {char_count} characters from first page")
                return result
            else:
                print(f"[Textract] No text found on first page")
                return "[Textract: No text found on first page]"
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_msg = e.response['Error']['Message']
            print(f"[Textract ClientError]: {error_code} - {error_msg}")
            
            # Provide helpful error messages for common issues
            if error_code == 'UnsupportedDocumentException':
                return "[Error: PDF format not supported by Textract - file may be corrupted or encrypted]"
            elif error_code == 'InvalidParameterException':
                return "[Error: Invalid PDF data provided to Textract]"
            elif error_code == 'ProvisionedThroughputExceededException':
                return "[Error: Textract rate limit exceeded]"
            else:
                return f"[Error with Textract ({error_code}): {error_msg}]"
        except Exception as e:
            print(f"[Textract Error]: {e}")
            return f"[Error with Textract extraction: {str(e)}]"

    def pdf_to_text(self, max_pages=1, max_chars=None, use_textract_fallback=True, min_chars_threshold=50) -> None:
        """
        Extract text from PDFs with aggressive truncation for LLM efficiency.
        Falls back to AWS Textract if PyMuPDF (fitz) fails OR extracts minimal text.
        
        OPTIMIZED FOR BATCHING: Extracts only first page with strict character limits
        PARALLEL TEXTRACT: Processes multiple Textract calls concurrently for speed
        
        Args:
            max_pages: Maximum number of pages to extract per PDF (default: 1 for speed)
            max_chars: Maximum characters to extract per PDF (uses self.max_pdf_chars if None)
            use_textract_fallback: Whether to use Textract as fallback (default: True)
            min_chars_threshold: Minimum characters to consider extraction successful (default: 50)
        """
        if max_chars is None:
            max_chars = self.max_pdf_chars
            
        text_stored = {}
        
        # Collect all PDFs that need Textract processing
        textract_tasks = []

        if self.unique_pdfs:
            for key, pdf_list in self.unique_pdfs.items():
                text_stored[key] = []

                for pdf_idx, pdf in enumerate(pdf_list):
                    try:
                        pdf_bytes = base64.urlsafe_b64decode(pdf)
                        
                        # Validate PDF format before processing
                        if not pdf_bytes.startswith(b'%PDF-'):
                            print(f"[Invalid PDF] Thread {key}, PDF {pdf_idx + 1}: Not a valid PDF file")
                            text_stored[key].append("[Error: Invalid PDF format - not a PDF file]")
                            continue
                        
                        temp_path = None
                        fitz_succeeded = False
                        extracted_text_content = ""
                        
                        try:
                            # First, try PyMuPDF (fitz)
                            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp:
                                temp.write(pdf_bytes)
                                temp_path = temp.name

                            with fitz.open(temp_path) as doc:
                                total_pages = len(doc)
                                pages_to_extract = min(total_pages, max_pages)
                                
                                extracted_text = []
                                char_count = 0
                                
                                for page_num in range(pages_to_extract):
                                    page_text = doc[page_num].get_text()
                                    
                                    # Check if adding this page would exceed character limit
                                    if char_count + len(page_text) > max_chars:
                                        # Add partial text up to the limit
                                        remaining_chars = max_chars - char_count
                                        if remaining_chars > 0:
                                            extracted_text.append(page_text[:remaining_chars])
                                        extracted_text.append(f"\n[TRUNCATED at {max_chars} chars for LLM efficiency]")
                                        break
                                    
                                    extracted_text.append(page_text)
                                    char_count += len(page_text)
                                
                                text = "\n".join(extracted_text)
                                
                                # Check if we got enough text
                                #was  if char_count >= min_chars_threshold
                                if char_count >= 99999999:
                                    # Successful extraction with sufficient text
                                    fitz_succeeded = True
                                    extracted_text_content = text
                                    #print(f"[Fitz ✓] Thread {key}, PDF {pdf_idx + 1}: {char_count} chars (limit: {max_chars})")
                                else:
                                    # Extracted too little text - decide whether to use Textract
                                    #print(f"[Fitz - Low Text] Thread {key}, PDF {pdf_idx + 1}: Only {char_count} chars")
                                    
                                    # Smart Textract decision logic
                                    should_use_textract = use_textract_fallback
                                    
                                    if self.textract_only_if_empty and char_count > 0:
                                        # Extreme mode: only use Textract if we got ZERO chars
                                        should_use_textract = False
                                        extracted_text_content = text
                                        print(f"[Smart Skip] Using minimal fitz text, skipping Textract")
                                    elif self.smart_textract and char_count > 5:
                                        # Smart mode: skip Textract if we got at least SOME text
                                        should_use_textract = False
                                        extracted_text_content = text
                                        print(f"[Smart Skip] Got some text ({char_count} chars), skipping Textract")
                                    
                                    if should_use_textract:
                                        # Queue for parallel Textract processing
                                        if self.use_parallel_textract:
                                            textract_tasks.append({
                                                'key': key,
                                                'pdf_idx': pdf_idx,
                                                'pdf_bytes': pdf_bytes,
                                                'max_chars': max_chars
                                            })
                                            # Placeholder - will be replaced after parallel processing
                                            extracted_text_content = "__TEXTRACT_PENDING__"
                                        else:
                                            extracted_text_content = self._extract_with_textract(pdf_bytes, max_chars, thread_safe=False)
                                    else:
                                        extracted_text_content = text  # Use the minimal text we got
                            
                        except Exception as fitz_error:
                            # PyMuPDF failed completely, try Textract if enabled
                            print(f"[Fitz Error] Thread {key}, PDF {pdf_idx + 1}: {fitz_error}")
                            
                            if use_textract_fallback:
                                if self.use_parallel_textract:
                                    textract_tasks.append({
                                        'key': key,
                                        'pdf_idx': pdf_idx,
                                        'pdf_bytes': pdf_bytes,
                                        'max_chars': max_chars
                                    })
                                    extracted_text_content = "__TEXTRACT_PENDING__"
                                else:
                                    extracted_text_content = self._extract_with_textract(pdf_bytes, max_chars, thread_safe=False)
                            else:
                                extracted_text_content = "[Error reading PDF - Textract fallback disabled]"
                        
                        finally:
                            # Clean up temp file
                            if temp_path:
                                try:
                                    os.unlink(temp_path)
                                except:
                                    pass
                        
                        # Store the extracted text (may be placeholder for Textract)
                        text_stored[key].append(extracted_text_content)
                            
                    except Exception as e:
                        print(f"[Error parsing PDF for thread {key}, PDF {pdf_idx + 1}]: {e}")
                        text_stored[key].append("[Error reading PDF]")
        
        # Process all Textract tasks in parallel
        if textract_tasks and self.use_parallel_textract:
            print(f"[Parallel Textract] Processing {len(textract_tasks)} PDFs concurrently...")
            self._process_textract_parallel(textract_tasks, text_stored)
        
        self.text = text_stored
    
    def _process_textract_parallel(self, tasks, text_stored):
        """
        Process multiple Textract calls in parallel for speed.
        
        Args:
            tasks: List of dicts with keys: 'key', 'pdf_idx', 'pdf_bytes', 'max_chars'
            text_stored: Dictionary to update with results
        """
        def process_single_textract(task):
            """Process a single Textract task"""
            try:
                result = self._extract_with_textract(
                    task['pdf_bytes'], 
                    task['max_chars'],
                    thread_safe=True
                )
                return {
                    'key': task['key'],
                    'pdf_idx': task['pdf_idx'],
                    'result': result,
                    'success': True
                }
            except Exception as e:
                print(f"[Textract Parallel Error] Thread {task['key']}, PDF {task['pdf_idx']}: {e}")
                return {
                    'key': task['key'],
                    'pdf_idx': task['pdf_idx'],
                    'result': f"[Error with parallel Textract: {str(e)}]",
                    'success': False
                }
        
        # Process in parallel with ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_textract_workers) as executor:
            # Submit all tasks
            future_to_task = {executor.submit(process_single_textract, task): task for task in tasks}
            
            # Collect results as they complete
            completed = 0
            for future in concurrent.futures.as_completed(future_to_task):
                result = future.result()
                completed += 1
                
                # Replace placeholder with actual result
                key = result['key']
                pdf_idx = result['pdf_idx']
                
                # Find and replace the placeholder
                for idx, text in enumerate(text_stored[key]):
                    if text == "__TEXTRACT_PENDING__":
                        # Check if this is the right PDF (by counting pending items)
                        pending_count = text_stored[key][:idx+1].count("__TEXTRACT_PENDING__")
                        task_count = sum(1 for t in tasks if t['key'] == key and t['pdf_idx'] <= pdf_idx)
                        
                        if pending_count == task_count:
                            text_stored[key][idx] = result['result']
                            break
                
                print(f"[Parallel Textract] Completed {completed}/{len(tasks)} PDFs")
        
        print(f"[Parallel Textract] All {len(tasks)} PDFs processed")
    
    def combine_text(self) -> None:
        """
        Combine email bodies with character limit for LLM efficiency
        """
        self.combined_bodies = {}
        for keys in self.thread_keys:
            bodies = []
            char_count = 0
            
            for email in self.threads[keys]:
                body = email.get("body", "")
                
                # Apply character limit
                if char_count + len(body) > self.max_email_body_chars:
                    remaining_chars = self.max_email_body_chars - char_count
                    if remaining_chars > 0:
                        bodies.append(body[:remaining_chars])
                        bodies.append(f"\n[EMAIL BODY TRUNCATED at {self.max_email_body_chars} chars for LLM efficiency]")
                    break
                
                bodies.append(body)
                char_count += len(body)
            
            combined = ''.join(bodies)
            self.combined_bodies[keys] = combined
            
            if char_count > self.max_email_body_chars * 0.9:  # Log if close to limit
                print(f"[Email Body] Thread {keys}: {char_count} chars (limit: {self.max_email_body_chars})")

    def combining_pdf_text(self) -> None:
        """
        Combine PDF texts with overall character limit check
        """
        self.pdf_text_list = []

        for key in self.thread_keys:
            pdf_texts = self.text.get(key, [])

            if not pdf_texts:
                self.pdf_text_list.append(None)
            else:
                # Combine PDFs with separator
                combined_parts = []
                total_chars = 0
                
                for i, text in enumerate(pdf_texts):
                    text_stripped = text.strip()
                    header = f"===== PDF {i + 1} =====\n"
                    
                    # Check if adding this PDF would exceed limit
                    new_chars = len(header) + len(text_stripped) + 2  # +2 for \n\n
                    if total_chars + new_chars > self.max_combined_chars:
                        remaining = self.max_combined_chars - total_chars
                        if remaining > len(header) + 100:  # Only add if we can fit meaningful content
                            combined_parts.append(header + text_stripped[:remaining - len(header)])
                        combined_parts.append(f"\n[COMBINED PDF TEXT TRUNCATED at {self.max_combined_chars} chars]")
                        break
                    
                    combined_parts.append(header + text_stripped)
                    total_chars += new_chars
                
                combined = "\n\n".join(combined_parts)
                self.pdf_text_list.append(combined)
                
                if total_chars > self.max_combined_chars * 0.8:  # Log if close to limit
                    print(f"[Combined PDFs] Thread {key}: {total_chars} chars (limit: {self.max_combined_chars})")
    
    def get_textract_cost_estimate(self) -> dict:
        """
        Calculate estimated cost of Textract usage.
        
        Returns:
            Dictionary with usage stats and cost estimate
        """
        # AWS Textract pricing (as of 2024, US East region)
        # DetectDocumentText: $1.50 per 1,000 pages
        cost_per_page = 1.50 / 1000
        
        estimated_cost = self.textract_usage['pages'] * cost_per_page
        
        return {
            'pages_processed': self.textract_usage['pages'],
            'api_calls': self.textract_usage['calls'],
            'estimated_cost_usd': round(estimated_cost, 4),
            'rate_per_1000_pages': 1.50
        }
    
    def get_token_stats(self) -> dict:
        """
        Get statistics about token/character usage across all threads.
        Useful for monitoring batching efficiency.
        
        Returns:
            Dictionary with token usage statistics
        """
        stats = {
            'threads': len(self.thread_keys),
            'email_body_chars': [],
            'pdf_chars': [],
            'combined_chars': []
        }
        
        for key in self.thread_keys:
            # Email body stats
            if hasattr(self, 'combined_bodies') and key in self.combined_bodies:
                stats['email_body_chars'].append(len(self.combined_bodies[key]))
            
            # PDF stats
            if hasattr(self, 'text') and key in self.text:
                pdf_total = sum(len(t) for t in self.text[key])
                stats['pdf_chars'].append(pdf_total)
        
        # Calculate averages and totals
        if stats['email_body_chars']:
            stats['avg_email_chars'] = sum(stats['email_body_chars']) // len(stats['email_body_chars'])
            stats['total_email_chars'] = sum(stats['email_body_chars'])
        
        if stats['pdf_chars']:
            stats['avg_pdf_chars'] = sum(stats['pdf_chars']) // len(stats['pdf_chars'])
            stats['total_pdf_chars'] = sum(stats['pdf_chars'])
        
        # Estimate tokens (rough: 1 token ≈ 4 characters)
        total_chars = stats.get('total_email_chars', 0) + stats.get('total_pdf_chars', 0)
        stats['estimated_total_tokens'] = total_chars // 4
        stats['estimated_tokens_per_thread'] = stats['estimated_total_tokens'] // max(1, len(self.thread_keys))
        
        return stats