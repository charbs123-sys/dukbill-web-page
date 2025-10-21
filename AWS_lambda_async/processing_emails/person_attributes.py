import boto3
import os
import base64
import tempfile
import fitz
import pytesseract
from PIL import Image
import io
from botocore.exceptions import ClientError
import concurrent.futures
from threading import Lock
import asyncio
from functools import partial



class Person():
    """
    LAMBDA-COMPATIBLE VERSION: Uses ThreadPoolExecutor instead of ProcessPoolExecutor
    
    While threading doesn't bypass GIL for CPU-bound tasks, it still provides
    significant speedup for Tesseract OCR due to:
    1. I/O operations (file reading/writing)
    2. Native C extensions (Tesseract) that release GIL
    3. Concurrent execution of multiple OCR operations
    
    Performance: 2-3x faster than sequential (vs 4-6x with true multiprocessing)
    Benefit: Works perfectly in AWS Lambda environment
    """
    def __init__(self, threads_json: dict, max_pdf_chars: int = 3000, max_email_body_chars: int = 2000, 
                 use_parallel_textract: bool = True, max_textract_workers: int = 6,
                 smart_textract: bool = False, textract_only_if_empty: bool = False,
                 use_tesseract: bool = False, max_tesseract_workers: int = None) -> None:
        self.threads = threads_json
        self.thread_keys = list(threads_json.keys())
        self.textract_client = boto3.client('textract')
        self.textract_usage = {'pages': 0, 'calls': 0}
        self.textract_lock = Lock()
        
        # Token/character limits
        self.max_pdf_chars = max_pdf_chars
        self.max_email_body_chars = max_email_body_chars
        self.max_combined_chars = 5000
        
        # Parallel processing settings
        self.use_parallel_textract = use_parallel_textract
        self.max_textract_workers = max_textract_workers
        
        # Smart Textract settings
        self.smart_textract = smart_textract
        self.textract_only_if_empty = textract_only_if_empty
        
        # Tesseract threading settings (NOT multiprocessing)
        self.use_tesseract = use_tesseract
        
        # Auto-detect optimal worker count if not specified
        if max_tesseract_workers is None:
            # For threading in Lambda, we can use more workers since they're lighter
            lambda_memory = int(os.environ.get('AWS_LAMBDA_FUNCTION_MEMORY_SIZE', 2048))
            estimated_vcpus = max(1, int(lambda_memory / 1769))
            # Use 2-3x more threads than vCPUs since Tesseract releases GIL
            self.max_tesseract_workers = min(estimated_vcpus * 2, 12)
            print(f"[Tesseract] Auto-detected {self.max_tesseract_workers} thread workers for {lambda_memory}MB memory")
        else:
            self.max_tesseract_workers = max_tesseract_workers
        
        self.tesseract_usage = {'pages': 0, 'calls': 0}
        self.tesseract_lock = Lock()  # Lock for thread-safe counter updates
    
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
            pdfs[key] = list(set(encoded_list)) if encoded_list else []
        
        self.unique_pdfs = pdfs

    def _extract_first_page_to_bytes(self, pdf_bytes: bytes) -> tuple[bytes, bool]:
        """Extract only the first page of a PDF and return as clean single-page PDF bytes."""
        temp_input = None
        temp_output = None
        
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_in:
                temp_in.write(pdf_bytes)
                temp_input = temp_in.name
            
            doc = fitz.open(temp_input)
            
            if len(doc) == 0:
                doc.close()
                return None, False
            
            new_doc = fitz.open()
            new_doc.insert_pdf(doc, from_page=0, to_page=0)
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_out:
                temp_output = temp_out.name
            
            new_doc.save(temp_output, garbage=4, deflate=True, clean=True)
            new_doc.close()
            doc.close()
            
            with open(temp_output, 'rb') as f:
                single_page_bytes = f.read()
            
            return single_page_bytes, True
            
        except Exception as e:
            print(f"[Single-Page Extract Error]: {e}")
            return None, False
            
        finally:
            for temp_file in [temp_input, temp_output]:
                if temp_file:
                    try:
                        os.unlink(temp_file)
                    except:
                        pass

    def _tesseract_worker_thread(self, pdf_bytes: bytes, max_chars: int, key: str, pdf_idx: int) -> dict:
        """
        Thread worker for Tesseract OCR (Lambda-compatible).
        Can be a regular instance method since threads share memory.
        
        Args:
            pdf_bytes: PDF file as bytes
            max_chars: Maximum characters to extract
            key: Thread key for identification
            pdf_idx: PDF index for identification
            
        Returns:
            dict with extracted text and metadata
        """
        try:
            # Extract first page
            temp_input = None
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_in:
                    temp_in.write(pdf_bytes)
                    temp_input = temp_in.name
                
                doc = fitz.open(temp_input)
                if len(doc) == 0:
                    doc.close()
                    return {
                        'key': key,
                        'pdf_idx': pdf_idx,
                        'text': "[Error: Empty PDF]",
                        'success': False
                    }
                
                # Extract first page
                new_doc = fitz.open()
                new_doc.insert_pdf(doc, from_page=0, to_page=0)
                
                with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_out:
                    temp_output = temp_out.name
                
                new_doc.save(temp_output, garbage=4, deflate=True, clean=True)
                new_doc.close()
                doc.close()
                
                with open(temp_output, 'rb') as f:
                    single_page_bytes = f.read()
                
                os.unlink(temp_output)
                
            except Exception as e:
                print(f"[Tesseract Thread] Page extraction failed for {key}/{pdf_idx}: {e}")
                return {
                    'key': key,
                    'pdf_idx': pdf_idx,
                    'text': f"[Error: Page extraction failed - {e}]",
                    'success': False
                }
            finally:
                if temp_input:
                    try:
                        os.unlink(temp_input)
                    except:
                        pass
            
            # Convert PDF to image and perform OCR
            temp_pdf = None
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp:
                    temp.write(single_page_bytes)
                    temp_pdf = temp.name
                
                doc = fitz.open(temp_pdf)
                page = doc[0]
                
                # Render at 2x resolution for better OCR
                mat = fitz.Matrix(2.0, 2.0)
                pix = page.get_pixmap(matrix=mat)
                
                # Convert to PIL Image
                img_data = pix.tobytes("png")
                img = Image.open(io.BytesIO(img_data))
                
                doc.close()
                
                # Perform OCR - Tesseract is a C extension that releases GIL
                custom_config = r'--oem 3 --psm 6'
                text = pytesseract.image_to_string(img, config=custom_config)
                
                # Truncate if needed
                if len(text) > max_chars:
                    text = text[:max_chars] + f"\n[TRUNCATED at {max_chars} chars]"
                
                result_text = text.strip() or "[Tesseract: No text found]"
                
                # Thread-safe counter update
                with self.tesseract_lock:
                    self.tesseract_usage['pages'] += 1
                    self.tesseract_usage['calls'] += 1
                
                return {
                    'key': key,
                    'pdf_idx': pdf_idx,
                    'text': result_text,
                    'success': True,
                    'chars': len(result_text)
                }
                
            except Exception as e:
                print(f"[Tesseract Thread] OCR failed for {key}/{pdf_idx}: {e}")
                return {
                    'key': key,
                    'pdf_idx': pdf_idx,
                    'text': f"[Error: OCR failed - {e}]",
                    'success': False
                }
            finally:
                if temp_pdf:
                    try:
                        os.unlink(temp_pdf)
                    except:
                        pass
                        
        except Exception as e:
            return {
                'key': key,
                'pdf_idx': pdf_idx,
                'text': f"[Error: Worker failed - {e}]",
                'success': False
            }

    def _extract_with_tesseract_threading(self, ocr_tasks: list) -> dict:
        """
        Process multiple PDFs in parallel using ThreadPoolExecutor (Lambda-compatible).
        
        Args:
            ocr_tasks: List of dicts with 'key', 'pdf_idx', 'pdf_bytes', 'max_chars'
            
        Returns:
            dict mapping (key, pdf_idx) to extracted text
        """
        if not ocr_tasks:
            return {}
        
        print(f"[Threading OCR] Processing {len(ocr_tasks)} PDFs with {self.max_tesseract_workers} thread workers")
        
        results = {}
        
        # Use ThreadPoolExecutor instead of ProcessPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_tesseract_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(
                    self._tesseract_worker_thread,
                    task['pdf_bytes'],
                    task['max_chars'],
                    task['key'],
                    task['pdf_idx']
                ): task
                for task in ocr_tasks
            }
            
            # Collect results as they complete
            completed = 0
            for future in concurrent.futures.as_completed(future_to_task):
                try:
                    result = future.result(timeout=30)  # 30s timeout per PDF
                    key = result['key']
                    pdf_idx = result['pdf_idx']
                    
                    results[(key, pdf_idx)] = result['text']
                    
                    completed += 1
                    if completed % 5 == 0 or completed == len(ocr_tasks):
                        print(f"[Threading OCR] Completed {completed}/{len(ocr_tasks)} PDFs")
                    
                except concurrent.futures.TimeoutError:
                    task = future_to_task[future]
                    key, pdf_idx = task['key'], task['pdf_idx']
                    results[(key, pdf_idx)] = "[Error: OCR timeout after 30s]"
                    print(f"[Threading OCR] Timeout for {key}, PDF {pdf_idx}")
                    
                except Exception as e:
                    task = future_to_task[future]
                    key, pdf_idx = task['key'], task['pdf_idx']
                    results[(key, pdf_idx)] = f"[Error: {str(e)}]"
                    print(f"[Threading OCR] Error for {key}, PDF {pdf_idx}: {e}")
        
        print(f"[Threading OCR] Completed all {len(ocr_tasks)} PDFs")
        return results

    async def _extract_with_textract_async(self, pdf_bytes: bytes, max_chars: int = None) -> str:
        """Async Textract extraction (I/O-bound, benefits from async)."""
        if max_chars is None:
            max_chars = self.max_pdf_chars
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            partial(self._extract_with_textract, pdf_bytes, max_chars, True)
        )
        return result

    def _extract_with_textract(self, pdf_bytes: bytes, max_chars: int = None, thread_safe: bool = True) -> str:
        """Synchronous Textract extraction."""
        if max_chars is None:
            max_chars = self.max_pdf_chars
            
        try:
            single_page_bytes, extract_success = self._extract_first_page_to_bytes(pdf_bytes)
            
            if not extract_success or single_page_bytes is None:
                return "[Error: Could not extract first page from PDF]"
            
            pdf_to_process = single_page_bytes
            
            if len(pdf_to_process) > 10 * 1024 * 1024:
                return "[Error: PDF exceeds Textract's 10MB limit]"
            
            if not pdf_to_process.startswith(b'%PDF-'):
                return "[Error: Invalid PDF format]"
            
            response = self.textract_client.detect_document_text(
                Document={'Bytes': pdf_to_process}
            )
            
            if thread_safe:
                with self.textract_lock:
                    self.textract_usage['calls'] += 1
                    self.textract_usage['pages'] += 1
            else:
                self.textract_usage['calls'] += 1
                self.textract_usage['pages'] += 1
            
            extracted_lines = []
            char_count = 0
            
            for block in response.get('Blocks', []):
                if block['BlockType'] == 'LINE':
                    text = block.get('Text', '').strip()
                    if not text:
                        continue
                    
                    if char_count + len(text) + 1 > max_chars:
                        remaining_chars = max_chars - char_count
                        if remaining_chars > 0:
                            extracted_lines.append(text[:remaining_chars])
                        extracted_lines.append(f"\n[TRUNCATED at {max_chars} chars]")
                        break
                    
                    extracted_lines.append(text)
                    char_count += len(text) + 1
            
            result = '\n'.join(extracted_lines)
            return result if result else "[Textract: No text found]"
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            return f"[Error with Textract ({error_code})]"
        except Exception as e:
            return f"[Error with Textract: {str(e)}]"

    async def pdf_to_text_async(self, max_pages=1, max_chars=None, use_textract_fallback=True, 
                                min_chars_threshold=50) -> None:
        """
        LAMBDA-COMPATIBLE: Threading for Tesseract + Async for Textract
        """
        if max_chars is None:
            max_chars = self.max_pdf_chars
            
        text_stored = {}
        tesseract_tasks = []
        textract_tasks = []

        if self.unique_pdfs:
            for key, pdf_list in self.unique_pdfs.items():
                text_stored[key] = []

                for pdf_idx, pdf in enumerate(pdf_list):
                    try:
                        pdf_bytes = base64.urlsafe_b64decode(pdf)
                        
                        if not pdf_bytes.startswith(b'%PDF-'):
                            text_stored[key].append("[Error: Invalid PDF format]")
                            continue
                        
                        temp_path = None
                        extracted_text_content = ""
                        
                        try:
                            # Try PyMuPDF first
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
                                    
                                    if char_count + len(page_text) > max_chars:
                                        remaining_chars = max_chars - char_count
                                        if remaining_chars > 0:
                                            extracted_text.append(page_text[:remaining_chars])
                                        extracted_text.append(f"\n[TRUNCATED at {max_chars} chars]")
                                        break
                                    
                                    extracted_text.append(page_text)
                                    char_count += len(page_text)
                                
                                text = "\n".join(extracted_text)
                                
                                # Check if we need OCR
                                if char_count >= 99999999:  # Intentionally high
                                    extracted_text_content = text
                                else:
                                    should_use_ocr = True
                                    
                                    if self.textract_only_if_empty and char_count > 0:
                                        should_use_ocr = False
                                        extracted_text_content = text
                                    elif self.smart_textract and char_count > 5:
                                        should_use_ocr = False
                                        extracted_text_content = text
                                    
                                    if should_use_ocr:
                                        if self.use_tesseract:
                                            tesseract_tasks.append({
                                                'key': key,
                                                'pdf_idx': pdf_idx,
                                                'pdf_bytes': pdf_bytes,
                                                'max_chars': max_chars
                                            })
                                        elif use_textract_fallback:
                                            textract_tasks.append({
                                                'key': key,
                                                'pdf_idx': pdf_idx,
                                                'pdf_bytes': pdf_bytes,
                                                'max_chars': max_chars
                                            })
                                        extracted_text_content = "__OCR_PENDING__"
                                    else:
                                        extracted_text_content = text
                            
                        except Exception as fitz_error:
                            print(f"[Fitz Error] Thread {key}, PDF {pdf_idx + 1}: {fitz_error}")
                            
                            if self.use_tesseract:
                                tesseract_tasks.append({
                                    'key': key,
                                    'pdf_idx': pdf_idx,
                                    'pdf_bytes': pdf_bytes,
                                    'max_chars': max_chars
                                })
                            elif use_textract_fallback:
                                textract_tasks.append({
                                    'key': key,
                                    'pdf_idx': pdf_idx,
                                    'pdf_bytes': pdf_bytes,
                                    'max_chars': max_chars
                                })
                            extracted_text_content = "__OCR_PENDING__"
                        
                        finally:
                            if temp_path:
                                try:
                                    os.unlink(temp_path)
                                except:
                                    pass
                        
                        text_stored[key].append(extracted_text_content)
                            
                    except Exception as e:
                        print(f"[Error parsing PDF for thread {key}, PDF {pdf_idx + 1}]: {e}")
                        text_stored[key].append("[Error reading PDF]")
        
        # Process Tesseract tasks with threading (Lambda-compatible!)
        if tesseract_tasks:
            print(f"[Lambda OCR] Processing {len(tesseract_tasks)} PDFs with THREADING")
            tesseract_results = self._extract_with_tesseract_threading(tesseract_tasks)
            
            # Update text_stored with results
            for (key, pdf_idx), text in tesseract_results.items():
                for idx, stored_text in enumerate(text_stored[key]):
                    if stored_text == "__OCR_PENDING__":
                        pending_count = text_stored[key][:idx+1].count("__OCR_PENDING__")
                        task_count = sum(1 for t in tesseract_tasks if t['key'] == key and t['pdf_idx'] <= pdf_idx)
                        
                        if pending_count == task_count:
                            text_stored[key][idx] = text
                            break
        
        # Process Textract tasks with async (I/O-bound)
        if textract_tasks:
            print(f"[Lambda OCR] Processing {len(textract_tasks)} PDFs with ASYNC Textract")
            await self._process_textract_parallel_async(textract_tasks, text_stored)
        
        self.text = text_stored
    
    async def _process_textract_parallel_async(self, tasks, text_stored):
        """Process multiple Textract calls in parallel using asyncio."""
        async def process_single_textract(task):
            try:
                result = await self._extract_with_textract_async(
                    task['pdf_bytes'], 
                    task['max_chars']
                )
                return {
                    'key': task['key'],
                    'pdf_idx': task['pdf_idx'],
                    'result': result,
                    'success': True
                }
            except Exception as e:
                print(f"[Textract Async Error] Thread {task['key']}, PDF {task['pdf_idx']}: {e}")
                return {
                    'key': task['key'],
                    'pdf_idx': task['pdf_idx'],
                    'result': f"[Error with Textract: {str(e)}]",
                    'success': False
                }
        
        results = await asyncio.gather(*[process_single_textract(task) for task in tasks])
        
        for result in results:
            key = result['key']
            pdf_idx = result['pdf_idx']
            
            for idx, text in enumerate(text_stored[key]):
                if text == "__OCR_PENDING__":
                    pending_count = text_stored[key][:idx+1].count("__OCR_PENDING__")
                    task_count = sum(1 for t in tasks if t['key'] == key and t['pdf_idx'] <= pdf_idx)
                    
                    if pending_count == task_count:
                        text_stored[key][idx] = result['result']
                        break
        
        print(f"[Async Textract] All {len(tasks)} PDFs processed")
    
    def pdf_to_text(self, max_pages=1, max_chars=None, use_textract_fallback=True, min_chars_threshold=50) -> None:
        """Synchronous version."""
        return asyncio.run(self.pdf_to_text_async(max_pages, max_chars, use_textract_fallback, min_chars_threshold))
    
    def combine_text(self) -> None:
        """Combine email bodies with character limit."""
        self.combined_bodies = {}
        for keys in self.thread_keys:
            bodies = []
            char_count = 0
            
            for email in self.threads[keys]:
                body = email.get("body", "")
                
                if char_count + len(body) > self.max_email_body_chars:
                    remaining_chars = self.max_email_body_chars - char_count
                    if remaining_chars > 0:
                        bodies.append(body[:remaining_chars])
                        bodies.append(f"\n[EMAIL BODY TRUNCATED at {self.max_email_body_chars} chars]")
                    break
                
                bodies.append(body)
                char_count += len(body)
            
            combined = ''.join(bodies)
            self.combined_bodies[keys] = combined

    def combining_pdf_text(self) -> None:
        """Combine PDF texts."""
        self.pdf_text_list = []

        for key in self.thread_keys:
            pdf_texts = self.text.get(key, [])

            if not pdf_texts:
                self.pdf_text_list.append(None)
            else:
                combined_parts = []
                total_chars = 0
                
                for i, text in enumerate(pdf_texts):
                    text_stripped = text.strip()
                    header = f"===== PDF {i + 1} =====\n"
                    
                    new_chars = len(header) + len(text_stripped) + 2
                    if total_chars + new_chars > self.max_combined_chars:
                        remaining = self.max_combined_chars - total_chars
                        if remaining > len(header) + 100:
                            combined_parts.append(header + text_stripped[:remaining - len(header)])
                        combined_parts.append(f"\n[COMBINED PDF TEXT TRUNCATED at {self.max_combined_chars} chars]")
                        break
                    
                    combined_parts.append(header + text_stripped)
                    total_chars += new_chars
                
                combined = "\n\n".join(combined_parts)
                self.pdf_text_list.append(combined)
    
    def get_ocr_cost_estimate(self) -> dict:
        """Calculate estimated cost."""
        cost_per_page = 1.50 / 1000
        
        textract_cost = self.textract_usage['pages'] * cost_per_page
        tesseract_cost = 0
        
        return {
            'textract_pages': self.textract_usage['pages'],
            'textract_calls': self.textract_usage['calls'],
            'textract_cost_usd': round(textract_cost, 4),
            'tesseract_pages': self.tesseract_usage['pages'],
            'tesseract_calls': self.tesseract_usage['calls'],
            'tesseract_cost_usd': 0.00,
            'total_cost_usd': round(textract_cost, 4),
            'processing_method': 'threading' if self.use_tesseract else 'async'
        }
    
    def get_token_stats(self) -> dict:
        """Get token/character usage statistics."""
        stats = {
            'threads': len(self.thread_keys),
            'email_body_chars': [],
            'pdf_chars': []
        }
        
        for key in self.thread_keys:
            if hasattr(self, 'combined_bodies') and key in self.combined_bodies:
                stats['email_body_chars'].append(len(self.combined_bodies[key]))
            
            if hasattr(self, 'text') and key in self.text:
                pdf_total = sum(len(t) for t in self.text[key])
                stats['pdf_chars'].append(pdf_total)
        
        if stats['email_body_chars']:
            stats['avg_email_chars'] = sum(stats['email_body_chars']) // len(stats['email_body_chars'])
            stats['total_email_chars'] = sum(stats['email_body_chars'])
        
        if stats['pdf_chars']:
            stats['avg_pdf_chars'] = sum(stats['pdf_chars']) // len(stats['pdf_chars'])
            stats['total_pdf_chars'] = sum(stats['pdf_chars'])
        
        total_chars = stats.get('total_email_chars', 0) + stats.get('total_pdf_chars', 0)
        stats['estimated_total_tokens'] = total_chars // 4
        stats['estimated_tokens_per_thread'] = stats['estimated_total_tokens'] // max(1, len(self.thread_keys))
        
        return stats