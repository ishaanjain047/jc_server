import os
import json
import PyPDF2
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
from openai import OpenAI
from dotenv import load_dotenv
from datetime import datetime
import re

# Load environment variables from .env file
load_dotenv()

class PDFToStructuredData:
    """
    A class to convert PDF supplier rate lists directly to structured data using an LLM,
    with chunking support for large PDFs.
    """
    
    def __init__(self, api_key: Optional[str] = None, output_dir: str = "processed_data", model: str = "gpt-4o"):
        """
        Initialize the processor.
        
        Args:
            api_key: API key for the OpenAI API (defaults to environment variable)
            output_dir: Directory to save processed files
            model: OpenAI model to use
        """
        # Initialize the OpenAI client
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            print("Warning: No API key provided. Set OPENAI_API_KEY environment variable or pass it as an argument.")
        
        self.client = OpenAI(api_key=self.api_key)
        self.output_dir = output_dir
        self.model = model
        os.makedirs(output_dir, exist_ok=True)
    
    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """
        Extract text from a PDF file using PyPDF2.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Extracted text as a string
        """
        text = ""
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                for page_num in range(len(pdf_reader.pages)):
                    page = pdf_reader.pages[page_num]
                    text += page.extract_text() + "\n"
                    
            return text
        except Exception as e:
            print(f"Error extracting text from {pdf_path}: {e}")
            return ""
    
    def chunk_text(self, text: str, chunk_size: int = 8000, overlap: int = 500) -> List[str]:
        """
        Split text into chunks with overlap.
        
        Args:
            text: Text to split
            chunk_size: Maximum size of each chunk
            overlap: Overlap between chunks
            
        Returns:
            List of text chunks
        """
        if len(text) <= chunk_size:
            return [text]
        
        chunks = []
        start = 0
        
        while start < len(text):
            end = start + chunk_size
            
            if end >= len(text):
                chunks.append(text[start:])
                break
                
            # Try to find a newline to break at
            nl_pos = text.rfind('\n', start, end)
            if nl_pos > start + overlap:
                end = nl_pos + 1
            
            chunks.append(text[start:end])
            start = end - overlap
        
        return chunks
    
    def process_text_chunk_with_llm(self, chunk: str, pdf_name: str, chunk_num: int, total_chunks: int) -> Dict[str, Any]:
        """
        Process a chunk of text with LLM.
        
        Args:
            chunk: Text chunk to process
            pdf_name: Name of the PDF file
            chunk_num: Current chunk number
            total_chunks: Total number of chunks
            
        Returns:
            Dictionary with structured data from the LLM
        """
        if not self.api_key:
            return {"error": "No API key available for LLM processing"}
        
        if not chunk.strip():
            return {"error": "Empty text chunk"}
        
        # Prepare system prompt
        system_prompt = """
        You are a specialized data extraction assistant for supplier rate lists and inventory documents. 
        Your task is to convert unstructured text from supplier PDFs into structured JSON data.
        
        Follow these guidelines:
        1. Carefully analyze the text structure to identify items, their prices, quantities, and other relevant details.
        2. Extract ALL fields present in the data, preserving information exactly as in the original.
        3. Maintain consistent field names across all items.
        4. Include ALL numeric values, units, packaging details, etc.
        5. For each item, extract fields like:
           - product_name: The full name of the product
           - price: The price information (maintain original format)
           - mrp: MRP if mentioned
           - packaging: Information about the packaging (e.g., number of pieces in a case)
           - quantity: The available quantity
           - unit: The unit of measurement
           - any other fields that appear in the document
        6. Return ONLY a JSON array of items with their details
        7. Be thorough - don't skip any items or fields that are present in the original text.
        8. DO NOT include metadata or any other wrapper objects - ONLY the array of items.
        
        The quality and completeness of data extraction is critical.
        """
        
        # Prepare user prompt
        user_prompt = f"""
        This is text extracted from a supplier rate list PDF named "{pdf_name}". 
        This is chunk {chunk_num} of {total_chunks}.
        
        Please convert it into a JSON array of items. Extract all items with their complete details.
        
        Here's the extracted text:
        
        {chunk}
        
        Extract ALL items and their details into a JSON array. Don't summarize or skip any items.
        IMPORTANT: RETURN ONLY THE JSON ARRAY OF ITEMS, with no wrapper object. 
        The response should begin with [ and end with ].
        """
        
        # Make the API call to OpenAI
        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.0,  # Zero temperature for deterministic results
                max_tokens=10000
            )
            
            llm_response = completion.choices[0].message.content
            
            # Extract the JSON part from the response
            try:
                # Find JSON content between triple backticks if present
                json_match = re.search(r'```json\n(.*?)\n```', llm_response, re.DOTALL)
                
                if json_match:
                    json_str = json_match.group(1)
                else:
                    # Otherwise try to parse the entire response as JSON
                    json_str = llm_response
                
                # Make sure it's a valid JSON array
                if not json_str.strip().startswith('['):
                    json_str = '[' + json_str.strip()
                if not json_str.strip().endswith(']'):
                    json_str = json_str.strip() + ']'
                
                items = json.loads(json_str)
                
                return {
                    "success": True,
                    "items": items,
                    "llm_raw_response": llm_response
                }
                
            except json.JSONDecodeError as e:
                return {
                    "error": f"Failed to parse JSON from LLM response for chunk {chunk_num}: {str(e)}",
                    "llm_raw_response": llm_response
                }
                
        except Exception as e:
            return {"error": f"Error during API call for chunk {chunk_num}: {str(e)}"}
    
    def save_extracted_text(self, text: str, output_path: str) -> None:
        """
        Save the extracted text to a file.
        
        Args:
            text: The extracted text
            output_path: Path to save the text file
        """
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(text)
    
    def save_structured_data(self, data: Dict[str, Any], output_base: str) -> Dict[str, str]:
        """
        Save the structured data to various formats.
        
        Args:
            data: The structured data
            output_base: Base path for output files (without extension)
            
        Returns:
            Dictionary with paths to saved files
        """
        output_files = {}
        
        # Save as JSON
        json_path = f"{output_base}.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        output_files["json"] = json_path
        
        # Save items as CSV and Excel if available
        items = data.get("structured_data", {}).get("items", [])
        if items:
            # Save as CSV
            csv_path = f"{output_base}.csv"
            df = pd.DataFrame(items)
            df.to_csv(csv_path, index=False, encoding='utf-8')
            output_files["csv"] = csv_path
            
            # Save as Excel
            excel_path = f"{output_base}.xlsx"
            df.to_excel(excel_path, index=False)
            output_files["excel"] = excel_path
        
        return output_files
    
    def process_pdf(self, pdf_path: str) -> Dict[str, Any]:
        """
        Process a PDF file into structured data using chunking for large PDFs.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Result dictionary with processing information
        """
        # Create a directory for this PDF's outputs
        pdf_name = os.path.basename(pdf_path)
        base_name = os.path.splitext(pdf_name)[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        pdf_output_dir = os.path.join(self.output_dir, f"{base_name}_{timestamp}")
        os.makedirs(pdf_output_dir, exist_ok=True)
        
        # Extract text from PDF
        print(f"Extracting text from {pdf_name}...")
        text = self.extract_text_from_pdf(pdf_path)
        
        if not text.strip():
            return {"error": f"Failed to extract text from {pdf_name}"}
        
        # Save the extracted text
        text_path = os.path.join(pdf_output_dir, f"{base_name}_extracted_text.txt")
        self.save_extracted_text(text, text_path)
        print(f"Extracted text saved to {text_path}")
        
        # Split text into chunks if it's large
        chunks = self.chunk_text(text)
        print(f"Split text into {len(chunks)} chunks for processing")
        
        # Process each chunk
        all_items = []
        all_chunk_results = []
        
        for i, chunk in enumerate(chunks, 1):
            print(f"Processing chunk {i} of {len(chunks)}...")
            
            chunk_result = self.process_text_chunk_with_llm(chunk, pdf_name, i, len(chunks))
            all_chunk_results.append(chunk_result)
            
            # Save individual chunk results for debugging
            chunk_result_path = os.path.join(pdf_output_dir, f"{base_name}_chunk_{i}_result.json")
            with open(chunk_result_path, 'w', encoding='utf-8') as f:
                json.dump(chunk_result, f, indent=2)
            
            if "error" in chunk_result:
                print(f"Warning: Error in chunk {i}: {chunk_result['error']}")
                continue
                
            if "items" in chunk_result and isinstance(chunk_result["items"], list):
                all_items.extend(chunk_result["items"])
        
        # Check if we successfully processed any chunks
        if not all_items:
            error_msg = "Failed to extract any items from the PDF"
            error_path = os.path.join(pdf_output_dir, f"{base_name}_error.json")
            with open(error_path, 'w', encoding='utf-8') as f:
                json.dump({"error": error_msg, "chunk_results": all_chunk_results}, f, indent=2)
            return {"error": error_msg, "text_path": text_path, "error_path": error_path}
        
        # Assign unique IDs to all items
        for idx, item in enumerate(all_items, 1):
            item["id"] = idx
        
        # Create final result with all items
        final_result = {
            "structured_data": {
                "items": all_items,
                "metadata": {
                    "source": pdf_name,
                    "extracted_chunks": len(chunks),
                    "total_items": len(all_items),
                    "processing_date": datetime.now().isoformat()
                }
            }
        }
        
        # Save the structured data
        output_base = os.path.join(pdf_output_dir, f"{base_name}_structured")
        output_files = self.save_structured_data(final_result, output_base)
        
        print(f"Processing completed successfully! Extracted {len(all_items)} items.")
        for file_type, file_path in output_files.items():
            print(f"{file_type.upper()} saved to: {file_path}")
        
        return {
            "success": True,
            "text_path": text_path,
            "output_files": output_files,
            "output_dir": pdf_output_dir,
            "structured_data": final_result
        }