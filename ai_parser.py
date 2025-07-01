#!/usr/bin/env python3
"""
AI Parser for Polymarket Data - Stage 2 of the Data Pipeline
Takes raw Jina Reader output and converts it to structured JSON using AI.
"""

import json
import logging
import os
from typing import Optional, Dict, Any, List
import google.generativeai as genai
from jina_reader_api import JinaReaderAPI
from content_preprocessor import ContentPreprocessor

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_env_file(env_file_path: str = '.env') -> None:
    """
    Load environment variables from a .env file
    
    Args:
        env_file_path: Path to the .env file
    """
    if os.path.exists(env_file_path):
        try:
            with open(env_file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip()
            logger.info(f"Loaded environment variables from {env_file_path}")
        except Exception as e:
            logger.warning(f"Failed to load .env file: {e}")
    else:
        logger.info(f"No .env file found at {env_file_path}")

class AIPolymarketParser:
    def __init__(self, gemini_api_key: str = None):
        """
        Initialize the AI Parser with Gemini API
        
        Args:
            gemini_api_key: Gemini API key. If None, will try to get from environment variable GEMINI_API_KEY
        """
        self.gemini_api_key = gemini_api_key or os.getenv('GEMINI_API_KEY')
        if not self.gemini_api_key:
            raise ValueError("Gemini API key is required. Set GEMINI_API_KEY environment variable or pass it to constructor.")
        
        # Configure Gemini
        genai.configure(api_key=self.gemini_api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash')
        
        # System prompt for market parsing
        self.system_prompt = """You are an expert data extraction assistant. Your task is to parse the provided markdown text from Polymarket and convert it into a structured JSON object that **preserves market groupings**.

The final output must be a single JSON object with a root key "markets", which is an array. This array can contain two different types of objects:

**1. For Grouped Markets (like the NYC Mayoral Election):**
When you see a category title followed by a list of related sub-markets, create a **group object**. This object must have the following structure:
```json
{
  "group_title": "string",
  "markets": [
    {
      "market_title": "string",
      "market_type": "binary",
      "options": [ { "name": "Yes", "odds": float }, { "name": "No", "odds": float } ]
    }
  ]
}
```
- The `market_title` for each item inside the group should be a full question combining the subject and the group title.

**2. For Standalone Markets (like the Fed or Tariff questions):**
For markets that are not part of a group, create a **standalone market object**. This object must have the following structure:
```json
{
  "market_title": "string",
  "market_type": "binary" | "multi_option",
  "options": [ { "name": "string", "odds": float } ]
}
```

**General Rules for all markets:**
- For all binary markets, the provided percentage is for the "Yes" option. You must calculate the "No" option as (100% - Yes%).
- Convert all percentages to decimals (e.g., 81% becomes 0.81). Handle "<1%" as 0.005.
- Do not include any other text or explanations outside of the final JSON object."""

    def extract_markdown_content(self, jina_data: Dict[str, Any]) -> Optional[str]:
        """
        Extract markdown content from Jina Reader API response
        
        Args:
            jina_data: Raw data from Jina Reader API
            
        Returns:
            Markdown content as string, or None if not found
        """
        try:
            # Try different possible content fields
            content_fields = ['markdown', 'content', 'text', 'html']
            
            for field in content_fields:
                if field in jina_data and jina_data[field]:
                    logger.info(f"Found content in field: {field}")
                    return str(jina_data[field])
            
            # If no specific content field, try to extract from the entire response
            if isinstance(jina_data, dict):
                # Look for any field that contains markdown-like content
                for key, value in jina_data.items():
                    if isinstance(value, str) and ('#' in value or '[' in value or '**' in value):
                        logger.info(f"Found markdown-like content in field: {key}")
                        return value
            
            logger.warning("No markdown content found in Jina data")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting markdown content: {e}")
            return None

    def chunk_content_by_markets(self, content: str, num_chunks: int = 2) -> List[str]:
        """
        Split content into chunks at market boundaries to avoid cutting off markets
        
        Args:
            content: Content to split
            num_chunks: Number of chunks to create
            
        Returns:
            List of content chunks
        """
        lines = content.split('\n')
        
        # Find market boundaries (lines that start with market indicators)
        market_boundaries = []
        for i, line in enumerate(lines):
            # Look for lines that indicate new markets
            if any(indicator in line.lower() for indicator in [
                'will ', 'what ', 'when ', 'who ', 'how '
            ]):
                market_boundaries.append(i)
        
        # If we don't have enough market boundaries, fall back to line-based chunking
        if len(market_boundaries) < num_chunks:
            logger.warning(f"Only found {len(market_boundaries)} market boundaries, using line-based chunking")
            return self.chunk_content(content, num_chunks)
        
        # Split at market boundaries
        chunk_size = len(market_boundaries) // num_chunks
        chunks = []
        
        for i in range(num_chunks):
            start_boundary = i * chunk_size
            end_boundary = start_boundary + chunk_size if i < num_chunks - 1 else len(market_boundaries)
            
            # Get the line indices for this chunk
            if i == 0:
                start_line = 0
            else:
                start_line = market_boundaries[start_boundary]
            
            if i == num_chunks - 1:
                end_line = len(lines)
            else:
                end_line = market_boundaries[end_boundary]
            
            chunk_lines = lines[start_line:end_line]
            chunks.append('\n'.join(chunk_lines))
            
            logger.info(f"Chunk {i+1}: lines {start_line}-{end_line} ({len(chunk_lines)} lines)")
        
        return chunks

    def chunk_content(self, content: str, num_chunks: int = 2) -> List[str]:
        """
        Split content into chunks for processing
        
        Args:
            content: Content to split
            num_chunks: Number of chunks to create
            
        Returns:
            List of content chunks
        """
        lines = content.split('\n')
        chunk_size = len(lines) // num_chunks
        
        chunks = []
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = start_idx + chunk_size if i < num_chunks - 1 else len(lines)
            chunk_lines = lines[start_idx:end_idx]
            chunks.append('\n'.join(chunk_lines))
        
        return chunks

    def combine_market_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Combine multiple market parsing results into one
        
        Args:
            results: List of market parsing results
            
        Returns:
            Combined market data
        """
        combined_markets = []
        
        for result in results:
            if result and 'markets' in result:
                combined_markets.extend(result['markets'])
        
        return {'markets': combined_markets}

    def parse_with_ai_chunked(self, markdown_content: str, num_chunks: int = 2) -> Optional[Dict[str, Any]]:
        """
        Parse markdown content using Gemini AI with chunking
        
        Args:
            markdown_content: Raw markdown content from Jina Reader
            num_chunks: Number of chunks to split content into
            
        Returns:
            Structured JSON data, or None if parsing failed
        """
        try:
            logger.info(f"Starting chunked AI parsing with {num_chunks} chunks...")
            
            # Split content into chunks at market boundaries
            chunks = self.chunk_content_by_markets(markdown_content, num_chunks)
            logger.info(f"Split content into {len(chunks)} chunks")
            
            # Process each chunk
            results = []
            for i, chunk in enumerate(chunks):
                logger.info(f"Processing chunk {i+1}/{len(chunks)} ({len(chunk)} characters)")
                
                # Create the prompt for this chunk
                chunk_prompt = f"{self.system_prompt}\n\nPlease parse the following Polymarket markdown content (chunk {i+1} of {len(chunks)}):\n\n{chunk}"
                
                # Generate response from Gemini
                response = self.model.generate_content(chunk_prompt)
                
                if response.text:
                    logger.info(f"Chunk {i+1} AI parsing completed successfully")
                    
                    # Try to extract JSON from the response
                    json_data = self.extract_json_from_response(response.text)
                    if json_data:
                        results.append(json_data)
                        logger.info(f"Chunk {i+1} extracted {len(json_data.get('markets', []))} markets")
                    else:
                        logger.error(f"Failed to extract valid JSON from chunk {i+1}")
                        # Try fallback to single-chunk processing
                        logger.info("Attempting fallback to single-chunk processing...")
                        return self.parse_with_ai(markdown_content)
                else:
                    logger.error(f"AI model returned empty response for chunk {i+1}")
                    # Try fallback to single-chunk processing
                    logger.info("Attempting fallback to single-chunk processing...")
                    return self.parse_with_ai(markdown_content)
            
            # Combine results
            if results:
                combined_result = self.combine_market_results(results)
                logger.info(f"Combined {len(results)} chunks into {len(combined_result.get('markets', []))} total markets")
                return combined_result
            else:
                logger.error("No valid results from any chunk")
                return None
                
        except Exception as e:
            logger.error(f"Error during chunked AI parsing: {e}")
            # Try fallback to single-chunk processing
            logger.info("Attempting fallback to single-chunk processing...")
            return self.parse_with_ai(markdown_content)

    def parse_with_ai(self, markdown_content: str) -> Optional[Dict[str, Any]]:
        """
        Parse markdown content using Gemini AI
        
        Args:
            markdown_content: Raw markdown content from Jina Reader
            
        Returns:
            Structured JSON data, or None if parsing failed
        """
        try:
            logger.info("Starting AI parsing of markdown content...")
            
            # Create the prompt
            prompt = f"{self.system_prompt}\n\nPlease parse the following Polymarket markdown content:\n\n{markdown_content}"
            
            # Generate response from Gemini
            response = self.model.generate_content(prompt)
            
            if response.text:
                logger.info("AI parsing completed successfully")
                
                # Try to extract JSON from the response
                json_data = self.extract_json_from_response(response.text)
                if json_data:
                    return json_data
                else:
                    logger.error("Failed to extract valid JSON from AI response")
                    return None
            else:
                logger.error("AI model returned empty response")
                return None
                
        except Exception as e:
            logger.error(f"Error during AI parsing: {e}")
            return None

    def extract_json_from_response(self, response_text: str) -> Optional[Dict[str, Any]]:
        """
        Extract JSON from AI response text
        
        Args:
            response_text: Raw response from AI model
            
        Returns:
            Parsed JSON data, or None if extraction failed
        """
        try:
            # Debug: Log the response for troubleshooting
            logger.info(f"AI Response length: {len(response_text)} characters")
            logger.info(f"AI Response preview: {response_text[:200]}...")
            
            # Try to find JSON in the response
            # Look for JSON blocks marked with ```json
            if '```json' in response_text:
                start = response_text.find('```json') + 7
                end = response_text.find('```', start)
                if end != -1:
                    json_str = response_text[start:end].strip()
                    logger.info(f"Found JSON block with ```json markers: {json_str[:100]}...")
                    try:
                        return json.loads(json_str)
                    except json.JSONDecodeError as e:
                        logger.warning(f"JSON decode error in ```json block: {e}")
                        # Try to fix common issues
                        fixed_json = self.fix_json_string(json_str)
                        if fixed_json:
                            return json.loads(fixed_json)
            
            # Look for JSON blocks marked with ```
            if '```' in response_text:
                start = response_text.find('```') + 3
                end = response_text.find('```', start)
                if end != -1:
                    json_str = response_text[start:end].strip()
                    logger.info(f"Found JSON block with ``` markers: {json_str[:100]}...")
                    # Try to parse as JSON
                    try:
                        return json.loads(json_str)
                    except json.JSONDecodeError as e:
                        logger.warning(f"JSON decode error in ``` block: {e}")
                        # Try to fix common issues
                        fixed_json = self.fix_json_string(json_str)
                        if fixed_json:
                            return json.loads(fixed_json)
            
            # Try to parse the entire response as JSON
            try:
                logger.info("Attempting to parse entire response as JSON...")
                return json.loads(response_text.strip())
            except json.JSONDecodeError as e:
                logger.warning(f"JSON decode error for entire response: {e}")
                pass
            
            # If all else fails, try to find JSON-like content
            import re
            json_pattern = r'\{.*\}'
            matches = re.findall(json_pattern, response_text, re.DOTALL)
            
            logger.info(f"Found {len(matches)} potential JSON matches with regex")
            for i, match in enumerate(matches):
                try:
                    logger.info(f"Trying to parse match {i+1}: {match[:100]}...")
                    return json.loads(match)
                except json.JSONDecodeError as e:
                    logger.warning(f"JSON decode error for match {i+1}: {e}")
                    # Try to fix common issues
                    fixed_json = self.fix_json_string(match)
                    if fixed_json:
                        try:
                            return json.loads(fixed_json)
                        except json.JSONDecodeError:
                            continue
                    continue
            
            logger.error("Could not extract valid JSON from AI response")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting JSON from response: {e}")
            return None

    def fix_json_string(self, json_str: str) -> Optional[str]:
        """
        Try to fix common JSON formatting issues
        
        Args:
            json_str: Potentially malformed JSON string
            
        Returns:
            Fixed JSON string, or None if cannot be fixed
        """
        try:
            # Remove trailing commas before closing braces/brackets
            import re
            fixed = re.sub(r',(\s*[}\]])', r'\1', json_str)
            
            # Try to parse the fixed version
            json.loads(fixed)
            return fixed
        except:
            return None

    def save_structured_data(self, data: Dict[str, Any], filename: str = "structured_polymarket_data.json") -> bool:
        """
        Save structured data to file
        
        Args:
            data: Structured JSON data
            filename: Output filename
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Structured data saved to {filename}")
            return True
        except Exception as e:
            logger.error(f"Error saving structured data: {e}")
            return False

    def process_text_content(self, text_file: str = "jina_polymarket_content.txt", save_files: bool = False) -> Optional[Dict[str, Any]]:
        """
        Process text content file directly and convert to structured JSON
        
        Args:
            text_file: Path to text content file
            save_files: Whether to save intermediate files (default: False)
            
        Returns:
            Structured JSON data, or None if processing failed
        """
        try:
            logger.info(f"Processing text content file: {text_file}")
            
            # Initialize content preprocessor
            preprocessor = ContentPreprocessor()
            
            # Check if we have cleaned content already
            cleaned_file = "cleaned_polymarket_content.txt"
            if os.path.exists(cleaned_file):
                logger.info(f"Found existing cleaned content: {cleaned_file}")
                with open(cleaned_file, 'r', encoding='utf-8') as f:
                    cleaned_content = f.read()
            else:
                # Load and clean text content
                if not os.path.exists(text_file):
                    logger.error(f"Text content file not found: {text_file}")
                    return None
                
                with open(text_file, 'r', encoding='utf-8') as f:
                    raw_content = f.read()
                
                if not raw_content:
                    logger.error("No content found in text file")
                    return None
                
                # Clean the content using preprocessor
                logger.info("Cleaning content with preprocessor...")
                cleaned_content = preprocessor.clean_content(raw_content)
                
                # Save cleaned content only if requested
                if save_files:
                    with open(cleaned_file, 'w', encoding='utf-8') as f:
                        f.write(cleaned_content)
                    logger.info(f"Saved cleaned content to {cleaned_file}")
            
            # Parse cleaned content with AI (using chunking)
            structured_data = self.parse_with_ai_chunked(cleaned_content)
            if not structured_data:
                logger.error("AI parsing failed")
                return None
            
            # Save structured data only if requested
            if save_files:
                self.save_structured_data(structured_data)
            
            return structured_data
            
        except Exception as e:
            logger.error(f"Error processing text content: {e}")
            return None

    def process_jina_output(self, jina_data_file: str = "jina_polymarket_data.json", save_files: bool = False) -> Optional[Dict[str, Any]]:
        """
        Process Jina Reader output file and convert to structured JSON
        
        Args:
            jina_data_file: Path to Jina Reader output file
            save_files: Whether to save intermediate files (default: False)
            
        Returns:
            Structured JSON data, or None if processing failed
        """
        try:
            logger.info(f"Processing Jina output file: {jina_data_file}")
            
            # Initialize content preprocessor
            preprocessor = ContentPreprocessor()
            
            # Check if we have cleaned content already
            cleaned_file = "cleaned_polymarket_content.txt"
            if os.path.exists(cleaned_file):
                logger.info(f"Found existing cleaned content: {cleaned_file}")
                with open(cleaned_file, 'r', encoding='utf-8') as f:
                    cleaned_content = f.read()
            else:
                # Load and clean Jina data
                if not os.path.exists(jina_data_file):
                    logger.error(f"Jina data file not found: {jina_data_file}")
                    return None
                
                with open(jina_data_file, 'r', encoding='utf-8') as f:
                    jina_data = json.load(f)
                
                # Extract markdown content
                markdown_content = self.extract_markdown_content(jina_data)
                if not markdown_content:
                    logger.error("No markdown content found in Jina data")
                    return None
                
                # Clean the content using preprocessor
                logger.info("Cleaning content with preprocessor...")
                cleaned_content = preprocessor.clean_content(markdown_content)
                
                # Save cleaned content only if requested
                if save_files:
                    with open(cleaned_file, 'w', encoding='utf-8') as f:
                        f.write(cleaned_content)
                    logger.info(f"Saved cleaned content to {cleaned_file}")
            
            # Parse cleaned content with AI (using chunking)
            structured_data = self.parse_with_ai_chunked(cleaned_content)
            if not structured_data:
                logger.error("AI parsing failed")
                return None
            
            # Save structured data only if requested
            if save_files:
                self.save_structured_data(structured_data)
            
            return structured_data
            
        except Exception as e:
            logger.error(f"Error processing Jina output: {e}")
            return None

    def run_full_pipeline(self, save_files: bool = False) -> Optional[Dict[str, Any]]:
        """
        Run the complete Stage 2 pipeline: Jina Reader + AI Parsing
        
        Args:
            save_files: Whether to save intermediate files (default: False)
            
        Returns:
            Structured JSON data, or None if pipeline failed
        """
        try:
            logger.info("🚀 Starting Stage 2: AI Intelligence Layer")
            
            # First, run Stage 1 (Jina Reader) if needed
            jina_parser = JinaReaderAPI()
            
            # Check if we already have Jina content
            if not os.path.exists("jina_polymarket_content.txt"):
                logger.info("No existing Jina content found. Running Stage 1 first...")
                text_content, jina_data = jina_parser.parse_and_save()
                if not text_content:
                    logger.error("Stage 1 (Jina Reader) failed")
                    return None
            else:
                logger.info("Found existing Jina content. Proceeding with AI parsing...")
            
            # Now run Stage 2 (AI Parsing) with text content
            structured_data = self.process_text_content(save_files=save_files)
            
            if structured_data:
                logger.info("✅ Stage 2 completed successfully!")
                return structured_data
            else:
                logger.error("❌ Stage 2 failed")
                return None
                
        except Exception as e:
            logger.error(f"Error in full pipeline: {e}")
            return None

def main():
    """Main function to run the AI Parser"""
    print("🧠 AI Polymarket Parser - Stage 2")
    print("=" * 50)
    
    # Load environment variables from .env file
    load_env_file()
    
    try:
        # Initialize AI parser
        parser = AIPolymarketParser()
        
        # Run the full pipeline
        structured_data = parser.run_full_pipeline()
        
        if structured_data:
            print(f"\n✅ Successfully parsed Polymarket data with AI!")
            print(f"📊 Markets found: {len(structured_data.get('markets', []))}")
            
            # Show some statistics
            markets = structured_data.get('markets', [])
            grouped_markets = [m for m in markets if 'group_title' in m]
            standalone_markets = [m for m in markets if 'group_title' not in m]
            
            print(f"📋 Market groups: {len(grouped_markets)}")
            print(f"📋 Standalone markets: {len(standalone_markets)}")
            
            # Show a preview of the first few markets
            print(f"\n📝 Preview of structured data:")
            preview_data = {
                "markets": markets[:3]  # Show first 3 markets
            }
            print(json.dumps(preview_data, indent=2))
            
            print(f"\n💾 Files created:")
            print(f"   - Raw data: jina_polymarket_data.json")
            print(f"   - Structured data: structured_polymarket_data.json")
            
        else:
            print("❌ Failed to parse Polymarket data")
            print("\n💡 Suggestions:")
            print("   - Check your Gemini API key")
            print("   - Verify the Jina data file exists")
            print("   - Try running Stage 1 first")
            
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("💡 Please set your GEMINI_API_KEY environment variable")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    main() 