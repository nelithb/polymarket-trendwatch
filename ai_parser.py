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

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
            # Try to find JSON in the response
            # Look for JSON blocks marked with ```
            if '```json' in response_text:
                start = response_text.find('```json') + 7
                end = response_text.find('```', start)
                if end != -1:
                    json_str = response_text[start:end].strip()
                    return json.loads(json_str)
            
            # Look for JSON blocks marked with ```
            if '```' in response_text:
                start = response_text.find('```') + 3
                end = response_text.find('```', start)
                if end != -1:
                    json_str = response_text[start:end].strip()
                    # Try to parse as JSON
                    try:
                        return json.loads(json_str)
                    except json.JSONDecodeError:
                        pass
            
            # Try to parse the entire response as JSON
            try:
                return json.loads(response_text.strip())
            except json.JSONDecodeError:
                pass
            
            # If all else fails, try to find JSON-like content
            import re
            json_pattern = r'\{.*\}'
            matches = re.findall(json_pattern, response_text, re.DOTALL)
            
            for match in matches:
                try:
                    return json.loads(match)
                except json.JSONDecodeError:
                    continue
            
            logger.error("Could not extract valid JSON from AI response")
            logger.debug(f"Response text: {response_text[:500]}...")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting JSON from response: {e}")
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

    def process_jina_output(self, jina_data_file: str = "jina_polymarket_data.json") -> Optional[Dict[str, Any]]:
        """
        Process Jina Reader output file and convert to structured JSON
        
        Args:
            jina_data_file: Path to Jina Reader output file
            
        Returns:
            Structured JSON data, or None if processing failed
        """
        try:
            logger.info(f"Processing Jina output file: {jina_data_file}")
            
            # Load Jina data
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
            
            # Parse with AI
            structured_data = self.parse_with_ai(markdown_content)
            if not structured_data:
                logger.error("AI parsing failed")
                return None
            
            # Save structured data
            self.save_structured_data(structured_data)
            
            return structured_data
            
        except Exception as e:
            logger.error(f"Error processing Jina output: {e}")
            return None

    def run_full_pipeline(self) -> Optional[Dict[str, Any]]:
        """
        Run the complete Stage 2 pipeline: Jina Reader + AI Parsing
        
        Returns:
            Structured JSON data, or None if pipeline failed
        """
        try:
            logger.info("🚀 Starting Stage 2: AI Intelligence Layer")
            
            # First, run Stage 1 (Jina Reader) if needed
            jina_parser = JinaReaderAPI()
            
            # Check if we already have Jina data
            if not os.path.exists("jina_polymarket_data.json"):
                logger.info("No existing Jina data found. Running Stage 1 first...")
                text_content, jina_data = jina_parser.parse_and_save()
                if not jina_data:
                    logger.error("Stage 1 (Jina Reader) failed")
                    return None
            else:
                logger.info("Found existing Jina data. Proceeding with AI parsing...")
            
            # Now run Stage 2 (AI Parsing)
            structured_data = self.process_jina_output()
            
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