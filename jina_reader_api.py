#!/usr/bin/env python3
"""
Jina Reader API script to parse text from Polymarket URL using the official Jina Reader API.
"""

import requests
import json
import time
import logging
from typing import Optional, Dict, Any, Tuple

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class JinaReaderAPI:
    def __init__(self, api_key: str = "jina_cf848c92ae3045a0920af55d77b8cf46Uy-iid2gwK4LYVd9VsxkxzCpz2Z5"):
        self.api_key = api_key
        self.base_url = "https://r.jina.ai"
        self.session = requests.Session()
        
        # Set headers for API requests
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        })
    
    def fetch_url_content(self, target_url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Fetch content from a URL using the Jina Reader API"""
        try:
            # Construct the Jina Reader URL
            jina_url = f"{self.base_url}/{target_url}"
            
            logger.info(f"Fetching content from: {jina_url}")
            
            # Set default parameters if none provided
            if params is None:
                params = {
                    'token_budget': 200000,
                    'timeout': 10,
                    'gfm': True,  # Github Flavored Markdown
                    'gather_links': True,  # Gather all links at the end
                    'gather_images': True,  # Gather all images at the end
                }
            
            # Make the request
            response = self.session.get(jina_url, params=params, timeout=60)
            
            if response.status_code == 200:
                logger.info(f"Successfully fetched content. Status: {response.status_code}")
                
                # Try to parse as JSON first
                try:
                    json_data = response.json()
                    return json_data
                except json.JSONDecodeError:
                    # If not JSON, return as text
                    logger.info("Response is not JSON, returning as text")
                    return {
                        'content': response.text,
                        'url': target_url,
                        'status': 'success',
                        'content_type': 'text'
                    }
                    
            else:
                logger.error(f"HTTP {response.status_code}: {response.text}")
                return None
                
        except requests.exceptions.Timeout:
            logger.error("Request timed out")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None
    
    def fetch_polymarket_content(self) -> Optional[Dict[str, Any]]:
        """Fetch Polymarket content using the Jina Reader API"""
        target_url = "https://polymarket.com/"
        
        # Try different parameter configurations
        configs = [
            {
                'token_budget': 200000,
                'timeout': 30,
                'gfm': True,
                'gather_links': True,
                'gather_images': True,
                'use_readerlm_v2': False,  # Don't use ReaderLM-v2 initially
            },
            {
                'token_budget': 100000,
                'timeout': 20,
                'gfm': True,
                'gather_links': True,
                'gather_images': False,
                'use_readerlm_v2': False,
            },
            {
                'token_budget': 50000,
                'timeout': 15,
                'gfm': False,
                'gather_links': False,
                'gather_images': False,
                'use_readerlm_v2': False,
            }
        ]
        
        for i, config in enumerate(configs):
            logger.info(f"Trying configuration {i + 1}/{len(configs)}")
            result = self.fetch_url_content(target_url, config)
            if result:
                return result
            
            # Wait before trying next configuration
            if i < len(configs) - 1:
                time.sleep(2)
        
        return None
    
    def save_to_file(self, content: str, filename: str = "jina_polymarket_content.txt") -> bool:
        """Save parsed content to a file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)
            logger.info(f"Content saved to {filename}")
            return True
        except Exception as e:
            logger.error(f"Error saving to file: {e}")
            return False
    
    def save_json_data(self, data: Dict[str, Any], filename: str = "jina_polymarket_data.json") -> bool:
        """Save JSON data to a file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"JSON data saved to {filename}")
            return True
        except Exception as e:
            logger.error(f"Error saving JSON data: {e}")
            return False
    
    def parse_and_save(self, save_files: bool = True) -> Tuple[Optional[str], Dict[str, Any]]:
        """Main method to parse content and optionally save to files"""
        logger.info("Starting Jina Reader API parsing...")
        
        # Fetch the content
        result = self.fetch_polymarket_content()
        if not result:
            logger.error("Failed to fetch content from Jina Reader API")
            return None, {}
        
        # Extract content based on response type
        if isinstance(result, dict):
            if 'content' in result:
                content = result['content']
            elif 'markdown' in result:
                content = result['markdown']
            elif 'text' in result:
                content = result['text']
            else:
                # If it's a dict but no clear content field, convert to string
                content = json.dumps(result, indent=2, ensure_ascii=False)
        else:
            content = str(result)
        
        # Save to files if requested
        if save_files:
            self.save_to_file(content)
            self.save_json_data(result)
        
        logger.info("Jina Reader API parsing completed successfully!")
        return content, result
    
    def test_api_connection(self) -> bool:
        """Test the API connection with a simple request"""
        try:
            logger.info("Testing Jina Reader API connection...")
            
            # Try a simple URL first
            test_url = "https://example.com"
            result = self.fetch_url_content(test_url, {'token_budget': 1000})
            
            if result:
                logger.info("✅ API connection successful")
                return True
            else:
                logger.error("❌ API connection failed")
                return False
                
        except Exception as e:
            logger.error(f"❌ API connection test failed: {e}")
            return False

def main():
    """Main function to run the Jina Reader API parser"""
    parser = JinaReaderAPI()
    
    print("🔍 Jina Reader API Polymarket Parser")
    print("=" * 50)
    
    # Test API connection first
    if not parser.test_api_connection():
        print("❌ Failed to connect to Jina Reader API")
        print("💡 Please check your API key and internet connection")
        return
    
    # Parse the content
    text_content, json_data = parser.parse_and_save()
    
    if text_content:
        print(f"\n✅ Successfully parsed Polymarket content!")
        print(f"📄 Text length: {len(text_content)} characters")
        print(f"📊 Response type: {type(json_data).__name__}")
        
        # Show response structure if it's a dict
        if isinstance(json_data, dict):
            print(f"📋 Response keys: {list(json_data.keys())}")
            
            # Show specific fields if available
            if 'title' in json_data:
                print(f"📋 Title: {json_data['title']}")
            
            if 'url' in json_data:
                print(f"🔗 URL: {json_data['url']}")
            
            if 'timestamp' in json_data:
                print(f"⏰ Timestamp: {json_data['timestamp']}")
        
        # Show a preview of the content
        preview_length = 1000
        preview = text_content[:preview_length] + "..." if len(text_content) > preview_length else text_content
        print(f"\n📝 Content Preview:\n{preview}")
        
        # Show file locations
        print(f"\n💾 Files saved:")
        print(f"   - Content: jina_polymarket_content.txt")
        print(f"   - JSON data: jina_polymarket_data.json")
    
    else:
        print("❌ Failed to parse content from Jina Reader API")
        print("\n💡 Suggestions:")
        print("   - Check if your API key is valid")
        print("   - Verify your token budget is sufficient")
        print("   - Try running the script again later")
        print("   - Check if Polymarket is accessible")

if __name__ == "__main__":
    main() 