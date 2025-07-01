#!/usr/bin/env python3
"""
Content Preprocessor for Polymarket Data - Stage 2.1
Simply removes URLs from Jina Reader output.
"""

import json
import logging
import re
from typing import Optional, Dict, Any, List, Tuple

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ContentPreprocessor:
    """Simple preprocessor that removes URLs from Jina Reader output"""
    
    def clean_content(self, raw_content: str) -> str:
        """
        Remove every URL (anything containing 'https:') from the content, preserving all other content exactly.
        """
        try:
            logger.info("Starting simple URL-only cleaning...")
            # Remove URLs: only the parentheses and URL part, not the text before it
            content = re.sub(r'\(https:[^)]*\)', '', raw_content)  # Remove (https://...)
            content = re.sub(r'https:[^\s]*', '', content)  # Remove any remaining https://... URLs
            logger.info(f"Content cleaned: {len(raw_content)} -> {len(content)} characters")
            return content
        except Exception as e:
            logger.error(f"Error cleaning content: {e}")
            return raw_content
    
    def process_jina_content(self, jina_data_file: str = "jina_polymarket_content.txt") -> str:
        """
        Process Jina Reader output and remove URLs
        
        Args:
            jina_data_file: Path to Jina Reader content file
            
        Returns:
            Cleaned content string
        """
        try:
            logger.info(f"Processing Jina content from {jina_data_file}")
            
            # Load raw content directly from text file
            with open(jina_data_file, 'r', encoding='utf-8') as f:
                raw_content = f.read()
            
            if not raw_content:
                raise ValueError("No content found in file")
            
            # Clean the content
            cleaned_content = self.clean_content(raw_content)
            
            # Save cleaned content to file
            output_file = "cleaned_polymarket_content.txt"
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(cleaned_content)
            
            logger.info(f"✅ Content preprocessing completed:")
            logger.info(f"   - Original size: {len(raw_content)} characters")
            logger.info(f"   - Cleaned size: {len(cleaned_content)} characters")
            logger.info(f"   - Saved to: {output_file}")
            
            return cleaned_content
            
        except Exception as e:
            logger.error(f"Error processing Jina content: {e}")
            return ""

def main():
    """Test the content preprocessor"""
    print("🧹 Content Preprocessor - Stage 2.1")
    print("=" * 50)
    
    try:
        preprocessor = ContentPreprocessor()
        cleaned_content = preprocessor.process_jina_content()
        
        if cleaned_content:
            print(f"\n✅ Successfully preprocessed content!")
            print(f"📝 Cleaned content size: {len(cleaned_content)} characters")
            
            # Show preview
            print(f"\n📋 Preview of cleaned content:")
            print(cleaned_content[:500] + "..." if len(cleaned_content) > 500 else cleaned_content)
            
        else:
            print("❌ Failed to preprocess content")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main() 