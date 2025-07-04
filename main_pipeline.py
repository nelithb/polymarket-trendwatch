#!/usr/bin/env python3
"""
Main Pipeline Orchestrator - Polymarket Data Pipeline
Runs all stages of the data pipeline step by step with progress tracking.
"""

import json
import logging
import os
import time
import shutil
from typing import Optional, Dict, Any, List
from datetime import datetime

# Import our stage modules
from jina_reader_api import JinaReaderAPI
from ai_parser import AIPolymarketParser

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

class PolymarketPipeline:
    def __init__(self, gemini_api_key: str = None):
        """
        Initialize the main pipeline orchestrator
        
        Args:
            gemini_api_key: Gemini API key for Stage 2
        """
        self.gemini_api_key = gemini_api_key
        self.pipeline_results = {}
        self.start_time = None
        self.end_time = None
        
        # Initialize stage components
        self.stage1_parser = None
        self.stage2_parser = None
        
        # Pipeline status
        self.stages_completed = {
            'stage1': False,
            'stage2': False,
            'stage3': False,
            'stage4': False
        }
        
        # Output files
        self.output_files = {
            'stage1_raw': 'jina_polymarket_content.txt',
            'stage1_json': 'jina_polymarket_data.json',
            'stage2_cleaned': 'cleaned_polymarket_content.txt',
            'stage2_structured': 'structured_polymarket_data.json',
            'stage3_analytics': 'analytics_report.json',
            'stage4_automation': 'automation_status.json'
        }
    
    def print_header(self):
        """Print the pipeline header"""
        print("🚀 Polymarket Data Pipeline - Main Orchestrator")
        print("=" * 60)
        print("🗺️  Four-Stage Rocket: Raw Data → Intelligence → Analytics → Automation")
        print("=" * 60)
        print(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
    
    def print_stage_header(self, stage_num: int, stage_name: str, description: str):
        """Print stage header with progress indicator"""
        print(f"🎯 STAGE {stage_num}: {stage_name}")
        print(f"📝 {description}")
        print("-" * 50)
    
    def print_stage_result(self, stage_num: int, success: bool, details: str = ""):
        """Print stage result"""
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"🎯 STAGE {stage_num} RESULT: {status}")
        if details:
            print(f"📋 Details: {details}")
        print()
    
    def run_stage1_foundation(self) -> bool:
        """
        Run Stage 1: The Foundation (Jina Reader API)
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.print_stage_header(1, "The Foundation", "Fetching raw data from Polymarket using Jina Reader API")
            
            # Initialize Stage 1 parser
            self.stage1_parser = JinaReaderAPI()
            
            # Test API connection first
            print("🔍 Testing Jina Reader API connection...")
            if not self.stage1_parser.test_api_connection():
                self.print_stage_result(1, False, "Failed to connect to Jina Reader API")
                return False
            
            # Run the parsing
            print("📡 Fetching Polymarket content...")
            text_content, jina_data = self.stage1_parser.parse_and_save()
            
            if not jina_data:
                self.print_stage_result(1, False, "Failed to fetch content from Polymarket")
                return False
            
            # Store results
            self.pipeline_results['stage1'] = {
                'text_content_length': len(text_content) if text_content else 0,
                'jina_data_keys': list(jina_data.keys()) if isinstance(jina_data, dict) else [],
                'output_files': [
                    self.output_files['stage1_raw'],
                    self.output_files['stage1_json']
                ]
            }
            
            # Check if output files were created
            files_created = []
            for filename in [self.output_files['stage1_raw'], self.output_files['stage1_json']]:
                if os.path.exists(filename):
                    files_created.append(filename)
            
            details = f"Content length: {len(text_content) if text_content else 0} chars, Files: {', '.join(files_created)}"
            self.print_stage_result(1, True, details)
            
            self.stages_completed['stage1'] = True
            return True
            
        except Exception as e:
            logger.error(f"Stage 1 failed: {e}")
            self.print_stage_result(1, False, f"Error: {str(e)}")
            return False
    
    def run_stage2_intelligence(self) -> bool:
        """
        Run Stage 2: The Intelligence Layer (AI Parsing)
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.print_stage_header(2, "The Intelligence Layer", "Preprocessing raw data and converting to structured JSON using Gemini AI")
            
            # Check if Stage 1 was completed
            if not self.stages_completed['stage1']:
                print("⚠️  Stage 1 not completed. Attempting to run Stage 1 first...")
                if not self.run_stage1_foundation():
                    self.print_stage_result(2, False, "Stage 1 failed, cannot proceed")
                    return False
            
            # Initialize Stage 2 parser
            self.stage2_parser = AIPolymarketParser(gemini_api_key=self.gemini_api_key)
            
            # Run the AI parsing (includes preprocessing) - don't save files during processing
            print("🧹 Preprocessing content...")
            print("🧠 Processing with Gemini AI...")
            structured_data = self.stage2_parser.run_full_pipeline(save_files=False)
            
            if not structured_data:
                self.print_stage_result(2, False, "AI parsing failed")
                return False
            
            # Save the structured data to the expected output file
            with open(self.output_files['stage2_structured'], 'w', encoding='utf-8') as f:
                json.dump(structured_data, f, indent=2, ensure_ascii=False)
            
            # Also save cleaned content if it exists (for reference)
            cleaned_file = "cleaned_polymarket_content.txt"
            if os.path.exists(cleaned_file):
                shutil.copyfile(cleaned_file, self.output_files['stage2_cleaned'])
            
            # Store results
            markets = structured_data.get('markets', [])
            grouped_markets = [m for m in markets if 'group_title' in m]
            standalone_markets = [m for m in markets if 'group_title' not in m]
            
            self.pipeline_results['stage2'] = {
                'total_markets': len(markets),
                'grouped_markets': len(grouped_markets),
                'standalone_markets': len(standalone_markets),
                'output_file': self.output_files['stage2_structured']
            }
            
            details = f"Total markets: {len(markets)}, Groups: {len(grouped_markets)}, Standalone: {len(standalone_markets)}"
            self.print_stage_result(2, True, details)
            
            self.stages_completed['stage2'] = True
            return True
            
        except Exception as e:
            logger.error(f"Stage 2 failed: {e}")
            self.print_stage_result(2, False, f"Error: {str(e)}")
            return False
    
    def run_stage3_storage(self) -> bool:
        """
        Run Stage 3: The Automation & Storage Layer (Scheduling & History)
        Moves Jina raw data and structured data (if available) in date-organized history directory.
        Always moves Jina data, and structured data if Stage 2 completed successfully.
        Returns True if successful, False otherwise.
        """
        try:
            self.print_stage_header(3, "The Automation & Storage Layer", "Saving dated snapshots of raw and structured data for historical tracking.")
            
            # Check if Jina data files exist (Stage 1 output)
            jina_raw_src = self.output_files['stage1_raw']
            jina_json_src = self.output_files['stage1_json']
            
            if not os.path.exists(jina_raw_src) or not os.path.exists(jina_json_src):
                print("⚠️  Jina data files not found. Cannot save snapshot.")
                self.print_stage_result(3, False, "Jina data files not found")
                return False
            
            # Ensure history directory exists
            history_dir = "history"
            os.makedirs(history_dir, exist_ok=True)
            
            # Today's date
            today_str = datetime.now().strftime("%Y-%m-%d")
            today_dir = os.path.join(history_dir, today_str)
            os.makedirs(today_dir, exist_ok=True)
            
            # Move all files to today's directory
            files_moved = []
            
            # Always move Jina raw data
            jina_raw_dest = os.path.join(today_dir, "jina_polymarket_content.txt")
            shutil.move(jina_raw_src, jina_raw_dest)
            files_moved.append("jina_polymarket_content.txt")
            
            # Always move Jina JSON data
            jina_json_dest = os.path.join(today_dir, "jina_polymarket_data.json")
            shutil.move(jina_json_src, jina_json_dest)
            files_moved.append("jina_polymarket_data.json")
            
            # Move structured data if Stage 2 completed successfully
            structured_src = self.output_files['stage2_structured']
            cleaned_src = self.output_files['stage2_cleaned']
            if self.stages_completed['stage2'] and os.path.exists(structured_src):
                structured_dest = os.path.join(today_dir, "structured_polymarket_data.json")
                shutil.move(structured_src, structured_dest)
                files_moved.append("structured_polymarket_data.json")
                
                # Also move cleaned content if it exists
                if os.path.exists(cleaned_src):
                    cleaned_dest = os.path.join(today_dir, "cleaned_polymarket_content.txt")
                    shutil.move(cleaned_src, cleaned_dest)
                    files_moved.append("cleaned_polymarket_content.txt")
                
                has_structured_data = True
            else:
                has_structured_data = False
            
            # Create a summary file for this day's data
            summary_data = {
                "date": today_str,
                "timestamp": datetime.now().isoformat(),
                "files_saved": files_moved,
                "has_structured_data": has_structured_data,
                "pipeline_stages_completed": {
                    "stage1": self.stages_completed['stage1'],
                    "stage2": self.stages_completed['stage2'],
                    "stage3": True,
                    "stage4": self.stages_completed['stage4']
                },
                "file_sizes": {}
            }
            # Add file sizes
            for fname in files_moved:
                fpath = os.path.join(today_dir, fname)
                summary_data["file_sizes"][fname] = os.path.getsize(fpath)
            
            summary_file = os.path.join(today_dir, "summary.json")
            with open(summary_file, 'w') as f:
                json.dump(summary_data, f, indent=2)
            
            if has_structured_data:
                details = f"Complete daily snapshot moved to: {today_dir} ({', '.join(files_moved)})"
            else:
                details = f"Raw data snapshot moved to: {today_dir} ({', '.join(files_moved)}) - No structured data available"
            
            self.print_stage_result(3, True, details)
            self.pipeline_results['stage3'] = {
                'snapshot_directory': today_dir,
                'files_saved': files_moved,
                'has_structured_data': has_structured_data,
                'summary_file': summary_file
            }
            self.stages_completed['stage3'] = True
            return True
            
        except Exception as e:
            logger.error(f"Stage 3 failed: {e}")
            self.print_stage_result(3, False, f"Error: {str(e)}")
            return False
    
    def run_stage4_automation(self) -> bool:
        """
        Run Stage 4: The Automation Hub (Placeholder for future implementation)
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.print_stage_header(4, "The Automation Hub", "Automated scheduling and data distribution (PLANNED)")
            
            # Check if previous stages were completed
            if not self.stages_completed['stage2']:
                print("⚠️  Stage 2 not completed. Cannot run automation without structured data.")
                self.print_stage_result(4, False, "Stage 2 required but not completed")
                return False
            
            # Placeholder for Stage 4 implementation
            print("🚧 Stage 4: Automation Hub - Coming Soon!")
            print("🤖 This stage will include:")
            print("   - Scheduled data collection")
            print("   - Alert systems")
            print("   - Data distribution APIs")
            print("   - Production deployment")
            
            # Create automation status
            automation_status = {
                "timestamp": datetime.now().isoformat(),
                "stage": "automation_placeholder",
                "status": "planned",
                "pipeline_status": {
                    "stage1": self.stages_completed['stage1'],
                    "stage2": self.stages_completed['stage2'],
                    "stage3": self.stages_completed['stage3'],
                    "stage4": True  # This stage
                },
                "message": "Automation hub will be implemented in future version"
            }
            
            # Save automation status
            with open(self.output_files['stage4_automation'], 'w') as f:
                json.dump(automation_status, f, indent=2)
            
            self.pipeline_results['stage4'] = {
                'status': 'planned',
                'output_file': self.output_files['stage4_automation']
            }
            
            self.print_stage_result(4, True, "Placeholder created - ready for implementation")
            self.stages_completed['stage4'] = True
            return True
            
        except Exception as e:
            logger.error(f"Stage 4 failed: {e}")
            self.print_stage_result(4, False, f"Error: {str(e)}")
            return False
    
    def run_full_pipeline(self, stages: List[int] = None) -> bool:
        """
        Run the complete pipeline or specific stages
        
        Args:
            stages: List of stage numbers to run (1-4). If None, runs all stages.
            
        Returns:
            True if all requested stages succeeded, False otherwise
        """
        self.start_time = time.time()
        self.print_header()
        
        # Default to all stages if none specified
        if stages is None:
            stages = [1, 2, 3, 4]
        
        print(f"🎯 Running stages: {stages}")
        print()
        
        # Run each requested stage
        stage_results = {}
        
        for stage_num in stages:
            if stage_num == 1:
                stage_results[1] = self.run_stage1_foundation()
            elif stage_num == 2:
                stage_results[2] = self.run_stage2_intelligence()
            elif stage_num == 3:
                # Stage 3 is now storage/snapshot
                stage_results[3] = self.run_stage3_storage()
            elif stage_num == 4:
                stage_results[4] = self.run_stage4_automation()
            else:
                print(f"❌ Unknown stage number: {stage_num}")
                stage_results[stage_num] = False
        
        # Calculate execution time
        self.end_time = time.time()
        execution_time = self.end_time - self.start_time
        
        # Print final summary
        self.print_final_summary(stage_results, execution_time)
        
        # Return success if all requested stages succeeded
        return all(stage_results.values())
    
    def print_final_summary(self, stage_results: Dict[int, bool], execution_time: float):
        """Print final pipeline summary"""
        print("=" * 60)
        print("📊 PIPELINE EXECUTION SUMMARY")
        print("=" * 60)
        
        # Stage results
        print("🎯 Stage Results:")
        for stage_num, success in stage_results.items():
            status = "✅ SUCCESS" if success else "❌ FAILED"
            stage_names = {1: "Foundation", 2: "Intelligence", 3: "Automation & Storage", 4: "Automation"}
            print(f"   Stage {stage_num} ({stage_names[stage_num]}): {status}")
        
        # Execution time
        print(f"\n⏱️  Total Execution Time: {execution_time:.2f} seconds")
        
        # Output files
        print(f"\n📁 Output Files Created:")
        for stage_num in stage_results.keys():
            if stage_num == 1:
                files = [self.output_files['stage1_raw'], self.output_files['stage1_json']]
            elif stage_num == 2:
                files = [self.output_files['stage2_cleaned'], self.output_files['stage2_structured']]
            elif stage_num == 3:
                files = [self.output_files['stage3_analytics']]
            elif stage_num == 4:
                files = [self.output_files['stage4_automation']]
            
            for file in files:
                if os.path.exists(file):
                    file_size = os.path.getsize(file)
                    print(f"   ✅ {file} ({file_size} bytes)")
                else:
                    print(f"   ❌ {file} (not created)")
        
        # Pipeline results
        if self.pipeline_results:
            print(f"\n📈 Pipeline Statistics:")
            for stage, results in self.pipeline_results.items():
                if isinstance(results, dict):
                    print(f"   {stage.upper()}:")
                    for key, value in results.items():
                        print(f"     {key}: {value}")
        
        print("=" * 60)
        print(f"🕐 Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

def main():
    """Main function to run the pipeline"""
    import argparse
    
    # Load environment variables from .env file
    load_env_file()
    
    parser = argparse.ArgumentParser(description='Polymarket Data Pipeline - Main Orchestrator')
    parser.add_argument('--stages', nargs='+', type=int, choices=[1, 2, 3, 4],
                       help='Specific stages to run (default: all stages)')
    parser.add_argument('--gemini-key', type=str, help='Gemini API key (or set GEMINI_API_KEY env var)')
    
    args = parser.parse_args()
    
    try:
        # Initialize pipeline
        pipeline = PolymarketPipeline(gemini_api_key=args.gemini_key)
        
        # Run pipeline
        success = pipeline.run_full_pipeline(stages=args.stages)
        
        if success:
            print("\n🎉 Pipeline completed successfully!")
            return 0
        else:
            print("\n❌ Pipeline completed with errors.")
            return 1
            
    except KeyboardInterrupt:
        print("\n⏹️  Pipeline interrupted by user.")
        return 1
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    exit(main()) 