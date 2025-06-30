#!/bin/bash

# Polymarket Pipeline Runner
# This script runs the complete pipeline and logs the output

# Set the working directory
cd "/Users/nelith/Library/CloudStorage/GoogleDrive-nelith.bandularatne@gmail.com/My Drive/Adi/polymarket-jina"

# Load environment variables from .env file
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
else
    echo "Warning: .env file not found. Make sure GEMINI_API_KEY is set."
fi

# Activate virtual environment
source venv/bin/activate

# Run the pipeline with all stages (1, 2, 3)
echo "=== Polymarket Pipeline Started: $(date) ===" >> pipeline.log
python main_pipeline.py --stages 1 2 3 >> pipeline.log 2>&1
echo "=== Polymarket Pipeline Completed: $(date) ===" >> pipeline.log
echo "" >> pipeline.log 