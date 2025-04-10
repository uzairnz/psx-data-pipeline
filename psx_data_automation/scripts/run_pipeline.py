#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Main script to run the PSX data pipeline.

This script orchestrates the process of fetching ticker symbols and their details
from the PSX Data Portal, and saves the results to a JSON file.

Usage:
    Run directly: python -m psx_data_automation.scripts.run_pipeline
"""

import json
import logging
import os
import sys
from datetime import datetime

from psx_data_automation.scripts.scrape_tickers import fetch_tickers_from_psx

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"psx_data_automation/logs/pipeline_{datetime.now().strftime('%Y-%m-%d')}.log")
    ]
)
logger = logging.getLogger("psx_pipeline")

def ensure_output_dir():
    """Ensure the output directory exists."""
    os.makedirs("psx_data_automation/data", exist_ok=True)

def run_pipeline():
    """Run the PSX data pipeline."""
    logger.info("Starting PSX data pipeline...")
    
    # Ensure output directory exists
    ensure_output_dir()
    
    # Fetch ticker symbols with company details
    logger.info("Fetching ticker symbols with company details...")
    tickers = fetch_tickers_from_psx(fetch_details=True)
    
    # Save the results
    output_file = f"psx_data_automation/data/tickers_{datetime.now().strftime('%Y%m%d')}.json"
    logger.info(f"Saving ticker symbols to {output_file}...")
    
    with open(output_file, "w") as f:
        json.dump(tickers, f, indent=2)
    
    logger.info(f"Successfully saved {len(tickers)} ticker symbols to {output_file}")
    logger.info("PSX data pipeline completed successfully")
    
    return tickers

if __name__ == "__main__":
    run_pipeline() 