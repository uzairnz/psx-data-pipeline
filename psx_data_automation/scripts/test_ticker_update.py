#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test script for the ticker information update functionality.

This script tests the ticker information update process by:
1. Creating a sample ticker JSON file with missing name/sector data
2. Running the update process on this test file
3. Verifying the updates were properly applied

Usage:
    Run directly: python -m psx_data_automation.scripts.test_ticker_update
"""

import json
import logging
import sys
import os
from pathlib import Path
from datetime import datetime

from psx_data_automation.config import DATA_DIR
from psx_data_automation.scripts.scrape_tickers import fetch_company_details
from psx_data_automation.scripts.update_ticker_info import update_ticker_info

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("psx_pipeline.test_update")

def create_test_file():
    """
    Create a test ticker file with missing information.
    
    Returns:
        Path: Path to the created test file
    """
    # Sample tickers with missing information
    test_tickers = [
        {"symbol": "CNERGY", "name": "No record found", "sector": "Unknown", "url": ""},
        {"symbol": "HBL", "name": "No record found", "sector": "Unknown", "url": ""},
        {"symbol": "ENGRO", "name": "No record found", "sector": "Unknown", "url": ""},
        {"symbol": "PSO", "name": "No record found", "sector": "Unknown", "url": ""},
        {"symbol": "LUCK", "name": "No record found", "sector": "Unknown", "url": ""}
    ]
    
    # Ensure test directory exists
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Create test file
    test_file = DATA_DIR / f"test_tickers_{datetime.now().strftime('%Y%m%d')}.json"
    with open(test_file, "w", encoding="utf-8") as f:
        json.dump(test_tickers, f, indent=2)
    
    logger.info(f"Created test file with {len(test_tickers)} tickers at {test_file}")
    return test_file

def save_updated_test_tickers(tickers):
    """
    Save updated test ticker data.
    
    Args:
        tickers (list): Updated ticker list
    
    Returns:
        Path: Path to the saved file
    """
    output_file = DATA_DIR / f"test_tickers_updated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(tickers, f, indent=2)
    
    logger.info(f"Saved updated test tickers to {output_file}")
    return output_file

def test_ticker_update():
    """
    Test the ticker information update functionality.
    """
    logger.info("Starting ticker update test")
    
    # Create test file
    test_file = create_test_file()
    
    # Load test data
    with open(test_file, "r", encoding="utf-8") as f:
        test_tickers = json.load(f)
    
    # Run update
    logger.info("Running ticker information update...")
    updated_tickers, stats = update_ticker_info(test_tickers)
    
    # Save updated tickers
    save_updated_test_tickers(updated_tickers)
    
    # Display results
    logger.info(f"\nUpdate Statistics:")
    logger.info(f"  Total tickers processed: {stats['total']}")
    logger.info(f"  Names updated: {stats['updated_names']}")
    logger.info(f"  Sectors updated: {stats['updated_sectors']}")
    logger.info(f"  URLs updated: {stats['updated_urls']}")
    logger.info(f"  No changes needed: {stats['no_change']}")
    logger.info(f"  Failed updates: {stats['failed']}")
    
    # Validate results
    success = (stats['updated_names'] > 0 or stats['updated_sectors'] > 0 or stats['updated_urls'] > 0)
    
    if success:
        logger.info("TEST PASSED: Successfully updated ticker information")
    else:
        logger.warning("TEST FAILED: No ticker information was updated")
    
    # Show updated information
    logger.info("\nUpdated ticker information:")
    for ticker in updated_tickers:
        logger.info(f"  {ticker['symbol']}: {ticker['name']} - {ticker['sector']} - URL: {ticker.get('url', 'None')}")

if __name__ == "__main__":
    test_ticker_update() 