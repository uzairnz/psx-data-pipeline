#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test script for the company details fetching functionality.

This script runs a simple test of the fetch_company_details function
to verify that it can correctly extract company information from PSX.

Usage:
    Run directly: python -m psx_data_automation.scripts.test_company_details
"""

import logging
import sys
from psx_data_automation.scripts.scrape_tickers import fetch_company_details

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("psx_pipeline.test")

def test_fetch_company_details():
    """
    Test the fetch_company_details function with a sample ticker.
    """
    # Choose a few sample tickers to test
    sample_tickers = ["CNERGY", "HBL", "ENGRO", "PSO", "LUCK"]
    
    logger.info("Starting company details test...")
    
    for ticker in sample_tickers:
        logger.info(f"Testing fetch_company_details for {ticker}...")
        details = fetch_company_details(ticker)
        
        # Print the results
        logger.info(f"Results for {ticker}:")
        logger.info(f"  Symbol: {details['symbol']}")
        logger.info(f"  Name: {details['name']}")
        logger.info(f"  Sector: {details['sector']}")
        logger.info("--------------------------------------------------")
    
    logger.info("Company details test completed")

if __name__ == "__main__":
    test_fetch_company_details() 