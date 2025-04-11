#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Main entry point for the PSX Data Pipeline.

This script provides a command-line interface to:
- Sync ticker lists from PSX
- Download historical OHLC data
- Update existing data with latest figures
- Update ticker names and sectors
- Run the complete pipeline
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

# Use absolute imports
from psx_data_automation.config import __version__, LOG_DIR, DATA_DIR, TICKERS_FILE
from psx_data_automation.scripts.scrape_tickers import fetch_tickers_from_psx
from psx_data_automation.scripts.update_ticker_info import update_ticker_info
from psx_data_automation.scripts.historical_data import download_ticker_data as download_synthetic_data
from psx_data_automation.investing_data import download_historical_data as download_investing_data
from psx_data_automation.scripts.crawler import test_crawl4ai

# Set up logging
log_file = LOG_DIR / f"pipeline_{datetime.now().strftime('%Y-%m-%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("psx_pipeline")


def save_tickers(tickers: List[Dict], filename: str) -> None:
    """Save ticker data to a JSON file with datestamp."""
    today = datetime.now().strftime("%Y%m%d")
    filename = filename.replace(".json", f"_{today}.json")
    filepath = os.path.join(DATA_DIR, filename)
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, "w") as f:
        json.dump(tickers, f, indent=2)
    
    logger.info(f"Saved {len(tickers)} tickers to {filepath}")
    
    # Also save as the main tickers file without date
    main_filepath = os.path.join(DATA_DIR, TICKERS_FILE)
    with open(main_filepath, "w") as f:
        json.dump(tickers, f, indent=2)


def load_tickers(filename: Optional[str] = None) -> List[Dict]:
    """Load ticker data from a JSON file."""
    if filename is None:
        filename = TICKERS_FILE
    
    filepath = os.path.join(DATA_DIR, filename)
    
    if not os.path.exists(filepath):
        logger.warning(f"Tickers file {filepath} does not exist.")
        return []
    
    with open(filepath, "r") as f:
        tickers = json.load(f)
    
    logger.info(f"Loaded {len(tickers)} tickers from {filepath}")
    return tickers


def run_pipeline(
    scrape_tickers: bool = False,
    update_info: bool = False,
    download_historical: bool = False,
    mock_mode: bool = False,
    max_tickers: Optional[int] = None,
    tickers_file: Optional[str] = None,
    use_synthetic_data: bool = False,
) -> None:
    """Run the PSX data pipeline."""
    tickers = []
    
    # Step 1: Scrape tickers from PSX
    if scrape_tickers:
        logger.info("Scraping tickers from PSX...")
        tickers = fetch_tickers_from_psx(mock=mock_mode)
        save_tickers(tickers, "tickers_scraped.json")
    
    # Step 2: Update ticker information
    if update_info:
        # Load tickers if we didn't scrape them
        if not scrape_tickers or not tickers:
            tickers = load_tickers(tickers_file)
        
        if not tickers:
            logger.error("No tickers available. Run with --scrape-tickers first.")
            return
        
        logger.info("Updating ticker information...")
        updated_tickers, stats = update_ticker_info(tickers)
        save_tickers(updated_tickers, "tickers_updated.json")
        tickers = updated_tickers
    
    # Step 3: Download historical data for tickers
    if download_historical:
        # Load tickers if we didn't update them or scrape them
        if not update_info and not scrape_tickers or not tickers:
            tickers = load_tickers(tickers_file)
        
        if not tickers:
            logger.error("No tickers available. Run with --scrape-tickers or --update-info first.")
            return
        
        # Limit the number of tickers if specified
        if max_tickers is not None and max_tickers > 0:
            tickers = tickers[:max_tickers]
            logger.info(f"Limited to {max_tickers} tickers for processing")
        
        # Get the list of ticker symbols
        symbols = [ticker["symbol"] for ticker in tickers]
        
        logger.info(f"Downloading historical data for {len(symbols)} tickers...")
        
        if mock_mode or use_synthetic_data:
            # Use synthetic data generation
            logger.info("Using synthetic data generation...")
            download_synthetic_data(symbols, mock_mode=True)
        else:
            # Use investing.com data source with fallback to synthetic
            logger.info("Using investing.com data source with synthetic fallback...")
            download_investing_data(symbols)


def main():
    """Main entry point for the PSX data pipeline."""
    parser = argparse.ArgumentParser(description="PSX Data Pipeline")
    
    parser.add_argument(
        "--scrape-tickers",
        action="store_true",
        help="Scrape tickers from PSX",
    )
    
    parser.add_argument(
        "--update-info",
        action="store_true",
        help="Update ticker information",
    )
    
    parser.add_argument(
        "--download-historical",
        action="store_true",
        help="Download historical data for tickers",
    )
    
    parser.add_argument(
        "--full-run",
        action="store_true",
        help="Run the full pipeline (scrape, update, download)",
    )
    
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Use mock data for testing",
    )
    
    parser.add_argument(
        "--synthetic",
        action="store_true",
        help="Force using synthetic data instead of real data",
    )
    
    parser.add_argument(
        "--max-tickers",
        type=int,
        default=None,
        help="Maximum number of tickers to process",
    )
    
    parser.add_argument(
        "--tickers-file",
        type=str,
        default=None,
        help="JSON file containing tickers (in DATA_DIR)",
    )
    
    parser.add_argument(
        "--test-crawl4ai",
        action="store_true",
        help="Test the Crawl4AI library",
    )
    
    args = parser.parse_args()
    
    # Create data directory if it doesn't exist
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Test crawl4ai if requested
    if args.test_crawl4ai:
        test_crawl4ai()
        return
    
    # If full run, enable all steps
    if args.full_run:
        args.scrape_tickers = True
        args.update_info = True
        args.download_historical = True
    
    # Make sure at least one step is specified
    if not (args.scrape_tickers or args.update_info or args.download_historical):
        parser.print_help()
        logger.error("No action specified. Please specify at least one action.")
        sys.exit(1)
    
    # Run the pipeline
    run_pipeline(
        scrape_tickers=args.scrape_tickers,
        update_info=args.update_info,
        download_historical=args.download_historical,
        mock_mode=args.mock,
        max_tickers=args.max_tickers,
        tickers_file=args.tickers_file,
        use_synthetic_data=args.synthetic,
    )


if __name__ == "__main__":
    main() 