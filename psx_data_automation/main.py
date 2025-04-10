#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Main entry point for the PSX Data Pipeline.

This script provides a command-line interface to:
- Sync ticker lists from PSX
- Download historical OHLC data
- Update existing data with latest figures
- Run the complete pipeline
"""

import argparse
import logging
import sys
from datetime import datetime

from config import __version__, LOG_DIR
from scripts.scrape_tickers import sync_tickers

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


def setup_argparser():
    """Set up command line arguments."""
    parser = argparse.ArgumentParser(
        description="PSX Data Pipeline: Collect and maintain historical OHLC data for PSX tickers",
        epilog="Example: python main.py --full-run"
    )
    
    parser.add_argument('--sync-tickers', action='store_true', help='Sync ticker list from PSX')
    parser.add_argument('--download-historical', action='store_true', help='Download historical data for all tickers')
    parser.add_argument('--daily-update', action='store_true', help='Update data with latest OHLC values')
    parser.add_argument('--full-run', action='store_true', help='Execute complete pipeline')
    parser.add_argument('--version', action='version', version=f'PSX Data Pipeline v{__version__}')
    
    return parser


def main():
    """Main function to run the PSX data pipeline."""
    parser = setup_argparser()
    args = parser.parse_args()
    
    logger.info(f"Starting PSX Data Pipeline v{__version__}")
    
    if args.sync_tickers:
        logger.info("Starting ticker synchronization...")
        success = sync_tickers()
        if not success:
            logger.error("Ticker synchronization failed")
            return 1
    
    if args.download_historical:
        logger.info("Starting historical data download...")
        # Future: Import and call historical data download function
        # from scripts.download_data import download_historical
        # download_historical()
        logger.info("Historical data download not yet implemented")
    
    if args.daily_update:
        logger.info("Starting daily data update...")
        # Future: Import and call daily update function
        # from scripts.update_data import update_daily
        # update_daily()
        logger.info("Daily update not yet implemented")
    
    if args.full_run or not any([args.sync_tickers, args.download_historical, args.daily_update]):
        logger.info("Starting full pipeline run...")
        
        # Step 1: Sync tickers
        logger.info("Step 1: Synchronizing tickers...")
        success = sync_tickers()
        if not success:
            logger.error("Ticker synchronization failed - aborting pipeline")
            return 1
        
        # Step 2: Download historical data (to be implemented)
        logger.info("Step 2: Downloading historical data...")
        logger.info("Historical data download not yet implemented")
        
        # Step 3: Daily update (to be implemented)
        logger.info("Step 3: Performing daily update...")
        logger.info("Daily update not yet implemented")
    
    logger.info("Pipeline execution completed")
    return 0


if __name__ == "__main__":
    sys.exit(main()) 