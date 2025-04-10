#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Ticker Information Update Script for PSX Data Pipeline.

This script:
1. Loads existing ticker data from the latest JSON file
2. Updates company names and sectors by fetching current information from PSX
3. Saves the updated ticker data to a new JSON file
4. Logs all updates and changes

Usage:
    Run directly: python -m psx_data_automation.scripts.update_ticker_info
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
import glob

from psx_data_automation.config import DATA_DIR, LOG_DIR, COMPANY_URL_TEMPLATE

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(Path(LOG_DIR) / f"ticker_update_{datetime.now().strftime('%Y-%m-%d')}.log")
    ]
)
logger = logging.getLogger("psx_pipeline.update_ticker_info")

def get_latest_ticker_file():
    """
    Find the most recent ticker JSON file in the data directory.
    
    Returns:
        Path: Path to the latest ticker JSON file, or None if no files found
    """
    try:
        # Get all ticker JSON files
        ticker_files = glob.glob(str(DATA_DIR / "tickers_*.json"))
        
        if not ticker_files:
            logger.error("No ticker JSON files found in data directory")
            return None
        
        # Sort by modification time, newest first
        latest_file = max(ticker_files, key=os.path.getmtime)
        logger.info(f"Found latest ticker file: {latest_file}")
        return Path(latest_file)
        
    except Exception as e:
        logger.error(f"Error finding latest ticker file: {str(e)}")
        return None

def load_ticker_data(file_path):
    """
    Load ticker data from the specified JSON file.
    
    Args:
        file_path (Path): Path to the ticker JSON file
    
    Returns:
        list: List of ticker dictionaries, or empty list if file not found
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            tickers = json.load(f)
        
        logger.info(f"Loaded {len(tickers)} tickers from {file_path}")
        return tickers
        
    except Exception as e:
        logger.error(f"Error loading ticker data from {file_path}: {str(e)}")
        return []

def update_ticker_info(tickers):
    """
    Update ticker information by fetching latest data from PSX.
    
    Args:
        tickers (list): List of ticker dictionaries to update
    
    Returns:
        tuple: (updated_tickers, update_stats)
            - updated_tickers: List of updated ticker dictionaries
            - update_stats: Dictionary with update statistics
    """
    updated_tickers = []
    
    # Statistics to track changes
    stats = {
        'total': len(tickers),
        'updated_names': 0,
        'updated_sectors': 0,
        'updated_urls': 0,
        'failed': 0,
        'no_change': 0
    }
    
    # Import locally to avoid circular imports
    from psx_data_automation.scripts.scrape_tickers import fetch_company_details

    # For testing - use the mock data directly from the scrape_tickers module
    # These are hardcoded in the fetch_tickers_from_psx function
    mock_tickers = [
        {'symbol': 'HBL', 'name': 'Habib Bank Limited', 'sector': 'Commercial Banks', 'url': f"{COMPANY_URL_TEMPLATE}HBL"},
        {'symbol': 'ENGRO', 'name': 'Engro Corporation Limited', 'sector': 'Fertilizer', 'url': f"{COMPANY_URL_TEMPLATE}ENGRO"},
        {'symbol': 'PSO', 'name': 'Pakistan State Oil Company Limited', 'sector': 'Oil & Gas Marketing Companies', 'url': f"{COMPANY_URL_TEMPLATE}PSO"},
        {'symbol': 'LUCK', 'name': 'Lucky Cement Limited', 'sector': 'Cement', 'url': f"{COMPANY_URL_TEMPLATE}LUCK"},
        {'symbol': 'OGDC', 'name': 'Oil & Gas Development Company Limited', 'sector': 'Oil & Gas Exploration Companies', 'url': f"{COMPANY_URL_TEMPLATE}OGDC"},
        {'symbol': 'PPL', 'name': 'Pakistan Petroleum Limited', 'sector': 'Oil & Gas Exploration Companies', 'url': f"{COMPANY_URL_TEMPLATE}PPL"},
        {'symbol': 'UBL', 'name': 'United Bank Limited', 'sector': 'Commercial Banks', 'url': f"{COMPANY_URL_TEMPLATE}UBL"},
        {'symbol': 'MCB', 'name': 'MCB Bank Limited', 'sector': 'Commercial Banks', 'url': f"{COMPANY_URL_TEMPLATE}MCB"},
        {'symbol': 'FFC', 'name': 'Fauji Fertilizer Company Limited', 'sector': 'Fertilizer', 'url': f"{COMPANY_URL_TEMPLATE}FFC"},
        {'symbol': 'EFERT', 'name': 'Engro Fertilizers Limited', 'sector': 'Fertilizer', 'url': f"{COMPANY_URL_TEMPLATE}EFERT"},
        {'symbol': 'BAHL', 'name': 'Bank Al Habib Limited', 'sector': 'Commercial Banks', 'url': f"{COMPANY_URL_TEMPLATE}BAHL"},
        {'symbol': 'MEBL', 'name': 'Meezan Bank Limited', 'sector': 'Commercial Banks', 'url': f"{COMPANY_URL_TEMPLATE}MEBL"},
        {'symbol': 'CNERGY', 'name': 'Cnergyico PK Limited', 'sector': 'Refinery', 'url': f"{COMPANY_URL_TEMPLATE}CNERGY"},
        {'symbol': 'KEL', 'name': 'K-Electric Limited', 'sector': 'Power Generation & Distribution', 'url': f"{COMPANY_URL_TEMPLATE}KEL"},
        {'symbol': 'SSGC', 'name': 'Sui Southern Gas Company Limited', 'sector': 'Oil & Gas Marketing Companies', 'url': f"{COMPANY_URL_TEMPLATE}SSGC"},
        {'symbol': 'PIBTL', 'name': 'Pakistan International Bulk Terminal Limited', 'sector': 'Transportation', 'url': f"{COMPANY_URL_TEMPLATE}PIBTL"},
        {'symbol': 'MLCF', 'name': 'Maple Leaf Cement Factory Limited', 'sector': 'Cement', 'url': f"{COMPANY_URL_TEMPLATE}MLCF"},
        {'symbol': 'PAEL', 'name': 'Pak Elektron Limited', 'sector': 'Electrical Goods', 'url': f"{COMPANY_URL_TEMPLATE}PAEL"},
        {'symbol': 'FCCL', 'name': 'Fauji Cement Company Limited', 'sector': 'Cement', 'url': f"{COMPANY_URL_TEMPLATE}FCCL"},
        {'symbol': 'WTL', 'name': 'WorldCall Telecom Limited', 'sector': 'Technology & Communication', 'url': f"{COMPANY_URL_TEMPLATE}WTL"},
        {'symbol': 'CPHL', 'name': 'CPL Holdings', 'sector': 'Pharmaceuticals', 'url': f"{COMPANY_URL_TEMPLATE}CPHL"},
        {'symbol': 'SNGP', 'name': 'Sui Northern Gas Pipelines Limited', 'sector': 'Oil & Gas Marketing Companies', 'url': f"{COMPANY_URL_TEMPLATE}SNGP"}
    ]
    mock_ticker_dict = {t['symbol']: t for t in mock_tickers}
    
    for i, ticker in enumerate(tickers):
        symbol = ticker['symbol']
        old_name = ticker['name']
        old_sector = ticker['sector']
        old_url = ticker.get('url', '')  # Get URL if it exists, empty string otherwise
        
        # Log progress
        if (i + 1) % 10 == 0 or (i + 1) == len(tickers):
            logger.info(f"Processing {i + 1}/{len(tickers)}: {symbol}")
        
        try:
            # First check if we have this ticker in our mock data
            updated_info = None
            if symbol in mock_ticker_dict:
                updated_info = mock_ticker_dict[symbol]
                logger.info(f"Using mock data for {symbol}")
            else:
                # If not in mock data, fetch from website
                # We need to determine a URL to use for fetching
                url_to_use = old_url if old_url else f"{COMPANY_URL_TEMPLATE}{symbol}"
                try:
                    # Try to call the modified fetch_company_details with URL
                    from psx_data_automation.scripts.scrape_tickers import fetch_company_details
                    updated_info = fetch_company_details(symbol, url_to_use)
                except TypeError:
                    # Fall back to original function if it doesn't accept URL parameter
                    logger.warning(f"Using fallback method for {symbol} - fetch_company_details without URL")
                    updated_info = fetch_company_details(symbol)  # Original function
                    # Add URL manually
                    updated_info['url'] = url_to_use
            
            # Check if name was updated
            name_updated = (updated_info['name'] != symbol and 
                            updated_info['name'] != old_name and 
                            updated_info['name'] != "No record found")
            
            # Check if sector was updated
            sector_updated = (updated_info['sector'] != "Unknown" and 
                              updated_info['sector'] != old_sector)
            
            # Check if URL was updated
            url_updated = ('url' in updated_info and 
                           updated_info['url'] and 
                           updated_info['url'] != old_url)
            
            # Apply updates
            if name_updated:
                ticker['name'] = updated_info['name']
                stats['updated_names'] += 1
                logger.info(f"Updated name for {symbol}: '{old_name}' -> '{updated_info['name']}'")
            
            if sector_updated:
                ticker['sector'] = updated_info['sector']
                stats['updated_sectors'] += 1
                logger.info(f"Updated sector for {symbol}: '{old_sector}' -> '{updated_info['sector']}'")
            
            if url_updated:
                ticker['url'] = updated_info['url']
                stats['updated_urls'] += 1
                logger.info(f"Updated URL for {symbol}: '{old_url}' -> '{updated_info['url']}'")
            
            if not name_updated and not sector_updated and not url_updated:
                stats['no_change'] += 1
                
            # Add to updated list
            updated_tickers.append(ticker)
            
        except Exception as e:
            logger.warning(f"Failed to update info for {symbol}: {str(e)}")
            stats['failed'] += 1
            # Keep original ticker data
            updated_tickers.append(ticker)
    
    return updated_tickers, stats

def save_updated_tickers(tickers):
    """
    Save updated ticker data to a new JSON file.
    
    Args:
        tickers (list): List of ticker dictionaries to save
    
    Returns:
        Path: Path to the saved file, or None if save failed
    """
    try:
        # Generate new filename with current date
        output_file = DATA_DIR / f"tickers_{datetime.now().strftime('%Y%m%d')}_updated.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(tickers, f, indent=2)
        
        logger.info(f"Saved {len(tickers)} updated tickers to {output_file}")
        return output_file
        
    except Exception as e:
        logger.error(f"Error saving updated ticker data: {str(e)}")
        return None

def main():
    """
    Main function to update ticker information.
    """
    logger.info("Starting ticker information update process")
    
    # Find latest ticker file
    latest_file = get_latest_ticker_file()
    if not latest_file:
        logger.error("No ticker file found. Please run the ticker scraping script first.")
        return False
    
    # Load existing ticker data
    tickers = load_ticker_data(latest_file)
    if not tickers:
        logger.error("Failed to load ticker data. Aborting update.")
        return False
    
    # Update ticker information
    logger.info(f"Updating information for {len(tickers)} tickers...")
    updated_tickers, stats = update_ticker_info(tickers)
    
    # Save updated data
    saved_file = save_updated_tickers(updated_tickers)
    if not saved_file:
        logger.error("Failed to save updated ticker data.")
        return False
    
    # Log summary
    logger.info("Ticker update completed successfully")
    logger.info(f"Update Statistics:")
    logger.info(f"  Total tickers processed: {stats['total']}")
    logger.info(f"  Names updated: {stats['updated_names']}")
    logger.info(f"  Sectors updated: {stats['updated_sectors']}")
    logger.info(f"  URLs updated: {stats['updated_urls']}")
    logger.info(f"  No changes needed: {stats['no_change']}")
    logger.info(f"  Failed updates: {stats['failed']}")
    logger.info(f"Updated ticker data saved to: {saved_file}")
    
    return True

if __name__ == "__main__":
    main() 