#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Ticker Management System for PSX Data Pipeline.

This script:
1. Scrapes the PSX Data Portal to fetch the current list of all tickers
2. Compares with previous ticker list (if exists)
3. Identifies additions, deletions, and potential renames
4. Updates the ticker list CSV
5. Logs all changes

Usage:
    Run directly: python scripts/scrape_tickers.py
    Import: from scripts.scrape_tickers import sync_tickers
"""

import csv
import logging
import os
from datetime import datetime
from pathlib import Path

from ..config import METADATA_DIR, PSX_BASE_URL
from .utils import fetch_url, parse_html, ensure_directory_exists, format_ticker_symbol

# Set up logging
logger = logging.getLogger("psx_pipeline.tickers")

# File paths
TICKERS_CSV = METADATA_DIR / "all_tickers.csv"
CHANGES_LOG = METADATA_DIR / "ticker_changes.log"


def fetch_tickers_from_psx():
    """
    Scrape the PSX Data Portal website to get the current list of tickers.
    
    Returns:
        list: List of ticker dictionaries with symbol, name, and sector
    """
    logger.info("Fetching current ticker list from PSX Data Portal")
    
    tickers = []
    
    try:
        # Main URL for PSX data portal symbols page
        url = f"{PSX_BASE_URL}/dps/symbol-list"
        
        # Use our utility function to fetch the page with retries
        html_content = fetch_url(url)
        
        # Parse the HTML content
        soup = parse_html(html_content)
        
        # Find the table with tickers
        # Note: This selector needs to be updated based on actual PSX website structure
        table = soup.select_one('table.symbols-list')
        
        if not table:
            # Try alternative selectors if the first one doesn't work
            table = soup.select_one('table.table')  # Try a more generic selector
            
            if not table:
                # If still not found, look for any table on the page
                tables = soup.select('table')
                if tables:
                    table = tables[0]  # Use the first table
        
        if not table:
            logger.error("Could not find ticker table on PSX website")
            return tickers
        
        # Extract tickers from table rows
        rows = table.select('tbody tr')
        
        for row in rows:
            columns = row.select('td')
            if len(columns) >= 2:  # Ensure we have at least symbol and name
                ticker = {
                    'symbol': format_ticker_symbol(columns[0].text),
                    'name': columns[1].text.strip(),
                    'sector': columns[2].text.strip() if len(columns) > 2 else "Unknown"
                }
                tickers.append(ticker)
        
        logger.info(f"Successfully fetched {len(tickers)} tickers from PSX")
        
    except Exception as e:
        logger.error(f"Error processing PSX ticker data: {str(e)}")
    
    return tickers


def load_existing_tickers():
    """
    Load the previous list of tickers from CSV file if it exists.
    
    Returns:
        list: List of ticker dictionaries with symbol, name, and sector
               Empty list if the file doesn't exist
    """
    tickers = []
    
    if not TICKERS_CSV.exists():
        logger.info("No existing ticker list found")
        return tickers
    
    try:
        with open(TICKERS_CSV, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            tickers = list(reader)
        
        logger.info(f"Loaded {len(tickers)} tickers from existing list")
        
    except Exception as e:
        logger.error(f"Error loading existing ticker list: {str(e)}")
    
    return tickers


def save_tickers(tickers):
    """
    Save the current list of tickers to CSV file.
    
    Args:
        tickers (list): List of ticker dictionaries with symbol, name, and sector
    """
    try:
        # Ensure directory exists using our utility function
        ensure_directory_exists(METADATA_DIR)
        
        # Write to CSV
        with open(TICKERS_CSV, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['symbol', 'name', 'sector']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            writer.writeheader()
            writer.writerows(tickers)
        
        logger.info(f"Saved {len(tickers)} tickers to {TICKERS_CSV}")
        
    except Exception as e:
        logger.error(f"Error saving ticker list: {str(e)}")


def log_changes(added, deleted, renamed):
    """
    Log changes to the ticker list to both the log file and the changes log.
    
    Args:
        added (list): List of added ticker symbols
        deleted (list): List of deleted ticker symbols
        renamed (list): List of renamed tickers as (old_symbol, new_symbol) tuples
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Log changes to application log
    if added:
        logger.info(f"Added {len(added)} tickers: {', '.join(added)}")
    if deleted:
        logger.info(f"Deleted {len(deleted)} tickers: {', '.join(deleted)}")
    if renamed:
        rename_strs = [f"{old} → {new}" for old, new in renamed]
        logger.info(f"Renamed {len(renamed)} tickers: {', '.join(rename_strs)}")
    
    # Log changes to the dedicated changes log file
    try:
        # Ensure directory exists
        ensure_directory_exists(METADATA_DIR)
        
        # Create the file if it doesn't exist
        if not CHANGES_LOG.exists():
            with open(CHANGES_LOG, 'w', encoding='utf-8') as f:
                f.write("# PSX Ticker Changes Log\n\n")
        
        # Append changes
        with open(CHANGES_LOG, 'a', encoding='utf-8') as f:
            f.write(f"\n=== {timestamp} ===\n")
            
            if added:
                f.write(f"ADDED ({len(added)}):\n")
                for symbol in added:
                    f.write(f"+ {symbol}\n")
            
            if deleted:
                f.write(f"DELETED ({len(deleted)}):\n")
                for symbol in deleted:
                    f.write(f"- {symbol}\n")
            
            if renamed:
                f.write(f"RENAMED ({len(renamed)}):\n")
                for old, new in renamed:
                    f.write(f"* {old} → {new}\n")
            
            f.write("\n")
        
        logger.info(f"Changes logged to {CHANGES_LOG}")
        
    except Exception as e:
        logger.error(f"Error logging ticker changes: {str(e)}")


def identify_changes(current_tickers, previous_tickers):
    """
    Compare current and previous ticker lists to identify changes.
    
    Args:
        current_tickers (list): List of current ticker dictionaries
        previous_tickers (list): List of previous ticker dictionaries
    
    Returns:
        tuple: (added, deleted, renamed) lists
    """
    # Extract symbols for easier comparison
    current_symbols = {t['symbol'] for t in current_tickers}
    previous_symbols = {t['symbol'] for t in previous_tickers}
    
    # Find added and deleted tickers
    added_symbols = current_symbols - previous_symbols
    deleted_symbols = previous_symbols - current_symbols
    
    # Simple heuristic for detecting renames:
    # If a ticker is deleted and another is added on the same day,
    # and they have similar company names, it might be a rename
    renamed = []
    
    # Only try to detect renames if we have both additions and deletions
    if added_symbols and deleted_symbols:
        # Create dictionaries for easier lookup
        current_dict = {t['symbol']: t for t in current_tickers if t['symbol'] in added_symbols}
        previous_dict = {t['symbol']: t for t in previous_tickers if t['symbol'] in deleted_symbols}
        
        # Simple name similarity check (this can be improved with more sophisticated algorithms)
        for old_symbol in list(deleted_symbols):
            old_name = previous_dict[old_symbol]['name'].lower()
            
            for new_symbol in list(added_symbols):
                new_name = current_dict[new_symbol]['name'].lower()
                
                # Check name similarity
                if (old_name in new_name or new_name in old_name or 
                    (len(old_name) > 10 and len(new_name) > 10 and 
                     any(word in new_name for word in old_name.split() if len(word) > 3))):
                    
                    # This looks like a rename
                    renamed.append((old_symbol, new_symbol))
                    
                    # Remove from added/deleted lists
                    deleted_symbols.remove(old_symbol)
                    added_symbols.remove(new_symbol)
                    break
    
    return list(added_symbols), list(deleted_symbols), renamed


def sync_tickers():
    """
    Main function to synchronize the ticker list with PSX.
    
    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("Starting ticker synchronization")
    
    # Fetch current tickers from PSX
    current_tickers = fetch_tickers_from_psx()
    
    if not current_tickers:
        logger.error("Failed to fetch current tickers. Aborting sync.")
        return False
    
    # Load previous tickers
    previous_tickers = load_existing_tickers()
    
    # Identify changes
    if previous_tickers:
        added, deleted, renamed = identify_changes(current_tickers, previous_tickers)
        log_changes(added, deleted, renamed)
    else:
        logger.info(f"First run - added {len(current_tickers)} initial tickers")
    
    # Save current tickers
    save_tickers(current_tickers)
    
    logger.info("Ticker synchronization completed successfully")
    return True


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    # Run the sync
    sync_tickers() 