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
    Run directly: python -m psx_data_automation.scripts.scrape_tickers
    Import: from psx_data_automation.scripts.scrape_tickers import sync_tickers
"""

import csv
import logging
import os
import time
import random
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import re

# Use absolute imports instead of relative
from psx_data_automation.config import METADATA_DIR, PSX_BASE_URL, PSX_DATA_PORTAL_URL, COMPANY_URL_TEMPLATE
from psx_data_automation.scripts.utils import parse_html, ensure_directory_exists, format_ticker_symbol, retry
# Import our new crawler module
from psx_data_automation.scripts.crawler import fetch_company_page, fetch_ticker_list_sync, MOCK_TICKERS

# Set up logging
logger = logging.getLogger("psx_pipeline.tickers")

# File paths
TICKERS_CSV = METADATA_DIR / "all_tickers.csv"
CHANGES_LOG = METADATA_DIR / "ticker_changes.log"

# URL for Market Watch on PSX Data Portal
MARKET_WATCH_URL = f"{PSX_DATA_PORTAL_URL}/market-watch"

# Maximum number of concurrent requests for company details
MAX_CONCURRENT_REQUESTS = 10


def fetch_url(url, headers=None, max_retries=3, retry_delay=1.0):
    """
    Fetch the HTML content from a given URL with retry logic and headers.
    
    Args:
        url (str): The URL to fetch
        headers (dict): Optional headers to send with the request
        max_retries (int): Maximum number of retries
        retry_delay (float): Delay between retries in seconds
    
    Returns:
        str: HTML content of the page
    
    Raises:
        Exception: If the URL couldn't be fetched after max retries
    """
    if headers is None:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': PSX_DATA_PORTAL_URL,
        }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an exception for 4xx/5xx status codes
            return response.text
        except Exception as e:
            if attempt < max_retries - 1:
                # Wait with exponential backoff before retrying
                wait_time = retry_delay * (2 ** attempt) * random.uniform(0.8, 1.2)
                logger.warning(f"Error fetching {url}: {str(e)}. Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            else:
                # Last attempt failed
                raise Exception(f"Failed to fetch {url} after {max_retries} attempts: {str(e)}")


@retry(max_attempts=5, delay=5, backoff=2, max_delay=120)
def fetch_company_details(symbol, url=None):
    """
    Fetch detailed information about a company from its individual page.
    
    Args:
        symbol (str): The ticker symbol of the company
        url (str): The URL of the company's individual page
    
    Returns:
        dict: Company details including name, sector, and URL
    """
    details = {
        'symbol': symbol,
        'name': symbol,  # Default to symbol
        'sector': "Unknown",  # Default sector
        'url': url if url else f"{COMPANY_URL_TEMPLATE}{symbol}"
    }
    
    company_url = details['url']
    logger.debug(f"Fetching company details for {symbol} from {company_url}")
    
    # Add randomized headers to avoid detection patterns
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': f"{PSX_DATA_PORTAL_URL}/market-watch",
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
    }
    
    try:
        html_content = fetch_url(company_url, headers=headers)
        soup = parse_html(html_content)
        
        # Handle "No record found" pages
        if "no record found" in soup.text.lower() or "not found" in soup.text.lower():
            details['name'] = "No record found"
            return details
            
        # First try to extract from the company profile section
        # In dps.psx.com.pk/company/SYMBOL format, company name and sector are in specific places
        
        # For company name - usually in the header near the symbol
        company_name_elem = soup.select_one('h1, h2, h3, .company-name')
        if company_name_elem:
            company_name = company_name_elem.text.strip()
            if company_name and company_name != symbol:
                details['name'] = company_name
        
        # Try to find sector in a specific element or associated with "REFINERY" or similar text
        sector_elem = soup.select_one('.sector, .industry, .category')
        if not sector_elem:
            # In the DPS portal, sector is often displayed prominently near the company name
            for elem in soup.select('strong, b, h4, h5, .badge, .sector-badge'):
                if elem.text and len(elem.text.strip()) < 50:  # Reasonable length for a sector
                    text = elem.text.strip().upper()
                    # Common sectors in PSX
                    sectors = ['REFINERY', 'CEMENT', 'COMMERCIAL BANKS', 'FERTILIZER', 
                            'OIL & GAS', 'POWER', 'TEXTILE', 'PHARMACEUTICALS']
                    if any(sector in text for sector in sectors):
                        details['sector'] = elem.text.strip()
                        break
        else:
            details['sector'] = sector_elem.text.strip()
        
        # If we still don't have a company name, look for it in the page title
        if details['name'] == symbol:
            title_elem = soup.select_one('title')
            if title_elem and title_elem.text:
                title = title_elem.text.strip()
                # Extract company name from title (often in format "Company Name - PSX")
                if ' - ' in title:
                    company_name = title.split(' - ')[0].strip()
                    if company_name and company_name != symbol:
                        details['name'] = company_name
        
        # Extract from the business description if available
        business_desc = soup.select_one('.business-description, #company-profile')
        if business_desc:
            # If we have a business description, try to extract sector from first paragraph
            paragraphs = business_desc.select('p')
            if paragraphs:
                first_para = paragraphs[0].text.lower()
                sector_keywords = {
                    'bank': 'Commercial Banks',
                    'cement': 'Cement',
                    'oil': 'Oil & Gas',
                    'gas': 'Oil & Gas Marketing Companies',
                    'pharma': 'Pharmaceuticals',
                    'fertilizer': 'Fertilizer',
                    'textile': 'Textile',
                    'power': 'Power Generation & Distribution',
                    'refinery': 'Refinery',
                    'insurance': 'Insurance',
                    'investment': 'Investment',
                    'automobile': 'Automobile',
                    'chemical': 'Chemical',
                    'technology': 'Technology & Communication',
                    'food': 'Food & Personal Care Products'
                }
                
                for keyword, sector_name in sector_keywords.items():
                    if keyword in first_para:
                        details['sector'] = sector_name
                        break
            
            # If still no name, try to extract it from the business description
            if details['name'] == symbol:
                match = re.search(r'([A-Za-z\s]+)\s+(?:was|is|has been)\s+incorporated', business_desc.text)
                if match:
                    company_name = match.group(1).strip()
                    if company_name and len(company_name) > 3:  # Avoid too short matches
                        details['name'] = company_name
        
        logger.debug(f"Fetched details for {symbol}: {details['name']} - {details['sector']}")
    
    except requests.HTTPError as e:
        if hasattr(e, 'response') and e.response.status_code >= 500:
            logger.error(f"Server error ({e.response.status_code}) when fetching {symbol} from {company_url}")
        else:
            logger.warning(f"HTTP error when fetching {symbol}: {str(e)}")
        raise
    except Exception as e:
        logger.warning(f"Failed to fetch company details for {symbol}: {str(e)}")
        raise
    
    return details


def fetch_tickers_from_psx(fetch_details=True, mock=False):
    """
    Scrape the PSX Data Portal website to get the current list of tickers from Market Watch.
    Then fetch detailed information for each ticker from individual company pages.
    
    Args:
        fetch_details (bool): Whether to fetch detailed company information
        mock (bool): Whether to use mock data instead of fetching from web
    
    Returns:
        list: List of ticker dictionaries with symbol, name, sector and URL
    """
    logger.info("Fetching current ticker list from PSX Data Portal")
    
    tickers = []
    
    # If mock mode is enabled, return mock tickers immediately
    if mock:
        logger.info("Using mock data as requested")
        return MOCK_TICKERS
    
    try:
        # Use the new crawler to fetch ticker data
        ticker_data = fetch_ticker_list_sync()
        
        if ticker_data:
            logger.info(f"Successfully fetched {len(ticker_data)} ticker symbols from PSX Market Watch")
            
            # If we have ticker data and want detailed information
            if fetch_details:
                logger.info(f"Fetching detailed company information for {len(ticker_data)} tickers...")
                
                # Use a thread pool to fetch company details concurrently
                with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
                    # Submit tasks for each ticker - using our enhanced crawler
                    future_to_ticker = {
                        executor.submit(fetch_company_page, data['symbol'], data['url']): data 
                        for data in ticker_data
                    }
                    
                    # Process results as they complete
                    completed = 0
                    for future in as_completed(future_to_ticker):
                        ticker_data_item = future_to_ticker[future]
                        symbol = ticker_data_item['symbol']
                        url = ticker_data_item['url']
                        
                        try:
                            ticker_details = future.result()
                            tickers.append(ticker_details)
                            
                            # Log progress
                            completed += 1
                            if completed % 50 == 0 or completed == len(ticker_data):
                                logger.info(f"Fetched details for {completed}/{len(ticker_data)} companies")
                            
                        except Exception as e:
                            logger.warning(f"Error processing {symbol}: {str(e)}")
                            # Add with default values if there's an error
                            tickers.append({
                                'symbol': symbol,
                                'name': symbol,
                                'sector': "Unknown",
                                'url': url
                            })
                        
                        # Add a randomized delay to avoid server detection patterns
                        time.sleep(random.uniform(0.2, 0.8))
            else:
                # If we don't want details or have no ticker data, create basic ticker entries
                for data in ticker_data:
                    tickers.append({
                        'symbol': data['symbol'],
                        'name': data['symbol'],
                        'sector': "Unknown",
                        'url': data['url']
                    })
            
            # If we successfully got tickers, return them
            if tickers:
                return tickers
        else:
            logger.warning("Could not fetch tickers from PSX Market Watch")
        
        # Fall back to alternative scraping methods if all previous methods fail
        logger.info("Using mock data for testing purposes")
        
        # Use the mock tickers from the crawler module
        tickers = MOCK_TICKERS
        logger.info(f"Created {len(tickers)} mock tickers for testing")
        
    except Exception as e:
        logger.error(f"Error processing PSX ticker data: {str(e)}")
        # Fall back to mock tickers if we hit an error
        tickers = MOCK_TICKERS
        logger.warning(f"Using {len(tickers)} mock tickers after error")
    
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
        tickers (list): List of ticker dictionaries with symbol, name, sector and url
    """
    try:
        # Ensure directory exists using our utility function
        ensure_directory_exists(METADATA_DIR)
        
        # Write to CSV
        with open(TICKERS_CSV, 'w', newline='', encoding='utf-8') as f:
            # Include url in the fieldnames
            fieldnames = ['symbol', 'name', 'sector', 'url']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            writer.writeheader()
            
            # Filter out any fields not in fieldnames
            cleaned_tickers = []
            for ticker in tickers:
                cleaned_ticker = {field: ticker.get(field, '') for field in fieldnames}
                cleaned_tickers.append(cleaned_ticker)
            
            writer.writerows(cleaned_tickers)
        
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


def sync_tickers(fetch_details=True):
    """
    Main function to synchronize the ticker list with PSX.
    
    Args:
        fetch_details (bool): Whether to fetch detailed company information
    
    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("Starting ticker synchronization")
    
    # Fetch current tickers from PSX
    current_tickers = fetch_tickers_from_psx(fetch_details)
    
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