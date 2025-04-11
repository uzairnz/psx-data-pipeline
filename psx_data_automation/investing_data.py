#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module for fetching historical stock data from investing.com for PSX tickers.
"""

import os
import re
import time
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
import requests
from bs4 import BeautifulSoup

from psx_data_automation.config import (
    DATA_DIR,
    INVESTING_BASE_URL,
    INVESTING_SEARCH_URL,
    INVESTING_HISTORICAL_URL,
    TICKER_TO_INVESTING_MAP,
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Headers for requests to avoid being blocked
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
}

def search_ticker_on_investing(psx_ticker: str) -> Optional[str]:
    """
    Search for a PSX ticker on investing.com and return the URL-friendly name.
    
    Args:
        psx_ticker (str): The PSX ticker symbol (e.g., 'LUCK')
        
    Returns:
        Optional[str]: The investing.com URL-friendly name (e.g., 'lucky-cement') or None if not found
    """
    # Check if we already have a mapping for this ticker
    if psx_ticker in TICKER_TO_INVESTING_MAP and TICKER_TO_INVESTING_MAP[psx_ticker]:
        logger.info(f"Using cached mapping for {psx_ticker}: {TICKER_TO_INVESTING_MAP[psx_ticker]}")
        return TICKER_TO_INVESTING_MAP[psx_ticker]
    
    try:
        logger.info(f"Searching for {psx_ticker} on investing.com...")
        
        # Different search approaches to try
        search_queries = [
            f"{psx_ticker}+pakistan+stock",
            f"{psx_ticker}+pakistan",
            f"{psx_ticker}+karachi+stock"
        ]
        
        for query in search_queries:
            search_url = f"{INVESTING_SEARCH_URL}{query}"
            
            # Add referer and cookie headers to appear more like a browser
            headers = HEADERS.copy()
            headers["Referer"] = INVESTING_BASE_URL
            
            response = requests.get(search_url, headers=headers, timeout=15)
            
            if response.status_code != 200:
                logger.warning(f"Failed to search for '{query}'. Status code: {response.status_code}")
                continue
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Try different selectors for results
            for selector in ['.js-inner-all-results-quote-item', '.searchSectionMain tr', '.searchSectionInner tr']:
                result_elements = soup.select(selector)
                
                if not result_elements:
                    logger.debug(f"No results found with selector '{selector}'")
                    continue
                
                logger.info(f"Found {len(result_elements)} potential matches with selector '{selector}'")
                
                for element in result_elements:
                    # Look for Pakistan flag or text
                    country_found = False
                    for flag_selector in ['.flag.Pakistan', '.flag.pk', '.countryFlag', 'td:contains("Pakistan")', 'td:contains("Karachi")']:
                        country_element = element.select_one(flag_selector) if ':contains' not in flag_selector else None
                        if country_element or (hasattr(element, 'text') and ('Pakistan' in element.text or 'Karachi' in element.text)):
                            country_found = True
                            break
                    
                    if not country_found:
                        continue
                    
                    # Extract URL from different potential elements
                    href = None
                    link_selectors = ['a.js-inner-all-results-quote-item-title', 'a.bold', 'a[href*="/equities/"]']
                    
                    for link_selector in link_selectors:
                        link_element = element.select_one(link_selector)
                        if link_element and link_element.get('href'):
                            href = link_element.get('href')
                            break
                    
                    if not href or '/equities/' not in href:
                        continue
                    
                    # Extract the URL-friendly name from the href
                    url_friendly_name = href.split('/equities/')[1]
                    # Handle possible trailing segments
                    for segment in ['-historical-data', '?', '#']:
                        if segment in url_friendly_name:
                            url_friendly_name = url_friendly_name.split(segment)[0]
                    
                    # Save mapping for future use
                    TICKER_TO_INVESTING_MAP[psx_ticker] = url_friendly_name
                    
                    # Save the entire mapping to disk for future runs
                    config_file = os.path.join(DATA_DIR, 'investing_ticker_map.json')
                    try:
                        with open(config_file, 'w') as f:
                            json.dump(TICKER_TO_INVESTING_MAP, f, indent=2)
                        logger.info(f"Saved ticker mapping to {config_file}")
                    except Exception as e:
                        logger.warning(f"Failed to save ticker mapping: {str(e)}")
                    
                    logger.info(f"Successfully mapped {psx_ticker} to {url_friendly_name}")
                    return url_friendly_name
    
    except Exception as e:
        logger.error(f"Error searching for {psx_ticker} on investing.com: {str(e)}")
    
    logger.warning(f"Could not find {psx_ticker} on investing.com")
    return None

def load_ticker_mappings() -> None:
    """
    Load saved ticker mappings from disk if available.
    """
    global TICKER_TO_INVESTING_MAP
    
    config_file = os.path.join(DATA_DIR, 'investing_ticker_map.json')
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r') as f:
                TICKER_TO_INVESTING_MAP.update(json.load(f))
            logger.info(f"Loaded {len(TICKER_TO_INVESTING_MAP)} ticker mappings from {config_file}")
        except Exception as e:
            logger.warning(f"Failed to load ticker mappings: {str(e)}")

def fetch_historical_data(
    psx_ticker: str,
    start_date: datetime,
    end_date: datetime = None
) -> Optional[pd.DataFrame]:
    """
    Fetch historical data for a PSX ticker from investing.com.
    
    Args:
        psx_ticker (str): The PSX ticker symbol (e.g., 'LUCK')
        start_date (datetime): Start date for historical data
        end_date (datetime, optional): End date for historical data. Defaults to today.
        
    Returns:
        Optional[pd.DataFrame]: DataFrame with historical data or None if failed
    """
    if end_date is None:
        end_date = datetime.now()
    
    # Convert dates to required format (MM/DD/YYYY)
    start_str = start_date.strftime("%m/%d/%Y")
    end_str = end_date.strftime("%m/%d/%Y")
    
    # Find the investing.com ticker name
    investing_ticker = search_ticker_on_investing(psx_ticker)
    if not investing_ticker:
        logger.error(f"Could not find {psx_ticker} on investing.com")
        return None
    
    # Format the URL
    url = INVESTING_HISTORICAL_URL.format(ticker=investing_ticker)
    
    try:
        logger.info(f"Fetching historical data for {psx_ticker} from {url}")
        
        # Enhanced headers to better mimic a browser
        headers = HEADERS.copy()
        headers["Referer"] = INVESTING_BASE_URL
        headers["X-Requested-With"] = "XMLHttpRequest"
        
        # First attempt: Try direct access to the historical data page
        session = requests.Session()
        
        # Make initial request to get cookies and page structure
        response = session.get(url, headers=headers, timeout=20)
        
        if response.status_code != 200:
            logger.error(f"Failed to access {url}. Status code: {response.status_code}")
            # Try simplified URL
            simple_url = f"{INVESTING_BASE_URL}/equities/{investing_ticker}"
            logger.info(f"Trying simplified URL: {simple_url}")
            response = session.get(simple_url, headers=headers, timeout=20)
            
            if response.status_code != 200:
                logger.error(f"Failed to access simplified URL. Status code: {response.status_code}")
                return None
        
        # Parse the HTML to extract data from the table
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for the historical data table
        table_selectors = ['#curr_table', '.genTbl.closedTbl.historicalTbl', '.common-table.medium.js-table']
        table = None
        
        for selector in table_selectors:
            table = soup.select_one(selector)
            if table:
                logger.info(f"Found historical data table with selector: {selector}")
                break
        
        if not table:
            # If no table is found on the initial page, we need to try the date selection form
            logger.warning(f"No data table found on initial page for {psx_ticker}. Trying date selection...")
            
            # Try to extract any form tokens needed for the date selection request
            form_selectors = ['#datePickerForm', 'form[action*="historical-data"]']
            form = None
            form_data = {}
            
            for selector in form_selectors:
                form = soup.select_one(selector)
                if form:
                    logger.info(f"Found form with selector: {selector}")
                    # Extract hidden fields
                    for input_field in form.select('input[type="hidden"]'):
                        name = input_field.get('name')
                        value = input_field.get('value')
                        if name and value:
                            form_data[name] = value
                    break
            
            if form:
                # Add date parameters to form data
                form_data.update({
                    'dateFrom': start_str,
                    'dateTo': end_str,
                    'period': ''  # Daily data
                })
                
                # Get form action URL
                form_action = form.get('action')
                date_selection_url = form_action if form_action and form_action.startswith('http') else url
                
                # Make POST request to get historical data with specified date range
                post_headers = headers.copy()
                post_headers['Content-Type'] = 'application/x-www-form-urlencoded'
                
                try:
                    response = session.post(date_selection_url, data=form_data, headers=post_headers, timeout=20)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        # Try finding the table again
                        for selector in table_selectors:
                            table = soup.select_one(selector)
                            if table:
                                logger.info(f"Found historical data table after date selection with selector: {selector}")
                                break
                except Exception as e:
                    logger.error(f"Error in POST request for date selection: {str(e)}")
        
        # If we still don't have a table, try a different approach
        if not table:
            logger.warning(f"Still no data table found for {psx_ticker}. Checking for data in page content...")
            
            # Check if there's a JavaScript variable containing the data
            data_pattern = re.search(r'var\s+historyData\s*=\s*(\[.*?\]);', str(soup), re.DOTALL)
            if data_pattern:
                try:
                    import json
                    history_data = json.loads(data_pattern.group(1))
                    logger.info(f"Found history data in JavaScript variable with {len(history_data)} entries")
                    
                    # Convert JavaScript data to DataFrame
                    df = pd.DataFrame(history_data)
                    # Map column names
                    col_mapping = {'date': 'date', 'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume'}
                    df = df.rename(columns={k: v for k, v in col_mapping.items() if k in df.columns})
                    
                    # Save to file
                    file_path = os.path.join(DATA_DIR, f"{psx_ticker}.csv")
                    df.to_csv(file_path, index=False)
                    logger.info(f"Saved historical data for {psx_ticker} to {file_path}")
                    
                    return df
                    
                except Exception as e:
                    logger.error(f"Error processing JavaScript data: {str(e)}")
        
        if not table:
            logger.error(f"Could not find historical data table for {psx_ticker}")
            return None
        
        # Extract table headers
        headers = []
        for th in table.select('thead th, tr.first th'):
            header_text = th.text.strip()
            if header_text:
                headers.append(header_text)
        
        if not headers:
            logger.error(f"No headers found in table for {psx_ticker}")
            return None
            
        logger.info(f"Found table headers: {headers}")
        
        # Extract table rows
        rows = []
        for tr in table.select('tbody tr'):
            row = [td.text.strip() for td in tr.select('td')]
            if len(row) == len(headers):
                rows.append(row)
        
        if not rows:
            logger.error(f"No data rows found in table for {psx_ticker}")
            return None
            
        logger.info(f"Extracted {len(rows)} data rows")
        
        # Create DataFrame
        df = pd.DataFrame(rows, columns=headers)
        
        # Convert and clean the data
        df = clean_investing_data(df)
        
        # Save to file
        file_path = os.path.join(DATA_DIR, f"{psx_ticker}.csv")
        df.to_csv(file_path, index=False)
        logger.info(f"Saved historical data for {psx_ticker} to {file_path}")
        
        return df
    
    except Exception as e:
        logger.error(f"Error fetching historical data for {psx_ticker}: {str(e)}")
        return None

def clean_investing_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and convert the data from investing.com to a standard format.
    
    Args:
        df (pd.DataFrame): Raw DataFrame from investing.com
        
    Returns:
        pd.DataFrame: Cleaned DataFrame with standardized column names and data types
    """
    # Rename columns to standardized format
    col_mapping = {
        'Date': 'date',
        'Price': 'close',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Vol.': 'volume',
        'Change %': 'change_pct'
    }
    
    # Apply column renaming for columns that exist
    rename_cols = {old: new for old, new in col_mapping.items() if old in df.columns}
    df = df.rename(columns=rename_cols)
    
    # Convert date
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    
    # Convert numeric columns
    numeric_cols = ['open', 'high', 'low', 'close']
    for col in numeric_cols:
        if col in df.columns:
            # Remove commas and convert to float
            df[col] = df[col].replace(',', '', regex=True).astype(float)
    
    # Convert volume (may contain K, M, B for thousands, millions, billions)
    if 'volume' in df.columns:
        df['volume'] = df['volume'].apply(convert_volume)
    
    # Sort by date descending (newest first)
    if 'date' in df.columns:
        df = df.sort_values('date', ascending=False)
    
    return df

def convert_volume(vol_str: str) -> int:
    """
    Convert volume string with K, M, B suffixes to integer.
    
    Args:
        vol_str (str): Volume string (e.g., '1.5M', '500K')
        
    Returns:
        int: Volume as integer
    """
    try:
        vol_str = vol_str.replace(',', '')
        
        if vol_str.endswith('K'):
            return int(float(vol_str[:-1]) * 1000)
        elif vol_str.endswith('M'):
            return int(float(vol_str[:-1]) * 1000000)
        elif vol_str.endswith('B'):
            return int(float(vol_str[:-1]) * 1000000000)
        else:
            return int(float(vol_str))
    except (ValueError, AttributeError):
        return 0

def download_historical_data(symbols: List[str], days: int = 3650) -> Dict[str, pd.DataFrame]:
    """
    Download historical data for multiple ticker symbols from investing.com.
    
    Args:
        symbols (list): List of ticker symbols
        days (int): Number of days to look back (default: 3650, ~10 years)
        
    Returns:
        dict: Dictionary mapping symbols to their price DataFrames
    """
    # Load existing ticker mappings
    load_ticker_mappings()
    
    logger.info(f"Downloading historical data for {len(symbols)} symbols from investing.com")
    
    results = {}
    success_count = 0
    fallback_count = 0
    failed_count = 0
    
    # Calculate start date
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    for i, symbol in enumerate(symbols):
        try:
            logger.info(f"[{i+1}/{len(symbols)}] Processing {symbol}")
            
            # Fetch from investing.com
            df = fetch_historical_data(symbol, start_date, end_date)
            
            if df is not None and not df.empty:
                results[symbol] = df
                success_count += 1
                logger.info(f"Successfully downloaded data for {symbol} from investing.com")
            else:
                logger.warning(f"No data found for {symbol} on investing.com, falling back to synthetic data")
                
                # Fallback to synthetic data
                from psx_data_automation.historical_data import generate_realistic_ticker_data
                df = generate_realistic_ticker_data(symbol, days=days)
                
                if df is not None and not df.empty:
                    file_path = os.path.join(DATA_DIR, f"{symbol}.csv")
                    df.to_csv(file_path, index=False)
                    logger.info(f"Saved synthetic data for {symbol} to {file_path}")
                    
                    results[symbol] = df
                    fallback_count += 1
                else:
                    logger.error(f"Failed to generate synthetic data for {symbol}")
                    failed_count += 1
                
            # Add a delay to avoid hitting rate limits
            time.sleep(3)
            
        except Exception as e:
            logger.error(f"Error processing {symbol}: {str(e)}")
            failed_count += 1
    
    # Log final summary
    logger.info(f"Historical data download summary:")
    logger.info(f"  - Total tickers processed: {len(symbols)}")
    logger.info(f"  - Successfully downloaded from investing.com: {success_count}")
    logger.info(f"  - Fallback to synthetic data: {fallback_count}")
    logger.info(f"  - Failed to get data: {failed_count}")
    
    return results

if __name__ == "__main__":
    # Test with the most common Pakistan stocks
    symbols = ["LUCK", "ENGRO", "HBL", "PSO", "OGDC", "MCB", "UBL", "PPL", "FFC", "MEBL"]
    
    # Test single stock to debug
    def test_single_stock(symbol):
        print(f"\n----- Testing {symbol} -----")
        investing_ticker = search_ticker_on_investing(symbol)
        print(f"Mapping: {symbol} -> {investing_ticker}")
        
        if investing_ticker:
            # Test date range to last 6 months
            end_date = datetime.now()
            start_date = end_date - timedelta(days=180)
            
            df = fetch_historical_data(symbol, start_date, end_date)
            if df is not None and not df.empty:
                print(f"Successful! Got {len(df)} rows of data.")
                print("First 5 rows:")
                print(df.head())
            else:
                print(f"Failed to get data for {symbol}")
        else:
            print(f"Failed to map {symbol} to investing.com ticker")
    
    # Test the entire pipeline on a few stocks
    download_historical_data(symbols[:3], days=180)  # Get 6 months of data for 3 stocks
    
    # Test single stock for debugging if needed
    # Uncomment to test specific stock
    # test_single_stock("LUCK") 