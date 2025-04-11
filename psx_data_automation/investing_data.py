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
        # First try to search by PSX ticker
        search_url = f"{INVESTING_SEARCH_URL}{psx_ticker}+pakistan"
        response = requests.get(search_url, headers=HEADERS, timeout=10)
        
        if response.status_code != 200:
            logger.error(f"Failed to search for {psx_ticker}. Status code: {response.status_code}")
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for equities in search results
        result_elements = soup.select('.js-inner-all-results-quote-item')
        
        for element in result_elements:
            # Check if it's a Pakistan stock
            country_element = element.select_one('.flag.Pakistan')
            if not country_element:
                continue
            
            # Extract the URL from the anchor tag
            link_element = element.select_one('a.js-inner-all-results-quote-item-title')
            if not link_element:
                continue
            
            href = link_element.get('href', '')
            if '/equities/' in href:
                # Extract the URL-friendly name from the href
                url_friendly_name = href.split('/equities/')[1].split('-historical-data')[0]
                
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
        
        # First, get the page to extract any required tokens
        response = requests.get(url, headers=HEADERS, timeout=15)
        
        if response.status_code != 200:
            logger.error(f"Failed to access {url}. Status code: {response.status_code}")
            return None
        
        # Parse the HTML to extract data from the table
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the data table
        table = soup.select_one('#curr_table')
        if not table:
            logger.error(f"Could not find historical data table for {psx_ticker}")
            return None
        
        # Extract table headers
        headers = [th.text.strip() for th in table.select('thead th')]
        
        # Extract table rows
        rows = []
        for tr in table.select('tbody tr'):
            row = [td.text.strip() for td in tr.select('td')]
            if len(row) == len(headers):
                rows.append(row)
        
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
    
    # Calculate start date
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    for symbol in symbols:
        try:
            logger.info(f"Fetching data for {symbol}")
            
            # Fetch from investing.com
            df = fetch_historical_data(symbol, start_date, end_date)
            
            if df is not None and not df.empty:
                results[symbol] = df
                success_count += 1
                logger.info(f"Successfully downloaded data for {symbol}")
            else:
                logger.warning(f"No data found for {symbol}, will use synthetic data instead")
                
                # Fallback to synthetic data
                from psx_data_automation.historical_data import generate_realistic_ticker_data
                df = generate_realistic_ticker_data(symbol)
                
                file_path = os.path.join(DATA_DIR, f"{symbol}.csv")
                df.to_csv(file_path, index=False)
                logger.info(f"Saved synthetic data for {symbol} to {file_path}")
                
                results[symbol] = df
                
            # Add a delay to avoid hitting rate limits
            time.sleep(2)
            
        except Exception as e:
            logger.error(f"Error downloading data for {symbol}: {str(e)}")
    
    logger.info(f"Successfully downloaded data for {success_count} out of {len(symbols)} symbols")
    return results

if __name__ == "__main__":
    # Test with a few symbols
    symbols = ["LUCK", "ENGRO", "HBL", "PSO", "OGDC"]
    download_historical_data(symbols, days=365)  # Get 1 year of data for testing 