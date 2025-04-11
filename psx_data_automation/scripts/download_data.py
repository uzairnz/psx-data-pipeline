#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Historical Data Collection Module for PSX Data Pipeline.

This script:
1. Downloads historical OHLC (Open, High, Low, Close) data for PSX tickers
2. Fetches data since START_DATE (defined in config)
3. Saves data to CSV files in data directory
4. Implements retry logic for handling network issues

Usage:
    Run directly: python -m psx_data_automation.scripts.download_data
    Import: from psx_data_automation.scripts.download_data import download_historical_data
"""

import csv
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from psx_data_automation.config import (DATA_DIR, PSX_DATA_PORTAL_URL, 
                                        START_DATE, COMPANY_URL_TEMPLATE)

# Set up logging
logger = logging.getLogger("psx_pipeline.historical_data")

# Constants
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
MAX_CONCURRENT_DOWNLOADS = 5
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
}

# Historical data URLs
HISTORICAL_DATA_URL_TEMPLATE = f"{PSX_DATA_PORTAL_URL}/company/{{symbol}}/historical"


def find_latest_ticker_file():
    """
    Find the most recent ticker file in the data directory.
    
    Returns:
        Path: Path to the latest ticker file, or None if no file found
    """
    ticker_files = list(DATA_DIR.glob("tickers_*_updated.json"))
    if not ticker_files:
        ticker_files = list(DATA_DIR.glob("tickers_*.json"))
    
    if not ticker_files:
        logger.error("No ticker files found in data directory")
        return None
    
    latest_file = max(ticker_files, key=lambda x: x.stat().st_mtime)
    logger.info(f"Found latest ticker file: {latest_file}")
    return latest_file


def load_tickers(file_path):
    """
    Load ticker data from the JSON file.
    
    Args:
        file_path (Path): Path to the ticker JSON file
    
    Returns:
        list: List of ticker dictionaries
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            tickers = json.load(f)
        logger.info(f"Loaded {len(tickers)} tickers from {file_path}")
        return tickers
    except Exception as e:
        logger.error(f"Error loading ticker data: {str(e)}")
        return []


def fetch_historical_data(symbol, start_date=None, end_date=None, use_mock=False):
    """
    Fetch historical OHLC data for a specific ticker.
    
    Args:
        symbol (str): The ticker symbol (e.g., 'HBL')
        start_date (str, optional): Start date in YYYY-MM-DD format. Defaults to config.START_DATE.
        end_date (str, optional): End date in YYYY-MM-DD format. Defaults to today.
        use_mock (bool, optional): Whether to use mock data for testing. Defaults to False.
    
    Returns:
        pd.DataFrame: DataFrame with historical OHLC data, or empty DataFrame if error
    """
    if use_mock:
        logger.info(f"Using mock data for {symbol}")
        return generate_mock_data(symbol, start_date, end_date)
    
    if not start_date:
        start_date = START_DATE
    
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    url = HISTORICAL_DATA_URL_TEMPLATE.format(symbol=symbol)
    logger.debug(f"Fetching historical data for {symbol} from {url}")
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                url, 
                headers=HEADERS,
                params={
                    'from': start_date,
                    'to': end_date
                },
                timeout=30
            )
            
            if response.status_code == 200:
                # Parse the HTML response to extract the historical data table
                soup = BeautifulSoup(response.text, 'html.parser')
                data = extract_historical_data_from_html(soup, symbol)
                
                if data.empty:
                    logger.warning(f"No historical data found for {symbol}")
                    return pd.DataFrame()
                
                return data
            else:
                logger.warning(f"Failed to fetch historical data for {symbol}: HTTP {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            wait_time = RETRY_DELAY * (2 ** attempt)
            logger.warning(f"Error fetching historical data for {symbol}: {str(e)}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    
    logger.error(f"Failed to fetch historical data for {symbol} after {MAX_RETRIES} attempts")
    return pd.DataFrame()


def extract_historical_data_from_html(soup, symbol):
    """
    Extract historical OHLC data from HTML.
    
    Args:
        soup (BeautifulSoup): BeautifulSoup object with the HTML content
        symbol (str): Ticker symbol for logging
    
    Returns:
        pd.DataFrame: DataFrame with the extracted data
    """
    try:
        # Find the table that contains historical data
        table = soup.select_one('table.historical-data-table')
        
        # If table not found, try alternative selectors
        if not table:
            table = soup.select_one('table.table')
        
        if not table:
            logger.warning(f"Could not find historical data table for {symbol}")
            return pd.DataFrame()
        
        # Extract headers and rows
        headers = [th.text.strip() for th in table.select('thead th')]
        rows = []
        
        for row in table.select('tbody tr'):
            row_data = [td.text.strip() for td in row.select('td')]
            if len(row_data) == len(headers):
                rows.append(row_data)
        
        # Create DataFrame
        df = pd.DataFrame(rows, columns=headers)
        
        # Standardize column names
        column_mapping = {
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume',
            'Change': 'change',
            'Change %': 'change_pct'
        }
        
        # Rename columns based on the mapping, keeping only matching columns
        columns_to_rename = {col: column_mapping[col] for col in df.columns if col in column_mapping}
        df.rename(columns=columns_to_rename, inplace=True)
        
        # Convert date column to datetime
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df.sort_values('date', inplace=True)
        
        # Convert numeric columns
        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
        
        return df
    
    except Exception as e:
        logger.error(f"Error extracting historical data for {symbol}: {str(e)}")
        return pd.DataFrame()


def generate_mock_data(symbol, start_date=None, end_date=None):
    """
    Generate mock OHLC data for testing purposes.
    
    Args:
        symbol (str): Ticker symbol
        start_date (str, optional): Start date. Defaults to config.START_DATE.
        end_date (str, optional): End date. Defaults to today.
    
    Returns:
        pd.DataFrame: DataFrame with mock OHLC data
    """
    if not start_date:
        start_date = START_DATE
    
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    # Convert dates to datetime
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    
    # Generate date range (excluding weekends)
    dates = pd.date_range(start=start_dt, end=end_dt, freq='B')  # 'B' for business days
    
    # Generate random OHLC data
    import numpy as np
    
    base_price = np.random.uniform(10, 100)
    daily_volatility = np.random.uniform(0.01, 0.03)
    
    # Generate a price series with random walk
    closes = np.random.normal(0, daily_volatility, size=len(dates))
    closes = pd.Series(closes).cumsum() + base_price
    
    # Set a min price floor
    closes = np.maximum(closes, 1.0)
    
    # Generate other OHLC values
    df = pd.DataFrame({
        'date': dates,
        'open': closes.shift(1).fillna(base_price) * np.random.uniform(0.98, 1.02, size=len(dates)),
        'high': closes * np.random.uniform(1.01, 1.05, size=len(dates)),
        'low': closes * np.random.uniform(0.95, 0.99, size=len(dates)),
        'close': closes,
        'volume': np.random.randint(10000, 1000000, size=len(dates))
    })
    
    # Ensure high >= open >= close >= low
    df['high'] = df[['high', 'open', 'close']].max(axis=1)
    df['low'] = df[['low', 'open', 'close']].min(axis=1)
    
    return df


def save_historical_data(symbol, data):
    """
    Save historical data to a CSV file.
    
    Args:
        symbol (str): Ticker symbol
        data (pd.DataFrame): DataFrame with historical data
    
    Returns:
        bool: True if successful, False otherwise
    """
    if data.empty:
        logger.warning(f"No data to save for {symbol}")
        return False
    
    try:
        file_path = DATA_DIR / f"{symbol}.csv"
        
        # Ensure date is the first column
        if 'date' in data.columns:
            cols = ['date'] + [col for col in data.columns if col != 'date']
            data = data[cols]
        
        # Save to CSV
        data.to_csv(file_path, index=False)
        logger.info(f"Saved historical data for {symbol} to {file_path}")
        return True
    
    except Exception as e:
        logger.error(f"Error saving historical data for {symbol}: {str(e)}")
        return False


def download_for_ticker(ticker, use_mock=False):
    """
    Download historical data for a single ticker.
    
    Args:
        ticker (dict): Ticker dictionary with 'symbol' key
        use_mock (bool, optional): Whether to use mock data. Defaults to False.
    
    Returns:
        tuple: (symbol, success_flag)
    """
    symbol = ticker['symbol']
    logger.info(f"Downloading historical data for {symbol}")
    
    try:
        # Fetch historical data
        data = fetch_historical_data(symbol, use_mock=use_mock)
        
        if data.empty:
            logger.warning(f"No historical data available for {symbol}")
            return symbol, False
        
        # Save data to CSV
        success = save_historical_data(symbol, data)
        return symbol, success
    
    except Exception as e:
        logger.error(f"Error processing {symbol}: {str(e)}")
        return symbol, False


def download_historical_data(tickers=None, max_tickers=None, use_mock=False):
    """
    Download historical data for all tickers.
    
    Args:
        tickers (list, optional): List of ticker dictionaries. If None, loads from the latest file.
        max_tickers (int, optional): Maximum number of tickers to process. Defaults to None (all).
        use_mock (bool, optional): Whether to use mock data for testing. Defaults to False.
    
    Returns:
        dict: Statistics about the download process
    """
    # Load ticker list if not provided
    if tickers is None:
        latest_file = find_latest_ticker_file()
        if not latest_file:
            logger.error("No ticker file found, cannot download historical data")
            return {'status': 'error', 'error': 'No ticker file found'}
        
        tickers = load_tickers(latest_file)
    
    # Limit the number of tickers if requested
    if max_tickers and len(tickers) > max_tickers:
        logger.info(f"Limiting to {max_tickers} tickers (out of {len(tickers)} total)")
        tickers = tickers[:max_tickers]
    
    logger.info(f"Starting historical data download for {len(tickers)} tickers")
    start_time = time.time()
    
    # Statistics
    stats = {
        'total': len(tickers),
        'successful': 0,
        'failed': 0,
        'tickers_processed': []
    }
    
    # Process tickers with a thread pool
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DOWNLOADS) as executor:
        # Submit download tasks
        future_to_symbol = {executor.submit(download_for_ticker, ticker, use_mock): ticker['symbol'] 
                           for ticker in tickers}
        
        # Process results as they complete
        for i, future in enumerate(as_completed(future_to_symbol)):
            symbol = future_to_symbol[future]
            try:
                _, success = future.result()
                
                if success:
                    stats['successful'] += 1
                else:
                    stats['failed'] += 1
                
                stats['tickers_processed'].append(symbol)
                
                # Log progress
                if (i + 1) % 10 == 0 or (i + 1) == len(tickers):
                    logger.info(f"Downloaded {i + 1}/{len(tickers)} tickers - "
                               f"{stats['successful']} successful, {stats['failed']} failed")
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {str(e)}")
                stats['failed'] += 1
    
    # Log summary
    elapsed_time = time.time() - start_time
    logger.info(f"Historical data download completed in {elapsed_time:.2f} seconds")
    logger.info(f"Successfully downloaded data for {stats['successful']}/{stats['total']} tickers")
    
    stats['elapsed_time'] = elapsed_time
    stats['status'] = 'success'
    
    return stats


def main():
    """Main function to run the historical data download."""
    logger.info("Starting historical data download process")
    
    # Get the latest ticker list
    latest_file = find_latest_ticker_file()
    if not latest_file:
        logger.error("No ticker file found, cannot download historical data")
        return False
    
    # Load tickers
    tickers = load_tickers(latest_file)
    if not tickers:
        logger.error("Failed to load tickers")
        return False
    
    # Download historical data
    stats = download_historical_data(tickers)
    
    if stats['status'] == 'success':
        logger.info("Historical data download completed successfully")
        success_rate = (stats['successful'] / stats['total']) * 100
        logger.info(f"Success rate: {success_rate:.2f}% ({stats['successful']}/{stats['total']})")
        return True
    else:
        logger.error(f"Historical data download failed: {stats.get('error', 'Unknown error')}")
        return False


if __name__ == "__main__":
    # Set up logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )
    
    # Execute
    main() 